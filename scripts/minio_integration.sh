#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
IMG="layerdb/minio-local:arm64"
NAME="layerdb-minio"
PORT_API="${LAYERDB_MINIO_API_PORT:-19000}"
PORT_CONSOLE="${LAYERDB_MINIO_CONSOLE_PORT:-19001}"
WORK="/tmp/layerdb-minio-image"

build_image() {
  if docker image inspect "$IMG" >/dev/null 2>&1; then
    return
  fi

  mkdir -p "$WORK"
  curl -L --fail https://dl.min.io/server/minio/release/linux-arm64/minio -o "$WORK/minio"
  chmod +x "$WORK/minio"
  cat >"$WORK/Dockerfile" <<'EOF'
FROM scratch
COPY minio /minio
ENTRYPOINT ["/minio"]
EOF
  docker build -t "$IMG" "$WORK"
}

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}

build_image
cleanup
trap cleanup EXIT

docker run -d \
  --name "$NAME" \
  -p "${PORT_API}:9000" \
  -p "${PORT_CONSOLE}:9001" \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  "$IMG" server /data --console-address :9001 >/dev/null

for _ in $(seq 1 30); do
  if docker logs "$NAME" 2>&1 | rg -q "API:"; then
    break
  fi
  sleep 1
done

echo "MinIO ready at http://127.0.0.1:${PORT_API}"

cd "$ROOT_DIR"
LAYERDB_MINIO_INTEGRATION=1 \
LAYERDB_S3_ENDPOINT="http://127.0.0.1:${PORT_API}" \
LAYERDB_S3_ACCESS_KEY=minioadmin \
LAYERDB_S3_SECRET_KEY=minioadmin \
LAYERDB_S3_BUCKET=layerdb-test \
LAYERDB_S3_REGION=us-east-1 \
LAYERDB_S3_PREFIX=layerdb-integration \
LAYERDB_S3_SECURE=0 \
LAYERDB_S3_AUTO_CREATE_BUCKET=1 \
cargo test --test freeze_s3_minio -- --nocapture
