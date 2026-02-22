#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/docker-compose.integration.yml"
PROJECT="${VDB_INTEGRATION_PROJECT:-vectdb-it}"
TIMEOUT_SECS="${VDB_IT_TIMEOUT_SECS:-120}"
NODE1_PORT="${VDB_NODE1_PORT:-18080}"
NODE2_PORT="${VDB_NODE2_PORT:-18081}"
NODE3_PORT="${VDB_NODE3_PORT:-18082}"

NODE1_URL="http://127.0.0.1:${NODE1_PORT}"
NODE2_URL="http://127.0.0.1:${NODE2_PORT}"
NODE3_URL="http://127.0.0.1:${NODE3_PORT}"
NODE_URLS=("${NODE1_URL}" "${NODE2_URL}" "${NODE3_URL}")

VEC_A='[0.11,0.11,0.11,0.11,0.11,0.11,0.11,0.11]'
VEC_B='[0.33,0.33,0.33,0.33,0.33,0.33,0.33,0.33]'
VEC_C='[0.77,0.77,0.77,0.77,0.77,0.77,0.77,0.77]'

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ "${VDB_IT_KEEP_STACK:-0}" == "1" ]]; then
    echo "keeping docker stack up because VDB_IT_KEEP_STACK=1"
    return
  fi
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
}

service_for_url() {
  case "$1" in
    "${NODE1_URL}") echo "node1" ;;
    "${NODE2_URL}") echo "node2" ;;
    "${NODE3_URL}") echo "node3" ;;
    *)
      echo "unknown-node-url" >&2
      return 1
      ;;
  esac
}

wait_for_http() {
  local url="$1"
  local timeout="$2"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout )); then
      return 1
    fi
    sleep 1
  done
}

discover_single_leader() {
  local leader=""
  local leaders=0
  local role
  for url in "${NODE_URLS[@]}"; do
    role="$(curl -fsS "${url}/v1/role" 2>/dev/null || true)"
    if [[ -z "${role}" ]]; then
      continue
    fi
    if echo "${role}" | grep -Eq '"leader"[[:space:]]*:[[:space:]]*true'; then
      leader="${url}"
      leaders=$((leaders + 1))
    fi
  done
  if (( leaders == 1 )); then
    echo "${leader}"
    return 0
  fi
  return 1
}

wait_for_single_leader() {
  local timeout="$1"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if leader="$(discover_single_leader 2>/dev/null)"; then
      echo "${leader}"
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout )); then
      return 1
    fi
    sleep 1
  done
}

post_json() {
  local url="$1"
  local payload="$2"
  local out_file="$3"
  local status
  status="$(
    curl -sS \
      -o "${out_file}" \
      -w "%{http_code}" \
      -H "content-type: application/json" \
      -X POST \
      "${url}" \
      --data "${payload}"
  )"
  echo "${status}"
}

assert_quorum_replication_summary() {
  local response_file="$1"
  local context="$2"
  if ! grep -Eq '"attempted"[[:space:]]*:[[:space:]]*[1-9][0-9]*' "${response_file}" \
    || ! grep -Eq '"succeeded"[[:space:]]*:[[:space:]]*[1-9][0-9]*' "${response_file}"; then
    echo "unexpected replication summary for ${context}" >&2
    cat "${response_file}" >&2
    return 1
  fi
}

assert_search_top_id() {
  local base_url="$1"
  local expected_id="$2"
  local query_vec="$3"
  local out_file
  out_file="$(mktemp)"
  local payload="{\"query\":${query_vec},\"k\":1}"
  local status
  status="$(post_json "${base_url}/v1/search" "${payload}" "${out_file}")"
  if [[ "${status}" != "200" ]]; then
    echo "search failed on ${base_url}, status=${status}" >&2
    cat "${out_file}" >&2
    rm -f "${out_file}"
    exit 1
  fi
  if ! grep -Eq "\"id\"[[:space:]]*:[[:space:]]*${expected_id}" "${out_file}"; then
    echo "unexpected search result on ${base_url}, expected id=${expected_id}" >&2
    cat "${out_file}" >&2
    rm -f "${out_file}"
    exit 1
  fi
  rm -f "${out_file}"
}

require_command docker
require_command curl
require_command grep

trap cleanup EXIT

echo "bringing up integration stack: ${PROJECT}"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" up -d --build --remove-orphans

for url in "${NODE_URLS[@]}"; do
  echo "waiting for health endpoint on ${url}"
  if ! wait_for_http "${url}/v1/health" "${TIMEOUT_SECS}"; then
    echo "timed out waiting for ${url}/v1/health" >&2
    docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" logs --tail 200 >&2 || true
    exit 1
  fi
done

leader_url="$(wait_for_single_leader "${TIMEOUT_SECS}")"
echo "leader elected: ${leader_url}"

follower_url="${NODE1_URL}"
if [[ "${follower_url}" == "${leader_url}" ]]; then
  follower_url="${NODE2_URL}"
fi
if [[ "${follower_url}" == "${leader_url}" ]]; then
  follower_url="${NODE3_URL}"
fi

tmp_out="$(mktemp)"
follower_write_payload="{\"mutations\":[{\"kind\":\"upsert\",\"id\":101,\"values\":${VEC_A}}],\"commit_mode\":\"durable\"}"
follower_status="$(post_json "${follower_url}/v1/mutations" "${follower_write_payload}" "${tmp_out}")"
if [[ "${follower_status}" != "200" ]]; then
  echo "expected routed write success (200) via non-leader endpoint, got ${follower_status}" >&2
  cat "${tmp_out}" >&2
  rm -f "${tmp_out}"
  exit 1
fi
if ! assert_quorum_replication_summary "${tmp_out}" "routed write"; then
  rm -f "${tmp_out}"
  exit 1
fi
rm -f "${tmp_out}"

leader_write_payload="{\"mutations\":[{\"kind\":\"upsert\",\"id\":101,\"values\":${VEC_A}}],\"commit_mode\":\"durable\"}"
tmp_out="$(mktemp)"
leader_status="$(post_json "${leader_url}/v1/mutations" "${leader_write_payload}" "${tmp_out}")"
if [[ "${leader_status}" != "200" ]]; then
  echo "leader write failed with status ${leader_status}" >&2
  cat "${tmp_out}" >&2
  rm -f "${tmp_out}"
  exit 1
fi
if ! assert_quorum_replication_summary "${tmp_out}" "leader write"; then
  rm -f "${tmp_out}"
  exit 1
fi
rm -f "${tmp_out}"

for url in "${NODE_URLS[@]}"; do
  assert_search_top_id "${url}" 101 "${VEC_A}"
done

old_leader_service="$(service_for_url "${leader_url}")"
echo "restarting current leader container: ${old_leader_service}"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" stop "${old_leader_service}" >/dev/null

new_leader_url="$(wait_for_single_leader "${TIMEOUT_SECS}")"
echo "leader after stop: ${new_leader_url}"

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" start "${old_leader_service}" >/dev/null
if ! wait_for_http "${leader_url}/v1/health" "${TIMEOUT_SECS}"; then
  echo "timed out waiting for restarted node ${old_leader_service}" >&2
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" logs --tail 200 "${old_leader_service}" >&2 || true
  exit 1
fi

assert_search_top_id "${leader_url}" 101 "${VEC_A}"

stable_leader_url="$(wait_for_single_leader "${TIMEOUT_SECS}")"
echo "stable leader for second write: ${stable_leader_url}"

second_write_payload="{\"mutations\":[{\"kind\":\"upsert\",\"id\":202,\"values\":${VEC_B}}],\"commit_mode\":\"durable\"}"
tmp_out="$(mktemp)"
second_status="$(post_json "${stable_leader_url}/v1/mutations" "${second_write_payload}" "${tmp_out}")"
if [[ "${second_status}" != "200" ]]; then
  echo "second leader write failed with status ${second_status}" >&2
  cat "${tmp_out}" >&2
  rm -f "${tmp_out}"
  exit 1
fi
if ! assert_quorum_replication_summary "${tmp_out}" "second write"; then
  rm -f "${tmp_out}"
  exit 1
fi
rm -f "${tmp_out}"

for url in "${NODE_URLS[@]}"; do
  assert_search_top_id "${url}" 202 "${VEC_B}"
done

lagging_url="${NODE1_URL}"
if [[ "${lagging_url}" == "${stable_leader_url}" ]]; then
  lagging_url="${NODE2_URL}"
fi
if [[ "${lagging_url}" == "${stable_leader_url}" ]]; then
  lagging_url="${NODE3_URL}"
fi
lagging_service="$(service_for_url "${lagging_url}")"

echo "stopping lagging follower to force snapshot catch-up: ${lagging_service}"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" stop "${lagging_service}" >/dev/null

for id in $(seq 300 339); do
  payload="{\"mutations\":[{\"kind\":\"upsert\",\"id\":${id},\"values\":${VEC_A}}],\"commit_mode\":\"durable\"}"
  tmp_out="$(mktemp)"
  status="$(post_json "${stable_leader_url}/v1/mutations" "${payload}" "${tmp_out}")"
  if [[ "${status}" != "200" ]]; then
    echo "leader write during lag scenario failed for id=${id} status=${status}" >&2
    cat "${tmp_out}" >&2
    rm -f "${tmp_out}"
    exit 1
  fi
  if ! assert_quorum_replication_summary "${tmp_out}" "lag write id=${id}"; then
    rm -f "${tmp_out}"
    exit 1
  fi
  rm -f "${tmp_out}"
done

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" start "${lagging_service}" >/dev/null
if ! wait_for_http "${lagging_url}/v1/health" "${TIMEOUT_SECS}"; then
  echo "timed out waiting for restarted lagging node ${lagging_service}" >&2
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" logs --tail 200 "${lagging_service}" >&2 || true
  exit 1
fi

snapshot_write_payload="{\"mutations\":[{\"kind\":\"upsert\",\"id\":999,\"values\":${VEC_C}}],\"commit_mode\":\"durable\"}"
tmp_out="$(mktemp)"
snapshot_status="$(post_json "${stable_leader_url}/v1/mutations" "${snapshot_write_payload}" "${tmp_out}")"
if [[ "${snapshot_status}" != "200" ]]; then
  echo "snapshot catch-up write failed with status ${snapshot_status}" >&2
  cat "${tmp_out}" >&2
  rm -f "${tmp_out}"
  exit 1
fi
if ! assert_quorum_replication_summary "${tmp_out}" "snapshot catch-up write"; then
  rm -f "${tmp_out}"
  exit 1
fi
rm -f "${tmp_out}"

for url in "${NODE_URLS[@]}"; do
  assert_search_top_id "${url}" 999 "${VEC_C}"
done

echo "docker integration test passed"
