#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/docker-compose.integration.yml"
PROJECT="${VDB_LOAD_PROJECT:-vectdb-load}"
TIMEOUT_SECS="${VDB_LOAD_TIMEOUT_SECS:-180}"
NODE1_PORT="${VDB_NODE1_PORT:-18080}"
NODE2_PORT="${VDB_NODE2_PORT:-18081}"
NODE3_PORT="${VDB_NODE3_PORT:-18082}"

TOTAL_WRITES="${VDB_LOAD_TOTAL_WRITES:-1200}"
CONCURRENCY="${VDB_LOAD_CONCURRENCY:-12}"
SEARCH_EVERY="${VDB_LOAD_SEARCH_EVERY:-10}"
COMMIT_MODE="${VDB_LOAD_COMMIT_MODE:-durable}"
SAMPLE_COUNT="${VDB_LOAD_SAMPLE_COUNT:-32}"

NODE1_URL="http://127.0.0.1:${NODE1_PORT}"
NODE2_URL="http://127.0.0.1:${NODE2_PORT}"
NODE3_URL="http://127.0.0.1:${NODE3_PORT}"
NODE_URLS=("${NODE1_URL}" "${NODE2_URL}" "${NODE3_URL}")

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ "${VDB_LOAD_KEEP_STACK:-0}" == "1" ]]; then
    echo "keeping docker stack up because VDB_LOAD_KEEP_STACK=1"
    return
  fi
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" down -v --remove-orphans >/dev/null 2>&1 || true
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
    echo "replication summary failed for ${context}" >&2
    cat "${response_file}" >&2
    return 1
  fi
}

vector_for_id() {
  local id="$1"
  local b0=$(( (id >> 0) & 255 ))
  local b1=$(( (id >> 8) & 255 ))
  local b2=$(( (id >> 16) & 255 ))
  local b3=$(( (id >> 24) & 255 ))
  local b4=$(( (id >> 32) & 255 ))
  local b5=$(( (id >> 40) & 255 ))
  local b6=$(( (id >> 48) & 255 ))
  local b7=$(( (id >> 56) & 255 ))
  awk -v b0="$b0" -v b1="$b1" -v b2="$b2" -v b3="$b3" -v b4="$b4" -v b5="$b5" -v b6="$b6" -v b7="$b7" \
    'BEGIN { printf "[%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f]", b0/255.0, b1/255.0, b2/255.0, b3/255.0, b4/255.0, b5/255.0, b6/255.0, b7/255.0 }'
}

extract_first_id() {
  local response_file="$1"
  grep -oE '"id"[[:space:]]*:[[:space:]]*[0-9]+' "${response_file}" | head -n1 | tr -cd '0-9'
}

assert_search_top_id() {
  local base_url="$1"
  local expected_id="$2"
  local vec
  vec="$(vector_for_id "${expected_id}")"
  local out_file
  out_file="$(mktemp)"
  local payload="{\"query\":${vec},\"k\":1}"
  local status
  status="$(post_json "${base_url}/v1/search" "${payload}" "${out_file}")"
  if [[ "${status}" != "200" ]]; then
    echo "search failed on ${base_url} status=${status}" >&2
    cat "${out_file}" >&2
    rm -f "${out_file}"
    exit 1
  fi
  local found_id
  found_id="$(extract_first_id "${out_file}")"
  if [[ "${found_id}" != "${expected_id}" ]]; then
    echo "unexpected search result on ${base_url}: expected ${expected_id} got ${found_id}" >&2
    cat "${out_file}" >&2
    rm -f "${out_file}"
    exit 1
  fi
  rm -f "${out_file}"
}

run_worker() {
  local worker_id="$1"
  local total="$2"
  local stride="$3"
  local leader_url="$4"
  local search_every="$5"
  local commit_mode="$6"
  local stats_file="$7"

  local writes=0
  local searches=0

  local id
  for ((id = worker_id + 1; id <= total; id += stride)); do
    local vec
    vec="$(vector_for_id "${id}")"
    local payload
    payload="{\"mutations\":[{\"kind\":\"upsert\",\"id\":${id},\"values\":${vec}}],\"commit_mode\":\"${commit_mode}\"}"
    local out_file
    out_file="$(mktemp)"
    local status
    status="$(post_json "${leader_url}/v1/mutations" "${payload}" "${out_file}")"
    if [[ "${status}" != "200" ]]; then
      echo "worker ${worker_id}: write id=${id} failed status=${status}" >&2
      cat "${out_file}" >&2
      rm -f "${out_file}"
      return 1
    fi
    if ! assert_quorum_replication_summary "${out_file}" "worker=${worker_id} id=${id}"; then
      rm -f "${out_file}"
      return 1
    fi
    rm -f "${out_file}"
    writes=$((writes + 1))

    if (( search_every > 0 )) && (( id % search_every == 0 )); then
      local search_out
      search_out="$(mktemp)"
      local query_payload
      query_payload="{\"query\":${vec},\"k\":1}"
      local search_status
      search_status="$(post_json "${leader_url}/v1/search" "${query_payload}" "${search_out}")"
      if [[ "${search_status}" != "200" ]]; then
        echo "worker ${worker_id}: search id=${id} failed status=${search_status}" >&2
        cat "${search_out}" >&2
        rm -f "${search_out}"
        return 1
      fi
      local found_id
      found_id="$(extract_first_id "${search_out}")"
      if [[ "${found_id}" != "${id}" ]]; then
        echo "worker ${worker_id}: search mismatch expected ${id} got ${found_id}" >&2
        cat "${search_out}" >&2
        rm -f "${search_out}"
        return 1
      fi
      rm -f "${search_out}"
      searches=$((searches + 1))
    fi
  done

  echo "${writes} ${searches}" >"${stats_file}"
}

require_command docker
require_command curl
require_command awk
require_command grep

if (( TOTAL_WRITES <= 0 )); then
  echo "VDB_LOAD_TOTAL_WRITES must be > 0" >&2
  exit 1
fi
if (( CONCURRENCY <= 0 )); then
  echo "VDB_LOAD_CONCURRENCY must be > 0" >&2
  exit 1
fi
if [[ "${COMMIT_MODE}" != "durable" && "${COMMIT_MODE}" != "acknowledged" ]]; then
  echo "VDB_LOAD_COMMIT_MODE must be durable or acknowledged" >&2
  exit 1
fi

trap cleanup EXIT

echo "bringing up load-test stack: ${PROJECT}"
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

start_ns="$(date +%s%N)"

stats_dir="$(mktemp -d)"
pids=()
for ((worker = 0; worker < CONCURRENCY; worker++)); do
  stats_file="${stats_dir}/worker-${worker}.txt"
  run_worker "${worker}" "${TOTAL_WRITES}" "${CONCURRENCY}" "${leader_url}" "${SEARCH_EVERY}" "${COMMIT_MODE}" "${stats_file}" &
  pids+=($!)
done

failed=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    failed=1
  fi
done
if (( failed != 0 )); then
  echo "load workers failed" >&2
  exit 1
fi

end_ns="$(date +%s%N)"
elapsed_ns=$((end_ns - start_ns))

total_searches=0
for stats_file in "${stats_dir}"/worker-*.txt; do
  read -r _writes searches <"${stats_file}"
  total_searches=$((total_searches + searches))
done
rm -rf "${stats_dir}"

elapsed_sec="$(awk -v ns="${elapsed_ns}" 'BEGIN { printf "%.6f", ns / 1000000000.0 }')"
write_qps="$(awk -v n="${TOTAL_WRITES}" -v ns="${elapsed_ns}" 'BEGIN { printf "%.2f", n * 1000000000.0 / ns }')"
search_qps="$(awk -v n="${total_searches}" -v ns="${elapsed_ns}" 'BEGIN { printf "%.2f", n * 1000000000.0 / ns }')"
total_ops=$((TOTAL_WRITES + total_searches))
total_qps="$(awk -v n="${total_ops}" -v ns="${elapsed_ns}" 'BEGIN { printf "%.2f", n * 1000000000.0 / ns }')"

echo "load summary: writes=${TOTAL_WRITES} searches=${total_searches} elapsed_sec=${elapsed_sec} write_qps=${write_qps} search_qps=${search_qps} total_qps=${total_qps}"

sample_ids=()
for ((i = 0; i < SAMPLE_COUNT; i++)); do
  sample_id=$((1 + (i * TOTAL_WRITES / SAMPLE_COUNT)))
  if (( sample_id > TOTAL_WRITES )); then
    sample_id="${TOTAL_WRITES}"
  fi
  sample_ids+=("${sample_id}")
done

for url in "${NODE_URLS[@]}"; do
  for id in "${sample_ids[@]}"; do
    assert_search_top_id "${url}" "${id}"
  done
done

echo "docker cluster load test passed"
