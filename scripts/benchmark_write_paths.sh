#!/usr/bin/env bash
# benchmark_write_paths.sh — Compare tur's write paths on Linux
#
# Runs tur against itself with different storage backends to measure the
# performance impact of the pwrite zero-copy path vs tokio seek+write.
#
# Usage:
#   ./scripts/benchmark_write_paths.sh <url> [runs] [connections]
#
# Example:
#   ./scripts/benchmark_write_paths.sh \
#     "https://static.rust-lang.org/dist/rust-1.86.0-x86_64-unknown-linux-gnu.tar.gz" \
#     5 4
#
# Results are written to benchmarks/write-paths/<timestamp>-<name>/
#
# Benchmarks three configurations (where applicable):
#   1. "default"    — pwrite path (Linux default)
#   2. "no-splice"  — --no-pwrite → tokio seek+write (falls through to tokio)
#   3. "no-io-uring" — --no-io-uring (splice still active, no attempt at uring)
#
# Each config runs <runs> iterations. Results show avg throughput, min, max,
# stddev, and memory usage.

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Usage
# ─────────────────────────────────────────────────────────────
if [[ $# -lt 1 || $# -gt 3 ]]; then
  cat >&2 <<'EOF'
usage: benchmark_write_paths.sh <url> [runs] [connections]

  url           download target (http:// or https://)
  runs          iterations per write path config  (default: 5)
  connections   parallel connections per run      (default: 4)

Dedicated Linux write-path microbenchmark. Compares:
  - default       pwrite (Linux default write path)
  - no-splice     tokio seek+write (--no-pwrite)
  - no-io-uring   pwrite path with uring disabled (--no-io-uring)

example:
  ./scripts/benchmark_write_paths.sh \
    "https://static.rust-lang.org/dist/rust-1.86.0-x86_64-unknown-linux-gnu.tar.gz" \
    5 4
EOF
  exit 1
fi

URL="$1"
RUNS="${2:-5}"
CONNECTIONS="${3:-4}"

# Only meaningful on Linux
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "warning: this benchmark is designed for Linux; write path comparisons"
  echo "         may not be representative on other platforms."
fi

# ─────────────────────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────────────────────
read_kv() { grep -F "${2}=" "${1}" 2>/dev/null | tail -n 1 | cut -d= -f2- || echo ""; }

format_mib_s() { awk -v bps="${1:-0}" 'BEGIN { printf "%.2f", bps / (1048576) }'; }

run_config() {
  local config_label="$1"; shift
  local stamp
  stamp="$(date +%s%N)"
  local stdout_log="/tmp/tur-write-bench-${stamp}.log"

  ./target/release/tur \
    --headless \
    --url "${URL}" \
    --connections "${CONNECTIONS}" \
    --schedule-mode equal \
    --http-mode http1 \
    "$@" \
    >"${stdout_log}" 2>&1 &
  local pid=$!

  local peak_rss=0
  while kill -0 "${pid}" 2>/dev/null; do
    if [[ -r "/proc/${pid}/status" ]]; then
      local r
      r="$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || true)"
      [[ -n "${r}" ]] && (( r > peak_rss )) && peak_rss="${r}"
    fi
    sleep 0.05
  done

  wait "${pid}" && local status=$? || local status=$?

  # Read stdout for timing info
  local elapsed_line downloaded_line
  elapsed_line=$(grep '^progress' "${stdout_log}" | tail -1 || true)
  downloaded_line=$(grep '^elapsed' "${stdout_log}" | tail -1 || true)

  # Parse progress= id downloaded=X speed=Y
  local downloaded=0
  if [[ -n "${elapsed_line}" ]]; then
    downloaded=$(echo "${elapsed_line}" | grep -oP 'downloaded=\K[0-9]+' || echo "0")
  fi

  # Fallback: find the file and check its size
  if [[ "${downloaded}" == "0" ]]; then
    local f
    f=$(ls -t /tmp/tur-* 2>/dev/null | head -1 || true)
    [[ -n "${f}" && -f "${f}" ]] && downloaded=$(stat --format='%s' "${f}" 2>/dev/null || echo "0")
  fi

  # Get timing from log output or derive from start/end
  local elapsed_s
  elapsed_s=$(echo "${elapsed_line}" | grep -oP 'speed=\K[0-9.]+' | head -1 || echo "0")
  # speed is in Bps, so if we have downloaded and speed we can derive elapsed
  if [[ "${elapsed_s}" != "0" && "${downloaded}" != "0" ]]; then
    elapsed_s=$(awk -v d="${downloaded}" -v s="${elapsed_s}" 'BEGIN { if (s>0) printf "%.3f", d/s; else print "0" }')
  else
    elapsed_s="0"
  fi

  rm -f "${stdout_log}"

  echo "${status}|${elapsed_s}|${peak_rss}|${downloaded}"
}

# ─────────────────────────────────────────────────────────────
# Build
# ─────────────────────────────────────────────────────────────
echo "━━━ building release binary ━━━"
cargo build --release 2>&1 | tail -3
echo

# ─────────────────────────────────────────────────────────────
# Resolve URL
# ─────────────────────────────────────────────────────────────
EFFECTIVE_URL="$(curl -L --silent --show-error --output /dev/null --max-time 20 --write-out '%{url_effective}' "${URL}" 2>/dev/null || echo "${URL}")"
echo "effective URL: ${EFFECTIVE_URL}"

EXPECTED_SIZE="$(curl -sI --max-time 15 --location "${EFFECTIVE_URL}" \
  | grep -i '^content-length:' | tail -1 | awk '{print $2}' | tr -d '\r' || true)"
SIZE_MIB="$(awk -v b="${EXPECTED_SIZE:-0}" 'BEGIN{printf "%.1f", b/1048576}')"
echo "file size: ${SIZE_MIB} MiB (${EXPECTED_SIZE:-unknown} bytes)"
echo

# ─────────────────────────────────────────────────────────────
# Generate probe for baseline speed
# ─────────────────────────────────────────────────────────────
echo "━━━ single-connection probe ━━━"
PROBE_SPEED=$(curl -L --silent --show-error --output /dev/null \
  --max-time 30 \
  --write-out '%{speed_download}' \
  "${EFFECTIVE_URL}" 2>/dev/null || echo "0")
PROBE_MIB=$(format_mib_s "${PROBE_SPEED}")
echo "  probe speed: ${PROBE_MIB} MiB/s"
echo

# ─────────────────────────────────────────────────────────────
# Define configs
# Config: (label, extra_args...)
# ─────────────────────────────────────────────────────────────
CONFIGS=(
  "default"
  "no-splice|--no-pwrite"
  "no-io-uring|--no-io-uring"
)

STAMP="$(date +%Y%m%d-%H%M%S)"
ROOT_DIR="benchmarks/write-paths/${STAMP}-write-paths"
SUMMARY="${ROOT_DIR}/summary.tsv"
mkdir -p "${ROOT_DIR}"

# TSV header
printf '%s\t%s\t%s\t%s\t%s\n' \
  "config" "run" "elapsed_seconds" "peak_rss_kb" "throughput_Bps" \
  >"${SUMMARY}"

echo "━━━ benchmark config ━━━"
echo "  url:          ${EFFECTIVE_URL}"
echo "  runs:         ${RUNS}"
echo "  connections:  ${CONNECTIONS}"
echo "  output:       ${ROOT_DIR}"
echo "  configs:      default (splice), no-splice (tokio), no-io-uring (splice)"
echo

# ─────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────
for config_entry in "${CONFIGS[@]}"; do
  IFS='|' read -r label args <<< "${config_entry}"

  # Single run of 5 seconds warmup
  echo "  --- warming up ${label} ---"
  eval run_config "${label}" "${args}" >/dev/null 2>&1 || true

  for RUN in $(seq 1 "${RUNS}"); do
    RUN_NAME="run-${RUN}"
    CONFIG_DIR="${ROOT_DIR}/${label}"
    mkdir -p "${CONFIG_DIR}"

    echo "  [${label}] ${RUN_NAME}..."

    # Parse args safely
    IFS='|' read -r _ extra_args_raw <<< "${config_entry}"
    declare -a EXTRA_ARGS=()
    if [[ -n "${extra_args_raw}" ]]; then
      read -r -a EXTRA_ARGS <<< "${extra_args_raw}"
    fi

    # Run the config
    result=$(run_config "${label}" "${EXTRA_ARGS[@]}")
    IFS='|' read -r status elapsed_s peak_rss downloaded <<< "${result}"

    # Compute throughput
    if [[ -n "${EXPECTED_SIZE}" && "${EXPECTED_SIZE}" != "0" && -n "${elapsed_s}" && "${elapsed_s}" != "0" ]]; then
      throughput=$(awk -v sz="${EXPECTED_SIZE}" -v t="${elapsed_s}" 'BEGIN { printf "%.0f", sz / t }')
    else
      throughput=0
    fi

    echo "    elapsed=${elapsed_s}s  rss=${peak_rss}KB  throughput=$(format_mib_s "${throughput}") MiB/s  status=${status}"

    printf '%s\t%s\t%s\t%s\t%s\n' \
      "${label}" "${RUN_NAME}" "${elapsed_s}" "${peak_rss}" "${throughput}" \
      >>"${SUMMARY}"

    # Cooldown between runs
    [[ ${RUN} -lt ${RUNS} ]] && sleep 2
  done
  echo
done

# ─────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────
echo "━━━ aggregated results ━━━"
echo "  probe speed: ${PROBE_MIB} MiB/s"
echo
awk -F'\t' '
  NR == 1 { next }
  {
    config = $1
    elapsed = $3 + 0
    rss = $4 + 0
    bps = $5 + 0

    n[config]++
    cnt = n[config]

    d1 = elapsed - mean_e[config]
    mean_e[config] += d1 / cnt
    M2_e[config] += d1 * (elapsed - mean_e[config])

    d2 = bps - mean_t[config]
    mean_t[config] += d2 / cnt
    M2_t[config] += d2 * (bps - mean_t[config])

    sum_rss[config] += rss

    if (cnt == 1 || elapsed < min_e[config]) min_e[config] = elapsed
    if (cnt == 1 || elapsed > max_e[config]) max_e[config] = elapsed
  }
  END {
    MiB = 1048576

    printf "  %-14s %5s  %9s %9s %9s  %9s %9s %9s  %9s\n", \
      "config", "runs", "avg_MiB/s", "min_MiB/s", "max_MiB/s", \
      "avg_elap_s", "min_elap_s", "max_elap_s", "avg_rss_KB"
    printf "  %s\n", \
      "────────────────────────────────────────────────────────────────────────────────"

    for (c in n) {
      cnt = n[c]
      stddev_t = (cnt > 1 ? sqrt(M2_t[c] / cnt) : 0)
      printf "  %-14s %5d  %9.2f %9.2f %9.2f  %9.3f %9.3f %9.3f  %9.0f\n", \
        c, cnt, \
        mean_t[c] / MiB, \
        (min_t[c] ? min_t[c] / MiB : 0), \
        (max_t[c] ? max_t[c] / MiB : 0), \
        mean_e[c], min_e[c], max_e[c], \
        sum_rss[c] / cnt
    }
  }
' "${SUMMARY}"

echo
echo "━━━ verdict ━━━"
echo "  Higher throughput (MiB/s)  = faster write path"
echo "  Lower elapsed (s)          = faster download"
echo "  Lower RSS (KB)             = more memory efficient"
echo
echo "  default       = pwrite path  (Linux default)"
echo "  no-splice     = tokio seek+write     (--no-pwrite)"
echo "  no-io-uring   = splice with uring disabled  (--no-io-uring)"
echo
echo "  raw results: ${SUMMARY}"
