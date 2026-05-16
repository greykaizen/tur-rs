#!/usr/bin/env bash
# benchmark_compare.sh — TUR tournament benchmark
# Requires bash 4+ (CachyOS/Arch: bash 5.x, fine)
set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Usage
# ─────────────────────────────────────────────────────────────
if [[ $# -lt 2 || $# -gt 7 ]]; then
  cat >&2 <<'EOF'
usage: benchmark_compare.sh <url> <name> [connections] [runs] [schedule_mode] [http_mode] [tools_csv]

  url           download target (http:// or https://)
  name          label used in output directory names
  connections   parallel connections per tool  (default: 4)
  runs          timed repetitions per tool     (default: 5)
  schedule_mode tur --schedule-mode flag       (default: equal)
  http_mode     tur --http-mode flag           (default: http1)
  tools_csv     comma-separated tool list      (default: tur,aria2c,axel,wget2,lftp,curl)

Improvements over the original script:
  - Actual throughput (file_size / elapsed_s) is the primary metric — not probe speed
  - Pre-flight stability check warns if the network is too jittery for reliable results
  - Tool order rotates each run: no tool always occupies the same network time slot
  - 5s cooldown between tools + 30s cooldown between runs for the network to settle
  - Integrity verification: downloaded file size checked against Content-Length
  - Summary shows avg, min, max, stddev of actual throughput and elapsed time
  - Skew detection uses actual throughput variance across tools in the same run

example:
  ./scripts/benchmark_compare.sh \
    "https://static.rust-lang.org/dist/rust-1.86.0-x86_64-unknown-linux-gnu.tar.gz" \
    full-tournament 4 5 fib-adaptive http1 "tur,aria2c,axel,wget2,lftp"

optional env:
  TUR_CARGO_FEATURES   cargo features for building tur (example: linux-io-uring-experimental)
  TUR_SKIP_BUILD=1     skip cargo build and use the existing ./target/release/tur binary
  TUR_INITIAL_CONNECTIONS  initial tur --connections value (default: connections arg)
  TUR_MIN_CONNECTIONS      optional tur --min-connections value
  TUR_MAX_CONNECTIONS      optional tur --max-connections value
EOF
  exit 1
fi

# ─────────────────────────────────────────────────────────────
# Arguments & constants
# ─────────────────────────────────────────────────────────────
URL="$1"
NAME="$2"
CONNECTIONS="${3:-4}"
RUNS="${4:-5}"
SCHEDULE_MODE="${5:-equal}"
HTTP_MODE="${6:-http1}"
TOOLS_CSV="${7:-tur,aria2c,axel,wget2,lftp,curl}"
TUR_CARGO_FEATURES="${TUR_CARGO_FEATURES:-}"
TUR_SKIP_BUILD="${TUR_SKIP_BUILD:-0}"
TUR_INITIAL_CONNECTIONS="${TUR_INITIAL_CONNECTIONS:-${CONNECTIONS}}"
TUR_MIN_CONNECTIONS="${TUR_MIN_CONNECTIONS:-}"
TUR_MAX_CONNECTIONS="${TUR_MAX_CONNECTIONS:-}"

COOLDOWN_BETWEEN_TOOLS=5    # seconds — lets CDN rate-limiters settle between tools
COOLDOWN_BETWEEN_RUNS=30    # seconds — lets network fully settle between full rounds
STABILITY_PROBES=3          # number of 1MB probes for pre-flight stability check
STABILITY_CV_WARN=0.25      # warn if probe CV exceeds this (25% relative stddev)
PROBE_RANGE_BYTES=1048576   # 1 MiB per probe

# ─────────────────────────────────────────────────────────────
# URL normalisation
# ─────────────────────────────────────────────────────────────
[[ "${URL}" == //* ]] && URL="https:${URL}"
if [[ ! "${URL}" =~ ^https?:// ]]; then
  echo "error: url must start with https:// or http://" >&2
  exit 1
fi

IFS=',' read -r -a REQUESTED_TOOLS <<< "${TOOLS_CSV}"

# ─────────────────────────────────────────────────────────────
# Utility functions
# ─────────────────────────────────────────────────────────────
resolve_effective_url() {
  curl -L --silent --show-error --output /dev/null \
    --max-time 20 --write-out '%{url_effective}' "$1"
}

read_kv() {
  grep -F "${2}=" "${1}" 2>/dev/null | tail -n 1 | cut -d= -f2- || true
}

format_mib_s() {
  awk -v bps="${1:-0}" 'BEGIN { printf "%.2f", bps / (1048576) }'
}

tool_binary_available() {
  case "$1" in
    tur)    return 0 ;;
    aria2c|wget|wget2|lftp|axel|curl) command -v "$1" >/dev/null 2>&1 ;;
    *)      return 1 ;;
  esac
}

# Rotate array left by N positions so each run starts with a different tool.
rotate_tools() {
  local shift="$1"; shift
  local -a arr=("$@")
  local n="${#arr[@]}"
  local s=$(( shift % n ))
  printf '%s\n' "${arr[@]:$s}" "${arr[@]:0:$s}"
}

# Fetch file size from Content-Length header.
get_expected_size() {
  curl -sI --max-time 15 --location "$1" \
    | grep -i '^content-length:' \
    | tail -1 \
    | awk '{print $2}' \
    | tr -d '\r' \
    || true
}

# Verify downloaded file size. Returns "ok", "unknown", or "FAIL(...)".
verify_download() {
  local dir="$1" expected="$2"
  [[ -z "${expected}" ]] && { echo "unknown"; return; }
  local actual
  actual=$(find "${dir}" -maxdepth 3 -type f -printf '%s\n' 2>/dev/null \
    | awk '{s+=$1} END {print s+0}')
  if [[ "${actual}" == "${expected}" ]]; then
    echo "ok"
  else
    echo "FAIL(exp=${expected},got=${actual})"
  fi
}

# Compute actual throughput in bytes/sec from file size and elapsed time.
# This is the primary benchmark metric — not probe speed.
compute_throughput() {
  local file_size="$1" elapsed_s="$2"
  if [[ -z "${file_size}" || -z "${elapsed_s}" ]] \
     || [[ "${file_size}" == "0" ]] \
     || [[ "${elapsed_s}" == "0" || "${elapsed_s}" == "0.000" ]]; then
    echo "0"
    return
  fi
  awk -v sz="${file_size}" -v t="${elapsed_s}" 'BEGIN { printf "%.0f", sz / t }'
}

# Single 1MiB probe — returns speed in bytes/sec.
run_single_probe() {
  local url="$1"
  curl -L --silent --show-error --output /dev/null \
    --range "0-$(( PROBE_RANGE_BYTES - 1 ))" \
    --max-time 30 \
    --write-out '%{speed_download}' \
    "${url}" 2>/dev/null || echo "0"
}

show_live_status() {
  local pid="$1" label="$2" log="$3" started="$4"
  while kill -0 "${pid}" 2>/dev/null; do
    local now elapsed rss line
    now="$(date +%s)"
    elapsed=$(( now - started ))
    rss=0
    [[ -r "/proc/${pid}/status" ]] && \
      rss="$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)"
    line="$(tail -n 1 "${log}" 2>/dev/null | tr '\r' ' ' | cut -c1-80)"
    printf '\r  [%s] %ds rss=%sKB %s' "${label}" "${elapsed}" "${rss:-0}" "${line:-...}"
    sleep 1
  done
  printf '\n'
}

run_and_measure() {
  local prefix="$1" label="$2"; shift 2
  local stdout_log="${prefix}/stdout.log"
  local time_log="${prefix}/time.log"
  local start_ns start_s

  start_ns="$(date +%s%N)"
  start_s="$(date +%s)"

  "$@" >"${stdout_log}" 2>&1 &
  local pid=$!
  local peak_rss=0

  show_live_status "${pid}" "${label}" "${stdout_log}" "${start_s}" &
  local spid=$!

  while kill -0 "${pid}" 2>/dev/null; do
    if [[ -r "/proc/${pid}/status" ]]; then
      local r
      r="$(awk '/VmRSS:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || true)"
      [[ -n "${r}" ]] && (( r > peak_rss )) && peak_rss="${r}"
    fi
    sleep 0.05
  done

  local status=0
  wait "${pid}" || status=$?
  wait "${spid}" 2>/dev/null || true

  local end_ns elapsed_ns elapsed_ms elapsed_s
  end_ns="$(date +%s%N)"
  elapsed_ns=$(( end_ns - start_ns ))
  elapsed_ms=$(( elapsed_ns / 1000000 ))
  elapsed_s="$(awk -v ms="${elapsed_ms}" 'BEGIN { printf "%.3f", ms/1000 }')"

  { echo "elapsed_seconds=${elapsed_s}"
    echo "peak_rss_kb=${peak_rss}"
    echo "exit_status=${status}"
  } >"${time_log}"

  return "${status}"
}

run_tool() {
  local tool="$1" run_dir="$2"
  local dl="${run_dir}/downloads"
  local logs="${run_dir}/logs"

  case "${tool}" in
    tur)
      local -a tur_args=(
        ./target/release/tur
        --headless --url "${EFFECTIVE_URL}" --dir "${dl}"
        --connections "${TUR_INITIAL_CONNECTIONS}"
        --schedule-mode "${SCHEDULE_MODE}"
        --http-mode "${HTTP_MODE}"
        --log-root "${logs}"
      )
      [[ -n "${TUR_MIN_CONNECTIONS}" ]] && tur_args+=(--min-connections "${TUR_MIN_CONNECTIONS}")
      [[ -n "${TUR_MAX_CONNECTIONS}" ]] && tur_args+=(--max-connections "${TUR_MAX_CONNECTIONS}")
      run_and_measure "${run_dir}" "tur" \
        "${tur_args[@]}"
      ;;
    aria2c)
      run_and_measure "${run_dir}" "aria2c" \
        aria2c \
        --dir="${dl}" --out="${NAME}" \
        --max-connection-per-server="${CONNECTIONS}" \
        --split="${CONNECTIONS}" --min-split-size=1M \
        --file-allocation=none \
        --log="${logs}/aria2c.log" \
        "${EFFECTIVE_URL}"
      ;;
    wget)
      run_and_measure "${run_dir}" "wget" \
        wget --no-config \
        --output-file="${logs}/wget.log" \
        --output-document="${dl}/${NAME}" \
        "${EFFECTIVE_URL}"
      ;;
    wget2)
      run_and_measure "${run_dir}" "wget2" \
        wget2 \
        --output-file="${logs}/wget2.log" \
        --output-document="${dl}/${NAME}" \
        --chunk-size=1M --max-threads="${CONNECTIONS}" \
        "${EFFECTIVE_URL}"
      ;;
    lftp)
      run_and_measure "${run_dir}" "lftp" \
        lftp --norc -c \
        "set xfer:clobber true; pget -n ${CONNECTIONS} -O \"${dl}\" \"${EFFECTIVE_URL}\" -o \"${NAME}\"; bye"
      ;;
    axel)
      run_and_measure "${run_dir}" "axel" \
        axel --num-connections="${CONNECTIONS}" \
        --output="${dl}/${NAME}" "${EFFECTIVE_URL}"
      ;;
    curl)
      run_and_measure "${run_dir}" "curl" \
        curl -L --fail --silent --show-error \
        --output "${dl}/${NAME}" "${EFFECTIVE_URL}"
      ;;
    *)
      echo "unknown tool: ${tool}" >&2; return 1 ;;
  esac
}

# ─────────────────────────────────────────────────────────────
# Pre-flight: resolve URL and measure network stability
# ─────────────────────────────────────────────────────────────
EFFECTIVE_URL="$(resolve_effective_url "${URL}" 2>/dev/null || true)"
[[ -z "${EFFECTIVE_URL}" ]] && EFFECTIVE_URL="${URL}"

echo "resolving file size..."
EXPECTED_SIZE="$(get_expected_size "${EFFECTIVE_URL}")"
if [[ -n "${EXPECTED_SIZE}" ]]; then
  SIZE_MIB="$(awk -v b="${EXPECTED_SIZE}" 'BEGIN{printf "%.1f", b/1048576}')"
  echo "  file size: ${EXPECTED_SIZE} bytes (${SIZE_MIB} MiB)"
else
  echo "  warning: Content-Length unavailable — integrity checks will show 'unknown'"
  echo "  warning: actual throughput metric requires file size; will use elapsed time only"
fi

echo
echo "pre-flight network stability check (${STABILITY_PROBES} probes)..."
PROBE_SPEEDS=()
for i in $(seq 1 "${STABILITY_PROBES}"); do
  spd="$(run_single_probe "${EFFECTIVE_URL}")"
  spd_mib="$(format_mib_s "${spd}")"
  echo "  probe ${i}: ${spd_mib} MiB/s"
  PROBE_SPEEDS+=("${spd}")
  [[ $i -lt ${STABILITY_PROBES} ]] && sleep 2
done

# Compute mean and CV of probe speeds
PREFLIGHT_RESULT="$(awk \
  -v speeds="${PROBE_SPEEDS[*]}" \
  -v warn="${STABILITY_CV_WARN}" '
  BEGIN {
    n = split(speeds, a, " ")
    s = 0; s2 = 0
    for (i=1; i<=n; i++) { s += a[i]; s2 += a[i]^2 }
    mean = s / n
    variance = s2/n - mean^2
    stddev = (variance > 0 ? sqrt(variance) : 0)
    cv = (mean > 0 ? stddev / mean : 0)
    status = (cv > warn ? "UNSTABLE" : "ok")
    printf "mean=%.0f cv=%.3f status=%s\n", mean, cv, status
  }')"

PROBE_MEAN="$(echo "${PREFLIGHT_RESULT}" | grep -oP 'mean=\K[0-9]+')"
PROBE_CV="$(echo "${PREFLIGHT_RESULT}" | grep -oP 'cv=\K[0-9.]+')"
PROBE_STATUS="$(echo "${PREFLIGHT_RESULT}" | grep -oP 'status=\K\S+')"

echo "  mean: $(format_mib_s "${PROBE_MEAN}") MiB/s   cv: ${PROBE_CV}"
if [[ "${PROBE_STATUS}" == "UNSTABLE" ]]; then
  echo "  ⚠ WARNING: network CV=${PROBE_CV} exceeds ${STABILITY_CV_WARN}"
  echo "    Speed is varying by more than $(awk -v cv="${PROBE_CV}" 'BEGIN{printf "%.0f", cv*100}')%."
  echo "    Results will have high variance. Consider waiting for a more stable connection."
  echo "    Proceeding anyway — increase --runs to compensate."
else
  echo "  network looks stable (cv=${PROBE_CV} < ${STABILITY_CV_WARN})"
fi

# ─────────────────────────────────────────────────────────────
# Build and configure
# ─────────────────────────────────────────────────────────────
echo
if [[ "${TUR_SKIP_BUILD}" == "1" ]]; then
  echo "skipping build (TUR_SKIP_BUILD=1)"
else
  echo "building release binary..."
  if [[ -n "${TUR_CARGO_FEATURES}" ]]; then
    cargo build --release --features "${TUR_CARGO_FEATURES}" 2>&1 | tail -3
  else
    cargo build --release 2>&1 | tail -3
  fi
fi

AVAILABLE_TOOLS=()
for tool in "${REQUESTED_TOOLS[@]}"; do
  if tool_binary_available "${tool}"; then
    AVAILABLE_TOOLS+=("${tool}")
  else
    echo "skipping unavailable tool: ${tool}"
  fi
done
[[ "${#AVAILABLE_TOOLS[@]}" -eq 0 ]] && { echo "error: no tools available" >&2; exit 1; }
[[ " ${AVAILABLE_TOOLS[*]} " != *" tur "* ]] && AVAILABLE_TOOLS=(tur "${AVAILABLE_TOOLS[@]}")

STAMP="$(date +%Y%m%d-%H%M%S)"
ROOT_DIR="benchmarks/runs/${STAMP}-${NAME}"
SUMMARY="${ROOT_DIR}/summary.tsv"
mkdir -p "${ROOT_DIR}"
for tool in "${AVAILABLE_TOOLS[@]}"; do mkdir -p "${ROOT_DIR}/${tool}"; done

echo
echo "━━━ tournament config ━━━"
echo "  url:              ${EFFECTIVE_URL}"
echo "  file size:        ${SIZE_MIB:-unknown} MiB"
echo "  schedule:         ${SCHEDULE_MODE}"
echo "  http mode:        ${HTTP_MODE}"
echo "  connections:      ${CONNECTIONS}"
echo "  tur init conn:    ${TUR_INITIAL_CONNECTIONS}"
echo "  tur min conn:     ${TUR_MIN_CONNECTIONS:-default}"
echo "  tur max conn:     ${TUR_MAX_CONNECTIONS:-default}"
echo "  runs:             ${RUNS}"
echo "  tools:            ${AVAILABLE_TOOLS[*]}"
echo "  tool cooldown:    ${COOLDOWN_BETWEEN_TOOLS}s"
echo "  run cooldown:     ${COOLDOWN_BETWEEN_RUNS}s"
echo "  output:           ${ROOT_DIR}"

# ─────────────────────────────────────────────────────────────
# TSV header
# Columns: tool | run | elapsed_s | rss_kb | exit_status |
#          actual_throughput_Bps | probe_speed_Bps |
#          probe_connect_s | probe_ttfb_s | integrity
# ─────────────────────────────────────────────────────────────
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  tool run elapsed_seconds peak_rss_kb exit_status \
  actual_throughput_Bps probe_speed_Bps \
  probe_connect_s probe_ttfb_s integrity \
  >"${SUMMARY}"

# ─────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────
for RUN in $(seq 1 "${RUNS}"); do
  RUN_NAME="run-${RUN}"

  # Inter-run cooldown (skip before run 1)
  if [[ "${RUN}" -gt 1 ]]; then
    echo
    printf '━━━ inter-run cooldown %ds (network settling) ━━━\n' "${COOLDOWN_BETWEEN_RUNS}"
    sleep "${COOLDOWN_BETWEEN_RUNS}"
  fi

  echo
  echo "━━━ ${RUN_NAME} / ${RUNS} ━━━"

  # Rotate tool order — run N shifts the list by (N-1) positions
  mapfile -t RUN_TOOLS < <(rotate_tools $(( RUN - 1 )) "${AVAILABLE_TOOLS[@]}")
  echo "  tool order: ${RUN_TOOLS[*]}"

  FIRST_IN_RUN=true
  for tool in "${RUN_TOOLS[@]}"; do

    if [[ "${FIRST_IN_RUN}" != "true" ]]; then
      printf '  cooldown %ds...\n' "${COOLDOWN_BETWEEN_TOOLS}"
      sleep "${COOLDOWN_BETWEEN_TOOLS}"
    fi
    FIRST_IN_RUN=false

    TOOL_RUN_DIR="${ROOT_DIR}/${tool}/${RUN_NAME}"
    mkdir -p "${TOOL_RUN_DIR}/downloads" "${TOOL_RUN_DIR}/logs"

    echo
    echo "── ${tool} (${RUN_NAME}) ──"

    # Run a lightweight probe right before — used only for connect/ttfb timing,
    # NOT for speed fairness (actual throughput is the real metric).
    PROBE_LOG="${TOOL_RUN_DIR}/probe.log"
    curl -L --silent --show-error --output /dev/null \
      --range "0-$(( PROBE_RANGE_BYTES - 1 ))" --max-time 20 \
      --write-out $'speed_download_Bps=%{speed_download}\ntime_connect_s=%{time_connect}\ntime_starttransfer_s=%{time_starttransfer}\n' \
      "${EFFECTIVE_URL}" >"${PROBE_LOG}" 2>&1 \
      || { echo "probe_error=1"; } >"${PROBE_LOG}"

    # Run the actual download
    tool_status=0
    run_tool "${tool}" "${TOOL_RUN_DIR}" || tool_status=$?

    # Metrics
    ELAPSED="$(read_kv "${TOOL_RUN_DIR}/time.log" "elapsed_seconds")"
    RSS="$(read_kv "${TOOL_RUN_DIR}/time.log" "peak_rss_kb")"
    PROBE_BPS="$(read_kv "${PROBE_LOG}" "speed_download_Bps")"
    PROBE_CONNECT="$(read_kv "${PROBE_LOG}" "time_connect_s")"
    PROBE_TTFB="$(read_kv "${PROBE_LOG}" "time_starttransfer_s")"
    INTEGRITY="$(verify_download "${TOOL_RUN_DIR}/downloads" "${EXPECTED_SIZE}")"

    # Actual throughput: file_size / elapsed — primary metric
    ACTUAL_BPS="$(compute_throughput "${EXPECTED_SIZE:-}" "${ELAPSED}")"
    ACTUAL_MIB="$(format_mib_s "${ACTUAL_BPS}")"
    PROBE_MIB="$(format_mib_s "${PROBE_BPS}")"

    # Efficiency: how much of the single-connection probe speed did the tool achieve?
    EFFICIENCY=""
    if [[ -n "${PROBE_BPS}" && "${PROBE_BPS}" != "0" && "${ACTUAL_BPS}" != "0" ]]; then
      EFFICIENCY="$(awk -v a="${ACTUAL_BPS}" -v p="${PROBE_BPS}" \
        'BEGIN{printf "efficiency=%.0f%%", (a/p)*100}')"
    fi

    INTEGRITY_WARN=""
    [[ "${INTEGRITY}" != "ok" && "${INTEGRITY}" != "unknown" ]] \
      && INTEGRITY_WARN=" ⚠ INTEGRITY FAIL"

    echo "  elapsed:    ${ELAPSED}s"
    echo "  throughput: ${ACTUAL_MIB} MiB/s actual  |  ${PROBE_MIB} MiB/s probe  ${EFFICIENCY}"
    echo "  memory:     ${RSS} KB peak RSS"
    echo "  integrity:  ${INTEGRITY}${INTEGRITY_WARN}"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${tool}" "${RUN_NAME}" "${ELAPSED}" "${RSS}" "${tool_status}" \
      "${ACTUAL_BPS}" "${PROBE_BPS}" \
      "${PROBE_CONNECT}" "${PROBE_TTFB}" "${INTEGRITY}" \
      >>"${SUMMARY}"
  done
done

# ─────────────────────────────────────────────────────────────
# Aggregated statistics
# Primary sort key: avg actual throughput (higher = better)
# Integrity failures excluded from timing/throughput averages
# ─────────────────────────────────────────────────────────────
print_final_summary() {
  local summary="$1"
  echo
  echo "━━━ raw results ━━━"
  command -v column >/dev/null 2>&1 \
    && column -t -s $'\t' "${summary}" \
    || cat "${summary}"

  echo
  echo "━━━ aggregated statistics (integrity failures excluded) ━━━"
  echo "  primary metric: actual throughput = file_size / elapsed_seconds"
  echo "  probe speed reflects single-connection speed before each tool ran"
  echo
  awk -F'\t' '
    NR == 1 { next }
    {
      tool       = $1
      elapsed    = $3 + 0
      rss        = $4 + 0
      actual_bps = $6 + 0
      integrity  = $10

      if (integrity ~ /^FAIL/) { fails[tool]++; next }

      n[tool]++
      cnt = n[tool]

      # Welford online algorithm — numerically stable mean and variance
      d1 = elapsed - mean_e[tool]
      mean_e[tool] += d1 / cnt
      M2_e[tool]   += d1 * (elapsed - mean_e[tool])

      d2 = actual_bps - mean_t[tool]
      mean_t[tool] += d2 / cnt
      M2_t[tool]   += d2 * (actual_bps - mean_t[tool])

      sum_rss[tool] += rss

      # Track min/max in data pass — no division by zero needed
      if (cnt == 1 || elapsed    < min_e[tool])   min_e[tool]   = elapsed
      if (cnt == 1 || elapsed    > max_e[tool])   max_e[tool]   = elapsed
      if (cnt == 1 || actual_bps < min_t[tool])   min_t[tool]   = actual_bps
      if (cnt == 1 || actual_bps > max_t[tool])   max_t[tool]   = actual_bps
    }
    END {
      MiB = 1048576

      printf "  %-8s %5s  %9s %9s %9s %9s  %9s %9s %9s  %9s  %5s\n",
        "tool", "runs",
        "avg_MiB/s", "min_MiB/s", "max_MiB/s", "stdev_MiB/s",
        "avg_elap_s", "min_elap_s", "max_elap_s",
        "avg_rss_KB", "fails"
      printf "  %s\n", \
        "─────────────────────────────────────────────────────────────────────────────────────────────────"

      # Build sort index: higher avg throughput = better = sort first
      for (tool in n) sort_key[tool] = -mean_t[tool]
      PROCINFO["sorted_in"] = "@val_num_asc"

      for (tool in sort_key) {
        cnt      = n[tool]
        stddev_t = (cnt > 1 ? sqrt(M2_t[tool] / cnt) : 0)
        f        = (tool in fails ? fails[tool] : 0)
        printf "  %-8s %5d  %9.2f %9.2f %9.2f %9.2f  %9.3f %9.3f %9.3f  %9.0f  %5d\n",
          tool, cnt,
          mean_t[tool] / MiB,
          min_t[tool]  / MiB,
          max_t[tool]  / MiB,
          stddev_t     / MiB,
          mean_e[tool], min_e[tool], max_e[tool],
          sum_rss[tool] / cnt,
          f
      }
    }
  ' "${summary}"
}

# ─────────────────────────────────────────────────────────────
# Per-run verdicts — based on actual throughput, not probe speed
# ─────────────────────────────────────────────────────────────
print_run_verdicts() {
  local summary="$1"
  echo
  echo "━━━ per-run verdicts vs tur ━━━"
  echo "  speed comparison uses actual throughput (MiB/s), not probe speed"
  echo "  skew detection: if any tool's throughput differs >30% from tur's in the same run,"
  echo "  the network gave that tool a different experience — flag the run as skewed"
  echo
  awk -F'\t' '
    NR == 1 { next }
    {
      tool = $1; run = $2
      elapsed[run, tool]    = $3 + 0
      rss[run, tool]        = $4 + 0
      actual_bps[run, tool] = $6 + 0
      probe_bps[run, tool]  = ($7 == "" ? 0 : $7 + 0)
      integrity[run, tool]  = $10
      seen_runs[run]  = 1
      seen_tools[tool] = 1
    }
    END {
      PROCINFO["sorted_in"] = "@ind_str_asc"
      for (run in seen_runs) {
        tur_bps   = actual_bps[run, "tur"]
        tur_rss   = rss[run, "tur"]
        tur_int   = integrity[run, "tur"]

        # Check for within-run throughput skew using actual achieved speeds
        max_bps = 0; min_bps = 999999999
        for (t in seen_tools) {
          b = actual_bps[run, t]
          if (b > 0 && b > max_bps) max_bps = b
          if (b > 0 && b < min_bps) min_bps = b
        }
        run_skew = (min_bps > 0 && max_bps > 0 ? max_bps / min_bps : 1)
        skew_note = (run_skew > 1.3 \
          ? sprintf("  ⚠ RUN SKEWED: fastest tool got %.1fx more throughput than slowest (network varied during run)", run_skew) \
          : "")

        for (tool in seen_tools) {
          if (tool == "tur") continue

          other_bps = actual_bps[run, tool]
          other_rss = rss[run, tool]
          other_int = integrity[run, tool]
          if (other_bps == 0 && other_rss == 0) continue

          # Integrity flags
          int_note = ""
          if (tur_int   ~ /^FAIL/) int_note = int_note " [TUR:INTEGRITY_FAIL]"
          if (other_int ~ /^FAIL/) int_note = int_note " [" tool ":INTEGRITY_FAIL]"

          # Speed: compare actual throughput in MiB/s
          tur_mib   = tur_bps   / 1048576
          other_mib = other_bps / 1048576
          gap_mib   = tur_mib - other_mib
          gap_pct   = (other_mib > 0 ? (gap_mib / other_mib) * 100.0 : 0)
          thresh_mib = (other_mib * 0.10 > 0.05 ? other_mib * 0.10 : 0.05)

          if      (gap_mib >=  thresh_mib) spd = "GOOD for tur"
          else if (gap_mib > -thresh_mib)  spd = "CLOSE       "
          else                              spd = "BAD  for tur"

          # Memory
          rss_gap = other_rss - tur_rss
          rss_pct = (other_rss > 0 ? (rss_gap / other_rss) * 100.0 : 0)
          if      (rss_gap > 0) mem = "GOOD for tur"
          else if (rss_gap < 0) mem = "BAD  for tur"
          else                   mem = "TIED"

          printf "  %s | %-8s | speed: %s (tur=%.2f %s=%.2f MiB/s, %+.2f/%+.1f%%) | mem: %s (tur=%dKB %s=%dKB saves=%.0f%%)%s\n",
            run, tool,
            spd, tur_mib, tool, other_mib, gap_mib, gap_pct,
            mem, tur_rss, tool, other_rss, rss_pct,
            int_note
        }
        if (skew_note != "") print skew_note
        print ""
      }
    }
  ' "${summary}"

  echo "guide:"
  echo "  GOOD for tur      — tur achieved meaningfully higher throughput / lower memory"
  echo "  BAD  for tur      — tur achieved meaningfully lower throughput / higher memory"
  echo "  CLOSE             — difference under 10%, inconclusive; run more iterations"
  echo "  ⚠ RUN SKEWED      — network gave different speeds to different tools in this run"
  echo "                       aggregate across runs, do not trust individual skewed runs"
  echo "  INTEGRITY FAIL    — downloaded file does not match expected size; run is invalid"
}

# ─────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────
echo
echo "━━━ benchmark complete ━━━"
echo "  directory: ${ROOT_DIR}"
echo "  summary:   ${SUMMARY}"
print_final_summary "${SUMMARY}"
print_run_verdicts "${SUMMARY}"
