#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 <url> <name> [connections] [runs]"
  exit 1
fi

URL="$1"
NAME="$2"
CONNECTIONS="${3:-4}"
RUNS="${4:-1}"

# Accept schemeless copy-pastes like //host/path and normalize them.
if [[ "${URL}" == //* ]]; then
  URL="https:${URL}"
fi

if [[ ! "${URL}" =~ ^https?:// ]]; then
  echo "error: url must start with https:// or http://"
  echo "got: ${URL}"
  exit 1
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
ROOT_DIR="benchmarks/runs/${STAMP}-${NAME}"
TUR_DIR="${ROOT_DIR}/tur"
ARIA_DIR="${ROOT_DIR}/aria2c"

mkdir -p "${TUR_DIR}/downloads" "${TUR_DIR}/logs" "${ARIA_DIR}/downloads" "${ARIA_DIR}/logs"

echo "building release binary..."
cargo build --release

format_mb_s() {
  local bps="${1:-0}"
  awk -v bps="${bps}" 'BEGIN { printf "%.2f", bps / (1024 * 1024) }'
}

read_kv() {
  local file="$1"
  local key="$2"
  grep -F "${key}=" "${file}" 2>/dev/null | tail -n 1 | cut -d= -f2- || true
}

show_live_status() {
  local pid="$1"
  local label="$2"
  local stdout_log="$3"
  local started_at="$4"

  while kill -0 "${pid}" 2>/dev/null; do
    local now_s elapsed_s rss_kb latest_line
    now_s="$(date +%s)"
    elapsed_s=$(( now_s - started_at ))
    rss_kb=0
    if [[ -r "/proc/${pid}/status" ]]; then
      rss_kb="$(awk '/VmRSS:/ { print $2 }' "/proc/${pid}/status" 2>/dev/null || echo 0)"
    fi
    latest_line="$(tail -n 1 "${stdout_log}" 2>/dev/null | tr '\r' ' ' | cut -c1-100)"
    printf '\r[%s] elapsed=%ss rss=%sKB %s' "${label}" "${elapsed_s}" "${rss_kb:-0}" "${latest_line:-running...}"
    sleep 1
  done
  printf '\n'
}

run_and_measure() {
  local prefix="$1"
  local label="$2"
  shift
  shift

  local stdout_log="${prefix}/stdout.log"
  local time_log="${prefix}/time.log"
  local status_log="${prefix}/exit_status.txt"

  local start_ns
  start_ns="$(date +%s%N)"
  local start_s
  start_s="$(date +%s)"

  "$@" > "${stdout_log}" 2>&1 &
  local pid=$!
  local peak_rss_kb=0

  show_live_status "${pid}" "${label}" "${stdout_log}" "${start_s}" &
  local progress_pid=$!

  while kill -0 "${pid}" 2>/dev/null; do
    if [[ -r "/proc/${pid}/status" ]]; then
      local rss_kb
      rss_kb="$(awk '/VmRSS:/ { print $2 }' "/proc/${pid}/status" 2>/dev/null || true)"
      if [[ -n "${rss_kb}" ]] && (( rss_kb > peak_rss_kb )); then
        peak_rss_kb="${rss_kb}"
      fi
    fi
    sleep 0.05
  done

  local cmd_status=0
  if ! wait "${pid}"; then
    cmd_status=$?
  fi
  wait "${progress_pid}" 2>/dev/null || true

  local end_ns elapsed_ns elapsed_ms elapsed_s
  end_ns="$(date +%s%N)"
  elapsed_ns=$(( end_ns - start_ns ))
  elapsed_ms=$(( elapsed_ns / 1000000 ))
  elapsed_s="$(awk -v ms="${elapsed_ms}" 'BEGIN { printf "%.3f", ms / 1000 }')"

  {
    echo "elapsed_seconds=${elapsed_s}"
    echo "peak_rss_kb=${peak_rss_kb}"
    echo "exit_status=${cmd_status}"
  } > "${time_log}"

  printf '%s\n' "${cmd_status}" > "${status_log}"
  return "${cmd_status}"
}

probe_network() {
  local prefix="$1"
  local url="$2"
  local probe_log="${prefix}/network_probe.log"

  curl \
    -L \
    --silent \
    --show-error \
    --output /dev/null \
    --range 0-1048575 \
    --max-time 20 \
    --write-out $'probe_exit=0\nremote_ip=%{remote_ip}\nhttp_code=%{http_code}\nsize_download=%{size_download}\nspeed_download_Bps=%{speed_download}\ntime_namelookup_s=%{time_namelookup}\ntime_connect_s=%{time_connect}\ntime_starttransfer_s=%{time_starttransfer}\ntime_total_s=%{time_total}\n' \
    "${url}" \
    > "${probe_log}" 2>&1 || {
      local status=$?
      {
        echo "probe_exit=${status}"
        echo "probe_error=1"
      } > "${probe_log}"
      return 0
    }
}

for RUN in $(seq 1 "${RUNS}"); do
  RUN_NAME="run-${RUN}"
  TUR_RUN_DIR="${TUR_DIR}/${RUN_NAME}"
  ARIA_RUN_DIR="${ARIA_DIR}/${RUN_NAME}"

  mkdir -p "${TUR_RUN_DIR}/downloads" "${TUR_RUN_DIR}/logs" "${ARIA_RUN_DIR}/downloads" "${ARIA_RUN_DIR}/logs"

  TUR_OUT_NAME="${NAME}"
  ARIA_OUT_NAME="${NAME}"

  echo
  echo "== ${RUN_NAME}/${RUNS} tur =="
  printf '%s\n' \
    "./target/release/tur --headless --url ${URL} --dir ${TUR_RUN_DIR}/downloads --connections ${CONNECTIONS} --log-root ${TUR_RUN_DIR}/logs" \
    > "${TUR_RUN_DIR}/command.txt"
  probe_network "${TUR_RUN_DIR}" "${URL}"
  run_and_measure "${TUR_RUN_DIR}" "tur ${RUN_NAME}" \
    ./target/release/tur \
    --headless \
    --url "${URL}" \
    --dir "${TUR_RUN_DIR}/downloads" \
    --connections "${CONNECTIONS}" \
    --log-root "${TUR_RUN_DIR}/logs"
  echo "tur ${RUN_NAME}: elapsed=$(read_kv "${TUR_RUN_DIR}/time.log" "elapsed_seconds")s rss=$(read_kv "${TUR_RUN_DIR}/time.log" "peak_rss_kb")KB status=$(read_kv "${TUR_RUN_DIR}/time.log" "exit_status") probe=$(format_mb_s "$(read_kv "${TUR_RUN_DIR}/network_probe.log" "speed_download_Bps")")MiB/s"

  echo
  echo "== ${RUN_NAME}/${RUNS} aria2c =="
  printf '%s\n' \
    "aria2c --dir=${ARIA_RUN_DIR}/downloads --out=${ARIA_OUT_NAME} --max-connection-per-server=${CONNECTIONS} --split=${CONNECTIONS} --min-split-size=1M --file-allocation=none --log=${ARIA_RUN_DIR}/logs/aria2c.log ${URL}" \
    > "${ARIA_RUN_DIR}/command.txt"
  probe_network "${ARIA_RUN_DIR}" "${URL}"
  run_and_measure "${ARIA_RUN_DIR}" "aria2c ${RUN_NAME}" \
    aria2c \
    --dir="${ARIA_RUN_DIR}/downloads" \
    --out="${ARIA_OUT_NAME}" \
    --max-connection-per-server="${CONNECTIONS}" \
    --split="${CONNECTIONS}" \
    --min-split-size=1M \
    --file-allocation=none \
    --log="${ARIA_RUN_DIR}/logs/aria2c.log" \
    "${URL}"
  echo "aria2c ${RUN_NAME}: elapsed=$(read_kv "${ARIA_RUN_DIR}/time.log" "elapsed_seconds")s rss=$(read_kv "${ARIA_RUN_DIR}/time.log" "peak_rss_kb")KB status=$(read_kv "${ARIA_RUN_DIR}/time.log" "exit_status") probe=$(format_mb_s "$(read_kv "${ARIA_RUN_DIR}/network_probe.log" "speed_download_Bps")")MiB/s"
done

SUMMARY="${ROOT_DIR}/summary.tsv"
printf "tool\trun\telapsed_seconds\tpeak_rss_kb\texit_status\tprobe_speed_Bps\tprobe_total_s\tprobe_connect_s\tprobe_ttfb_s\tprobe_bytes\n" > "${SUMMARY}"

for TOOL in tur aria2c; do
  for RUN_PATH in "${ROOT_DIR}/${TOOL}"/run-*; do
    RUN_BASENAME="$(basename "${RUN_PATH}")"
    ELAPSED="$(grep -F "elapsed_seconds=" "${RUN_PATH}/time.log" | cut -d= -f2)"
    MAX_RSS="$(grep -F "peak_rss_kb=" "${RUN_PATH}/time.log" | cut -d= -f2)"
    STATUS="$(grep -F "exit_status=" "${RUN_PATH}/time.log" | cut -d= -f2)"
    PROBE_SPEED="$(grep -F "speed_download_Bps=" "${RUN_PATH}/network_probe.log" | cut -d= -f2 || true)"
    PROBE_TOTAL="$(grep -F "time_total_s=" "${RUN_PATH}/network_probe.log" | cut -d= -f2 || true)"
    PROBE_CONNECT="$(grep -F "time_connect_s=" "${RUN_PATH}/network_probe.log" | cut -d= -f2 || true)"
    PROBE_TTFB="$(grep -F "time_starttransfer_s=" "${RUN_PATH}/network_probe.log" | cut -d= -f2 || true)"
    PROBE_BYTES="$(grep -F "size_download=" "${RUN_PATH}/network_probe.log" | cut -d= -f2 || true)"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "${TOOL}" "${RUN_BASENAME}" "${ELAPSED}" "${MAX_RSS}" "${STATUS}" \
      "${PROBE_SPEED}" "${PROBE_TOTAL}" "${PROBE_CONNECT}" "${PROBE_TTFB}" "${PROBE_BYTES}" \
      >> "${SUMMARY}"
  done
done

print_final_summary() {
  local summary="$1"
  echo
  echo "== final summary =="
  if command -v column >/dev/null 2>&1; then
    column -t -s $'\t' "${summary}"
  else
    cat "${summary}"
  fi
  echo
  awk -F'\t' '
    NR == 1 { next }
    {
      runs[$1] += 1
      elapsed[$1] += $3
      rss[$1] += $4
      probe[$1] += $6
    }
    END {
      printf "%-8s %-12s %-14s %-14s\n", "tool", "avg_elapsed_s", "avg_rss_kb", "avg_probe_MiB_s"
      for (tool in runs) {
        printf "%-8s %-12.3f %-14.0f %-14.2f\n",
          tool,
          elapsed[tool] / runs[tool],
          rss[tool] / runs[tool],
          (probe[tool] / runs[tool]) / (1024 * 1024)
      }
    }
  ' "${summary}"
}

print_run_verdicts() {
  local summary="$1"
  echo
  echo "== run verdicts =="
  awk -F'\t' '
    NR == 1 { next }
    {
      tool = $1
      run = $2
      elapsed[run, tool] = $3 + 0
      rss[run, tool] = $4 + 0
      probe[run, tool] = ($6 == "" ? -1 : $6 + 0)
      seen[run] = 1
    }
    END {
      for (run in seen) {
        tur_elapsed = elapsed[run, "tur"]
        aria_elapsed = elapsed[run, "aria2c"]
        tur_rss = rss[run, "tur"]
        aria_rss = rss[run, "aria2c"]
        tur_probe = probe[run, "tur"]
        aria_probe = probe[run, "aria2c"]

        speed_gap_s = tur_elapsed - aria_elapsed
        speed_gap_pct = (aria_elapsed > 0 ? (speed_gap_s / aria_elapsed) * 100.0 : 0)
        rss_gap_kb = aria_rss - tur_rss
        rss_gap_pct = (aria_rss > 0 ? (rss_gap_kb / aria_rss) * 100.0 : 0)

        if (speed_gap_s <= -5) {
          speed_label = "GOOD for tur"
        } else if (speed_gap_s < 5) {
          speed_label = "CLOSE"
        } else {
          speed_label = "BAD for tur"
        }

        if (rss_gap_kb > 0) {
          memory_label = "GOOD for tur"
        } else if (rss_gap_kb < 0) {
          memory_label = "BAD for tur"
        } else {
          memory_label = "TIED"
        }

        probe_label = "probe unavailable"
        if (tur_probe > 0 && aria_probe > 0) {
          ratio = tur_probe / aria_probe
          if (ratio < 0.70 || ratio > 1.30) {
            probe_label = "network skewed"
          } else {
            probe_label = "network roughly comparable"
          }
        } else if (tur_probe > 0 || aria_probe > 0) {
          probe_label = "probe incomplete"
        }

        printf "%s: speed=%s (tur %.3fs vs aria2c %.3fs, %+0.3fs / %+0.1f%%), memory=%s (tur %dKB vs aria2c %dKB, tur uses %.1f%% less), probe=%s\n",
          run,
          speed_label,
          tur_elapsed,
          aria_elapsed,
          -speed_gap_s,
          -speed_gap_pct,
          memory_label,
          tur_rss,
          aria_rss,
          rss_gap_pct,
          probe_label
      }
    }
  ' "${summary}"
  echo
  echo "guide:"
  echo "  GOOD for tur speed: tur is meaningfully faster"
  echo "  BAD for tur speed: tur is meaningfully slower"
  echo "  CLOSE: speed gap is small enough that more runs are needed"
  echo "  network skewed: probe speeds differed too much, so the run is not a clean fairness check"
}

echo "benchmark complete: ${ROOT_DIR}"
echo "summary: ${SUMMARY}"
print_final_summary "${SUMMARY}"
print_run_verdicts "${SUMMARY}"
