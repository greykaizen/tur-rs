#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 7 ]]; then
  echo "usage: $0 <url> <name> [connections] [runs] [schedule_mode] [http_mode] [tools_csv]"
  exit 1
fi

URL="$1"
NAME="$2"
CONNECTIONS="${3:-4}"
RUNS="${4:-1}"
SCHEDULE_MODE="${5:-equal}"
HTTP_MODE="${6:-http1}"
TOOLS_CSV="${7:-tur,aria2c,wget,wget2,lftp,axel,curl}"

if [[ "${URL}" == //* ]]; then
  URL="https:${URL}"
fi

if [[ ! "${URL}" =~ ^https?:// ]]; then
  echo "error: url must start with https:// or http://"
  echo "got: ${URL}"
  exit 1
fi

IFS=',' read -r -a REQUESTED_TOOLS <<< "${TOOLS_CSV}"

resolve_effective_url() {
  local url="$1"
  curl \
    -L \
    --silent \
    --show-error \
    --output /dev/null \
    --max-time 20 \
    --write-out '%{url_effective}' \
    "${url}"
}

EFFECTIVE_URL="$(resolve_effective_url "${URL}" 2>/dev/null || true)"
if [[ -z "${EFFECTIVE_URL}" ]]; then
  EFFECTIVE_URL="${URL}"
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
ROOT_DIR="benchmarks/runs/${STAMP}-${NAME}"
SUMMARY="${ROOT_DIR}/summary.tsv"

mkdir -p "${ROOT_DIR}"

format_mb_s() {
  local bps="${1:-0}"
  awk -v bps="${bps}" 'BEGIN { printf "%.2f", bps / (1024 * 1024) }'
}

read_kv() {
  local file="$1"
  local key="$2"
  grep -F "${key}=" "${file}" 2>/dev/null | tail -n 1 | cut -d= -f2- || true
}

tool_binary_available() {
  local tool="$1"
  case "${tool}" in
    tur) return 0 ;;
    aria2c|wget|wget2|lftp|axel|curl) command -v "${tool}" >/dev/null 2>&1 ;;
    *) return 1 ;;
  esac
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
  shift 2

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

build_tool_command() {
  local tool="$1"
  local run_dir="$2"
  local downloads_dir="${run_dir}/downloads"
  local logs_dir="${run_dir}/logs"
  local out_name="${NAME}"

  case "${tool}" in
    tur)
      printf '%s\n' \
        "./target/release/tur --headless --url \"${EFFECTIVE_URL}\" --dir \"${downloads_dir}\" --connections ${CONNECTIONS} --schedule-mode ${SCHEDULE_MODE} --http-mode ${HTTP_MODE} --log-root \"${logs_dir}\""
      ;;
    aria2c)
      printf '%s\n' \
        "aria2c --dir=\"${downloads_dir}\" --out=\"${out_name}\" --max-connection-per-server=${CONNECTIONS} --split=${CONNECTIONS} --min-split-size=1M --file-allocation=none --log=\"${logs_dir}/aria2c.log\" \"${EFFECTIVE_URL}\""
      ;;
    wget)
      printf '%s\n' \
        "wget --no-config --output-file=\"${logs_dir}/wget.log\" --output-document=\"${downloads_dir}/${out_name}\" \"${EFFECTIVE_URL}\""
      ;;
    wget2)
      printf '%s\n' \
        "wget2 --output-file=\"${logs_dir}/wget2.log\" --output-document=\"${downloads_dir}/${out_name}\" --chunk-size=1M --max-threads=${CONNECTIONS} \"${EFFECTIVE_URL}\""
      ;;
    lftp)
      printf '%s\n' \
        "lftp --norc -c 'set xfer:clobber true; pget -n ${CONNECTIONS} -O \"${downloads_dir}\" \"${EFFECTIVE_URL}\" -o \"${out_name}\"; bye'"
      ;;
    axel)
      printf '%s\n' \
        "axel --num-connections=${CONNECTIONS} --output=\"${downloads_dir}/${out_name}\" \"${EFFECTIVE_URL}\""
      ;;
    curl)
      printf '%s\n' \
        "curl -L --fail --silent --show-error --output \"${downloads_dir}/${out_name}\" \"${EFFECTIVE_URL}\""
      ;;
    *)
      return 1
      ;;
  esac
}

run_tool() {
  local tool="$1"
  local run_dir="$2"
  local downloads_dir="${run_dir}/downloads"
  local logs_dir="${run_dir}/logs"
  local out_name="${NAME}"

  case "${tool}" in
    tur)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        ./target/release/tur \
        --headless \
        --url "${EFFECTIVE_URL}" \
        --dir "${downloads_dir}" \
        --connections "${CONNECTIONS}" \
        --schedule-mode "${SCHEDULE_MODE}" \
        --http-mode "${HTTP_MODE}" \
        --log-root "${logs_dir}"
      ;;
    aria2c)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        aria2c \
        --dir="${downloads_dir}" \
        --out="${out_name}" \
        --max-connection-per-server="${CONNECTIONS}" \
        --split="${CONNECTIONS}" \
        --min-split-size=1M \
        --file-allocation=none \
        --log="${logs_dir}/aria2c.log" \
        "${EFFECTIVE_URL}"
      ;;
    wget)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        wget \
        --no-config \
        --output-file="${logs_dir}/wget.log" \
        --output-document="${downloads_dir}/${out_name}" \
        "${EFFECTIVE_URL}"
      ;;
    wget2)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        wget2 \
        --output-file="${logs_dir}/wget2.log" \
        --output-document="${downloads_dir}/${out_name}" \
        --chunk-size=1M \
        --max-threads="${CONNECTIONS}" \
        "${EFFECTIVE_URL}"
      ;;
    lftp)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        lftp \
        --norc \
        -c "set xfer:clobber true; pget -n ${CONNECTIONS} -O \"${downloads_dir}\" \"${EFFECTIVE_URL}\" -o \"${out_name}\"; bye"
      ;;
    axel)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        axel \
        --num-connections="${CONNECTIONS}" \
        --output="${downloads_dir}/${out_name}" \
        "${EFFECTIVE_URL}"
      ;;
    curl)
      run_and_measure "${run_dir}" "${tool} ${RUN_NAME}" \
        curl \
        -L \
        --fail \
        --silent \
        --show-error \
        --output "${downloads_dir}/${out_name}" \
        "${EFFECTIVE_URL}"
      ;;
    *)
      return 1
      ;;
  esac
}

AVAILABLE_TOOLS=()
for tool in "${REQUESTED_TOOLS[@]}"; do
  if tool_binary_available "${tool}"; then
    AVAILABLE_TOOLS+=("${tool}")
  else
    echo "skipping unavailable tool: ${tool}"
  fi
done

if [[ "${#AVAILABLE_TOOLS[@]}" -eq 0 ]]; then
  echo "error: no benchmark tools available"
  exit 1
fi

if [[ " ${AVAILABLE_TOOLS[*]} " != *" tur "* ]]; then
  AVAILABLE_TOOLS=(tur "${AVAILABLE_TOOLS[@]}")
fi

echo "building release binary..."
cargo build --release
echo "original url: ${URL}"
echo "effective url: ${EFFECTIVE_URL}"
echo "schedule mode: ${SCHEDULE_MODE}"
echo "http mode: ${HTTP_MODE}"
echo "tools: ${AVAILABLE_TOOLS[*]}"

for tool in "${AVAILABLE_TOOLS[@]}"; do
  mkdir -p "${ROOT_DIR}/${tool}"
done

for RUN in $(seq 1 "${RUNS}"); do
  RUN_NAME="run-${RUN}"
  echo
  echo "== ${RUN_NAME}/${RUNS} =="

  for tool in "${AVAILABLE_TOOLS[@]}"; do
    TOOL_RUN_DIR="${ROOT_DIR}/${tool}/${RUN_NAME}"
    mkdir -p "${TOOL_RUN_DIR}/downloads" "${TOOL_RUN_DIR}/logs"

    echo
    echo "-- ${tool} --"
    build_tool_command "${tool}" "${TOOL_RUN_DIR}" > "${TOOL_RUN_DIR}/command.txt"
    probe_network "${TOOL_RUN_DIR}" "${EFFECTIVE_URL}"
    run_tool "${tool}" "${TOOL_RUN_DIR}"
    echo "${tool} ${RUN_NAME}: elapsed=$(read_kv "${TOOL_RUN_DIR}/time.log" "elapsed_seconds")s rss=$(read_kv "${TOOL_RUN_DIR}/time.log" "peak_rss_kb")KB status=$(read_kv "${TOOL_RUN_DIR}/time.log" "exit_status") probe=$(format_mb_s "$(read_kv "${TOOL_RUN_DIR}/network_probe.log" "speed_download_Bps")")MiB/s"
  done
done

printf "tool\trun\telapsed_seconds\tpeak_rss_kb\texit_status\tprobe_speed_Bps\tprobe_total_s\tprobe_connect_s\tprobe_ttfb_s\tprobe_bytes\n" > "${SUMMARY}"

for tool in "${AVAILABLE_TOOLS[@]}"; do
  for run_path in "${ROOT_DIR}/${tool}"/run-*; do
    [[ -d "${run_path}" ]] || continue
    run_basename="$(basename "${run_path}")"
    elapsed="$(grep -F "elapsed_seconds=" "${run_path}/time.log" | cut -d= -f2)"
    max_rss="$(grep -F "peak_rss_kb=" "${run_path}/time.log" | cut -d= -f2)"
    status="$(grep -F "exit_status=" "${run_path}/time.log" | cut -d= -f2)"
    probe_speed="$(grep -F "speed_download_Bps=" "${run_path}/network_probe.log" | cut -d= -f2 || true)"
    probe_total="$(grep -F "time_total_s=" "${run_path}/network_probe.log" | cut -d= -f2 || true)"
    probe_connect="$(grep -F "time_connect_s=" "${run_path}/network_probe.log" | cut -d= -f2 || true)"
    probe_ttfb="$(grep -F "time_starttransfer_s=" "${run_path}/network_probe.log" | cut -d= -f2 || true)"
    probe_bytes="$(grep -F "size_download=" "${run_path}/network_probe.log" | cut -d= -f2 || true)"
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "${tool}" "${run_basename}" "${elapsed}" "${max_rss}" "${status}" \
      "${probe_speed}" "${probe_total}" "${probe_connect}" "${probe_ttfb}" "${probe_bytes}" \
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
  echo "== run verdicts vs tur =="
  awk -F'\t' '
    NR == 1 { next }
    {
      tool = $1
      run = $2
      elapsed[run, tool] = $3 + 0
      rss[run, tool] = $4 + 0
      probe[run, tool] = ($6 == "" ? -1 : $6 + 0)
      seen_runs[run] = 1
      seen_tools[tool] = 1
    }
    END {
      for (run in seen_runs) {
        tur_elapsed = elapsed[run, "tur"]
        tur_rss = rss[run, "tur"]
        tur_probe = probe[run, "tur"]
        for (tool in seen_tools) {
          if (tool == "tur") {
            continue
          }
          other_elapsed = elapsed[run, tool]
          other_rss = rss[run, tool]
          other_probe = probe[run, tool]
          if (other_elapsed == 0 && other_rss == 0) {
            continue
          }

          speed_gap_s = tur_elapsed - other_elapsed
          speed_gap_pct = (other_elapsed > 0 ? (speed_gap_s / other_elapsed) * 100.0 : 0)
          rss_gap_kb = other_rss - tur_rss
          rss_gap_pct = (other_rss > 0 ? (rss_gap_kb / other_rss) * 100.0 : 0)

          close_threshold_s = 5
          close_threshold_pct = 10
          if (other_elapsed > 0 && (other_elapsed * close_threshold_pct / 100.0) > close_threshold_s) {
            close_threshold_s = other_elapsed * close_threshold_pct / 100.0
          }

          if (speed_gap_s <= -close_threshold_s) {
            speed_label = "GOOD for tur"
          } else if (speed_gap_s < close_threshold_s) {
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
          if (tur_probe > 0 && other_probe > 0) {
            ratio = tur_probe / other_probe
            if (ratio < 0.70 || ratio > 1.30) {
              probe_label = "network skewed"
            } else {
              probe_label = "network roughly comparable"
            }
          } else if (tur_probe > 0 || other_probe > 0) {
            probe_label = "probe incomplete"
          }

          printf "%s vs %s: speed=%s (tur %.3fs vs %s %.3fs, %+0.3fs / %+0.1f%%), memory=%s (tur %dKB vs %s %dKB, tur uses %.1f%% less), probe=%s\n",
            run,
            tool,
            speed_label,
            tur_elapsed,
            tool,
            other_elapsed,
            -speed_gap_s,
            -speed_gap_pct,
            memory_label,
            tur_rss,
            tool,
            other_rss,
            rss_gap_pct,
            probe_label
        }
      }
    }
  ' "${summary}"
  echo
  echo "guide:"
  echo "  GOOD for tur speed: tur is meaningfully faster"
  echo "  BAD for tur speed: tur is meaningfully slower"
  echo "  CLOSE: speed gap is under 5s or 10%, so more runs are needed"
  echo "  network skewed: probe speeds differed too much, so the run is not a clean fairness check"
}

echo "benchmark complete: ${ROOT_DIR}"
echo "summary: ${SUMMARY}"
print_final_summary "${SUMMARY}"
print_run_verdicts "${SUMMARY}"
