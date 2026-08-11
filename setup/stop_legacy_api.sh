#!/usr/bin/env bash
set -euo pipefail

pbgui_dir=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pbgui-dir)
      pbgui_dir="$2"
      shift 2
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$pbgui_dir" || "$pbgui_dir" != /* ]]; then
  printf 'An absolute --pbgui-dir is required.\n' >&2
  exit 2
fi
pbgui_dir="$(realpath -- "$pbgui_dir")"
api_script="$pbgui_dir/PBApiServer.py"
pidfile="$pbgui_dir/data/pid/api_server.pid"

declare -a legacy_pids=()
for proc_dir in /proc/[0-9]*; do
  pid="${proc_dir##*/}"
  [[ "$pid" != "$$" && -r "$proc_dir/cmdline" ]] || continue
  executable="$(readlink -f -- "$proc_dir/exe" 2>/dev/null || true)"
  [[ "${executable##*/}" == python* ]] || continue
  argv=()
  mapfile -d '' -t argv < "$proc_dir/cmdline" || true
  script_arg="${argv[1]:-}"
  [[ "$script_arg" == "-u" ]] && script_arg="${argv[2]:-}"
  [[ -n "$script_arg" ]] || continue
  if [[ "$script_arg" == /* ]]; then
    resolved_script="$(readlink -f -- "$script_arg" 2>/dev/null || true)"
  else
    process_cwd="$(readlink -f -- "$proc_dir/cwd" 2>/dev/null || true)"
    resolved_script="$(readlink -f -- "$process_cwd/$script_arg" 2>/dev/null || true)"
  fi
  if [[ "$resolved_script" == "$api_script" ]]; then
    legacy_pids+=("$pid")
  fi
done

process_running() {
  local pid="$1"
  local stat_line rest state
  [[ -r "/proc/$pid/stat" ]] || return 1
  stat_line="$(<"/proc/$pid/stat")"
  rest="${stat_line##*) }"
  state="${rest%% *}"
  [[ "$state" != "Z" ]]
}

if [[ ${#legacy_pids[@]} -gt 0 ]]; then
  printf 'Stopping legacy PBGui API process(es): %s\n' "${legacy_pids[*]}"
  for pid in "${legacy_pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done

  for _ in $(seq 1 30); do
    alive=false
    for pid in "${legacy_pids[@]}"; do
      if process_running "$pid"; then
        alive=true
        break
      fi
    done
    [[ "$alive" == false ]] && break
    sleep 1
  done

  for pid in "${legacy_pids[@]}"; do
    if process_running "$pid"; then
      printf 'Legacy PBGui API process did not stop: %s\n' "$pid" >&2
      exit 1
    fi
  done
fi

rm -f -- "$pidfile"
