#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: vps_service_control.sh start|stop|restart PBRun [PBCluster] [PBCoinData] [PBMonitorAgent]

Controls VPS PBGui services. Uses systemd user units when all requested units
exist and the user manager is available; otherwise falls back to starter.py.

Environment:
  PBGUI_DIR      Remote PBGui directory. Default: current working directory.
  PBGUI_PYTHON   PBGui virtualenv Python. Default: ../venv_pbgui/bin/python.
  PBGUI_USER     Target service user when executed with become/root.
  PBGUI_CREDENTIAL_ACTIVE  true/false when pool capability is known; empty means preserve state.
EOF
}

if [[ $# -lt 2 ]]; then
  usage >&2
  exit 2
fi

action="$1"
shift
case "$action" in
  start|stop|restart) ;;
  *)
    usage >&2
    exit 2
    ;;
esac

pbgui_dir="${PBGUI_DIR:-$(pwd)}"
python_bin="${PBGUI_PYTHON:-$(dirname "$pbgui_dir")/venv_pbgui/bin/python}"
target_user="${PBGUI_USER:-}"
if [[ -n "$target_user" ]]; then
  target_uid="$(id -u "$target_user")"
  target_home="$(getent passwd "$target_user" | cut -d: -f6)"
else
  target_uid="$(id -u)"
  target_home="$HOME"
fi
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$target_uid}"

run_as_service_user() {
  if [[ -n "$target_user" && "$(id -u)" -eq 0 && "$target_user" != "root" ]]; then
    sudo -H -u "$target_user" env XDG_RUNTIME_DIR="/run/user/$target_uid" "$@"
  else
    env XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR}" "$@"
  fi
}

unit_for() {
  case "$1" in
    PBRun) printf '%s\n' 'pbgui-pbrun.service' ;;
    PBCluster) printf '%s\n' 'pbgui-pbcluster.service' ;;
    PBCoinData) printf '%s\n' 'pbgui-pbcoindata.service' ;;
    PBMonitorAgent) printf '%s\n' 'pbgui-monitor-agent.service' ;;
    *)
      printf 'Unknown service: %s\n' "$1" >&2
      return 1
      ;;
  esac
}

script_for() {
  case "$1" in
    PBRun) printf '%s\n' 'PBRun.py' ;;
    PBCluster) printf '%s\n' 'PBCluster.py' ;;
    PBCoinData) printf '%s\n' 'PBCoinData.py' ;;
    PBMonitorAgent) printf '%s\n' 'monitor_agent.py' ;;
    *) return 1 ;;
  esac
}

service_capability_state() {
  case "$1" in
    PBRun|PBCluster|PBMonitorAgent) printf '%s\n' enabled ;;
    PBCoinData)
      local active="${PBGUI_CREDENTIAL_ACTIVE:-}"
      active="${active,,}"
      case "$active" in
        1|true|yes|on) printf '%s\n' enabled ;;
        0|false|no|off) printf '%s\n' disabled ;;
        *) printf '%s\n' unknown ;;
      esac
      ;;
    *) printf '%s\n' disabled ;;
  esac
}

disable_optional_service() {
  local service="$1"
  local unit
  unit="$(unit_for "$service")"

  echo "Disabling $service: credential capability is inactive"
  if command -v systemctl >/dev/null 2>&1 && [[ -f "$target_home/.config/systemd/user/$unit" ]]; then
    run_as_service_user systemctl --user daemon-reload >/dev/null 2>&1 || true
    run_as_service_user systemctl --user stop "$unit" >/dev/null 2>&1 || true
    run_as_service_user systemctl --user disable "$unit" >/dev/null 2>&1 || true
    run_as_service_user systemctl --user reset-failed "$unit" >/dev/null 2>&1 || true
    rm -f "$target_home/.config/systemd/user/default.target.wants/$unit"
    run_as_service_user systemctl --user daemon-reload >/dev/null 2>&1 || true
  fi

  if [[ -x "$python_bin" && -f "$pbgui_dir/starter.py" ]]; then
    (cd "$pbgui_dir" && run_as_service_user "$python_bin" "$pbgui_dir/starter.py" -k "$service") >/dev/null 2>&1 || true
  fi
}

flag_for() {
  case "$action" in
    start) printf '%s\n' '-s' ;;
    stop) printf '%s\n' '-k' ;;
    restart) printf '%s\n' '-r' ;;
  esac
}

service_is_running() {
  local service="$1"
  local unit
  local script
  unit="$(unit_for "$service")"
  if command -v systemctl >/dev/null 2>&1 && [[ -f "$target_home/.config/systemd/user/$unit" ]]; then
    run_as_service_user systemctl --user is-active --quiet "$unit" >/dev/null 2>&1
    return
  fi
  script="$(script_for "$service")"
  pgrep -f "$pbgui_dir/$script" >/dev/null 2>&1
}

requested_services=("$@")
active_services=()
for service in "${requested_services[@]}"; do
  unit_for "$service" >/dev/null
  capability_state="$(service_capability_state "$service")"
  if [[ "$service" == "PBCoinData" && "$capability_state" == "unknown" ]]; then
    if [[ "$action" == "restart" ]] && service_is_running "$service"; then
      echo "Restarting active $service while credential capability is unknown"
    else
      echo "Leaving $service unchanged: credential capability is unknown"
      continue
    fi
  fi
  if [[ "$action" != "stop" ]]; then
    if [[ "$capability_state" == "disabled" ]]; then
      disable_optional_service "$service"
      continue
    fi
  fi
  active_services+=("$service")
done

if [[ ${#active_services[@]} -eq 0 ]]; then
  echo "No configured services requested."
  exit 0
fi

units=()
for service in "${active_services[@]}"; do
  units+=("$(unit_for "$service")")
done

can_use_systemd() {
  command -v systemctl >/dev/null 2>&1 || return 1
  run_as_service_user systemctl --user show-environment >/dev/null 2>&1 || return 1
  for unit in "${units[@]}"; do
    [[ -f "$target_home/.config/systemd/user/$unit" ]] || return 1
  done
}

if can_use_systemd; then
  echo "Using systemd user units: ${units[*]}"
  run_as_service_user systemctl --user "$action" "${units[@]}"
  if [[ "$action" != "stop" ]]; then
    for unit in "${units[@]}"; do
      run_as_service_user systemctl --user is-active "$unit" >/dev/null
    done
  fi
  exit 0
fi

for service in "${active_services[@]}"; do
  if [[ "$service" == "PBMonitorAgent" ]]; then
    echo "PBMonitorAgent requires pbgui-monitor-agent.service; legacy starter.py fallback is not supported." >&2
    exit 1
  fi
done

echo "Using legacy starter.py fallback for: ${active_services[*]}"
if [[ ! -x "$python_bin" ]]; then
  printf 'PBGui Python not executable: %s\n' "$python_bin" >&2
  exit 1
fi
if [[ ! -f "$pbgui_dir/starter.py" ]]; then
  printf 'starter.py not found: %s\n' "$pbgui_dir/starter.py" >&2
  exit 1
fi

cd "$pbgui_dir"
run_as_service_user "$python_bin" "$pbgui_dir/starter.py" "$(flag_for)" "${active_services[@]}"

if [[ "$action" != "stop" ]]; then
  missing=0
  for service in "${active_services[@]}"; do
    script="$(script_for "$service")"
    if ! pgrep -f "$pbgui_dir/$script" >/dev/null; then
      echo "Missing process: $script"
      missing=1
    fi
  done
  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
fi
