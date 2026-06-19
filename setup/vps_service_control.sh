#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: vps_service_control.sh start|stop|restart PBRun [PBCluster] [PBCoinData]

Controls VPS PBGui services. Uses systemd user units when all requested units
exist and the user manager is available; otherwise falls back to starter.py.

Environment:
  PBGUI_DIR      Remote PBGui directory. Default: current working directory.
  PBGUI_PYTHON   PBGui virtualenv Python. Default: ../venv_pbgui/bin/python.
  PBGUI_USER     Target service user when executed with become/root.
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
config_python="$python_bin"
if [[ ! -x "$config_python" ]]; then
  config_python="$(command -v python3 || true)"
fi
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
    *) return 1 ;;
  esac
}

ini_value() {
  local section="$1"
  local option="$2"
  local ini_path="$pbgui_dir/pbgui.ini"
  [[ -r "$ini_path" && -n "$config_python" ]] || return 0
  "$config_python" - "$ini_path" "$section" "$option" <<'PY'
import configparser
import sys

cfg = configparser.ConfigParser()
cfg.read(sys.argv[1])
print(cfg.get(sys.argv[2], sys.argv[3], fallback=''))
PY
}

configured_value() {
  local value="$1"
  value="${value//$'\r'/}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  [[ -n "$value" ]] || return 1
  local lowered="${value,,}"
  case "$lowered" in
    none|null|false|'<api_key>') return 1 ;;
  esac
  if [[ "$value" == '<'*'>' ]]; then
    return 1
  fi
  return 0
}

service_configured() {
  case "$1" in
    PBRun) return 0 ;;
    PBCluster) return 0 ;;
    PBCoinData) configured_value "$(ini_value coinmarketcap api_key)" ;;
    *) return 1 ;;
  esac
}

disable_optional_service() {
  local service="$1"
  local unit
  unit="$(unit_for "$service")"

  echo "Skipping $service: required configuration is missing in pbgui.ini"
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

requested_services=("$@")
active_services=()
for service in "${requested_services[@]}"; do
  unit_for "$service" >/dev/null
  if [[ "$action" != "stop" ]] && ! service_configured "$service"; then
    disable_optional_service "$service"
    continue
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
