#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: vps_service_control.sh start|stop|restart PBRun [PBRemote] [PBCoinData]

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
    PBRemote) printf '%s\n' 'pbgui-pbremote.service' ;;
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
    PBRemote) printf '%s\n' 'PBRemote.py' ;;
    PBCoinData) printf '%s\n' 'PBCoinData.py' ;;
    *) return 1 ;;
  esac
}

flag_for() {
  case "$action" in
    start) printf '%s\n' '-s' ;;
    stop) printf '%s\n' '-k' ;;
    restart) printf '%s\n' '-r' ;;
  esac
}

units=()
for service in "$@"; do
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

echo "Using legacy starter.py fallback for: $*"
if [[ ! -x "$python_bin" ]]; then
  printf 'PBGui Python not executable: %s\n' "$python_bin" >&2
  exit 1
fi
if [[ ! -f "$pbgui_dir/starter.py" ]]; then
  printf 'starter.py not found: %s\n' "$pbgui_dir/starter.py" >&2
  exit 1
fi

cd "$pbgui_dir"
run_as_service_user "$python_bin" "$pbgui_dir/starter.py" "$(flag_for)" "$@"

if [[ "$action" != "stop" ]]; then
  missing=0
  for service in "$@"; do
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
