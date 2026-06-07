#!/usr/bin/env bash
set -euo pipefail

TARGET_USER="${USER:-}"
PBGUI_DIR=""
PYTHON_BIN=""
ENABLE_SERVICES="api,pbrun,pbdata,pbcoindata"
START_SERVICES=true
INSTALL_PBREMOTE=false

info() { printf '\033[36m[INFO]\033[0m %s\n' "$*"; }
success() { printf '\033[32m[ OK ]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[ERR ]\033[0m %s\n' "$*" >&2; }

validate_unit_path() {
  local label="$1"
  local value="$2"
  if [[ -z "$value" ]]; then
    err "$label is required."
    exit 1
  fi
  if [[ "$value" == *$'\n'* || "$value" == *$'\r'* || "$value" == *'{{'* || "$value" == *'}}'* ]]; then
    err "$label contains invalid characters."
    exit 1
  fi
  if [[ ! "$value" =~ ^[A-Za-z0-9._~/-]+$ ]]; then
    err "$label may only contain letters, numbers, '/', '.', '_', '-' and '~'."
    exit 1
  fi
  IFS='/' read -r -a _parts <<< "$value"
  for _part in "${_parts[@]}"; do
    if [[ "$_part" == "." || "$_part" == ".." ]]; then
      err "$label cannot contain '.' or '..' path segments."
      exit 1
    fi
  done
}

usage() {
  cat <<'EOF'
Usage: setup/setup_systemd.sh [options]

Install PBGui systemd user services.

Options:
  --user USER                 Target Linux user. Default: current user.
  --pbgui-dir PATH            PBGui directory. Default: current directory.
  --python PATH               PBGui venv Python. Default: ../venv_pbgui/bin/python.
  --enable LIST               Comma-separated services to enable. Default: api,pbrun,pbdata,pbcoindata.
  --no-start                  Enable services but do not start/restart them now.
  --include-pbremote          Also install PBRemote unit template.
  -h, --help                  Show help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) TARGET_USER="$2"; shift 2 ;;
    --pbgui-dir) PBGUI_DIR="$2"; shift 2 ;;
    --python) PYTHON_BIN="$2"; shift 2 ;;
    --enable) ENABLE_SERVICES="$2"; shift 2 ;;
    --no-start) START_SERVICES=false; shift ;;
    --include-pbremote) INSTALL_PBREMOTE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) err "Unknown option: $1"; usage; exit 2 ;;
  esac
done

if [[ -z "$TARGET_USER" ]]; then
  err "Could not determine target user."
  exit 1
fi

if ! id "$TARGET_USER" >/dev/null 2>&1; then
  err "User does not exist: $TARGET_USER"
  exit 1
fi

TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6)"
if [[ -z "$TARGET_HOME" || ! -d "$TARGET_HOME" ]]; then
  err "Could not determine home directory for $TARGET_USER."
  exit 1
fi
TARGET_GROUP="$(id -gn "$TARGET_USER")"
RUNNING_AS_ROOT=false
if [[ "$(id -u)" -eq 0 ]]; then
  RUNNING_AS_ROOT=true
fi

if [[ -z "$PBGUI_DIR" ]]; then
  PBGUI_DIR="$(pwd)"
fi
PBGUI_DIR="$(realpath "$PBGUI_DIR")"
validate_unit_path "PBGui directory" "$PBGUI_DIR"

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$(dirname "$PBGUI_DIR")/venv_pbgui/bin/python" ]]; then
    PYTHON_BIN="$(dirname "$PBGUI_DIR")/venv_pbgui/bin/python"
  elif [[ -x "$(dirname "$PBGUI_DIR")/venv/bin/python" ]]; then
    PYTHON_BIN="$(dirname "$PBGUI_DIR")/venv/bin/python"
  else
    PYTHON_BIN="$(command -v python3)"
  fi
fi
if [[ "$PYTHON_BIN" != /* ]]; then
  PYTHON_BIN="$(pwd)/$PYTHON_BIN"
fi
PYTHON_BIN="$(cd "$(dirname "$PYTHON_BIN")" && pwd)/$(basename "$PYTHON_BIN")"
validate_unit_path "Python executable" "$PYTHON_BIN"

unit_dir="$TARGET_HOME/.config/systemd/user"
wants_dir="$unit_dir/default.target.wants"
mkdir -p "$unit_dir" "$wants_dir"
if [[ "$RUNNING_AS_ROOT" == true ]]; then
  chown "$TARGET_USER:$TARGET_GROUP" "$TARGET_HOME/.config" "$TARGET_HOME/.config/systemd" "$unit_dir" "$wants_dir"
fi

write_unit() {
  local unit_name="$1"
  local description="$2"
  local script_name="$3"
  local unit_path="$unit_dir/$unit_name"
  cat > "$unit_path" <<EOF
[Unit]
Description=$description
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$PBGUI_DIR
ExecStart=$PYTHON_BIN -u $PBGUI_DIR/$script_name
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=default.target
EOF
  if [[ "$RUNNING_AS_ROOT" == true ]]; then
    chown "$TARGET_USER:$TARGET_GROUP" "$unit_path"
  fi
}

write_unit "pbgui-api.service" "PBGui API Server" "PBApiServer.py"
write_unit "pbgui-pbrun.service" "PBGui PBRun Service" "PBRun.py"
write_unit "pbgui-pbdata.service" "PBGui PBData Service" "PBData.py"
write_unit "pbgui-pbcoindata.service" "PBGui PBCoinData Service" "PBCoinData.py"
if [[ "$INSTALL_PBREMOTE" == true || "$ENABLE_SERVICES" == *pbremote* ]]; then
  write_unit "pbgui-pbremote.service" "PBGui PBRemote Service" "PBRemote.py"
fi

if [[ "$RUNNING_AS_ROOT" == true ]]; then
  loginctl enable-linger "$TARGET_USER" >/dev/null 2>&1 || warn "Could not enable linger for $TARGET_USER."
  uid="$(id -u "$TARGET_USER")"
  systemctl start "user@$uid.service" >/dev/null 2>&1 || true
  run_user_systemctl() {
    sudo -H -u "$TARGET_USER" env XDG_RUNTIME_DIR="/run/user/$uid" systemctl --user "$@"
  }
else
  uid="$(id -u "$TARGET_USER")"
  run_user_systemctl() {
    env XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$uid}" systemctl --user "$@"
  }
fi

IFS=',' read -r -a enabled <<< "$ENABLE_SERVICES"

service_requested() {
  local wanted="$1"
  local service
  for service in "${enabled[@]}"; do
    service="$(printf '%s' "$service" | tr -d '[:space:]')"
    [[ "$service" == "$wanted" ]] && return 0
  done
  return 1
}

disable_optional_if_excluded() {
  local service="$1"
  local unit="pbgui-$service.service"
  if service_requested "$service"; then
    return 0
  fi
  rm -f "$wants_dir/$unit"
  if [[ -f "$unit_dir/$unit" ]]; then
    run_user_systemctl stop "$unit" >/dev/null 2>&1 || true
    run_user_systemctl disable "$unit" >/dev/null 2>&1 || true
    success "Disabled optional $unit"
  fi
}

run_user_systemctl daemon-reload
disable_optional_if_excluded pbremote
disable_optional_if_excluded pbcoindata

for service in "${enabled[@]}"; do
  service="$(printf '%s' "$service" | tr -d '[:space:]')"
  [[ -z "$service" ]] && continue
  unit="pbgui-$service.service"
  if [[ ! -f "$unit_dir/$unit" ]]; then
    warn "Skipping unknown service unit: $unit"
    continue
  fi
  rm -f "$wants_dir/$unit"
  run_user_systemctl enable "$unit_dir/$unit" >/dev/null
  if [[ "$START_SERVICES" == true ]]; then
    run_user_systemctl restart "$unit"
  fi
  success "Enabled $unit"
done

success "PBGui systemd user services installed for $TARGET_USER."
