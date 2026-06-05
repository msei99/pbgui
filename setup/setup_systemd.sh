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

if [[ -z "$PBGUI_DIR" ]]; then
  PBGUI_DIR="$(pwd)"
fi
PBGUI_DIR="$(realpath "$PBGUI_DIR")"

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

unit_dir="$TARGET_HOME/.config/systemd/user"
wants_dir="$unit_dir/default.target.wants"
mkdir -p "$unit_dir" "$wants_dir"
chown "$TARGET_USER:$TARGET_GROUP" "$TARGET_HOME/.config" "$TARGET_HOME/.config/systemd" "$unit_dir" "$wants_dir"

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
  chown "$TARGET_USER:$TARGET_GROUP" "$unit_path"
}

write_unit "pbgui-api.service" "PBGui API Server" "PBApiServer.py"
write_unit "pbgui-pbrun.service" "PBGui PBRun Service" "PBRun.py"
write_unit "pbgui-pbdata.service" "PBGui PBData Service" "PBData.py"
write_unit "pbgui-pbcoindata.service" "PBGui PBCoinData Service" "PBCoinData.py"
if [[ "$INSTALL_PBREMOTE" == true || "$ENABLE_SERVICES" == *pbremote* ]]; then
  write_unit "pbgui-pbremote.service" "PBGui PBRemote Service" "PBRemote.py"
fi

if [[ "$(id -u)" -eq 0 ]]; then
  loginctl enable-linger "$TARGET_USER" >/dev/null 2>&1 || warn "Could not enable linger for $TARGET_USER."
  uid="$(id -u "$TARGET_USER")"
  systemctl start "user@$uid.service" >/dev/null 2>&1 || true
  run_user_systemctl() {
    sudo -H -u "$TARGET_USER" env XDG_RUNTIME_DIR="/run/user/$uid" systemctl --user "$@"
  }
else
  run_user_systemctl() {
    systemctl --user "$@"
  }
fi

run_user_systemctl daemon-reload

IFS=',' read -r -a enabled <<< "$ENABLE_SERVICES"
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
