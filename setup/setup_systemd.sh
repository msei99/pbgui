#!/usr/bin/env bash
set -euo pipefail

TARGET_USER="${USER:-}"
PBGUI_DIR=""
PYTHON_BIN=""
ENABLE_SERVICES="api,pbcluster,pbrun,pbdata,pbcoindata,monitor-agent"
START_SERVICES=true
DISABLE_EXCLUDED=true
CHANGED=false
UNIT_FILES_CHANGED=false
TEMP_FILES=()

info() { printf '\033[36m[INFO]\033[0m %s\n' "$*"; }
success() { printf '\033[32m[ OK ]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[ERR ]\033[0m %s\n' "$*" >&2; }

cleanup_temp_files() {
  if [[ ${#TEMP_FILES[@]} -gt 0 ]]; then
    rm -f -- "${TEMP_FILES[@]}"
  fi
}
trap cleanup_temp_files EXIT

mark_changed() {
  CHANGED=true
}

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
  --enable LIST               Comma-separated services to enable. Default: api,pbcluster,pbrun,pbdata,pbcoindata,monitor-agent.
  --no-start                  Enable services but do not start/restart them now.
  --no-disable-excluded       Do not stop/disable services missing from --enable.
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
    --no-disable-excluded) DISABLE_EXCLUDED=false; shift ;;
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
TARGET_UID="$(id -u "$TARGET_USER")"
TARGET_GID="$(id -g "$TARGET_USER")"
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

declare -a enabled=()
declare -A allowed_services=(
  [api]=1
  [pbcluster]=1
  [pbrun]=1
  [pbdata]=1
  [pbcoindata]=1
  [monitor-agent]=1
)
IFS=',' read -r -a requested_services <<< "$ENABLE_SERVICES"
for service in "${requested_services[@]}"; do
  service="$(printf '%s' "$service" | tr -d '[:space:]')"
  if [[ -z "$service" || -z "${allowed_services[$service]:-}" ]]; then
    err "Invalid service in --enable: ${service:-<empty>}"
    exit 2
  fi
  enabled+=("$service")
done
if [[ ${#enabled[@]} -eq 0 ]]; then
  err "--enable must contain at least one service."
  exit 2
fi

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
  local exec_start_pre="${4:-}"
  local unit_path="$unit_dir/$unit_name"
  local temp_path
  temp_path="$(mktemp "$unit_dir/.${unit_name}.tmp.XXXXXX")"
  TEMP_FILES+=("$temp_path")
  cat > "$temp_path" <<EOF
[Unit]
Description=$description
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$PBGUI_DIR
$exec_start_pre
ExecStart=$PYTHON_BIN -u $PBGUI_DIR/$script_name
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30
Environment=PYTHONUNBUFFERED=1
LimitNOFILE=65536

[Install]
WantedBy=default.target
EOF
  chmod 0644 "$temp_path"
  if [[ "$RUNNING_AS_ROOT" == true ]]; then
    chown "$TARGET_USER:$TARGET_GROUP" "$temp_path"
  fi
  if [[ -f "$unit_path" && ! -L "$unit_path" ]] && cmp -s "$temp_path" "$unit_path"; then
    rm -f -- "$temp_path"
    unit_changed["$unit_name"]=false
    metadata_changed=false
    if [[ "$(stat -c '%u:%g' -- "$unit_path")" != "$TARGET_UID:$TARGET_GID" ]]; then
      if [[ "$RUNNING_AS_ROOT" != true ]]; then
        err "$unit_path has incorrect ownership and root privileges are required to repair it."
        exit 1
      fi
      chown "$TARGET_USER:$TARGET_GROUP" "$unit_path"
      metadata_changed=true
    fi
    if [[ "$(stat -c '%a' -- "$unit_path")" != "644" ]]; then
      chmod 0644 "$unit_path"
      metadata_changed=true
    fi
    if [[ "$metadata_changed" == true ]]; then
      UNIT_FILES_CHANGED=true
      mark_changed
      success "Repaired metadata for $unit_name"
    fi
    return
  fi
  if [[ -d "$unit_path" && ! -L "$unit_path" ]]; then
    err "Refusing to replace directory at managed unit path: $unit_path"
    exit 1
  fi
  mv -f -- "$temp_path" "$unit_path"
  unit_changed["$unit_name"]=true
  UNIT_FILES_CHANGED=true
  mark_changed
  success "Updated $unit_name"
}

declare -A unit_changed
write_unit "pbgui-api.service" "PBGui API Server" "PBApiServer.py" "ExecStartPre=/bin/bash $PBGUI_DIR/setup/stop_legacy_api.sh --pbgui-dir $PBGUI_DIR"
write_unit "pbgui-pbcluster.service" "PBGui PBCluster Service" "PBCluster.py"
write_unit "pbgui-pbrun.service" "PBGui PBRun Service" "PBRun.py"
write_unit "pbgui-pbdata.service" "PBGui PBData Service" "PBData.py"
write_unit "pbgui-pbcoindata.service" "PBGui PBCoinData Service" "PBCoinData.py"
write_unit "pbgui-monitor-agent.service" "PBGui Monitor Agent" "monitor_agent.py"

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

show_unit_diagnostics() {
  local unit="$1"
  run_user_systemctl show "$unit" --no-pager \
    --property=LoadState,UnitFileState,ActiveState,SubState,Result,ExecMainStatus,NRestarts,FragmentPath,NeedDaemonReload >&2 || true
  run_user_systemctl status "$unit" --no-pager -l >&2 || true
}

run_unit_action() {
  local action="$1"
  local unit="$2"
  if run_user_systemctl "$action" "$unit" >/dev/null; then
    return 0
  fi
  err "systemctl --user $action failed for $unit."
  show_unit_diagnostics "$unit"
  return 1
}

show_unit_property() {
  local unit="$1"
  local property="$2"
  run_user_systemctl show "$unit" --property="$property" --value 2>/dev/null || true
}

service_requested() {
  local wanted="$1"
  local service
  for service in "${enabled[@]}"; do
    service="$(printf '%s' "$service" | tr -d '[:space:]')"
    [[ "$service" == "$wanted" ]] && return 0
  done
  return 1
}

disable_service_if_excluded() {
  local service="$1"
  local unit="pbgui-$service.service"
  local enabled_state
  local active_state
  if service_requested "$service"; then
    return 0
  fi
  if [[ ! -f "$unit_dir/$unit" ]]; then
    return 0
  fi
  enabled_state="$(run_user_systemctl is-enabled "$unit" 2>/dev/null || true)"
  case "$enabled_state" in
    enabled|enabled-runtime)
      run_unit_action disable "$unit" || exit 1
      mark_changed
      success "Disabled $unit"
      ;;
  esac
  if [[ "$START_SERVICES" == true ]]; then
    active_state="$(run_user_systemctl show "$unit" --property=ActiveState --value 2>/dev/null || true)"
    case "$active_state" in
      active|activating|reloading|deactivating)
        run_unit_action stop "$unit" || exit 1
        mark_changed
        success "Stopped excluded $unit"
        ;;
    esac
  fi
}

remove_obsolete_unit() {
  local unit="$1"
  local enabled_state
  local active_state
  enabled_state="$(run_user_systemctl is-enabled "$unit" 2>/dev/null || true)"
  case "$enabled_state" in
    enabled|enabled-runtime)
      run_unit_action disable "$unit" || exit 1
      mark_changed
      ;;
  esac
  if [[ "$START_SERVICES" == true ]]; then
    active_state="$(run_user_systemctl show "$unit" --property=ActiveState --value 2>/dev/null || true)"
    case "$active_state" in
      active|activating|reloading|deactivating)
        run_unit_action stop "$unit" || exit 1
        mark_changed
        ;;
    esac
  fi
  if [[ -e "$wants_dir/$unit" || -L "$wants_dir/$unit" || -e "$unit_dir/$unit" ]]; then
    rm -f "$wants_dir/$unit" "$unit_dir/$unit"
    UNIT_FILES_CHANGED=true
    mark_changed
    success "Removed obsolete $unit"
  fi
}

remove_obsolete_unit "pbgui-pbremote.service"

reload_needed="$UNIT_FILES_CHANGED"
if [[ "$reload_needed" == false ]]; then
  need_daemon_reload="$(run_user_systemctl show --property=NeedDaemonReload --value 2>/dev/null || true)"
  if [[ "$need_daemon_reload" == yes ]]; then
    reload_needed=true
  fi
fi
if [[ "$reload_needed" == true ]]; then
  if ! run_user_systemctl daemon-reload; then
    err "systemctl --user daemon-reload failed."
    run_user_systemctl show --no-pager --property=NeedDaemonReload >&2 || true
    run_user_systemctl status --no-pager -l >&2 || true
    exit 1
  fi
  mark_changed
  success "Reloaded the systemd user manager"
fi

if [[ "$DISABLE_EXCLUDED" == true ]]; then
  for managed_service in api pbcluster pbrun pbdata pbcoindata monitor-agent; do
    disable_service_if_excluded "$managed_service"
  done
fi

verification_failed=false
for service in "${enabled[@]}"; do
  service="$(printf '%s' "$service" | tr -d '[:space:]')"
  [[ -z "$service" ]] && continue
  unit="pbgui-$service.service"
  if [[ ! -f "$unit_dir/$unit" ]]; then
    warn "Skipping unknown service unit: $unit"
    continue
  fi
  enabled_state="$(run_user_systemctl is-enabled "$unit" 2>/dev/null || true)"
  case "$enabled_state" in
    enabled|enabled-runtime) ;;
    *)
      run_unit_action enable "$unit" || exit 1
      mark_changed
      success "Enabled $unit"
      ;;
  esac
  if [[ "$START_SERVICES" == true ]]; then
    active_state="$(run_user_systemctl show "$unit" --property=ActiveState --value 2>/dev/null || true)"
    if [[ "${unit_changed[$unit]:-false}" == true ]]; then
      if [[ "$active_state" == failed ]]; then
        run_unit_action reset-failed "$unit" || exit 1
      fi
      run_unit_action restart "$unit" || exit 1
      mark_changed
      success "Restarted changed $unit"
    elif [[ "$active_state" != active ]]; then
      run_unit_action reset-failed "$unit" || exit 1
      run_unit_action start "$unit" || exit 1
      mark_changed
      success "Started inactive $unit"
    fi
  fi
done

if [[ "$START_SERVICES" == true ]]; then
  declare -A restarts_before
  for service in "${enabled[@]}"; do
    unit="pbgui-$service.service"
    [[ -f "$unit_dir/$unit" && ! -L "$unit_dir/$unit" ]] || continue
    restarts_before["$unit"]="$(show_unit_property "$unit" NRestarts)"
    [[ "${restarts_before[$unit]}" =~ ^[0-9]+$ ]] || restarts_before["$unit"]=0
  done
  sleep 12
  for service in "${enabled[@]}"; do
    unit="pbgui-$service.service"
    [[ -f "$unit_dir/$unit" && ! -L "$unit_dir/$unit" ]] || continue
    active_state="$(show_unit_property "$unit" ActiveState)"
    sub_state="$(show_unit_property "$unit" SubState)"
    result_state="$(show_unit_property "$unit" Result)"
    exec_status="$(show_unit_property "$unit" ExecMainStatus)"
    restarts_after="$(show_unit_property "$unit" NRestarts)"
    [[ "$restarts_after" =~ ^[0-9]+$ ]] || restarts_after=0
    printf 'unit=%s active=%s sub=%s result=%s status=%s restarts_before=%s restarts_after=%s\n' \
      "$unit" "$active_state" "$sub_state" "$result_state" "$exec_status" "${restarts_before[$unit]}" "$restarts_after"
    if [[ "$active_state" != active || "$sub_state" != running || "$result_state" != success || "${exec_status:-0}" != 0 || "$restarts_after" -gt "${restarts_before[$unit]}" ]]; then
      err "$unit failed the 12-second stability check."
      show_unit_diagnostics "$unit"
      verification_failed=true
    fi
  done
fi

if [[ "$verification_failed" == false ]]; then
  success "PBGui systemd user services installed for $TARGET_USER."
fi
printf 'changed=%s\n' "$CHANGED"
if [[ "$verification_failed" == true ]]; then
  exit 1
fi
