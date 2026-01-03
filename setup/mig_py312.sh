#!/usr/bin/env bash
set -euo pipefail

# Migrate Streamlit PBGui from Python 3.10 venv to Python 3.12 venv
# - Stops streamlit
# - Renames existing venv_pbgui -> venv_pbgui310 (or venv_pbgui310_<timestamp>)
# - Creates/updates symlink venv_pbgui -> venv_pbgui312
# - Restarts streamlit
#
# Assumptions (defaults):
# - This script lives in: <install_dir>/pbgui/setup/mig_py312.sh
# - install_dir is typically: ~/software
# - venvs are typically: ~/software/venv_pbgui and ~/software/venv_pbgui312
#
# You can override via env vars:
#   PBGUI_DIR, VENV_PBGUI, VENV_PBGUI312, STREAMLIT_SCRIPT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PBGUI_DIR_DEFAULT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASE_DIR_DEFAULT="$(cd "${PBGUI_DIR_DEFAULT}/.." && pwd)"

VENV_PBGUI_DEFAULT="${BASE_DIR_DEFAULT}/venv_pbgui"
VENV_PBGUI312_DEFAULT="${BASE_DIR_DEFAULT}/venv_pbgui312"

PBGUI_DIR="${PBGUI_DIR:-$PBGUI_DIR_DEFAULT}"
VENV_PBGUI="${VENV_PBGUI:-$VENV_PBGUI_DEFAULT}"
VENV_PBGUI312="${VENV_PBGUI312:-$VENV_PBGUI312_DEFAULT}"
STREAMLIT_SCRIPT="${STREAMLIT_SCRIPT:-pbgui.py}"

log() { printf '[%s] %s\n' "$(date +'%F %T')" "$*"; }

if [[ ! -d "$PBGUI_DIR" ]]; then
  log "ERROR: PBGUI_DIR not found: $PBGUI_DIR"
  exit 1
fi

if [[ ! -d "$VENV_PBGUI312" ]]; then
  log "ERROR: 3.12 venv not found: $VENV_PBGUI312"
  log "Hint: run the playbook master-pbgui-python312.yml first."
  exit 1
fi

cd "$PBGUI_DIR"

service_ctl_py="$PBGUI_DIR/service_ctl.py"
services_to_restart=()

# --- Detect + stop PBGui services (only those currently running) ---
if [[ -f "$service_ctl_py" && -x "$VENV_PBGUI/bin/python" ]]; then
  mapfile -t services_to_restart < <("$VENV_PBGUI/bin/python" "$service_ctl_py" status --format=lines || true)
  if (( ${#services_to_restart[@]} > 0 )); then
    log "Stopping PBGui services: ${services_to_restart[*]}"
    "$VENV_PBGUI/bin/python" "$service_ctl_py" stop "${services_to_restart[@]}" || true
  else
    log "No PBGui services running; nothing to stop."
  fi
else
  log "NOTE: Skipping service stop (missing service_ctl.py or $VENV_PBGUI/bin/python)"
fi

# --- Stop streamlit ---
log "Stopping streamlit (if running)..."
if pgrep -f "streamlit run $STREAMLIT_SCRIPT" >/dev/null 2>&1; then
  pkill -f "streamlit run $STREAMLIT_SCRIPT" || true
  for _ in {1..20}; do
    if ! pgrep -f "streamlit run $STREAMLIT_SCRIPT" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
fi

# --- Rename old venv_pbgui (py3.10) if it exists and is not already a symlink ---
if [[ -L "$VENV_PBGUI" ]]; then
  log "NOTE: $VENV_PBGUI is already a symlink. Leaving it as-is for now."
elif [[ -d "$VENV_PBGUI" ]]; then
  venv_dir="$(dirname "$VENV_PBGUI")"
  target="$venv_dir/venv_pbgui310"
  if [[ -e "$target" ]]; then
    ts="$(date +'%Y%m%d_%H%M%S')"
    target="${target}_$ts"
  fi
  log "Renaming $VENV_PBGUI -> $target"
  mv "$VENV_PBGUI" "$target"
else
  log "NOTE: No existing venv to rename at $VENV_PBGUI"
fi

# --- Create symlink venv_pbgui -> venv_pbgui312 ---
log "Linking $VENV_PBGUI -> $VENV_PBGUI312"
ln -sfn "$VENV_PBGUI312" "$VENV_PBGUI"

# --- Start PBGui services using the (now-linked) venv_pbgui (only those previously running) ---
if [[ -f "$service_ctl_py" && -x "$VENV_PBGUI/bin/python" ]]; then
  if (( ${#services_to_restart[@]} > 0 )); then
    log "Starting PBGui services: ${services_to_restart[*]}"
    "$VENV_PBGUI/bin/python" "$service_ctl_py" start "${services_to_restart[@]}" || true
  else
    log "No PBGui services were running before; nothing to start."
  fi
else
  log "NOTE: Skipping service start (missing service_ctl.py or $VENV_PBGUI/bin/python)"
fi

# --- Start streamlit using the (now-linked) venv_pbgui ---
log "Starting streamlit..."
source "$VENV_PBGUI/bin/activate"
nohup streamlit run "$STREAMLIT_SCRIPT" >/tmp/pbgui_streamlit.log 2>&1 &
log "Done. Log: /tmp/pbgui_streamlit.log"
