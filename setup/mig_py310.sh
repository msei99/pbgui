#!/usr/bin/env bash
set -euo pipefail

# Roll back Streamlit PBGui from Python 3.12 venv back to Python 3.10 venv
# - Stops streamlit
# - Points venv_pbgui symlink back to venv_pbgui310 (or latest venv_pbgui310_<timestamp>)
# - Restarts streamlit
#
# Assumptions (defaults):
# - This script lives in: <install_dir>/pbgui/setup/mig_py310.sh
# - install_dir is typically: ~/software
# - venvs are typically: ~/software/venv_pbgui (symlink) and ~/software/venv_pbgui310
#
# You can override via env vars:
#   PBGUI_DIR, VENV_PBGUI, STREAMLIT_SCRIPT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PBGUI_DIR_DEFAULT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASE_DIR_DEFAULT="$(cd "${PBGUI_DIR_DEFAULT}/.." && pwd)"

VENV_PBGUI_DEFAULT="${BASE_DIR_DEFAULT}/venv_pbgui"

PBGUI_DIR="${PBGUI_DIR:-$PBGUI_DIR_DEFAULT}"
VENV_PBGUI="${VENV_PBGUI:-$VENV_PBGUI_DEFAULT}"
STREAMLIT_SCRIPT="${STREAMLIT_SCRIPT:-pbgui.py}"

log() { printf '[%s] %s\n' "$(date +'%F %T')" "$*"; }

if [[ ! -d "$PBGUI_DIR" ]]; then
  log "ERROR: PBGUI_DIR not found: $PBGUI_DIR"
  exit 1
fi

venv_dir="$(dirname "$VENV_PBGUI")"

# Prefer exact venv_pbgui310, else newest venv_pbgui310_* in same directory
venv310_candidate="${venv_dir}/venv_pbgui310"
if [[ ! -d "$venv310_candidate" ]]; then
  newest="$(ls -1dt "${venv_dir}/venv_pbgui310_"* 2>/dev/null | head -n 1 || true)"
  if [[ -n "$newest" && -d "$newest" ]]; then
    venv310_candidate="$newest"
  fi
fi

if [[ ! -d "$venv310_candidate" ]]; then
  log "ERROR: Could not find a 3.10 venv to switch to. Looked for:"
  log "  - ${venv_dir}/venv_pbgui310"
  log "  - ${venv_dir}/venv_pbgui310_*"
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

# --- Point venv_pbgui back to 3.10 venv ---
log "Linking $VENV_PBGUI -> $venv310_candidate"
ln -sfn "$venv310_candidate" "$VENV_PBGUI"

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
