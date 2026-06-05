#!/usr/bin/env bash
set -euo pipefail

REPO_ARCHIVE_URL="${PBGUI_INSTALLER_ARCHIVE_URL:-https://github.com/msei99/pbgui/archive/refs/heads/main.tar.gz}"
WORK_DIR="${PBGUI_INSTALLER_WORKDIR:-${TMPDIR:-/tmp}/pbgui-master-installer}"
PYTHON_BIN="${PYTHON:-python3}"

info() { printf '\033[36m[INFO]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[ERR ]\033[0m %s\n' "$*" >&2; }

script_dir=""
if [[ "${BASH_SOURCE[0]}" != /dev/fd/* && "${BASH_SOURCE[0]}" != /proc/* ]]; then
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

if [[ -n "$script_dir" && -f "$script_dir/installer/master_installer.py" ]]; then
  source_dir="$(cd "$script_dir/.." && pwd)"
else
  if ! command -v curl >/dev/null 2>&1; then
    err "curl is required to download the PBGui installer."
    exit 1
  fi
  if ! command -v tar >/dev/null 2>&1; then
    err "tar is required to unpack the PBGui installer."
    exit 1
  fi
  rm -rf "$WORK_DIR"
  mkdir -p "$WORK_DIR"
  info "Downloading PBGui installer..."
  curl -fsSL "$REPO_ARCHIVE_URL" | tar -xz --strip-components=1 -C "$WORK_DIR"
  source_dir="$WORK_DIR"
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  err "python3 is required to run the PBGui installer."
  exit 1
fi

venv_dir="$WORK_DIR/.venv"
mkdir -p "$WORK_DIR"
if [[ ! -x "$venv_dir/bin/python" ]]; then
  info "Preparing installer virtualenv..."
  if ! "$PYTHON_BIN" -m venv "$venv_dir"; then
    err "Failed to create a Python virtualenv. Install python3-venv and retry."
    exit 1
  fi
  "$venv_dir/bin/python" -m pip install --upgrade pip >/dev/null
  "$venv_dir/bin/python" -m pip install paramiko >/dev/null
fi

installer="$source_dir/setup/installer/master_installer.py"
if [[ ! -f "$installer" ]]; then
  err "Installer entry point not found: $installer"
  exit 1
fi

info "Starting PBGui master installer..."
exec "$venv_dir/bin/python" "$installer" "$@"
