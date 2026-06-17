#!/usr/bin/env bash

set -uo pipefail

changed=0
dry_run=0
freed_bytes=0
log_max_bytes=$((64 * 1024))
home_dir="${HOME:-}"
vps_cleanup_default_max_bytes=$((64 * 1024))

if [ "${1:-}" = "--dry-run" ]; then
    dry_run=1
fi

exec 3>&1

resolve_script_dir() {
    local source_path="${BASH_SOURCE[0]:-$0}"
    local source_dir

    source_dir="$(dirname "$source_path")"
    if [ -d "$source_dir" ]; then
        (cd "$source_dir" && pwd -P)
        return
    fi

    printf '%s\n' "$PWD"
}

detect_pbgui_dir() {
    local script_dir=""

    if [ -n "${PBGUI_DIR:-}" ] && [ -f "${PBGUI_DIR}/pbgui.ini" ]; then
        printf '%s\n' "$PBGUI_DIR"
        return
    fi

    script_dir="$(resolve_script_dir)"
    if [ -f "${script_dir}/../pbgui.ini" ]; then
        (cd "${script_dir}/.." && pwd -P)
        return
    fi

    if [ -f "${home_dir}/software/pbgui/pbgui.ini" ]; then
        printf '%s\n' "${home_dir}/software/pbgui"
        return
    fi

    printf '%s\n' "${PBGUI_DIR:-${home_dir}/software/pbgui}"
}

read_pb7_dir_from_ini() {
    local ini_path="$1"

    if [ ! -f "$ini_path" ]; then
        return
    fi

    python3 - "$ini_path" <<'PY' 2>/dev/null || true
from configparser import ConfigParser
from pathlib import Path
import sys

ini_path = Path(sys.argv[1])
cfg = ConfigParser()
try:
    cfg.read(ini_path, encoding='utf-8')
except Exception:
    raise SystemExit(0)

value = cfg.get('main', 'pb7dir', fallback='').strip()
if value:
    print(value)
PY
}

read_cleanup_log_max_bytes_from_ini() {
    local ini_path="$1"

    if [ ! -f "$ini_path" ]; then
        return
    fi

    python3 - "$ini_path" <<'PY' 2>/dev/null || true
from configparser import ConfigParser
from pathlib import Path
import sys

ini_path = Path(sys.argv[1])
cfg = ConfigParser()
try:
    cfg.read(ini_path, encoding='utf-8')
except Exception:
    raise SystemExit(0)

value = cfg.get('logging', 'rotate_vps_cleanup_max_bytes', fallback='').strip()
if value:
    try:
        parsed = int(value)
    except Exception:
        parsed = 0
    if parsed > 0:
        print(parsed)
PY
}

read_cluster_role_from_identity() {
    local identity_path="$1"

    if [ ! -f "$identity_path" ]; then
        return
    fi
    python3 - "$identity_path" <<'PY' 2>/dev/null || true
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding='utf-8'))
except Exception:
    raise SystemExit(0)
role = str(payload.get('role') or '').strip().lower()
if role:
    print(role)
PY
}

pbgui_dir="$(detect_pbgui_dir)"
logs_dir="${pbgui_dir}/data/logs"
log_file="${logs_dir}/vps_cleanup.log"
pb7_dir="${PB7_DIR:-}"
configured_cleanup_log_max_bytes="$(read_cleanup_log_max_bytes_from_ini "${pbgui_dir}/pbgui.ini")"
cluster_role="$(read_cluster_role_from_identity "${pbgui_dir}/data/cluster/node_identity.json")"
if [ -n "$configured_cleanup_log_max_bytes" ]; then
    log_max_bytes="$configured_cleanup_log_max_bytes"
else
    log_max_bytes="$vps_cleanup_default_max_bytes"
fi
if [ -z "$pb7_dir" ]; then
    pb7_dir="$(read_pb7_dir_from_ini "${pbgui_dir}/pbgui.ini")"
fi
if [ -z "$pb7_dir" ]; then
    pb7_dir="$(dirname "$pbgui_dir")/pb7"
fi
pb7_rust_target_release_dir="${pb7_dir}/passivbot-rust/target/release"

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    printf '[%s] %s\n' "$(timestamp)" "$*"
}

trim_log_file() {
    local current_size=0
    local trim_size=0

    if [ ! -f "$log_file" ]; then
        return
    fi

    current_size="$(stat -c '%s' -- "$log_file" 2>/dev/null || printf '0')"
    if [ "$current_size" -le "$log_max_bytes" ]; then
        return
    fi

    trim_size=$((log_max_bytes - 1))
    if [ "$trim_size" -lt 1 ]; then
        trim_size=1
    fi

    python3 - "$log_file" "$trim_size" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
limit = max(int(sys.argv[2]), 1)
try:
    data = path.read_bytes()
except OSError:
    raise SystemExit(0)

if len(data) <= limit:
    raise SystemExit(0)

tail = data[-limit:]
newline = tail.find(b"\n")
if newline != -1 and newline + 1 < len(tail):
    tail = tail[newline + 1:]

tmp = path.with_suffix(path.suffix + ".tmp")
tmp.write_bytes(tail)
tmp.replace(path)
PY
}

setup_logging() {
    mkdir -p "$logs_dir"
    touch "$log_file"
    trim_log_file
    exec >> "$log_file" 2>&1
}

setup_logging

action_word() {
    if [ "$dry_run" -eq 1 ]; then
        printf 'Would remove'
    else
        printf 'Removed'
    fi
}

summary_word() {
    if [ "$dry_run" -eq 1 ]; then
        printf 'Would free'
    else
        printf 'Freed'
    fi
}

path_size_bytes() {
    local path="$1"
    local size="0"

    if [ -d "$path" ]; then
        size="$(du -sb -- "$path" 2>/dev/null | { read -r bytes _; printf '%s' "${bytes:-0}"; })"
    elif [ -e "$path" ]; then
        size="$(stat -c '%s' -- "$path" 2>/dev/null || printf '0')"
    fi

    printf '%s' "${size:-0}"
}

format_bytes() {
    local bytes="$1"
    local units=(B KB MB GB TB)
    local unit_index=0
    local whole="$bytes"
    local remainder=0

    while [ "$whole" -ge 1024 ] && [ "$unit_index" -lt 4 ]; do
        remainder=$((whole % 1024))
        whole=$((whole / 1024))
        unit_index=$((unit_index + 1))
    done

    if [ "$unit_index" -eq 0 ]; then
        printf '%s %s' "$whole" "${units[$unit_index]}"
        return
    fi

    printf '%s.%01d %s' "$whole" "$((remainder * 10 / 1024))" "${units[$unit_index]}"
}

remove_tree() {
    local path="$1"
    local label="$2"
    local size_bytes="0"

    if [ -e "$path" ]; then
        size_bytes="$(path_size_bytes "$path")"
        if [ "$dry_run" -eq 0 ]; then
            rm -rf -- "$path"
        fi
        freed_bytes=$((freed_bytes + size_bytes))
        log "$(action_word) ${label}: ${path} ($(format_bytes "$size_bytes"))"
        changed=1
    else
        log "Absent ${label}: ${path}"
    fi
}

remove_file() {
    local path="$1"
    local label="$2"
    local size_bytes="0"

    if [ -f "$path" ]; then
        size_bytes="$(path_size_bytes "$path")"
        if [ "$dry_run" -eq 0 ]; then
            rm -f -- "$path"
        fi
        freed_bytes=$((freed_bytes + size_bytes))
        log "$(action_word) ${label}: ${path} ($(format_bytes "$size_bytes"))"
        changed=1
    else
        log "Absent ${label}: ${path}"
    fi
}

cleanup_stale_bot_runtime() {
    local status_file="${pbgui_dir}/data/cmd/status_v7.json"
    local run_v7_dir="${pbgui_dir}/data/run_v7"
    local pb7_logs_dir="${pb7_dir}/logs"
    local host_name
    local cleanup_payload
    local python_bin=""

    if command -v python3 >/dev/null 2>&1; then
        python_bin="$(command -v python3)"
    elif command -v python >/dev/null 2>&1; then
        python_bin="$(command -v python)"
    else
        log "Skipping stale bot cleanup: no python interpreter available"
        return
    fi

    if [ ! -d "$run_v7_dir" ]; then
        log "Skipping stale bot cleanup: run_v7 directory missing (${run_v7_dir})"
        return
    fi

    host_name="$(hostname -s 2>/dev/null || hostname 2>/dev/null || printf '')"
    cleanup_payload="$($python_bin - "$host_name" "$status_file" "$run_v7_dir" "$pb7_logs_dir" <<'PY'
import json
import os
import re
import subprocess
import sys

host_name, status_file, run_v7_dir, pb7_logs_dir = sys.argv[1:5]

running = set()
try:
    proc = subprocess.run(
        ["ps", "-ef"],
        capture_output=True,
        text=True,
        check=False,
    )
    pattern = re.compile(r"/data/run_v7/([^/]+)/config_run\.json")
    for line in proc.stdout.splitlines():
        match = pattern.search(line)
        if match:
            running.add(match.group(1))
except Exception:
    pass

instances = {}
status_ok = False
try:
    with open(status_file, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    instances = payload.get("instances") or {}
    if isinstance(instances, dict):
        status_ok = True
    else:
        instances = {}
except Exception:
    instances = {}

non_running_bots = []
known_bots = set()
if status_ok and os.path.isdir(run_v7_dir):
    for name in sorted(os.listdir(run_v7_dir)):
        path = os.path.join(run_v7_dir, name)
        if not os.path.isdir(path):
            continue
        known_bots.add(name)
        if name in running:
            continue
        non_running_bots.append(name)

known_bots.update(running)

def extract_log_bot_name(filename):
    direct_name = filename[:-4]
    if direct_name in known_bots:
        return direct_name

    match = re.search(r"_data_run_v7_(.+?)(?:_config_run\.json|_con)?\.log$", filename)
    if match:
        candidate = match.group(1)
        if candidate in known_bots:
            return candidate

    return ""

stale_logs = []
seen_logs = set()

if status_ok and os.path.isdir(pb7_logs_dir):
    for filename in sorted(os.listdir(pb7_logs_dir)):
        full_path = os.path.join(pb7_logs_dir, filename)
        if not os.path.isfile(full_path):
            continue
        if filename == "candlestick_manager.log" or not filename.endswith(".log"):
            continue

        matched_bot = extract_log_bot_name(filename)

        if matched_bot in non_running_bots and full_path not in seen_logs:
            stale_logs.append(full_path)
            seen_logs.add(full_path)

if status_ok and os.path.isdir(run_v7_dir):
    for bot_name in non_running_bots:
        for filename in (
            "passivbot_err.log",
            "passivbot_err.log.old",
            "passivbot.log",
            "passivbot.log.old",
        ):
            full_path = os.path.join(run_v7_dir, bot_name, filename)
            if os.path.isfile(full_path) and full_path not in seen_logs:
                stale_logs.append(full_path)
                seen_logs.add(full_path)

print(json.dumps({
    "status_ok": status_ok,
    "running": sorted(running),
    "non_running_bots": non_running_bots,
    "stale_logs": stale_logs,
}, separators=(",", ":")))
PY
    )"

    if [ -z "$cleanup_payload" ]; then
        log "Skipping stale bot cleanup: empty analysis result"
        return
    fi

    mapfile -t cleanup_lines < <($python_bin - "$cleanup_payload" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
if not payload.get("status_ok"):
    print("STATUS_MISSING")
else:
    for name in payload.get("running") or []:
        print("RUNNING\t" + name)
    for name in payload.get("non_running_bots") or []:
        print("BOT\t" + name)
    for path in payload.get("stale_logs") or []:
        print("LOG\t" + path)
PY
    )

    if [ "${#cleanup_lines[@]}" -eq 0 ]; then
        log "No stale bot runtime data found"
        return
    fi

    if [ "${cleanup_lines[0]}" = "STATUS_MISSING" ]; then
        log "Skipping non-running bot log cleanup: status_v7.json missing or unreadable"
        return
    fi

    local running_count=0
    local stale_bot_count=0
    local stale_log_count=0
    local entry kind value
    for entry in "${cleanup_lines[@]}"; do
        kind="${entry%%$'\t'*}"
        value="${entry#*$'\t'}"
        case "$kind" in
            RUNNING)
                running_count=$((running_count + 1))
                ;;
            BOT)
                stale_bot_count=$((stale_bot_count + 1))
                ;;
            LOG)
                stale_log_count=$((stale_log_count + 1))
                remove_file "$value" "non-running bot log"
                ;;
        esac
    done

    if [ "$running_count" -gt 0 ]; then
        log "Keeping runtime data for ${running_count} running bots"
    fi
    if [ "$stale_bot_count" -gt 0 ]; then
        log "Keeping config directories for ${stale_bot_count} non-running bots"
    fi
    log "Non-running bot cleanup summary: ${stale_bot_count} bot config directories kept, ${stale_log_count} log files removable"
}

if [ "$dry_run" -eq 1 ]; then
    log "Starting VPS cleanup job (dry-run)"
else
    log "Starting VPS cleanup job"
fi
remove_tree "${home_dir}/.cache/pip" "pip cache"
remove_tree "${home_dir}/.rustup/downloads" "rustup downloads"
remove_tree "${home_dir}/.rustup/tmp" "rustup tmp"
remove_tree "${pb7_rust_target_release_dir}" "pb7 rust target release"
remove_tree "${pbgui_dir}/data/instances" "legacy instances directory"
remove_tree "${pbgui_dir}/data/multi" "legacy multi directory"
remove_file "${pbgui_dir}/data/logs/PBRemote.log" "legacy PBRemote log"
remove_file "${pbgui_dir}/data/logs/sync.log" "legacy sync log"
remove_file "${pbgui_dir}/data/logs/PBGui.log" "unexpected VPS PBGui log"
remove_tree "${pbgui_dir}/data/remote" "legacy PBRemote data cache"
remove_tree "${pbgui_dir}/data/state/pbremote" "legacy PBRemote state"
remove_tree "${pbgui_dir}/data/cmd" "legacy PBRun command/status directory"
remove_tree "${pbgui_dir}/cmd" "legacy root command directory"
if [ "$cluster_role" = "vps" ]; then
    remove_tree "${pbgui_dir}/data/backup" "legacy VPS backup directory"
    remove_tree "${pbgui_dir}/data/backups" "legacy VPS backups directory"
else
    log "Skipping backup directory cleanup: cluster role is '${cluster_role:-unknown}', expected vps"
fi
cleanup_stale_bot_runtime
if [ "$dry_run" -eq 1 ]; then
    log "Finished VPS cleanup job (dry-run)"
else
    log "Finished VPS cleanup job"
fi
log "$(summary_word) total disk space: $(format_bytes "$freed_bytes")"
printf 'changed=%s\n' "$changed" >&3
