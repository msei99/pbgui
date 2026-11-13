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

read_pb8_dir_from_ini() {
    local ini_path="$1"

    if [ ! -f "$ini_path" ]; then
        return
    fi

    python3 - "$ini_path" <<'PY' 2>/dev/null || true
from configparser import ConfigParser
from pathlib import Path
import sys

cfg = ConfigParser()
try:
    cfg.read(Path(sys.argv[1]), encoding='utf-8')
except Exception:
    raise SystemExit(0)

value = cfg.get('main', 'pb8dir', fallback='').strip()
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
pb8_dir="${PB8_DIR:-}"
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
if [ -z "$pb8_dir" ]; then
    pb8_dir="$(read_pb8_dir_from_ini "${pbgui_dir}/pbgui.ini")"
fi
if [ -z "$pb8_dir" ]; then
    pb8_dir="$(dirname "$pbgui_dir")/pb8"
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

cleanup_aged_pb7_logs() {
    local log_path
    local log_name
    local removed_count=0

    if [ ! -d "$pb7_dir/logs" ]; then
        log "Skipping aged PB7 log cleanup: log directory missing"
        return
    fi
    while IFS= read -r -d '' log_path; do
        log_name="${log_path##*/}"
        # Keep undated/current logs; only PB7's timestamped run logs expire.
        if [[ "$log_name" =~ ^[0-9]{8}_[0-9]{6}_.+\.log$ ]]; then
            remove_file "$log_path" "PB7 run log older than 30 days"
            removed_count=$((removed_count + 1))
        fi
    done < <(find "$pb7_dir/logs" -maxdepth 1 -type f -name '*.log' -mmin +43200 -print0)
    log "Aged PB7 log cleanup summary: ${removed_count} run logs removable"
}

cleanup_orphaned_pb8_data() {
    local run_v8_dir="${pbgui_dir}/data/run_v8"
    local monitor_dir="${pb8_dir}/monitor"
    local legacy_monitor_dir="${pb8_dir}/None"
    local pb8_logs_dir="${pb8_dir}/logs"
    local entry kind path
    local monitor_count=0
    local log_count=0
    local runtime_count=0
    local legacy_count=0
    local legacy_log_count=0
    local aged_log_count=0

    if [ "$cluster_role" != "vps" ]; then
        log "Skipping orphaned PB8 cleanup: cluster role is '${cluster_role:-unknown}', expected vps"
        return
    fi
    if [ ! -d "$run_v8_dir" ] || [ -L "$run_v8_dir" ]; then
        log "Skipping orphaned PB8 cleanup: run_v8 directory missing or linked"
        return
    fi
    if [ -L "$monitor_dir" ] || [ -L "$legacy_monitor_dir" ] || [ -L "$pb8_logs_dir" ]; then
        log "Skipping orphaned PB8 cleanup: monitor or logs directory is linked"
        return
    fi

    mapfile -t cleanup_lines < <(python3 - "$run_v8_dir" "$monitor_dir" "$legacy_monitor_dir" "$pb8_logs_dir" <<'PY'
from collections import Counter
from pathlib import Path
import math
import re
import subprocess
import sys
import time

run_dir, monitor_dir, legacy_dir, logs_dir = map(Path, sys.argv[1:5])
grace_seconds = 7 * 24 * 60 * 60
now = time.time()
retention_by_bot = {}
log_name = re.compile(r"_run_v8_(?P<bot>[^/]+?)_config\.json(?:_r[0-9a-f]{12})?\.log(?:\.[0-9]+)?$")
process_name = re.compile(r"(?:^|[\s/])data/run_v8/([^/\s]+)/config\.json(?:\s|$)")
safe_name = re.compile(r"^[^/\\\x00-\x1f\x7f.][^/\\\x00-\x1f\x7f]*$")

def valid(name):
    return bool(safe_name.fullmatch(name)) and ".." not in name

def old_file(path):
    try:
        return now - path.lstat().st_mtime >= grace_seconds
    except OSError:
        return False

def configured_log_retention(bot):
    if bot in retention_by_bot:
        return retention_by_bot[bot]
    runtime = run_dir / bot
    config_path = runtime / "config.json"
    if runtime.is_symlink() or config_path.is_symlink() or not config_path.is_file():
        retention_by_bot[bot] = None
        return None
    try:
        # Expose only the allowlisted scalar; never load or print the rest of
        # the potentially secret-bearing bot configuration during cleanup.
        query = subprocess.run(
            ["jq", "-er", ".monitor.retain_days // 7.0", str(config_path)],
            capture_output=True, text=True, check=True, timeout=5,
        )
        days = float(query.stdout.strip())
        if not math.isfinite(days) or days < 0:
            retention_by_bot[bot] = None
            return None
        retention_by_bot[bot] = days
        return days
    except (OSError, ValueError, TypeError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        retention_by_bot[bot] = None
        return None

def run_log_base(name):
    return name.split(".log", 1)[0] + ".log"

def recent_file(path):
    try:
        return path.is_file() and not path.is_symlink() and now - path.stat().st_mtime < 15 * 60
    except OSError:
        return False

def old_tree(path):
    latest = 0.0
    pending = [path]
    while pending:
        current = pending.pop()
        if current.is_symlink():
            return False
        try:
            latest = max(latest, current.stat().st_mtime)
            if current.is_dir():
                pending.extend(current.iterdir())
        except OSError:
            return False
    return now - latest >= grace_seconds

try:
    processes = subprocess.run(["ps", "-eo", "args="], capture_output=True, text=True, check=True)
    running_processes = Counter(process_name.findall(processes.stdout))
    running = set(running_processes)
    runtime_dirs = [path for path in run_dir.iterdir() if path.is_dir() or path.is_symlink()]
    configured = {path.name for path in runtime_dirs if (path / "config.json").is_file() or path.is_symlink()}
    # Recent partial directories may be installations or temporary handoffs.
    configured.update(path.name for path in runtime_dirs if not path.is_symlink() and not old_tree(path))
    for path in sorted(runtime_dirs):
        if (not path.is_symlink() and valid(path.name)
                and path.name not in configured | running and old_tree(path)):
            print(f"RUNTIME\t{path}")
    if monitor_dir.is_dir():
        for exchange in sorted(monitor_dir.iterdir()):
            if exchange.is_symlink() or not exchange.is_dir() or not valid(exchange.name):
                continue
            for bot in sorted(exchange.iterdir()):
                if bot.is_symlink() or not bot.is_dir() or not valid(bot.name):
                    continue
                if bot.name not in configured | running and old_tree(bot):
                    print(f"MONITOR\t{bot}")
    if legacy_dir.is_dir():
        for exchange in sorted(legacy_dir.iterdir()):
            if exchange.is_symlink() or not exchange.is_dir() or not valid(exchange.name):
                continue
            for bot in sorted(exchange.iterdir()):
                if bot.is_symlink() or not bot.is_dir() or not valid(bot.name) or not old_tree(bot):
                    continue
                active_count = running_processes[bot.name]
                # A running PB8 bot has one monitor root. Require fresh canonical
                # snapshots before discarding its old null-root data.
                canonical = monitor_dir / exchange.name / bot.name / "state.latest.json"
                if active_count == 0 or (active_count == 1 and recent_file(canonical)):
                    print(f"LEGACY\t{bot}")
        for path in sorted(legacy_dir.iterdir()):
            if path.is_symlink() or not path.is_file():
                continue
            match = log_name.search(path.name)
            if match and valid(match['bot']) and not running_processes[match['bot']] and old_file(path):
                print(f"LEGACY_LOG\t{path}")
        for alias in sorted(legacy_dir.iterdir()):
            if not alias.is_symlink() or not valid(alias.stem):
                continue
            target = alias.resolve(strict=False)
            match = log_name.search(target.name)
            if (match and match['bot'] == alias.stem and not running_processes[alias.stem]
                    and target.parent == legacy_dir.resolve() and (not target.exists() or old_file(target))):
                print(f"LEGACY_ALIAS\t{alias}")
    if logs_dir.is_dir():
        log_entries = sorted(logs_dir.iterdir())
        latest_runs = {}
        current_runs = {}
        run_modified_at = {}
        for path in log_entries:
            if path.is_symlink() and valid(path.stem):
                target = path.resolve(strict=False)
                match = log_name.search(target.name)
                if (match and match['bot'] == path.stem and target.parent == logs_dir.resolve()
                        and target.is_file()):
                    current_runs[path.stem] = run_log_base(target.name)
            elif path.is_file():
                match = log_name.search(path.name)
                if match and valid(match['bot']):
                    bot = match['bot']
                    base = run_log_base(path.name)
                    latest_runs[bot] = max(latest_runs.get(bot, ""), base)
                    run_modified_at[base] = max(run_modified_at.get(base, 0), path.stat().st_mtime)
        for path in log_entries:
            if not path.is_file() or path.is_symlink():
                continue
            match = log_name.search(path.name)
            if match and valid(match['bot']) and match['bot'] not in configured | running and old_file(path):
                print(f"LOG\t{path}")
            elif match and valid(match['bot']) and match['bot'] in configured | running:
                bot = match['bot']
                retention_days = configured_log_retention(bot)
                base = run_log_base(path.name)
                if (retention_days is None or now - run_modified_at.get(base, now)
                        < retention_days * 24 * 60 * 60):
                    continue
                if bot in running and bot not in current_runs:
                    continue
                if base not in {latest_runs.get(bot), current_runs.get(bot)}:
                    print(f"AGED_LOG\t{path}")
        for alias in log_entries:
            if not alias.is_symlink() or not valid(alias.stem):
                continue
            target = alias.resolve(strict=False)
            match = log_name.search(target.name)
            if (match and match['bot'] == alias.stem and alias.stem not in configured | running
                    and target.parent == logs_dir.resolve() and (not target.exists() or old_file(target))):
                print(f"ALIAS\t{alias}")
except (OSError, subprocess.CalledProcessError):
    # Fail closed if the desired-state or process inventory is incomplete.
    raise SystemExit(0)
PY
    )

    for entry in "${cleanup_lines[@]}"; do
        kind="${entry%%$'\t'*}"
        path="${entry#*$'\t'}"
        case "$kind" in
            RUNTIME)
                remove_tree "$path" "orphaned PB8 runtime data older than 7 days"
                runtime_count=$((runtime_count + 1))
                ;;
            MONITOR)
                remove_tree "$path" "orphaned PB8 monitor data older than 7 days"
                monitor_count=$((monitor_count + 1))
                ;;
            LEGACY)
                remove_tree "$path" "unused PB8 None monitor data older than 7 days"
                legacy_count=$((legacy_count + 1))
                ;;
            LEGACY_LOG)
                remove_file "$path" "unused PB8 None run log older than 7 days"
                legacy_log_count=$((legacy_log_count + 1))
                ;;
            LEGACY_ALIAS)
                if [ "$dry_run" -eq 0 ]; then
                    rm -f -- "$path"
                fi
                log "$(action_word) unused PB8 None log alias: ${path}"
                changed=1
                ;;
            LOG)
                remove_file "$path" "orphaned PB8 run log older than 7 days"
                log_count=$((log_count + 1))
                ;;
            AGED_LOG)
                remove_file "$path" "previous PB8 run log beyond config.monitor.retain_days"
                aged_log_count=$((aged_log_count + 1))
                ;;
            ALIAS)
                if [ "$dry_run" -eq 0 ]; then
                    rm -f -- "$path"
                fi
                log "$(action_word) orphaned PB8 log alias: ${path}"
                changed=1
                ;;
        esac
    done
    if [ "$dry_run" -eq 0 ] && [ -d "$legacy_monitor_dir" ]; then
        for legacy_parent in "$legacy_monitor_dir"/*; do
            if [ -d "$legacy_parent" ] && [ ! -L "$legacy_parent" ]; then
                rmdir -- "$legacy_parent" 2>/dev/null || true
            fi
        done
        rmdir -- "$legacy_monitor_dir" 2>/dev/null || true
    fi
    log "Orphaned PB8 cleanup summary: ${runtime_count} runtime directories, ${monitor_count} monitor directories, ${legacy_count} unused None directories, ${log_count} orphan run logs, ${aged_log_count} previous run logs, ${legacy_log_count} None run logs removable"
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
remove_file "${pbgui_dir}/data/logs/PBRemote.log.old" "legacy PBRemote rotated log"
remove_file "${pbgui_dir}/data/logs/sync.log" "legacy sync log"
remove_file "${pbgui_dir}/data/logs/sync.log.old" "legacy sync rotated log"
for legacy_log in "${pbgui_dir}"/data/logs/PBRemote.log.[0-9]* "${pbgui_dir}"/data/logs/sync.log.[0-9]*; do
    [ -e "$legacy_log" ] || continue
    legacy_generation="${legacy_log##*.}"
    case "$legacy_generation" in
        ''|*[!0-9]*) continue ;;
    esac
    case "$(basename "$legacy_log")" in
        PBRemote.log.[0-9]*|sync.log.[0-9]*) remove_file "$legacy_log" "legacy rotated service log" ;;
    esac
done
remove_file "${pbgui_dir}/data/logs/PBGui.log" "unexpected VPS PBGui log"
remove_tree "${pbgui_dir}/data/remote" "legacy PBRemote data cache"
remove_tree "${pbgui_dir}/data/state/pbremote" "legacy PBRemote state"
cleanup_stale_bot_runtime
cleanup_aged_pb7_logs
cleanup_orphaned_pb8_data
remove_tree "${pbgui_dir}/data/cmd" "legacy PBRun command/status directory"
remove_tree "${pbgui_dir}/cmd" "legacy root command directory"
if [ "$cluster_role" = "vps" ]; then
    remove_tree "${pbgui_dir}/data/backup" "legacy VPS backup directory"
    remove_tree "${pbgui_dir}/data/backups" "legacy VPS backups directory"
else
    log "Skipping backup directory cleanup: cluster role is '${cluster_role:-unknown}', expected vps"
fi
if [ "$dry_run" -eq 1 ]; then
    log "Finished VPS cleanup job (dry-run)"
else
    log "Finished VPS cleanup job"
fi
log "$(summary_word) total disk space: $(format_bytes "$freed_bytes")"
printf 'changed=%s\n' "$changed" >&3
