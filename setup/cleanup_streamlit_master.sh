#!/usr/bin/env bash
set -euo pipefail

# Cleanup helper for masters updated from the legacy Streamlit UI to FastAPI-only PBGui.
# It stops stale Streamlit processes, removes legacy autostart entries, and closes port 8501.

GREEN="\e[32m"
YELLOW="\e[33m"
RED="\e[31m"
BLUE="\e[36m"
RESET="\e[0m"

dry_run=0

info()    { echo -e "${BLUE}[INFO]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
success() { echo -e "${GREEN}[ OK ]${RESET} $*"; }
error()   { echo -e "${RED}[ERR ]${RESET} $*" >&2; }

usage() {
    cat <<'EOF'
Usage: cleanup_streamlit_master.sh [--dry-run]

Removes legacy Streamlit runtime leftovers after updating a PBGui master:
  - stops running Streamlit/pbgui.py processes
  - removes user crontab entries containing streamlit or pbgui.py
  - deletes active UFW rules containing port 8501
  - uninstalls direct legacy Streamlit packages from detected PBGui venvs
  - removes the obsolete .streamlit/config.toml file while keeping secrets.toml

The script does not delete data, auth secrets, or reboot the host.
EOF
}

resolve_script_dir() {
    local source_path="${BASH_SOURCE[0]:-$0}"
    local source_dir=""

    source_dir="$(dirname "$source_path")"
    if [ -d "$source_dir" ]; then
        (cd "$source_dir" && pwd -P)
        return
    fi

    printf '%s\n' "$PWD"
}

detect_pbgui_dir() {
    local script_dir=""
    local candidate=""

    if [ -n "${PBGUI_DIR:-}" ] && [ -d "$PBGUI_DIR" ]; then
        printf '%s\n' "$PBGUI_DIR"
        return
    fi

    script_dir="$(resolve_script_dir)"
    if [ -f "${script_dir}/../PBApiServer.py" ]; then
        (cd "${script_dir}/.." && pwd -P)
        return
    fi

    for candidate in \
        "${HOME:-}/software/pbgui" \
        "${HOME:-}/pbgui"; do
        if [ -f "${candidate}/PBApiServer.py" ]; then
            printf '%s\n' "$candidate"
            return
        fi
    done
}

collect_pbgui_pythons() {
    local pbgui_dir=""
    local base_dir=""
    local candidate=""
    local existing=""
    local candidates=()
    local seen=()

    if [ -n "${PBGUI_PYTHON:-}" ] && [ -x "$PBGUI_PYTHON" ]; then
        candidates+=("$PBGUI_PYTHON")
    fi

    if [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/python" ]; then
        candidates+=("${VIRTUAL_ENV}/bin/python")
    fi

    pbgui_dir="$(detect_pbgui_dir)"
    if [ -n "$pbgui_dir" ]; then
        base_dir="$(dirname "$pbgui_dir")"
        candidates+=(
            "${base_dir}/venv_pbgui/bin/python" \
            "${base_dir}/venv_pbgui312/bin/python" \
            "${base_dir}/venv/bin/python"
        )
    fi

    candidates+=(
        "${HOME:-}/software/venv_pbgui/bin/python" \
        "${HOME:-}/software/venv_pbgui312/bin/python" \
        "${HOME:-}/venv_pbgui/bin/python"
    )

    for candidate in "${candidates[@]}"; do
        [ -x "$candidate" ] || continue
        for existing in "${seen[@]}"; do
            [ "$existing" = "$candidate" ] && continue 2
        done
        seen+=("$candidate")
        printf '%s\n' "$candidate"
    done
}

cleanup_streamlit_packages_in_venv() {
    local python_bin="$1"
    local package=""
    local installed=()
    local packages=(
        streamlit
        streamlit-scrollable-textbox
        streamlit-autorefresh
        streamlit-bokeh
        bokeh
    )

    info "Checking legacy Streamlit packages in: $python_bin"
    for package in "${packages[@]}"; do
        if "$python_bin" -m pip show "$package" >/dev/null 2>&1; then
            installed+=("$package")
        fi
    done

    if [ "${#installed[@]}" -eq 0 ]; then
        success "No direct legacy Streamlit packages found in $python_bin."
        return
    fi

    if [ "$dry_run" -eq 1 ]; then
        info "Would uninstall from $python_bin: ${installed[*]}"
        return
    fi

    "$python_bin" -m pip uninstall -y "${installed[@]}"
    success "Removed direct legacy Streamlit packages from $python_bin: ${installed[*]}"
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dry-run)
            dry_run=1
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

collect_legacy_pids() {
    local line=""
    local pid=""
    local cmd=""

    if ! command -v pgrep >/dev/null 2>&1; then
        return 0
    fi

    while IFS= read -r line; do
        [ -n "$line" ] || continue
        pid="${line%% *}"
        cmd="${line#* }"

        [ "$pid" != "$$" ] || continue

        if [[ "$cmd" == *streamlit* && "$cmd" == *pbgui.py* ]]; then
            printf '%s\n' "$pid"
        elif [[ "$cmd" == *python* && "$cmd" == *pbgui.py* && "$cmd" != *cleanup_streamlit_master.sh* ]]; then
            printf '%s\n' "$pid"
        fi
    done < <(pgrep -af 'streamlit|pbgui\.py' || true)
}

stop_legacy_processes() {
    local pids=()
    local pid=""
    local cmd=""
    local alive=0
    local attempt=0

    mapfile -t pids < <(collect_legacy_pids)

    if [ "${#pids[@]}" -eq 0 ]; then
        success "No legacy Streamlit/pbgui.py processes found."
        return
    fi

    if [ "$dry_run" -eq 1 ]; then
        info "Found legacy Streamlit/pbgui.py process(es): ${pids[*]}"
    else
        info "Stopping legacy Streamlit/pbgui.py process(es): ${pids[*]}"
    fi

    for pid in "${pids[@]}"; do
        cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
        if [ "$dry_run" -eq 1 ]; then
            info "Would stop PID $pid: $cmd"
        else
            kill "$pid" 2>/dev/null || true
        fi
    done

    if [ "$dry_run" -eq 1 ]; then
        return 0
    fi

    for attempt in 1 2 3 4 5; do
        alive=0
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                alive=1
                break
            fi
        done
        [ "$alive" -eq 0 ] && break
        sleep 1
    done

    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            warn "PID $pid did not exit after TERM; sending KILL."
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done

    success "Legacy Streamlit/pbgui.py processes stopped."
}

cleanup_crontab() {
    local current_cron=""
    local new_cron=""

    if ! command -v crontab >/dev/null 2>&1; then
        warn "crontab command not found; skipping autostart cleanup."
        return
    fi

    current_cron="$(mktemp)"
    new_cron="$(mktemp)"

    if ! crontab -l > "$current_cron" 2>/dev/null; then
        success "No user crontab found."
        rm -f "$current_cron" "$new_cron"
        return
    fi

    if ! grep -Eiq 'streamlit|pbgui\.py' "$current_cron"; then
        success "No legacy Streamlit/pbgui.py crontab entries found."
        rm -f "$current_cron" "$new_cron"
        return
    fi

    warn "Removing legacy crontab entries containing streamlit or pbgui.py."
    grep -Ei 'streamlit|pbgui\.py' "$current_cron" || true
    grep -Eiv 'streamlit|pbgui\.py' "$current_cron" > "$new_cron" || true

    if [ "$dry_run" -eq 1 ]; then
        info "Dry run: user crontab was not changed."
        rm -f "$current_cron" "$new_cron"
        return
    fi

    crontab "$new_cron"
    rm -f "$current_cron" "$new_cron"
    success "Legacy crontab entries removed."
}

cleanup_ufw_8501() {
    local rule_numbers=()
    local rule_lines=()
    local idx=0
    local number=""

    if ! command -v ufw >/dev/null 2>&1; then
        warn "ufw command not found; skipping port 8501 firewall cleanup."
        return
    fi

    if [ "$dry_run" -eq 1 ]; then
        if ! sudo -n true 2>/dev/null; then
            warn "sudo is not available non-interactively; skipping UFW dry-run inspection."
            warn "Run without --dry-run in an interactive shell to remove port 8501 rules."
            return 0
        fi
    elif ! sudo -v; then
        error "sudo is required to inspect and update UFW rules."
        return 1
    fi

    mapfile -t rule_lines < <(sudo ufw status numbered 2>/dev/null | grep '8501' || true)
    mapfile -t rule_numbers < <(sudo ufw status numbered 2>/dev/null | sed -n '/8501/s/^\[ *\([0-9][0-9]*\)\].*/\1/p')

    if [ "${#rule_numbers[@]}" -eq 0 ]; then
        success "No active UFW rules for port 8501 found."
        return
    fi

    warn "Removing UFW rule(s) containing port 8501:"
    printf '%s\n' "${rule_lines[@]}"

    if [ "$dry_run" -eq 1 ]; then
        info "Dry run: UFW rules were not changed."
        return
    fi

    for ((idx=${#rule_numbers[@]}-1; idx>=0; idx--)); do
        number="${rule_numbers[$idx]}"
        if printf 'y\n' | sudo ufw delete "$number" >/dev/null; then
            success "Deleted UFW rule #$number."
        else
            warn "Could not delete UFW rule #$number; check 'sudo ufw status numbered'."
        fi
    done
}

cleanup_venv_packages() {
    local python_bins=()
    local python_bin=""

    mapfile -t python_bins < <(collect_pbgui_pythons)
    if [ "${#python_bins[@]}" -eq 0 ]; then
        warn "Could not detect a PBGui venv python; skipping Streamlit package cleanup."
        warn "Set PBGUI_PYTHON=/path/to/venv/bin/python and run again if needed."
        return
    fi

    for python_bin in "${python_bins[@]}"; do
        cleanup_streamlit_packages_in_venv "$python_bin"
    done
}

cleanup_streamlit_config_file() {
    local pbgui_dir=""
    local config_path=""

    pbgui_dir="$(detect_pbgui_dir)"
    if [ -z "$pbgui_dir" ]; then
        warn "Could not detect the PBGui directory; skipping .streamlit/config.toml cleanup."
        return
    fi

    config_path="${pbgui_dir}/.streamlit/config.toml"
    if [ ! -e "$config_path" ]; then
        success "No obsolete .streamlit/config.toml file found."
        return
    fi

    if [ "$dry_run" -eq 1 ]; then
        info "Would remove obsolete Streamlit config file: $config_path"
        return
    fi

    rm -f "$config_path"
    success "Removed obsolete Streamlit config file: $config_path"
}

check_port_8501() {
    if ! command -v ss >/dev/null 2>&1; then
        return
    fi

    if ss -ltn | grep -Eq '(^|[[:space:]]):8501[[:space:]]'; then
        warn "Port 8501 is still listening. Check remaining processes manually."
    else
        success "Port 8501 is not listening."
    fi
}

info "PBGui legacy Streamlit master cleanup starting."
if [ "$dry_run" -eq 1 ]; then
    warn "Dry-run mode: no processes, crontab entries, UFW rules, or venv packages will be changed."
fi

stop_legacy_processes
cleanup_crontab
cleanup_ufw_8501
cleanup_venv_packages
cleanup_streamlit_config_file
check_port_8501

success "Cleanup complete. No reboot is required."
