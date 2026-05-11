#!/usr/bin/env bash

set -uo pipefail

changed=0
home_dir="${HOME:-}"

timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

log() {
    printf '[%s] %s\n' "$(timestamp)" "$*"
}

remove_tree() {
    local path="$1"
    local label="$2"

    if [ -e "$path" ]; then
        rm -rf -- "$path"
        log "Removed ${label}: ${path}"
        changed=1
    else
        log "Absent ${label}: ${path}"
    fi
}

log "Starting VPS cleanup job"
remove_tree "${home_dir}/.cache/pip" "pip cache"
remove_tree "${home_dir}/.rustup/downloads" "rustup downloads"
remove_tree "${home_dir}/.rustup/tmp" "rustup tmp"
log "Finished VPS cleanup job"
printf 'changed=%s\n' "$changed"
