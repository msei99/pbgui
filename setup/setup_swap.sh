#!/usr/bin/env bash
set -euo pipefail

# Root needs no privilege wrapper; other users must have sudo.
SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Run as root or install sudo before running this script." >&2
        exit 1
    fi
    SUDO=(sudo)
fi

# ==========================================
# 🔧 Resize or create swapfile
# ==========================================

SWAPFILE="/swapfile"
DEFAULT_SIZE="6G"  # Default swap size if not provided

# --- Functions ---
info()    { echo -e "\e[36m[INFO]\e[0m $*"; }
success() { echo -e "\e[32m[ OK ]\e[0m $*"; }
error()   { echo -e "\e[31m[ERR ]\e[0m $*" >&2; }

usage() {
    echo -e "\nUsage: $0 [SIZE]"
    echo -e "Resize or create swapfile."
    echo -e "  SIZE    Optional swap size (e.g., 6G, 8G, 1024M). Default: $DEFAULT_SIZE\n"
    exit 1
}

# --- Parse arguments ---
if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
    usage
fi

SWAP_SIZE="${1:-$DEFAULT_SIZE}"

# --- Main Script ---
info "Checking current swapfile..."

if [ -f "$SWAPFILE" ]; then
    info "Swapfile exists. Disabling it..."
    "${SUDO[@]}" swapoff "$SWAPFILE"
    success "Swapfile disabled."
    info "Removing old swapfile..."
    "${SUDO[@]}" rm -f "$SWAPFILE"
    success "Old swapfile removed."
else
    info "No existing swapfile found. Creating a new one..."
fi

info "Creating new swapfile of size $SWAP_SIZE..."
"${SUDO[@]}" fallocate -l "$SWAP_SIZE" "$SWAPFILE"
"${SUDO[@]}" chmod 600 "$SWAPFILE"
"${SUDO[@]}" mkswap "$SWAPFILE"
"${SUDO[@]}" swapon "$SWAPFILE"
success "Swapfile created and enabled."

# Add to fstab if not already present
if ! grep -q "$SWAPFILE" /etc/fstab; then
    echo "$SWAPFILE none swap sw 0 0" | "${SUDO[@]}" tee -a /etc/fstab
    success "Swapfile entry added to /etc/fstab"
fi

info "Tuning swap settings..."
"${SUDO[@]}" sysctl -w vm.swappiness=10 > /dev/null
"${SUDO[@]}" sysctl -w vm.vfs_cache_pressure=50 > /dev/null

# Persist settings
"${SUDO[@]}" sed -i '/vm.swappiness/d' /etc/sysctl.conf
echo "vm.swappiness=10" | "${SUDO[@]}" tee -a /etc/sysctl.conf

"${SUDO[@]}" sed -i '/vm.vfs_cache_pressure/d' /etc/sysctl.conf
echo "vm.vfs_cache_pressure=50" | "${SUDO[@]}" tee -a /etc/sysctl.conf

success "Swap settings applied."
success "✅ Swapfile resize/create complete!"
