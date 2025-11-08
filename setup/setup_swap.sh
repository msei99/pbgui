#!/usr/bin/env bash
set -euo pipefail

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
    sudo swapoff "$SWAPFILE"
    success "Swapfile disabled."
    info "Removing old swapfile..."
    sudo rm -f "$SWAPFILE"
    success "Old swapfile removed."
else
    info "No existing swapfile found. Creating a new one..."
fi

info "Creating new swapfile of size $SWAP_SIZE..."
sudo fallocate -l "$SWAP_SIZE" "$SWAPFILE"
sudo chmod 600 "$SWAPFILE"
sudo mkswap "$SWAPFILE"
sudo swapon "$SWAPFILE"
success "Swapfile created and enabled."

# Add to fstab if not already present
if ! grep -q "$SWAPFILE" /etc/fstab; then
    echo "$SWAPFILE none swap sw 0 0" | sudo tee -a /etc/fstab
    success "Swapfile entry added to /etc/fstab"
fi

info "Tuning swap settings..."
sudo sysctl -w vm.swappiness=10 > /dev/null
sudo sysctl -w vm.vfs_cache_pressure=50 > /dev/null

# Persist settings
sudo sed -i '/vm.swappiness/d' /etc/sysctl.conf
echo "vm.swappiness=10" | sudo tee -a /etc/sysctl.conf

sudo sed -i '/vm.vfs_cache_pressure/d' /etc/sysctl.conf
echo "vm.vfs_cache_pressure=50" | sudo tee -a /etc/sysctl.conf

success "Swap settings applied."
success "✅ Swapfile resize/create complete!"
