#!/usr/bin/env bash
# ======================================================
# 🚀 Master VPS Initialization Script
# ------------------------------------------------------
# This script performs initial secure setup of a new VPS.
# Actions:
#   1. Set system hostname
#   2. Create a new sudo user
#   3. Disable direct root login
#
# Usage:
#   ./master_vps_init.sh <server_name> <user_name>
# ======================================================

set -euo pipefail

# ----------[ Colors for pretty output ]----------
GREEN="\e[32m"
YELLOW="\e[33m"
RED="\e[31m"
BLUE="\e[36m"
BOLD="\e[1m"
RESET="\e[0m"

info()    { echo -e "${BLUE}[INFO]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
success() { echo -e "${GREEN}[ OK ]${RESET} $*"; }
error()   { echo -e "${RED}[ERR ]${RESET} $*" >&2; }

# ----------[ Header ]----------
echo -e "${BOLD}==============================================="
echo -e " 🔧 Master VPS Initialization - Secure Setup "
echo -e "===============================================${RESET}\n"

# ----------[ OS Check ]----------
info "Checking operating system..."
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    error "This script only supports Ubuntu systems."
    exit 1
fi
success "Ubuntu detected."

# ----------[ Argument Validation ]----------
if [ "$#" -ne 2 ]; then
    echo -e "${YELLOW}Usage:${RESET} $0 <server_name> <user_name>"
    exit 1
fi

SERVER_NAME="$1"
USER_NAME="$2"

echo
info "Configuration:"
echo "  • Server Hostname : $SERVER_NAME"
echo "  • New User        : $USER_NAME"
echo

# ----------[ Password Prompt ]----------
user_pw=""
for attempt in 1 2 3; do
    read -s -p "🔑 Enter password for '$USER_NAME': " pw1; echo
    read -s -p "🔑 Confirm password: " pw2; echo
    if [ "$pw1" = "$pw2" ] && [ -n "$pw1" ]; then
        user_pw="$pw1"
        break
    fi
    warn "Passwords do not match or are empty. Attempts left: $((3 - attempt))"
done

if [ -z "$user_pw" ]; then
    error "Failed to read a valid user password after 3 attempts."
    exit 1
fi
success "Password confirmed."

# ----------[ Set Hostname ]----------
info "Setting system hostname to '${SERVER_NAME}'..."
if [ "$(id -u)" -eq 0 ]; then
    hostnamectl set-hostname "$SERVER_NAME"
else
    printf "%s\n" "$user_pw" | sudo -S hostnamectl set-hostname "$SERVER_NAME"
fi
success "Hostname set successfully."

# ----------[ Create New User ]----------
info "Creating new sudo user '$USER_NAME'..."
if ! id "$USER_NAME" &>/dev/null; then
    sudo useradd -m -s /bin/bash "$USER_NAME"
    echo "$USER_NAME:$user_pw" | sudo chpasswd
    sudo usermod -aG sudo "$USER_NAME"
    success "User '$USER_NAME' created and added to sudo group."
else
    warn "User '$USER_NAME' already exists. Skipping creation."
fi

# ----------[ Secure SSH Configuration ]----------
info "Securing SSH access (disabling root login)..."
sudo passwd -l root

# Ensure PermitRootLogin is set properly
if grep -q '^PermitRootLogin' /etc/ssh/sshd_config; then
    sudo sed -i 's/^PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
else
    echo 'PermitRootLogin no' | sudo tee -a /etc/ssh/sshd_config > /dev/null
fi

# Reload SSH service gracefully
if systemctl list-units --type=service | grep -q 'sshd.service'; then
    sudo systemctl reload sshd
else
    sudo systemctl reload ssh
fi
success "Root login disabled and SSH reloaded."

# ----------[ Summary ]----------
echo
echo -e "${BOLD}✅ Master VPS Initialization Complete${RESET}"
echo -e "-----------------------------------------------"
echo -e "• Hostname     : ${GREEN}${SERVER_NAME}${RESET}"
echo -e "• New User     : ${GREEN}${USER_NAME}${RESET}"
echo -e "• Root Login   : ${RED}Disabled${RESET}"
echo -e "-----------------------------------------------"
echo -e "🎉 You can now connect via:"
echo -e "   ${BOLD}ssh ${USER_NAME}@$(hostname -I | awk '{print $1}')${RESET}"
echo

exit 0
