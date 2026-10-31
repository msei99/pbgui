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

# Validate all inputs before package installation or system changes.
if [[ "$#" -ne 2 ]]; then
    echo "Usage: $0 <server_name> <user_name>" >&2
    exit 1
fi
SERVER_NAME="$1"
USER_NAME="$2"
if [[ ${#SERVER_NAME} -gt 253 || ! "$SERVER_NAME" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]]; then
    error "Invalid server hostname."
    exit 1
fi
IFS='.' read -r -a hostname_labels <<< "$SERVER_NAME"
for label in "${hostname_labels[@]}"; do
    if [[ ${#label} -gt 63 || ! "$label" =~ ^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$ ]]; then
        error "Invalid server hostname label."
        exit 1
    fi
done
if [[ "$USER_NAME" == root || ! "$USER_NAME" =~ ^[a-z_][a-z0-9_-]{0,31}$ ]]; then
    error "Invalid non-root administrator name."
    exit 1
fi

# Read distribution metadata in a subshell without changing script variables.
ubuntu_id() (
    ID=""
    if [[ -r /etc/os-release ]]; then
        . /etc/os-release
    fi
    if [[ -n "$ID" ]]; then
        printf '%s\n' "$ID"
    elif command -v lsb_release >/dev/null 2>&1; then
        lsb_release -si | tr '[:upper:]' '[:lower:]'
    fi
)
if [[ "$(ubuntu_id)" != "ubuntu" ]]; then
    echo "This script only supports Ubuntu systems." >&2
    exit 1
fi

# Existing accounts must already be administrators; never grant them new rights.
validate_admin_account() {
    local uid groups
    uid="$(id -u "$USER_NAME")" || return 1
    [[ "$uid" =~ ^[0-9]+$ && "$uid" -ne 0 ]] || return 1
    groups="$(id -nG "$USER_NAME")" || return 1
    [[ " $groups " == *" sudo "* ]]
}
if id "$USER_NAME" >/dev/null 2>&1 && ! validate_admin_account; then
    error "Existing target account must be a non-root member of the sudo group."
    exit 1
fi

# Root login will be disabled later: the administrator must have sudo first.
if [[ "$(id -u)" -eq 0 ]] && ! command -v sudo >/dev/null 2>&1; then
    if ! command -v apt-get >/dev/null 2>&1; then
        error "apt-get is required to install sudo before disabling root login."
        exit 1
    fi
    APT_OPTS=(-o Dpkg::Use-Pty=0 -o APT::Color=0)
    if ! DEBIAN_FRONTEND=noninteractive apt-get "${APT_OPTS[@]}" update ||
       ! DEBIAN_FRONTEND=noninteractive apt-get "${APT_OPTS[@]}" install -y sudo; then
        error "Could not install sudo; root login has not been changed."
        exit 1
    fi
    if ! command -v sudo >/dev/null 2>&1; then
        error "sudo is still unavailable; root login has not been changed."
        exit 1
    fi
fi

# Root needs no privilege wrapper; other users must have sudo.
SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Run as root or install sudo before running this script." >&2
        exit 1
    fi
    SUDO=(sudo)
fi

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
"${SUDO[@]}" hostnamectl set-hostname "$SERVER_NAME"
success "Hostname set successfully."

# ----------[ Create New User ]----------
info "Creating new sudo user '$USER_NAME'..."
if ! id "$USER_NAME" &>/dev/null; then
    "${SUDO[@]}" useradd -m -s /bin/bash "$USER_NAME"
    printf '%s:%s\n' "$USER_NAME" "$user_pw" | "${SUDO[@]}" chpasswd
    "${SUDO[@]}" usermod -aG sudo "$USER_NAME"
    success "User '$USER_NAME' created and added to sudo group."
else
    warn "User '$USER_NAME' already exists. Skipping creation."
fi

if ! validate_admin_account; then
    error "Administrator verification failed; root login has not been changed."
    exit 1
fi
unset user_pw pw1 pw2

# ----------[ Secure SSH Configuration ]----------
info "Securing SSH access (disabling root login)..."
"${SUDO[@]}" passwd -l root

# Ensure PermitRootLogin is set properly
if grep -q '^PermitRootLogin' /etc/ssh/sshd_config; then
    "${SUDO[@]}" sed -i 's/^PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
else
    echo 'PermitRootLogin no' | "${SUDO[@]}" tee -a /etc/ssh/sshd_config > /dev/null
fi

# Reload SSH service gracefully
if systemctl list-units --type=service | grep -q 'sshd.service'; then
    "${SUDO[@]}" systemctl reload sshd
else
    "${SUDO[@]}" systemctl reload ssh
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
