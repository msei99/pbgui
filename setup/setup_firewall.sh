#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# UFW Firewall Configuration for OpenVPN Server
# =============================================================================

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

# =============================================================================
# Command-line Argument Parsing
# =============================================================================

SSH_IPS=""   # Comma-separated list of IPs for SSH

while getopts ":i:" opt; do
  case $opt in
    i) SSH_IPS="$OPTARG" ;;
    \?) echo "Usage: $0 [-i SSH_IPS]"; exit 1 ;;
  esac
done

# =============================================================================
# Read OpenVPN network configuration
# =============================================================================

VPN_LINE=$(grep -E "^server " /etc/openvpn/server/server.conf || true)
if [[ -z "$VPN_LINE" ]]; then
  error "Could not find the VPN network in /etc/openvpn/server/server.conf"
  exit 1
fi

VPN_NET=$(echo "$VPN_LINE" | awk '{print $2}')
VPN_MASK=$(echo "$VPN_LINE" | awk '{print $3}')

# Convert subnet mask to CIDR
mask2cidr() {
  local nbits=0
  IFS=. read -r i1 i2 i3 i4 <<< "$1"
  for octet in $i1 $i2 $i3 $i4; do
    for ((i=0;i<8;i++)); do
      (( (octet >> i) & 1 )) && ((nbits++))
    done
  done
  echo "$nbits"
}

VPN_CIDR="$VPN_NET/$(mask2cidr $VPN_MASK)"

info "Detected OpenVPN network range: $VPN_CIDR"

# =============================================================================
# Header and Description
# =============================================================================

echo -e "${BOLD}${GREEN}==============================================="
echo -e " 🛡️ UFW Firewall Configuration for OpenVPN"
echo -e "===============================================${RESET}"
echo
echo -e "${BLUE}This script configures UFW firewall rules for OpenVPN and SSH access.${RESET}"
echo -e "${YELLOW}Usage option for SSH access:${RESET}"
echo
echo -e "  -i SSH_IPS  Comma-separated list of IPs for SSH access."
echo -e "               If provided, the VPN network will automatically be included."
echo -e "               If not provided, SSH will be allowed from all IPs (default)."
echo

# =============================================================================
# Firewall Configuration
# =============================================================================

info "Ensuring UFW is installed..."
sudo apt install -y ufw
success "UFW installed."

info "Resetting UFW rules..."
sudo ufw --force reset

info "Allowing essential services..."
sudo ufw allow 1194/udp comment 'OpenVPN UDP'

# --- SSH access logic ---
SSH_ALLOWED=()

if [[ -n "$SSH_IPS" ]]; then
  SSH_ALLOWED+=($(echo "$SSH_IPS" | tr ',' ' '))
  SSH_ALLOWED+=("$VPN_CIDR")
fi

if [[ ${#SSH_ALLOWED[@]} -eq 0 ]]; then
  info "Allowing SSH from all IPs (default)..."
  sudo ufw allow 22/tcp comment "Allow SSH from all IPs"
else
  info "Allowing SSH from the following sources: ${SSH_ALLOWED[*]}"
  for src in "${SSH_ALLOWED[@]}"; do
    sudo ufw allow from "$src" to any port 22 proto tcp comment "SSH from $src"
  done
fi

info "Setting default policies..."
sudo ufw default deny incoming
sudo ufw default allow outgoing

info "Enabling UFW..."
sudo ufw --force enable
success "Firewall configured and enabled."

# =============================================================================
# Summary
# =============================================================================

echo -e "\n${BOLD}${GREEN}✅ Firewall setup complete!${RESET}"
if [[ ${#SSH_ALLOWED[@]} -eq 0 ]]; then
    echo -e "   - SSH access: Allowed from all IPs (default)"
else
    echo -e "   - SSH access: Allowed from: ${SSH_ALLOWED[*]}"
fi
echo -e "   - OpenVPN access: Allowed on UDP port 1194"
echo -e "   - All other incoming traffic denied"
echo
