#!/usr/bin/env bash
# ======================================================
# 🚀 OpenVPN Server Setup Script
# ------------------------------------------------------
# This script installs and configures a secure OpenVPN server.
# It generates server certificates, keys, and config files.
#
# Usage:
#   bash setup_openvpn.sh <user_name>
#   OR
#   curl -fsSL <URL> | bash -s -- <user_name>
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
echo -e " 🔧 OpenVPN Server Setup - Secure Configuration "
echo -e "===============================================${RESET}\n"

# ----------[ OS Check ]----------
info "Checking operating system..."
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    error "This script only supports Ubuntu systems."
    exit 1
fi
success "Ubuntu detected."

# ----------[ Usage / Argument Validation ]----------
SCRIPT_NAME=${BASH_SOURCE[0]:-"setup_openvpn.sh"}

if [ "$#" -ne 1 ]; then
    echo -e "${YELLOW}Usage:${RESET} bash $SCRIPT_NAME <user_name>"
    echo -e "Or via curl: curl -fsSL <URL> | bash -s -- <user_name>"
    exit 1
fi

USER_NAME="$1"
info "User for OpenVPN setup: $USER_NAME"

# ----------[ Install dependencies ]----------
info "Installing required packages..."
sudo apt update
sudo apt install -y openvpn easy-rsa

# ----------[ Setup Easy-RSA ]----------
EASYRSA_DIR="/root/easy-rsa"
info "Setting up Easy-RSA directory..."
sudo rm -rf "$EASYRSA_DIR" || true
sudo mkdir -p "$EASYRSA_DIR"
sudo cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"

export EASYRSA_BATCH=1
export EASYRSA_REQ_CN="server"

info "Initializing PKI..."
./easyrsa init-pki
info "Building CA..."
./easyrsa build-ca nopass
info "Generating server request..."
./easyrsa gen-req server nopass
info "Signing server certificate..."
echo "yes" | ./easyrsa sign-req server server

# ----------[ Copy certs/keys to OpenVPN directory ]----------
OVPN_DIR="/etc/openvpn/server"
info "Copying server certificates to $OVPN_DIR..."
sudo mkdir -p "$OVPN_DIR"
sudo cp pki/ca.crt pki/issued/server.crt pki/private/server.key "$OVPN_DIR/"

info "Generating ECDH and TLS key..."
sudo openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
sudo openvpn --genkey secret "$OVPN_DIR/ta.key"

# ----------[ OpenVPN server configuration ]----------
info "Creating server configuration..."
sudo tee "$OVPN_DIR/server.conf" > /dev/null <<'EOF'
port 1194
proto udp
dev tun

user nobody
group nogroup
persist-key
persist-tun

ca ca.crt
cert server.crt
key server.key
dh none
ecdh-curve prime256v1

tls-server
tls-version-min 1.3
tls-ciphersuites TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256
tls-crypt ta.key
auth SHA256
cipher AES-256-GCM
ncp-ciphers AES-256-GCM:CHACHA20-POLY1305
data-ciphers AES-256-GCM:CHACHA20-POLY1305
data-ciphers-fallback AES-256-GCM

server 10.8.0.0 255.255.255.0
topology subnet

plugin /usr/lib/x86_64-linux-gnu/openvpn/plugins/openvpn-plugin-auth-pam.so openvpn
verify-client-cert none
username-as-common-name
auth-nocache

keepalive 10 120
explicit-exit-notify 1
client-to-client
duplicate-cn

status /var/log/openvpn-status.log
log-append /var/log/openvpn.log
verb 3
mute 10

script-security 2
capath /etc/ssl/certs
remote-cert-eku "TLS Web Client Authentication"
EOF

# ----------[ Ensure /dev/net/tun exists ]----------
if [ ! -c /dev/net/tun ]; then
    info "Creating /dev/net/tun..."
    sudo mkdir -p /dev/net
    sudo mknod /dev/net/tun c 10 200
    sudo chmod 600 /dev/net/tun
fi

# ----------[ Permissions ]----------
sudo chown -R root:root "$OVPN_DIR"
sudo chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
sudo chmod 644 "$OVPN_DIR/ca.crt" "$OVPN_DIR/server.crt"

# ----------[ Enable & start OpenVPN ]----------
info "Enabling and starting OpenVPN service..."
sudo systemctl daemon-reload
sudo systemctl enable openvpn-server@server.service
sudo systemctl restart openvpn-server@server.service
success "OpenVPN server is up and running."

echo
echo -e "${BOLD}✅ OpenVPN Server Setup Complete${RESET}"
echo -e "-----------------------------------------------"
echo -e "• User        : ${GREEN}$USER_NAME${RESET}"
echo -e "• Server dir  : ${GREEN}$OVPN_DIR${RESET}"
echo -e "• OpenVPN port: ${GREEN}1194/UDP${RESET}"
echo -e "-----------------------------------------------"
echo -e "🎉 Client configuration and MFA setup should be done separately."
echo
exit 0
