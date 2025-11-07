#!/usr/bin/env bash
# ======================================================
# 🚀 OpenVPN Server Setup Script
# ------------------------------------------------------
# This script installs and configures OpenVPN server
# on an Ubuntu VPS. It handles:
#   1. Easy-RSA PKI setup
#   2. Certificates & key generation
#   3. OpenVPN server configuration
#   4. OpenVPN service start
#
# Usage:
#   ./openvpn_setup.sh <server_name>
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

# ----------[ Argument Validation ]----------
if [ "$#" -ne 1 ]; then
    echo -e "${YELLOW}Usage:${RESET} $0 <server_name>"
    exit 1
fi

SERVER_NAME="$1"

echo
info "Configuration:"
echo "  • Server Hostname : $SERVER_NAME"
echo

# ----------[ Package Installation ]----------
info "Installing OpenVPN and Easy-RSA..."
sudo apt update
sudo apt install -y openvpn easy-rsa openssl
success "Required packages installed."

# ----------[ Easy-RSA PKI Setup ]----------
info "Setting up Easy-RSA PKI..."
EASYRSA_DIR="/etc/openvpn/easy-rsa"
sudo rm -rf "$EASYRSA_DIR" || true
sudo mkdir -p "$EASYRSA_DIR"
sudo cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"

export EASYRSA_BATCH=1
export EASYRSA_REQ_CN="server"

sudo ./easyrsa init-pki
sudo ./easyrsa build-ca nopass
sudo ./easyrsa gen-req server nopass
echo "yes" | sudo ./easyrsa sign-req server server
success "Easy-RSA PKI ready."

# ----------[ Certificates & Keys ]----------
info "Copying certificates and keys to OpenVPN directory..."
OVPN_DIR="/etc/openvpn/server"
sudo mkdir -p "$OVPN_DIR"
sudo cp pki/ca.crt pki/issued/server.crt pki/private/server.key "$OVPN_DIR/"

info "Generating ECDH and TLS keys..."
sudo openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
sudo openvpn --genkey secret "$OVPN_DIR/ta.key"
success "Certificates and keys ready."

# ----------[ OpenVPN Server Configuration ]----------
info "Creating OpenVPN server configuration..."
sudo tee "$OVPN_DIR/server.conf" > /dev/null <<'EOF'
port 1194
proto udp
dev tun

# Drop privileges
user nobody
group nogroup
persist-key
persist-tun

# Certificates
ca ca.crt
cert server.crt
key server.key
dh none
ecdh-curve prime256v1

# TLS security
tls-server
tls-version-min 1.3
tls-ciphersuites TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256
tls-crypt ta.key
auth SHA256
cipher AES-256-GCM
ncp-ciphers AES-256-GCM:CHACHA20-POLY1305
data-ciphers AES-256-GCM:CHACHA20-POLY1305
data-ciphers-fallback AES-256-GCM

# VPN network
server 10.8.0.0 255.255.255.0
topology subnet

# PAM plugin for MFA (handled in separate script)
plugin /usr/lib/x86_64-linux-gnu/openvpn/plugins/openvpn-plugin-auth-pam.so openvpn
verify-client-cert none
username-as-common-name
auth-nocache

# Keepalive & connection options
keepalive 10 120
explicit-exit-notify 1
client-to-client
duplicate-cn

# Logging
status /var/log/openvpn-status.log
log-append /var/log/openvpn.log
verb 3
mute 10

# Security
script-security 2
capath /etc/ssl/certs
remote-cert-eku "TLS Web Client Authentication"
EOF
success "OpenVPN server configuration created."

# ----------[ /dev/net/tun ]----------
info "Ensuring /dev/net/tun exists..."
if [ ! -c /dev/net/tun ]; then
    sudo mkdir -p /dev/net
    sudo mknod /dev/net/tun c 10 200
    sudo chmod 600 /dev/net/tun
fi
success "/dev/net/tun ready."

# ----------[ Permissions ]----------
info "Setting file permissions..."
sudo chown -R root:root "$OVPN_DIR"
sudo chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
sudo chmod 644 "$OVPN_DIR/ca.crt" "$OVPN_DIR/server.crt"
success "Permissions set."

# ----------[ Start OpenVPN Service ]----------
info "Enabling and starting OpenVPN service..."
sudo systemctl daemon-reload
sudo systemctl enable openvpn-server@server.service
sudo systemctl restart openvpn-server@server.service
success "OpenVPN server is up and running."

echo
echo -e "${BOLD}✅ OpenVPN Server Setup Complete${RESET}"
echo -e "-----------------------------------------------"
echo -e "• Server Hostname : ${GREEN}${SERVER_NAME}${RESET}"
echo -e "• OpenVPN Service : ${GREEN}Active${RESET}"
echo -e "-----------------------------------------------"
echo -e "⚡ Next Steps:"
echo -e "  1. Configure firewall in a separate script"
echo -e "  2. Set up Google Authenticator MFA using separate script"
echo

exit 0
