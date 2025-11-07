#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# 🚀 OpenVPN Server Setup Script
# Automatically uses hostname and current user
# Generates server and client config (.ovpn)
# ==========================================

# --- Colors for pretty output ---
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

# --- Variables ---
SERVER_NAME=$(hostname)
CLIENT_NAME=$(whoami)
OVPN_DIR="/etc/openvpn/server"
EASYRSA_DIR="/etc/openvpn/easy-rsa"
CLIENT_KEYS_DIR="/etc/openvpn/client_keys"
CLIENT_OVPN_DIR="$HOME/${CLIENT_NAME}_client"
CLIENT_FILE="$CLIENT_OVPN_DIR/${CLIENT_NAME}.ovpn"

# --- Header ---
echo -e "${BOLD}${GREEN}==============================================="
echo -e " 🔧 OpenVPN Server Setup Script"
echo -e "===============================================${RESET}"
echo
echo -e "${BLUE}This script sets up an OpenVPN server and generates a client config.${RESET}"
echo

# --- Show Server and Client Info ---
echo -e "${YELLOW}🔹 Server name: $SERVER_NAME"
echo -e "🔹 Client name: $CLIENT_NAME${RESET}"
echo

# --- Install dependencies ---
info "Installing OpenVPN, Easy-RSA, qrencode..."
sudo apt update
sudo apt install -y openvpn easy-rsa qrencode
success "Dependencies installed."

# --- Setup Easy-RSA ---
info "Setting up Easy-RSA..."
sudo rm -rf "$EASYRSA_DIR"
sudo mkdir -p "$EASYRSA_DIR"
sudo cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"

info "Initializing PKI..."
sudo bash -c "EASYRSA_BATCH=1 EASYRSA_REQ_CN=$SERVER_NAME ./easyrsa init-pki"
info "Building CA..."
sudo bash -c "EASYRSA_BATCH=1 EASYRSA_REQ_CN=$SERVER_NAME ./easyrsa build-ca nopass"
info "Generating client request..."
sudo bash -c "EASYRSA_BATCH=1 EASYRSA_REQ_CN=$SERVER_NAME ./easyrsa gen-req $CLIENT_NAME nopass"
info "Signing client request..."
sudo bash -c "echo yes | EASYRSA_BATCH=1 ./easyrsa sign-req client $CLIENT_NAME"
info "Generating server request..."
sudo bash -c "EASYRSA_BATCH=1 ./easyrsa gen-req server nopass"
info "Signing server request..."
sudo bash -c "echo yes | EASYRSA_BATCH=1 ./easyrsa sign-req server server"
success "Easy-RSA setup complete."

# --- Copy certs/keys to OpenVPN directory ---
info "Copying certificates and keys to OpenVPN directory..."
sudo mkdir -p "$OVPN_DIR"
sudo cp pki/ca.crt pki/issued/server.crt pki/private/server.key pki/issued/${CLIENT_NAME}.crt pki/private/${CLIENT_NAME}.key "$OVPN_DIR/"
success "Certificates and keys copied."

# --- Generate TLS and ECDH keys ---
info "Generating TLS and ECDH keys..."
sudo openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
sudo openvpn --genkey secret "$OVPN_DIR/ta.key"
success "TLS and ECDH keys generated."

# --- OpenVPN server config ---
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
#push "redirect-gateway def1 bypass-dhcp"
#push "dhcp-option DNS 1.1.1.1"
#push "dhcp-option DNS 9.9.9.9"

# PAM + Google Authenticator MFA
plugin /usr/lib/x86_64-linux-gnu/openvpn/plugins/openvpn-plugin-auth-pam.so openvpn
verify-client-cert require
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

# --- Ensure TUN device exists ---
info "Ensuring TUN device exists..."
sudo mkdir -p /dev/net || true
sudo mknod -m 600 /dev/net/tun c 10 200 || true
success "TUN device created."

# --- Set Permissions ---
info "Setting permissions for OpenVPN directory..."
sudo chown -R root:root "$OVPN_DIR"
sudo chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
sudo chmod 644 "$OVPN_DIR/server.crt" "$OVPN_DIR/ca.crt" "$OVPN_DIR/${CLIENT_NAME}.crt"
success "Permissions set."

# --- Enable & start OpenVPN ---
info "Enabling and starting OpenVPN service..."
sudo systemctl daemon-reload
sudo systemctl enable openvpn-server@server.service
sudo systemctl restart openvpn-server@server.service
success "OpenVPN service started."

# --- Prepare client files ---
info "Preparing client files..."

# Move client keys directory to a more secure location
sudo mkdir -p "$CLIENT_KEYS_DIR"
sudo cp "$OVPN_DIR/${CLIENT_NAME}.crt" "$OVPN_DIR/${CLIENT_NAME}.key" "$CLIENT_KEYS_DIR/"
sudo chown root:root "$CLIENT_KEYS_DIR"/*

mkdir -p "$CLIENT_OVPN_DIR"

cat > "$CLIENT_FILE" <<EOF
client
dev tun
proto udp
remote $SERVER_NAME 1194
resolv-retry infinite
nobind
user nobody
group nogroup
persist-key
persist-tun
auth SHA256
cipher AES-256-GCM
key-direction 1
remote-cert-tls server
auth-user-pass
verb 3

<ca>
$(sudo cat "$OVPN_DIR/ca.crt")
</ca>

<cert>
$(sudo cat "$CLIENT_KEYS_DIR/${CLIENT_NAME}.crt")
</cert>

<key>
$(sudo cat "$CLIENT_KEYS_DIR/${CLIENT_NAME}.key")
</key>

<tls-crypt>
$(sudo cat "$OVPN_DIR/ta.key")
</tls-crypt>
EOF
chmod 600 "$CLIENT_FILE"
success "Client configuration file created at $CLIENT_FILE"

# --- Final Message ---
echo -e "\n${BOLD}${GREEN}✅ OpenVPN Setup Complete!${RESET}"
echo -e "Client configuration file is ready: ${GREEN}$CLIENT_FILE${RESET}"
echo "Use this .ovpn file to connect to your VPN server."
echo
