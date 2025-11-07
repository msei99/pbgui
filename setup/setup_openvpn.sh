#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# 🧩 OpenVPN Server Setup Script (Ubuntu)
# =============================================================================
# Installs and configures a hardened OpenVPN 2.6+ server.
# Generates certificates, configures security, and creates a client .ovpn file.
# Usage: bash setup_openvpn.sh <user_name>
# =============================================================================

# --- Colors (safe) ---
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    BLUE='\033[0;34m'
    YELLOW='\033[1;33m'
    RED='\033[0;31m'
    BOLD='\033[1m'
    NC='\033[0m'
else
    GREEN=""; BLUE=""; YELLOW=""; RED=""; BOLD=""; NC=""
fi

echo -e "${BLUE}🚀 Starting OpenVPN server setup...${NC}"

# --- Check Ubuntu ---
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    echo -e "${RED}❌ This script only works on Ubuntu.${NC}"
    exit 1
fi

# --- Args ---
if [ "$#" -ne 1 ]; then
    echo -e "${YELLOW}Usage:${NC} bash $0 <user_name>"
    exit 1
fi

USER_NAME="$1"

echo -e "${BLUE}🔧 Installing dependencies...${NC}"
sudo apt update -y
sudo apt install -y openvpn easy-rsa

# --- Easy-RSA setup ---
EASYRSA_DIR="/root/easy-rsa"
rm -rf "$EASYRSA_DIR"
mkdir -p "$EASYRSA_DIR"
cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"

export EASYRSA_BATCH=1
export EASYRSA_REQ_CN="server"

echo -e "${BLUE}🔐 Generating CA and server certificates...${NC}"
./easyrsa init-pki
./easyrsa build-ca nopass
./easyrsa gen-req server nopass
echo "yes" | ./easyrsa sign-req server server

# --- Copy certs/keys to OpenVPN dir ---
OVPN_DIR="/etc/openvpn/server"
sudo mkdir -p "$OVPN_DIR"
sudo cp pki/ca.crt pki/issued/server.crt pki/private/server.key "$OVPN_DIR/"

# --- Generate ECDH and tls-crypt key ---
echo -e "${BLUE}🧬 Generating ECDH and tls-crypt keys...${NC}"
sudo openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
sudo openvpn --genkey secret "$OVPN_DIR/ta.key"

# --- OpenVPN configuration ---
echo -e "${BLUE}⚙️  Writing OpenVPN server configuration...${NC}"
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

# --- Ensure /dev/net/tun exists ---
if [ ! -c /dev/net/tun ]; then
    sudo mkdir -p /dev/net
    sudo mknod /dev/net/tun c 10 200
    sudo chmod 600 /dev/net/tun
fi

# --- Permissions ---
sudo chown -R root:root "$OVPN_DIR"
sudo chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
sudo chmod 644 "$OVPN_DIR/ca.crt" "$OVPN_DIR/server.crt"

# --- Enable & start OpenVPN ---
echo -e "${BLUE}🚦 Enabling and starting OpenVPN service...${NC}"
sudo systemctl daemon-reload
sudo systemctl enable openvpn-server@server.service
sudo systemctl restart openvpn-server@server.service

# --- Generate client .ovpn securely ---
echo -e "${BLUE}📦 Generating secure client configuration...${NC}"

USER_HOME=$(eval echo "~$USER_NAME")
CLIENT_DIR="$USER_HOME/openvpn_client"
CLIENT_FILE="$CLIENT_DIR/${USER_NAME}.ovpn"
SERVER_IP=$(hostname -I | awk '{print $1}')

sudo mkdir -p "$CLIENT_DIR"
sudo chmod 700 "$CLIENT_DIR"
sudo chown "$USER_NAME:$USER_NAME" "$CLIENT_DIR"

sudo tee "$CLIENT_FILE" > /dev/null <<EOF
client
dev tun
proto udp
remote $SERVER_IP 1194
resolv-retry infinite
nobind
persist-key
persist-tun
auth SHA256
cipher AES-256-GCM
key-direction 1
remote-cert-tls server
auth-user-pass
verb 3

<ca>
$(sudo cat /etc/openvpn/server/ca.crt)
</ca>

<cert>
$(sudo cat /etc/openvpn/server/server.crt)
</cert>

<key>
$(sudo cat /etc/openvpn/server/server.key)
</key>

<tls-crypt>
$(sudo cat /etc/openvpn/server/ta.key)
</tls-crypt>

# Username: $USER_NAME
# Enter your TOTP when prompted.
EOF

sudo chmod 600 "$CLIENT_FILE"
sudo chown "$USER_NAME:$USER_NAME" "$CLIENT_FILE"

echo -e "${GREEN}✅ Client configuration created:${NC} $CLIENT_FILE"
echo -e "   Only accessible by user '${BOLD}$USER_NAME${NC}'"
echo "-----------------------------------------------------"
echo -e "💾 To download securely:"
echo -e "   scp ${BOLD}$USER_NAME@<server_ip>:${CLIENT_FILE}${NC} ./"
echo "-----------------------------------------------------"

echo -e "${GREEN}🎉 OpenVPN setup complete!${NC}"
