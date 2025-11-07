#!/usr/bin/env bash
# ======================================================
# 🔧 OpenVPN Server Setup - Secure Configuration
# ------------------------------------------------------
# This script sets up an OpenVPN server on Ubuntu.
# Google Authenticator 2FA and firewall configuration
# should be done in separate scripts.
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
# Fix ugly /dev/fd/xx usage when using curl or process substitution
if [[ "${BASH_SOURCE[0]}" == /dev/fd/* ]]; then
    SCRIPT_NAME="setup_openvpn.sh"
else
    SCRIPT_NAME="${BASH_SOURCE[0]}"
fi

if [ "$#" -ne 1 ]; then
    echo -e "${YELLOW}Usage:${RESET} bash $SCRIPT_NAME <user_name>"
    echo -e "Or via curl: curl -fsSL <URL> | bash -s -- <user_name>"
    exit 1
fi

USER_NAME="$1"
echo
info "Configuration:"
echo "  • OpenVPN will be configured for user: $USER_NAME"
echo

# ----------[ Install dependencies ]----------
info "Installing necessary packages..."
sudo apt update
sudo apt install -y openvpn easy-rsa openssl
success "Required packages installed."

# ----------[ Setup Easy-RSA PKI ]----------
EASYRSA_DIR="/etc/openvpn/easy-rsa"
info "Setting up Easy-RSA in $EASYRSA_DIR..."
sudo rm -rf "$EASYRSA_DIR"
sudo mkdir -p "$EASYRSA_DIR"
sudo cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"

# Use hostname-based CA name to avoid prompts
HOSTNAME=$(hostname)
CA_NAME="OpenVPN-CA-$HOSTNAME"

sudo ./easyrsa init-pki
sudo ./easyrsa --batch --req-cn="$CA_NAME" build-ca nopass
sudo ./easyrsa gen-req server nopass
echo "yes" | sudo ./easyrsa sign-req server server
success "PKI and server certificates generated."

# ----------[ Copy certificates to OpenVPN directory ]----------
OVPN_DIR="/etc/openvpn/server"
sudo mkdir -p "$OVPN_DIR"
sudo cp pki/ca.crt pki/issued/server.crt pki/private/server.key "$OVPN_DIR/"

# ----------[ Generate ECDH and tls-crypt key ]----------
info "Generating ECDH and TLS keys..."
sudo openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
sudo openvpn --genkey secret "$OVPN_DIR/ta.key"
success "Keys generated."

# ----------[ OpenVPN server config ]----------
info "Writing OpenVPN server configuration..."
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
success "OpenVPN configuration written."

# ----------[ Ensure /dev/net/tun exists ]----------
info "Ensuring /dev/net/tun exists..."
if [ ! -c /dev/net/tun ]; then
    sudo mkdir -p /dev/net
    sudo mknod /dev/net/tun c 10 200
    sudo chmod 600 /dev/net/tun
fi
success "/dev/net/tun ready."

# ----------[ Set permissions ]----------
sudo chown -R root:root "$OVPN_DIR"
sudo chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
sudo chmod 644 "$OVPN_DIR/ca.crt" "$OVPN_DIR/server.crt"
success "Certificate and key permissions set."

# ----------[ Enable & start OpenVPN service ]----------
info "Enabling and starting OpenVPN service..."
sudo systemctl daemon-reload
sudo systemctl enable openvpn-server@server.service
sudo systemctl restart openvpn-server@server.service
success "OpenVPN service started."

# ----------[ Client .ovpn generation ]----------
info "Generating client .ovpn configuration..."
USER_HOME=$(eval echo "~$USER_NAME")
CLIENT_DIR="$USER_HOME/${USER_NAME}_client"
mkdir -p "$CLIENT_DIR"
CLIENT_FILE="$CLIENT_DIR/${USER_NAME}.ovpn"

cat > "$CLIENT_FILE" <<EOF
client
dev tun
proto udp
remote $(hostname -I | awk '{print $1}') 1194
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
EOF

sudo chown "$USER_NAME":"$USER_NAME" "$CLIENT_FILE"
chmod 600 "$CLIENT_FILE"
success "Client config created at $CLIENT_FILE"

# ----------[ Summary ]----------
echo
echo -e "${BOLD}✅ OpenVPN Setup Complete${RESET}"
echo -e "-----------------------------------------------"
echo -e "• Server Directory : ${GREEN}${OVPN_DIR}${RESET}"
echo -e "• Client Config   : ${GREEN}${CLIENT_FILE}${RESET}"
echo -e "-----------------------------------------------"
echo -e "🎉 Use username '$USER_NAME' and client .ovpn to connect"
echo

exit 0
