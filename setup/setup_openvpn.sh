#!/usr/bin/env bash
# 🚀 OpenVPN Server Setup with Google Authenticator (2FA)
# Usage:
#   bash setup_openvpn.sh <user_name>
#   OR
#   curl -fsSL <URL> | bash -s -- <user_name>

set -euo pipefail

# ----------------------
# Color definitions
# ----------------------
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
CYAN="\033[1;36m"
NC="\033[0m" # No Color

echo -e "${CYAN}🚀 Starting OpenVPN server setup...${NC}"

# ----------------------
# Argument handling
# ----------------------
if [ "$#" -ne 1 ]; then
    echo -e "${YELLOW}Usage:${NC} bash setup_openvpn.sh <user_name>"
    echo -e "Or via curl: curl -fsSL <URL> | bash -s -- <user_name>"
    exit 1
fi

USER_NAME="$1"

# ----------------------
# Check Ubuntu
# ----------------------
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    echo -e "${RED}Error:${NC} This script only works on Ubuntu."
    exit 1
fi

# ----------------------
# Install dependencies
# ----------------------
echo -e "${CYAN}📦 Installing dependencies...${NC}"
sudo apt update
sudo apt install -y openvpn easy-rsa libpam-google-authenticator oathtool qrencode ufw

# ----------------------
# Setup Easy-RSA
# ----------------------
EASYRSA_DIR="/root/easy-rsa"
rm -rf "$EASYRSA_DIR"
mkdir -p "$EASYRSA_DIR"
cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"

export EASYRSA_BATCH=1
export EASYRSA_REQ_CN="server"

echo -e "${CYAN}🛠 Initializing PKI and generating server certs...${NC}"
./easyrsa init-pki
./easyrsa build-ca nopass
./easyrsa gen-req server nopass
echo "yes" | ./easyrsa sign-req server server

# ----------------------
# Copy certs to OpenVPN
# ----------------------
OVPN_DIR="/etc/openvpn/server"
sudo mkdir -p "$OVPN_DIR"
sudo cp pki/ca.crt pki/issued/server.crt pki/private/server.key "$OVPN_DIR/"

# ----------------------
# Generate ECDH and tls-crypt key
# ----------------------
sudo openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
sudo openvpn --genkey secret "$OVPN_DIR/ta.key"

# ----------------------
# OpenVPN server config
# ----------------------
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

# ----------------------
# Ensure /dev/net/tun exists
# ----------------------
if [ ! -c /dev/net/tun ]; then
    sudo mkdir -p /dev/net
    sudo mknod /dev/net/tun c 10 200
    sudo chmod 600 /dev/net/tun
fi

# ----------------------
# Permissions
# ----------------------
sudo chown -R root:root "$OVPN_DIR"
sudo chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
sudo chmod 644 "$OVPN_DIR/ca.crt" "$OVPN_DIR/server.crt"

# ----------------------
# PAM service for OpenVPN + Google Authenticator
# ----------------------
GA_PAM_DIR="/etc/openvpn/google-auth"
sudo mkdir -p "$GA_PAM_DIR"
sudo chmod 700 "$GA_PAM_DIR"

sudo tee /etc/pam.d/openvpn > /dev/null <<EOF
auth required pam_google_authenticator.so secret=/etc/openvpn/google-auth/${USER_NAME} user=root
account required pam_unix.so
EOF

# ----------------------
# Enable & start OpenVPN
# ----------------------
sudo systemctl daemon-reload
sudo systemctl enable openvpn-server@server.service
sudo systemctl restart openvpn-server@server.service

# ----------------------
# Firewall setup (separate)
# ----------------------
sudo ufw allow 1194/udp
sudo ufw allow OpenSSH
sudo ufw --force enable

# ----------------------
# Google Authenticator setup
# ----------------------
USER_HOME=$(eval echo "~$USER_NAME")
GA_FILE="$USER_HOME/GA-QR.txt"
GA_SECRET_FILE="$USER_HOME/.google_authenticator"

SECRET=$(head -c 10 /dev/urandom | base32 | tr -d '=' | tr -d '\n' | cut -c1-16)
ISSUER="OpenVPN"
ACCOUNT="$USER_NAME@$(hostname)"

sudo -u "$USER_NAME" bash -c "cat > '$GA_SECRET_FILE' <<EOF
$SECRET
RESETTING_TIME_SKEW
RATE_LIMIT 3 30
WINDOW_SIZE 17
DISALLOW_REUSE
TOTP_AUTH
EOF"

sudo chmod 600 "$GA_SECRET_FILE"
sudo -u "$USER_NAME" bash -c "qrencode -t ASCII 'otpauth://totp/$ACCOUNT?secret=$SECRET&issuer=$ISSUER' > '$GA_FILE'"
sudo chmod 644 "$GA_FILE"

sudo cp "$GA_SECRET_FILE" "$GA_PAM_DIR/$USER_NAME"
sudo chown root:root "$GA_PAM_DIR/$USER_NAME"
sudo chmod 600 "$GA_PAM_DIR/$USER_NAME"

echo -e "${GREEN}✅ Google Authenticator QR code available at:${NC} $GA_FILE"

# ----------------------
# Generate client .ovpn securely
# ----------------------
CLIENT_DIR="$USER_HOME/${USER_NAME}_client"
mkdir -p "$CLIENT_DIR"
CLIENT_FILE="$CLIENT_DIR/${USER_NAME}.ovpn"

cat > "$CLIENT_FILE" <<EOF
client
dev tun
proto udp
remote $(hostname) 1194
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

# Username: $USER_NAME, TOTP from QR: $GA_FILE
EOF

chmod 600 "$CLIENT_FILE"
sudo chown "$USER_NAME:$USER_NAME" "$CLIENT_FILE"

echo -e "${GREEN}✅ Client config created:${NC} $CLIENT_FILE"
echo -e "Use username '$USER_NAME' and the TOTP from the ASCII QR code to connect."
echo -e "${CYAN}🎉 OpenVPN setup complete!${NC}"
