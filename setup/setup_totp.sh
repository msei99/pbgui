#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Google Authenticator Setup for OpenVPN
# =============================================================================
# Configures PAM + Google Authenticator MFA for OpenVPN users.
# =============================================================================

echo "🔐 Setting up Google Authenticator MFA for OpenVPN..."

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <vpn_username>"
    exit 1
fi

USER_NAME="$1"

sudo apt install -y libpam-google-authenticator oathtool qrencode

GA_PAM_DIR="/etc/openvpn/google-auth"
sudo mkdir -p "$GA_PAM_DIR"
sudo chmod 700 "$GA_PAM_DIR"

sudo tee /etc/pam.d/openvpn > /dev/null <<'EOF'
auth required pam_google_authenticator.so secret=/etc/openvpn/google-auth/${USER} user=root
account required pam_unix.so
EOF

USER_HOME=$(eval echo "~$USER_NAME")
GA_FILE="$USER_HOME/GA-QR.txt"
GA_SECRET_FILE="$USER_HOME/.google_authenticator"

SECRET=$(head -c 10 /dev/urandom | base32 | tr -d '=' | tr -d '\n' | cut -c1-16)
ISSUER="OpenVPN"
ACCOUNT="$USER_NAME@$(hostname)"

sudo -u "$USER_NAME" bash -c 'cat > "'"$GA_SECRET_FILE"'" <<EOF
'"$SECRET"'
" RESETTING_TIME_SKEW
" RATE_LIMIT 3 30
" WINDOW_SIZE 17
" DISALLOW_REUSE
" TOTP_AUTH
EOF'

sudo chmod 600 "$GA_SECRET_FILE"
sudo -u "$USER_NAME" bash -c "qrencode -t ASCII 'otpauth://totp/$ACCOUNT?secret=$SECRET&issuer=$ISSUER' > '$GA_FILE'"
sudo chmod 644 "$GA_FILE"

sudo cp "$GA_SECRET_FILE" "$GA_PAM_DIR/$USER_NAME"
sudo chown root:root "$GA_PAM_DIR/$USER_NAME"
sudo chmod 600 "$GA_PAM_DIR/$USER_NAME"

echo "✅ Google Authenticator configured for '$USER_NAME'."
echo "📄 ASCII QR code saved at: $GA_FILE"
