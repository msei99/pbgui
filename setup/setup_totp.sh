#!/usr/bin/env bash
set -euo pipefail

# ==============================================
# 🔐 Google Authenticator Setup for OpenVPN
# ==============================================
# Configures PAM + Google Authenticator MFA for OpenVPN users.
# ==============================================

# --- Colors for Pretty Output ---
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

# --- Script Header ---
echo -e "${BOLD}${GREEN}==============================================="
echo -e " 🔐 Google Authenticator Setup for OpenVPN"
echo -e "===============================================${RESET}"
echo

# --- Get the logged-in user ---
USER_NAME=$(whoami)

info "Setting up Google Authenticator MFA for OpenVPN user: $USER_NAME"

# --- Install Required Packages ---
info "Installing required packages..."
sudo apt update
sudo apt install -y libpam-google-authenticator oathtool qrencode
success "Packages installed."

# --- Set up PAM for OpenVPN ---
info "Configuring PAM for OpenVPN with Google Authenticator..."
GA_PAM_DIR="/etc/openvpn/google-auth"
sudo mkdir -p "$GA_PAM_DIR"
sudo chmod 700 "$GA_PAM_DIR"

# Add Google Authenticator PAM config for OpenVPN
sudo tee /etc/pam.d/openvpn > /dev/null <<'EOF'
auth required pam_google_authenticator.so secret=/etc/openvpn/google-auth/${USER} user=root
account required pam_unix.so
EOF
success "PAM configuration updated."

# --- Generate Google Authenticator Secrets ---
USER_HOME=$(eval echo "~$USER_NAME")
GA_FILE="$USER_HOME/GA-QR.txt"
GA_SECRET_FILE="$USER_HOME/.google_authenticator"

# Generate a random secret key
SECRET=$(head -c 10 /dev/urandom | base32 | tr -d '=' | tr -d '\n' | cut -c1-16)
ISSUER="OpenVPN"
ACCOUNT="$USER_NAME@$(hostname)"

# Create the Google Authenticator secret file
info "Creating Google Authenticator secret for user: $USER_NAME"
sudo -u "$USER_NAME" bash -c "cat > '$GA_SECRET_FILE' <<EOF
$SECRET
RESETTING_TIME_SKEW
RATE_LIMIT 3 30
WINDOW_SIZE 17
DISALLOW_REUSE
TOTP_AUTH
EOF"
sudo chmod 600 "$GA_SECRET_FILE"

# Generate the ASCII QR code for the user
info "Generating ASCII QR code..."
sudo -u "$USER_NAME" bash -c "qrencode -t ASCII 'otpauth://totp/$ACCOUNT?secret=$SECRET&issuer=$ISSUER' > '$GA_FILE'"
sudo chmod 644 "$GA_FILE"
success "QR code generated."

# --- Copy Google Authenticator Secrets to PAM Directory ---
info "Copying Google Authenticator secret to PAM directory..."
sudo cp "$GA_SECRET_FILE" "$GA_PAM_DIR/$USER_NAME"
sudo chown root:root "$GA_PAM_DIR/$USER_NAME"
sudo chmod 600 "$GA_PAM_DIR/$USER_NAME"
success "Google Authenticator setup complete."

# --- Final Output ---
echo
echo -e "${GREEN}✅ Google Authenticator configured for '$USER_NAME'.${RESET}"
echo -e "${BLUE}📄 ASCII QR code saved at: $GA_FILE${RESET}"
echo -e "${BLUE}You can now scan the QR code with your Google Authenticator app.${RESET}"
echo
