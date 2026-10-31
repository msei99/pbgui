#!/usr/bin/env bash
set -euo pipefail

# Root needs no privilege wrapper; other users must have sudo.
SUDO=()
if [[ "$(id -u)" -ne 0 ]]; then
    if ! command -v sudo >/dev/null 2>&1; then
        echo "Run as root or install sudo before running this script." >&2
        exit 1
    fi
    SUDO=(sudo)
fi

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
"${SUDO[@]}" apt update
"${SUDO[@]}" apt install -y libpam-google-authenticator oathtool qrencode
success "Packages installed."

# --- Set up PAM for OpenVPN ---
info "Configuring PAM for OpenVPN with Google Authenticator..."
GA_PAM_DIR="/etc/openvpn/google-auth"
"${SUDO[@]}" mkdir -p "$GA_PAM_DIR"
"${SUDO[@]}" chmod 700 "$GA_PAM_DIR"

# Add Google Authenticator PAM config for OpenVPN
"${SUDO[@]}" tee /etc/pam.d/openvpn > /dev/null <<'EOF'
auth required pam_google_authenticator.so secret=/etc/openvpn/google-auth/${USER} user=root
account required pam_unix.so
EOF
success "PAM configuration updated."

# --- Generate Google Authenticator Secrets ---
USER_HOME=$(getent passwd "$USER_NAME" | cut -d: -f6)
if [[ -z "$USER_HOME" || ! -d "$USER_HOME" ]]; then
    error "Could not determine the current user's home directory."
    exit 1
fi
GA_FILE="$USER_HOME/GA-QR.txt"
GA_SECRET_FILE="$USER_HOME/.google_authenticator"

# Generate a random secret key
SECRET=$(head -c 10 /dev/urandom | base32 | tr -d '=' | tr -d '\n' | cut -c1-16)
ISSUER="OpenVPN"
ACCOUNT="$USER_NAME@$(hostname)"

# Create the Google Authenticator secret file
info "Creating Google Authenticator secret for user: $USER_NAME"
(umask 077; cat > "$GA_SECRET_FILE" <<EOF
$SECRET
" RESETTING_TIME_SKEW
" RATE_LIMIT 3 30
" WINDOW_SIZE 17
" DISALLOW_REUSE
" TOTP_AUTH
EOF
)
"${SUDO[@]}" chmod 600 "$GA_SECRET_FILE"

# Generate the ASCII QR code for the user
info "Generating ASCII QR code..."
printf '%s' "otpauth://totp/$ACCOUNT?secret=$SECRET&issuer=$ISSUER" | qrencode -t ASCII > "$GA_FILE"
"${SUDO[@]}" chmod 644 "$GA_FILE"
success "QR code generated."

# --- Copy Google Authenticator Secrets to PAM Directory ---
info "Copying Google Authenticator secret to PAM directory..."
"${SUDO[@]}" cp "$GA_SECRET_FILE" "$GA_PAM_DIR/$USER_NAME"
"${SUDO[@]}" chown root:root "$GA_PAM_DIR/$USER_NAME"
"${SUDO[@]}" chmod 600 "$GA_PAM_DIR/$USER_NAME"
success "Google Authenticator setup complete."

# --- Final Output ---
echo
echo -e "${GREEN}✅ Google Authenticator configured for '$USER_NAME'.${RESET}"
echo -e "${BLUE}📄 ASCII QR code saved at: $GA_FILE${RESET}"
echo -e "${BLUE}You can now scan the QR code with your Google Authenticator app.${RESET}"
echo
