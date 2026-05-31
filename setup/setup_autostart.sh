#!/usr/bin/env bash
set -euo pipefail

# --- Variables ---
USER=$(whoami)
BASE_DIR="/home/$USER/pbgui"
START_SCRIPT="$BASE_DIR/start.sh"
CRON_JOB="@reboot $START_SCRIPT"

# --- Functions ---
info()    { echo -e "\e[36m[INFO]\e[0m $*"; }
success() { echo -e "\e[32m[ OK ]\e[0m $*"; }
error()   { echo -e "\e[31m[ERR ]\e[0m $*" >&2; }

# Make the start.sh script executable
chmod +x "$START_SCRIPT"
success "start.sh made executable."

# --- Add cron jobs to autostart on reboot ---
info "Adding cron jobs to autostart scripts on reboot..."

# Use a temporary file to safely update crontab
tmpfile=$(mktemp)
crontab -l 2>/dev/null > "$tmpfile" || true

# Add the jobs if they don't already exist
grep -Fq "$CRON_JOB" "$tmpfile" || echo "$CRON_JOB" >> "$tmpfile"

# Install the updated crontab
crontab "$tmpfile"
rm "$tmpfile"
success "Cron job added/verified successfully."

echo -e "\n[INFO] Setup complete! PBGui services will now autostart on reboot."
