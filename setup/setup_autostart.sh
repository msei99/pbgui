#!/usr/bin/env bash
set -euo pipefail

# --- Variables ---
USER=$(whoami)
BASE_DIR="/home/$USER/pbgui"
BASE_DIR_VENV="/home/$USER/venv_pbgui"
START_SCRIPT="$BASE_DIR/start_streamlit.sh"
CRON_JOB="@reboot $BASE_DIR/start_streamlit.sh"
CRON_JOB2="@reboot $BASE_DIR/start.sh"

# --- Functions ---
info()    { echo -e "\e[36m[INFO]\e[0m $*"; }
success() { echo -e "\e[32m[ OK ]\e[0m $*"; }
error()   { echo -e "\e[31m[ERR ]\e[0m $*" >&2; }

# --- Create start_streamlit.sh script ---
info "Creating start_streamlit.sh script for Streamlit..."
cat << EOF > "$START_SCRIPT"
#!/usr/bin/env bash
set -euo pipefail

STREAMLIT_SCRIPT="pbgui.py"

# Function to check if Streamlit is running
is_streamlit_running() {
    pgrep -f "streamlit run \$STREAMLIT_SCRIPT" > /dev/null 2>&1
}

# Change directory to where your app is located
cd "$BASE_DIR"

# Activate the virtual environment
source "$BASE_DIR_VENV/bin/activate"

# Check if Streamlit is already running
if is_streamlit_running; then
    echo "[INFO] Streamlit is already running. Skipping start."
else
    echo "[INFO] Streamlit is not running. Starting streamlit..."
    nohup streamlit run "\$STREAMLIT_SCRIPT" &
    echo "[INFO] Streamlit started."
fi
EOF

# Make the start_streamlit.sh script executable
chmod +x "$START_SCRIPT"
success "start_streamlit.sh script created and made executable."

# --- Add cron jobs to autostart on reboot ---
info "Adding cron jobs to autostart scripts on reboot..."

# Use a temporary file to safely update crontab
tmpfile=$(mktemp)
crontab -l 2>/dev/null > "$tmpfile" || true

# Add the jobs if they don't already exist
grep -Fq "$CRON_JOB" "$tmpfile" || echo "$CRON_JOB" >> "$tmpfile"
grep -Fq "$CRON_JOB2" "$tmpfile" || echo "$CRON_JOB2" >> "$tmpfile"

# Install the updated crontab
crontab "$tmpfile"
rm "$tmpfile"
success "Cron jobs added/verified successfully."

echo -e "\n[INFO] Setup complete! The Streamlit app will now autostart on reboot."
