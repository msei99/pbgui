#!/usr/bin/env bash
set -euo pipefail

# --- Variables ---
USER=$(whoami)
BASE_DIR="/home/$USER/pbgui"
BASE_DIR_VENV="/home/$USER/pbgui_venv"
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

# Path to the streamlit process name
STREAMLIT_PROCESS_NAME="streamlit"

# Function to check if streamlit is running
is_streamlit_running() {
    pgrep -f "\$STREAMLIT_PROCESS_NAME" > /dev/null 2>&1
}

# Change directory to where your app is located
cd "$BASE_DIR"

# Activate the virtual environment
source "$BASE_DIR_VENV/bin/activate"

# Check if streamlit is already running
if is_streamlit_running; then
    echo "[INFO] Streamlit is already running. Skipping start."
else
    echo "[INFO] Streamlit is not running. Starting streamlit..."
    streamlit run pbgui.py &
    echo "[INFO] Streamlit started."
fi
EOF

# Make the start_streamlit.sh script executable
chmod +x "$START_SCRIPT"
success "start_streamlit.sh script created and made executable."

# --- Add cron job to autostart on reboot ---
info "Adding cron jobs to autostart scripts on reboot..."

# Function to add a cron job if it doesn't already exist
add_cron_job() {
    local job="$1"
    if crontab -l 2>/dev/null | grep -Fq "$job"; then
        info "Cron job already exists: $job"
    else
        (crontab -l 2>/dev/null; echo "$job") | crontab -
        success "Cron job added: $job"
    fi
}

add_cron_job "$CRON_JOB"
add_cron_job "$CRON_JOB2"

echo -e "\n[INFO] Setup complete! The streamlit app will now autostart on reboot."
