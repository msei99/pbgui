#!/usr/bin/bash
set -euo pipefail

# check if Linux distribution using lsb_release is ubuntu else exit
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    echo "This script is only for Ubuntu"
    exit 1
fi

# Refresh apt metadata before checking package candidates
sudo apt update

# Add deadsnakes/ppa only when python3.12-venv is not provided by current apt sources
if apt-cache policy python3.12-venv | grep -Eq 'Candidate:\s+\(none\)'; then
    sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt update
fi

# Install git, python3.12-venv, rclone, rustc and cargo
sudo apt install git python3.12-venv rclone rustc cargo sshpass -y
sudo apt install rustup -y

# Update rust
rustup update 1.90.0

# get current directory
DIR="$(pwd)"
PB7_REF="befaa9b7aa89e00ee55704221b39621ad700ac36"
if [[ ! "$PB7_REF" =~ ^[0-9a-f]{40}$ ]]; then
    echo "Invalid embedded PB7 pin." >&2
    exit 1
fi

# Clone the passivbot repository pb7
git clone --no-checkout https://github.com/enarjord/passivbot.git pb7
PB7_VERSION="$(git -C pb7 show "$PB7_REF:src/passivbot_version.py")"
if [[ "$PB7_VERSION" != *'__version__ = "7.'* ]]; then
    echo "Pinned Passivbot checkout is not PB7; refusing installation." >&2
    exit 1
fi
git -C pb7 checkout --detach "$PB7_REF"
# Create a virtual environment for pb7
python3.12 -m venv "$DIR/venv_pb7"
# Activate the virtual environment
source "$DIR/venv_pb7/bin/activate"
# Upgrade pip
pip install --upgrade pip
# Install the requirements for pb7
pip install -r pb7/requirements.txt
# Build passivbot-rust with maturin
cd pb7/passivbot-rust
maturin develop
cd ../..
# deactivate the virtual environment
deactivate

# Clone the pbgui repository
git clone https://github.com/msei99/pbgui.git
# Create a virtual environment for pbgui
python3.12 -m venv "$DIR/venv_pbgui"
# Activate the virtual environment
source "$DIR/venv_pbgui/bin/activate"
# Upgrade pip
pip install --upgrade pip
# Install the requirements for pbgui
pip install -r pbgui/requirements.txt

# Create start.sh file to start pbgui scripts
cat > "$DIR/pbgui/start.sh" << EOF
#!/usr/bin/env bash
set -euo pipefail

# Go to app directory
cd "$DIR/pbgui"

# Activate virtual environment
source "$DIR/venv_pbgui/bin/activate"

# Python executable inside virtualenv
PYTHON_BIN="$DIR/venv_pbgui/bin/python"

# Start scripts with nohup so they persist after cron exits
nohup "\$PYTHON_BIN" PBRun.py &
nohup "\$PYTHON_BIN" PBData.py &
nohup "\$PYTHON_BIN" PBCoinData.py &
nohup "\$PYTHON_BIN" PBApiServer.py &
EOF

# Make start.sh executable
chmod +x "$DIR/pbgui/start.sh"

# Create pbgui.ini
echo "[main]" > pbgui/pbgui.ini
echo "pb7dir = $DIR/pb7" >> pbgui/pbgui.ini
echo "pb7venv = $DIR/venv_pb7/bin/python" >> pbgui/pbgui.ini
echo "role = master" >> pbgui/pbgui.ini

# start pbgui
cd pbgui
echo ""
echo "starting PBGui Services in background with command start.sh"
echo ""
echo 'Login with password: PBGui$Bot!'
./start.sh
