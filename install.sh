#!/usr/bin/bash
# check if Linux distribution using lsb_release is ubuntu else exit
if [ "$(lsb_release -si)" != "Ubuntu" ]; then
    echo "This script is only for Ubuntu"
    exit 1
fi
# Add deadsnakes/ppa for installing python3.10
sudo add-apt-repository ppa:deadsnakes/ppa -y

# Install git, python3.10-venv, rclone, rustc and cargo
sudo apt update
sudo apt install git python3.10-venv rclone rustc cargo sshpass -y
sudo apt install rustup -y

# Update rust
rustup update 1.90.0

# get current directory
DIR=$(pwd)

# Clone the pb6 repository to pb6
git clone https://github.com/enarjord/passivbot.git pb6
# Checkout v6.1.4b_latest_v6
cd pb6
git checkout v6.1.4b_latest_v6
cd ..
# Create a virtual environment for pb6
python3.10 -m venv $DIR/venv_pb6
# Activate the virtual environment
source $DIR/venv_pb6/bin/activate
# Upgrade pip
pip install --upgrade pip
# Install the requirements for pb6
pip install -r pb6/requirements.txt
# deactivate the virtual environment
deactivate

# Clone the passivbot repository pb7
git clone https://github.com/enarjord/passivbot.git pb7
# Create a virtual environment for pb7
python3.10 -m venv $DIR/venv_pb7
# Activate the virtual environment
source $DIR/venv_pb7/bin/activate
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
python3.10 -m venv $DIR/venv_pbgui
# Activate the virtual environment
source $DIR/venv_pbgui/bin/activate
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
nohup "\$PYTHON_BIN" PBRemote.py &
nohup "\$PYTHON_BIN" PBStat.py &
nohup "\$PYTHON_BIN" PBData.py &
nohup "\$PYTHON_BIN" PBCoinData.py &
EOF

# Make start.sh executable
chmod +x "$DIR/pbgui/start.sh"

# Create pbgui.ini
echo "[main]" > pbgui/pbgui.ini
echo "pbdir = $DIR/pb6" >> pbgui/pbgui.ini
echo "pb7dir = $DIR/pb7" >> pbgui/pbgui.ini
echo "pbvenv = $DIR/venv_pb6/bin/python" >> pbgui/pbgui.ini
echo "pb7venv = $DIR/venv_pb7/bin/python" >> pbgui/pbgui.ini
echo "role = master" >> pbgui/pbgui.ini

# start pbgui
cd pbgui
echo ""
echo "starting PBGui in background with command start_streamlit.sh"
echo "starting PBGui Services in background with command start.sh"
echo ""
echo 'Login with password: PBGui$Bot!'
./start_streamlit.sh
