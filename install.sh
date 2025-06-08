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

# Create a start.sh file to start pbgui
echo "cd $DIR/pbgui" > pbgui/start.sh
echo "source $DIR/venv_pbgui/bin/activate" >> pbgui/start.sh
echo "python PBRun.py &" >> pbgui/start.sh
echo "python PBRemote.py &" >> pbgui/start.sh
echo "python PBStat.py &" >> pbgui/start.sh
echo "python PBData.py &" >> pbgui/start.sh
echo "python PBCoinData.py &" >> pbgui/start.sh
# Make the start.sh file executable
chmod +x pbgui/start.sh

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
echo "starting pbgui with command streamlit run pbgui.py"
echo "press ctrl+c for stopping pbgui"
echo "you can run pbgui in background with this command: streamlit run pbgui.py &"
echo ""
echo 'Login with password: PBGui$Bot!'
streamlit run pbgui.py
