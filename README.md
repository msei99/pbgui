# GUI for Passivbot

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/Y8Y216Q3QS)

## Contact/Support on Telegram: https://t.me/+kwyeyrmjQ-lkYTJk
## Join one of my copytrading to support: https://manicpt.streamlit.app/
I offer API-Service where I run passivbot for you as a Service.
Just contact me on Telegram for more information.

## Release Notes

- Changelog index: [`CHANGELOG.md`](CHANGELOG.md)
- Current development notes: [`releases/unreleased.md`](releases/unreleased.md)
- Technical version source: `pbgui_purefunc.py` → `PBGUI_VERSION`

## Overview
Passivbot GUI (pbgui) is a FastAPI-based web interface for Passivbot.

It has the following functions:
- Running, backtesting, and optimization Passivbot v7 and v6.
- Installing Passivbot configurations on your VPS.
- Starting and stopping Passivbot instances on your VPS.
- Moving instances between your VPS.
- Monitoring your instances and restarting them if they crash.
- A dashboard for viewing trading performance.
- Pareto Explorer for exploring optimizer results (Pareto front, correlations, 2D/3D plots, config inspection, start backtests, generate optimize configs with goal/risk presets).
- An interface to CoinMarketCap for selecting and filtering coins.
- Installing and updating your VPS with just a few clicks.
- And much more to easily manage Passivbot.

## Updating Existing Masters

After updating an existing PBGui master from a pre-v1.81 Streamlit-based install to v1.81 or newer, run the one-time cleanup helper from the updated repository:

```
cd ~/software/pbgui
bash setup/cleanup_streamlit_master.sh --dry-run
bash setup/cleanup_streamlit_master.sh
```

The helper stops stale Streamlit processes, removes old Streamlit autostart entries, closes UFW port `8501`, removes direct Streamlit packages from detected PBGui virtualenvs, and deletes obsolete `.streamlit/config.toml`. It keeps `.streamlit/secrets.toml` so the new `data/auth/secrets.toml` path can import existing passwords during migration. No reboot is required.

### Requirements
- Python 3.12 (default)
- Linux

### Recommendation

- Master Server: Linux with 32GB of memory and 8 CPUs.
- VPS for Running Passivbot: Minimum specifications of 1 CPU, 1GB Memory, and 10GB SSD.

### Get your VPS for running passivbot

I currently recommend [IONOS](https://aklam.io/CBA3zSaZ).
For IONOS open `Server` -> `vServer (VPS)` -> `Linux VPS`.
For normal VPS bots I currently suggest `VPS S+` with 2 vCores CPU, 2 GB RAM and 80 GB NVMe.
For a remote master I currently suggest `VPS M+` with 4 vCores CPU, 4 GB RAM and 120 GB NVMe.

### Support:
If you like to support pbgui, please join one of my copytradings:\
If you don't have an bybit account, please use my Referral Code: XZAJLZ https://www.bybit.com/invite?ref=XZAJLZ \
Here are all my copytradings and statistics of them: https://manicpt.streamlit.app/

## Installation

### Install PBGui Master on a vps (Best Option)

Recommended remote master installer:

```
bash <(curl -fsSL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/master_installer.sh)
```

The command starts a local browser wizard at `http://127.0.0.1:8088/`. The wizard installs a fresh remote master VPS over SSH, configures OpenVPN and TOTP, installs PBGui/PB7, creates systemd user services, and starts PBGui. PBGui is opened to the VPN network only; SSH can be restricted to specific IPs plus VPN, VPN-only, or explicitly left open with a warning. Keep the PBGui bind address at `0.0.0.0` for remote masters; the firewall limits access to the configured OpenVPN network. If you install multiple remote masters, choose a different private OpenVPN CIDR for each one, for example `10.8.0.0/24`, `10.9.0.0/24`, or `10.10.0.0/24`. When importing the `.ovpn` profile with NetworkManager, enable `Use this connection only for resources on its network` and disable IPv6, or use the installer's NetworkManager import button to apply these settings automatically.

For headless systems, use the CLI mode:

```
bash <(curl -fsSL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/master_installer.sh) --cli
```

OpenVPN and TOTP are mandatory for the remote master installer. PBRemote/rclone is not started by default and remains a legacy/optional setup.

### Legacy manual remote master setup

Step 1: Get a Linux VPS from IONOS. Please use my [referral link](https://aklam.io/CBA3zSaZ)
- Select `Server` -> `vServer (VPS)` -> `Linux VPS`
- For normal VPS bots I currently suggest `VPS S+` with 2 vCores CPU, 2 GB RAM and 80 GB NVMe
- For a remote master I currently suggest `VPS M+` with 4 vCores CPU, 4 GB RAM and 120 GB NVMe
- For optimization you need a bigger system like `VPS XL+`, `VPS XXL+` or a dedicated server.
- Install the VPS with Ubuntu 24.04

Step 2: Connect to your new VPS and run Initial Setup
- Add your VPS IP and VPN IP to your hosts (/etc/hosts)

```
Syntax:
<ip> <hostname>
10.8.0.1 <hostname>-vpn

Example:
87.106.x.x manibot01
10.8.0.1 manibot01-vpn
```

- Connect with ssh to your new VPS and login as root with the temporary root pw
```
ssh root@<hostname>

# Setup hostname and user. Disable root login
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/master_vps_init.sh) <hostname> <user>
```

Step 3: Connect as new user and Setup PBGui Master by running this commands
```
# Disconnect as root
exit

# ssh to your vps
ssh <user>@<hostname>

# Create swap
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_swap.sh) <size>

#  Setup openvpn
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_openvpn.sh)

# Setup google-authenticator and add QR code to your TOTP App
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_totp.sh)
cat /home/mani/GA-QR.txt

# Setup Firewall
The Firewall Setup can be run in 3 ways.
1. Default — allow SSH from everywhere (low secure)
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_firewall.sh)
2. VPN-only SSH access (high secure)
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_firewall.sh) -i
3. Specific IPs + VPN
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_firewall.sh) -i 1.2.3.4,1.2.3.5

# Setup PBGui
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/install.sh)

# Legacy autostart via crontab. New installs should use the remote master installer/systemd path above.
bash <(curl -sL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/setup_autostart.sh)
```

Step 5: Setup OpenVPN Client
- Get <user>.ovpn
```
scp <hostanme>:/home/<user>/<user>_client/<user>.ovpn .
```
- Import the ovpn to your OpenVPN Client

Step 6: Connect your VPN
- Use the GUI or connect from shell with 
```
sudo openvpn --config <user>.ovpn
```

Step 7: Connect to PBGui
- Now you are ready to connect to PBGui by opening this URL: http://<hostname>-vpn:8000/


### Ubuntu installer

There is an Ubuntu `install.sh` for PBGui + PB7. It works on Ubuntu 24.04 and only adds Deadsnakes when `python3.12-venv` is not available from the current distro repositories.
```
curl -L https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/install.sh | bash
```

### Manual Installation for all Linux distributions

Clone pbgui and passivbot v7
```
git clone https://github.com/msei99/pbgui.git
git clone https://github.com/enarjord/passivbot.git pb7
```
Create virtual environments
```
python3.12 -m venv venv_pb7
python3.12 -m venv venv_pbgui
```
Install requirements for pb7 and pbgui
```
source venv_pb7/bin/activate
cd pb7
pip install --upgrade pip
pip install -r requirements.txt
cd passivbot-rust/
sudo apt-get install rustc
sudo apt-get install cargo
maturin develop --release
deactivate
cd ../..
source venv_pbgui/bin/activate
cd pbgui
pip install --upgrade pip
pip install -r requirements.txt
```
### Docker (Any OS)
Want to use **Docker** instead? See the actively maintained community Docker project [dreamelite96/pbgui-docker](https://github.com/dreamelite96/pbgui-docker).

It is an independent Docker integration for current PBGui and Passivbot v7 releases and replaces the previous Docker link in this README.

## Running
```
python PBApiServer.py

```
Open http://localhost:8000 with Browser\
Password = PBGui$Bot!\
Change Password on the Welcome page or in `data/auth/secrets.toml`\
On First Run, you have to select your passivbot and venv directories
For the venv you have to enter the full path to python.
Example path for venv_pb7: /home/mani/software/venv_pb7/bin/python
Select Master on Welcome Screen if this System is used to send configs to VPS

## PBRun Instance Manager
To enable the PBGui instance manager in the GUI, you can follow these steps:

1. Open the PBGui interface.
2. Go to Services and enable PBRun

New master installations use systemd user services from the remote master installer. The following `start.sh`/crontab method is legacy and only kept for older manual installations:

1. Create a script file, such as "start.sh", in your pbgui directory (e.g., ~/software/pbgui).
2. In the script file, include the following lines:

```
#!/usr/bin/bash
venv=~/software/pb_env # Path to your Python virtual environment
pbgui=~/software/pbgui # Path to your PBGui installation

source ${venv}/bin/activate
cd ${pbgui}
python PBRun.py &
```

3. Save the script file and make it executable by running the command: `chmod 755 start.sh`.
4. Open your crontab file by running the command: `crontab -e`.
5. Add the following line to the crontab file to execute the script at reboot:

```
@reboot ~/software/pbgui/start.sh
```

6. Save the crontab file.

Please make sure to adjust the paths in the script file and crontab entry according to your specific setup.

## PBData Database for Dashboard
New master installations use systemd user services from the remote master installer. For older manual installations, PBData can be started from `start.sh`:
```
python PBData.py &
```
This command will run the PBData.py in the background and filling the database for the dashboard

## PBRemote Server Manager
You can install rclone and configure bucket using PBGui. Go to Services/PBRemote/Show Details.

With PBRemote, you can efficiently manage passivbot instances on multiple servers directly from your PC.
This includes starting, stopping, removing, and syncing instances from and to your servers.
PBRemote utilizes rclone to establish communication via cloud storage with your servers.
The advantage is that you do not need to open any incoming firewall ports on your PC or servers.
Additionally, all your passivbot config data is securely transferred and stored on your preferred cloud storage provider.
rclone supports over 70 cloud storage providers, you can find more information at https://rclone.org/.
Manual install rclone, you can use the following command:
```
sudo -v ; curl https://rclone.org/install.sh | sudo bash
```
As a recommendation, Synology C2 Object Storage offers a reliable option.
They provide 15GB of free storage, and you can sign up at https://c2.synology.com/en-uk/object-storage/overview.
After registration, create your bucket using your own unique name. Please note that using "pbgui" as a bucket name will not work, as it has already been chosen by me.
Or do a manual Setup:
Configure rclone on your PC and servers by following the steps below:
Manual Rclone configuration (Synology):
```
rclone config create <bucket_name> s3 provider=Synology region=eu-002 endpoint=eu-002.s3.synologyc2.net no_check_bucket=true access_key_id=<key> secret_access_key=<secret>
```
You need to configure pbgui.ini with a minimum of this settings on your VPS.
Example pbgui.ini (replace parameters with your own correct settings).
```
[main]
pb7dir = /home/mani/software/pb7
pb7venv = /home/mani/software/venv_pb7/bin/python
pbname = manibot50
[pbremote]
bucket = pbgui:
```
On new master installations, PBRemote is not started by default and remains a legacy/optional rclone-based service. Older manual installations can still start PBRun.py and PBRemote using the start.sh script.

## PBCoinData CoinMarketCap Filters
With PBCoinData, you can download CoinMarketCap data for symbols and use this data to maintain your ignored_symbols and ignored_coins. You can filter out low market cap symbols or use vol/mcap to detect possible rug pulls early.

You need to configure the pbgui.ini file with a minimum of the following settings on your VPS. Here is an example of pbgui.ini (replace the parameters with your own correct settings):
```
[coinmarketcap]
api_key = <your_api_key>
fetch_limit = 1000
fetch_interval = 4
```
With these settings, PBCoinData will fetch the top 1000 symbols every 4 hours. You will need around 930 credits per month with this configuration. A Basic Free Plan from CoinMarketCap provides 10,000 credits per month, allowing you to run 1 master and 9 VPS instances with the same API key.
On new master installations, PBCoinData is managed by a systemd user service. Older manual installations can still start PBCoinData.py using the start.sh script.

## Links:
- Telegram https://t.me/+kwyeyrmjQ-lkYTJk
- Passivbot https://www.passivbot.com/en/latest/
