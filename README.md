# GUI for Passivbot

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/Y8Y216Q3QS)

## Contact/Support on Telegram: https://t.me/+kwyeyrmjQ-lkYTJk
## Join one of my copytrading to support: https://manicpt.streamlit.app/
I offer API-Service where I run passivbot for you as a Service.
Just contact me on Telegram for more information.

# v1.51

### Overview
Passivbot GUI (pbgui) is a WEB Interface for Passivbot programed in python with streamlit

It has the following functions:
- Running, backtesting, and optimization Passivbot v7 and v6 (single and multi).
- Installing Passivbot configurations on your VPS.
- Starting and stopping Passivbot instances on your VPS.
- Moving instances between your VPS.
- Monitoring your instances and restarting them if they crash.
- A dashboard for viewing trading performance.
- Pareto Explorer for exploring optimizer results (Pareto front, correlations, 2D/3D plots, config inspection, start backtests, generate optimize configs with goal/risk presets).
- An interface to CoinMarketCap for selecting and filtering coins.
- Installing and updating your VPS with just a few clicks.
- And much more to easily manage Passivbot.

### Requirements
- Python 3.12 (default)
- Python 3.10 (only required if you use PB6)
- Streamlit 1.52.0
- Linux

### Migration (Python 3.10 -> 3.12)

PBGui and PB7 use Python 3.12 by default. PB6 stays on Python 3.10.

If you already have PBGui running, you can upgrade in a few clicks:

Master (recommended):
- Open the VPS Manager on your Master.
- **PBGui**
  - Click "Update/Install PBGui venv" to prepare the new Python 3.12 environment.
  - Then run the switch script:
    - `pbgui/setup/mig_py312.sh`
  - Rollback (only if something goes wrong):
    - `pbgui/setup/mig_py310.sh`
- **PB7**
  - Update PB7 by clicking "Update PB7 venv".

VPS:
- Open the VPS Manager for the selected VPS.
- **PBGui**
  - Click "Update PBGui venv" (this will recreate the PBGui Python environment on that VPS; PBGui services will be restarted).
- **PB7**
  - Click "Update PB7 venv" if you also run PB7 on that VPS.
- Recommended (especially on small VPS): After the update, click "Cleanup VPS" once to free disk space.

Note: PB6 stays on Python 3.10. If you don't use PB6 on a VPS, Python 3.10 components may be removed to save disk space.

### Recommendation

- Master Server: Linux with 32GB of memory and 8 CPUs.
- VPS for Running Passivbot: Minimum specifications of 1 CPU, 1GB Memory, and 10GB SSD.

### Get your VPS for running passivbot

I recommend the provider IONOS, as their smallest VPS plan is available for only 1 Euro \
I have been using their services for over a year without any outages \
Please use my [referral link](https://aklam.io/esMFvG) to obtain a VPS from IONOS \
RackNerd has also nice small VPS for 11$ year. Please use my [referral link](https://my.racknerd.com/aff.php?aff=15714)
A good alternative is a VPS from Contabo. Please use my [referral link](https://www.tkqlhce.com/click-101296145-12454592)

### Support:
If you like to support pbgui, please join one of my copytradings:\
If you don't have an bybit account, please use my Referral Code: XZAJLZ https://www.bybit.com/invite?ref=XZAJLZ \
Here are all my copytradings and statistics of them: https://manicpt.streamlit.app/

## Installation

### Install PBGui Master on a vps (Best Option)

Step 1: Get a Linux VPS from IONOS. Please use my [referral link](https://aklam.io/esMFvG)
- Select Server Linux VPS
- For the beginning the VPS S is good for running a few bots, the dashboard and some backtests
- For optimization you need a bigger system like VPS XL, XXL or a dedicated server.
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

# Setup crontab for autostart
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
- Now you are ready to connect to PBGui by open this url: http://<hostname>-vpn:8501/


### Ubuntu installer

There is a install.sh for Ubuntu. Working on Ubunt24.04
```
curl -L https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/install.sh | bash
```

### Manual Installation for all Linux distributions

Clone pbgui and passivbot v6 and v7
```
git clone https://github.com/msei99/pbgui.git
git clone https://github.com/enarjord/passivbot.git pb6
git clone https://github.com/enarjord/passivbot.git pb7
```
Create virtual environments
```
python3.10 -m venv venv_pb6
python3.12 -m venv venv_pb7
python3.12 -m venv venv_pbgui
```
Install requirements for pb6, pb7 and pbgui
```
source venv_pb6/bin/activate
cd pb6
git checkout v6.1.4b_latest_v6
pip install --upgrade pip
pip install -r requirements.txt
deactivate
cd ..
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
Want to use **Docker** instead? Follow this [Quickstart guide](https://github.com/LeonSpors/passivbot-docker).

## Running
```
streamlit run pbgui.py

```
Open http://localhost:8501 with Browser\
Password = PBGui$Bot!\
Change Password in file: .streamlit/secrets.toml\
On First Run, you have to select your passivbot and venv directories
For the venv you have to enter the full path to python.
Example path for venv_pb7: /home/mani/software/venv_pb7/bin/python
Select Master on Welcome Screen if this System is used to send configs to VPS

## PBRun Instance Manager
To enable the PBGui instance manager in the GUI, you can follow these steps:

1. Open the PBGui interface.
2. Go to Services and enable PBRun

To ensure that the Instance Manager starts after rebooting your server, you can use the following method:

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

## PBStat Statistics
This is only needed if you trade spot and have some statistics
Actually, the best way to enable PBStat is by adding the following line to your start.sh script:
```
python PBStat.py &
```
This command will run the PBStat.py script in the background, allowing it to collect statistics.

## PBData Database for Dashboard
Actually, the best way to enable PBData is by adding the following line to your start.sh script:
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
pbdir = /home/mani/software/pb6
pbvenv = /home/mani/software/venv_pb6/bin/python
pb7dir = /home/mani/software/pb7
pb7venv = /home/mani/software/venv_pb7/bin/python
pbname = manibot50
[pbremote]
bucket = pbgui:
```
There is no need to install or run streamlit on your Remote Server.
Start PBRun.py and PBRemote using the start.sh script.

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
Start PBCoinData.py using the start.sh script.

## Running on Windows (Not tested with passivbot 7)
Copy the start.bat.example to start.bat
Edit pbguipath in the start.bat to your pbgui installation path
Add start.bat to Windows Task Scheduler and use Trigger "At system startup"

# Changelog

## v1.51 (29-01-2026)
- PB7 v7.7 compatibility: add `maker_fee_override`, `warmup_concurrency`, and `hedge_mode` to config + UI.
- BacktestV7: maker fee override can be enabled via checkbox (stores `null` when disabled).
- RunV7: expose `hedge_mode` and `warmup_concurrency`.
- Monitor: parse new PB7 fill/PnL log formats (keeps legacy support).
- VPS Manager: fix PB7 upstream switch logic.
- Strategy Explorer: Movie Builder fixes; ensure `n_positions` is treated as int.
- BacktestV7: handle `psutil.ZombieProcess` when checking PID.
- PB7 suite editor: refactor aggregation editor, fix UI state + aggregate metrics, and prevent invalid suite configs.
- Pareto Explorer: fix fast-mode pareto JSON loading order to keep config indices consistent.

## v1.50 (23-01-2026)
- PBv7 Strategy Explorer (successor to Grid Visualizer): compare Mode B/Mode C, inspect fills on candles, and build Movie replays.
- Strategy Explorer help/tutorial docs (EN/DE).
- Movie Builder: consistent frames, correct fill-marker placement (incl. 1d), responsive progress during long PB7 computations.
- MP4 export: codec/encoder selection.

## v1.49 (11-01-2026)
- Add: PB7 switch supports custom remotes and remote branch browsing.
- Improve: VPS Manager allows overriding PB7 remote URL (effective).
- Fix: OptimizeV7 queue now shows the correct exchange per job.
- Fix: Optimize bounds export now includes the `step` value (prevents missing/zero-step warnings in PB7).
- Add: PB7 live config parameters `warmup_jitter_seconds` and `max_concurrent_api_requests`.

## v1.48 (08-01-2026)
- Add: Scoring builder UI for PBv7 Optimize.
- Improve: Pareto Explorer correctness, caching, and performance.
- Improve: Metrics registry and optimizer limits tooltips.
- Fix: Deep Intelligence Evolution metric selectbox no longer loses options after selection (fast/full mode).
- Improve: Deep Intelligence parameter influence heatmap now renders in fast mode (small samples).

## v1.47 (06-01-2026)
- Improve: Pareto Explorer performance and stability for large `all_results.bin` (less recomputation, faster load paths).
- Improve: Enforce `crossover_probability + mutation_probability ≤ 1.0` in PBv7 Optimize without widget jumping.
- Add: Optional `msgspec` dependency for faster selected-config decoding.

## v1.46 (04-01-2026)
- Added step parameters to optimizer bounds
- Added candle_lock_timeout_seconds parameter to Live configuration

## v1.45 (03-01-2026)
- Fix: avoid nested Streamlit dialogs when importing configs (PBv7 Run/Backtest).
- Fix: show configs as real JSON (preserve `null` instead of `None`).
- Improve: VPS Manager updates are more robust on low-disk systems (disk checks, no pip cache, smaller PB7 installs).
- Improve: playbooks no longer error when trying to kill non-existing processes.

## v1.44 (02-01-2026)
- PBGui and PB7 now run on Python 3.12 by default (PB6 stays on Python 3.10).
- Easier installs/updates: dependencies are separated for Python 3.12 vs. legacy Python 3.10.
- New/updated upgrade helpers in the VPS Manager to update PBGui/PB7 on Master and VPS.

## v1.43 (01-01-2026)

**NEW: Pareto Explorer (PBv7)**
- Explore optimization results in a guided, multi-stage workflow (Command Center → Playground → Deep Intelligence)
- Fast mode loads `pareto/*.json`; optional full load from `all_results.bin` with progress + rank-range display filtering
- Interactive analysis: Pareto front exploration, 2D scatter, 3D projections, and correlations
- Drill into any config: full config JSON, key metrics + risk score, and a complete metrics table
- Quick actions: export results as CSV, run a PBv7 backtest from a selected config
- Create a follow-up PBv7 Optimize config from a selected config (refined bounds) with goal profiles + risk adjustment, and optionally open it directly in the Optimize editor

## v1.42 (28-12-2025)

**Updated to Streamlit 1.52.0**
- Migrated from deprecated `st.bokeh_chart` to `streamlit-bokeh` custom component
- Updated dependencies (bokeh 3.8.1, contourpy >=1.2) for Streamlit 1.52.0 compatibility
- Optimized chart heights (200px) for better screen utilization
- Note: Bokeh charts are only used in legacy PB6 single/multi backtest views

**Bugfixes & Improvements**
- Fixed missing Config.py setters for proper configuration management
- Fixed double reload issue on refresh button in VPS branch management
- Fixed force reload of PB7 branches data on refresh button
- Fixed Ansible playbooks PATH issues for rustup/maturin commands
- Improved VPS branch management stability and reload behavior

**Dependency Updates**
- streamlit 1.52.0 (updated from 1.50.0)
- streamlit-bokeh 3.8.1 (new - for legacy PB6 charts)
- bokeh 3.8.1 (updated from 2.4.3)
- contourpy >=1.2 (updated from 1.1.1)

## v1.41 (28-12-2025)

**VPS Branch Management**
- Interactive branch switching for PBGui and PB7 on Master and VPS servers
- View git branch history with commit details (author, date, message)
- Switch to specific commits or stay on branch HEAD for automatic updates
- Load more commits on demand (+50 or All)
- Real-time status showing current branch, commit hash, and commits behind origin
- Reload button updates VPS status directly from alive files without closing expanders

## v1.40 (23-12-2025)

**NEW: Suite for Backtest V7 and Optimize V7**
- Multi-scenario testing now available for Backtests and Optimzer
- Test your configuration across multiple scenarios simultaneously
- Each scenario can use different coins, date ranges, exchanges, and parameters
- Results are automatically aggregated (average, min, max, etc.)
- Find robust configs that work under different market conditions
- Works uniformly in both Backtest and Optimizer

**Bugfixes**
- Fixed display of backtest results (gain, ADG, drawdown, sharpe ratio) for new JSON format
- Support for both old and new analysis.json formats (_usd/_btc suffixes)

## v1.39 (20-12-2025)
- **Optimizer Limits**: Complete rewrite for PassivBot v7.5.x compatibility
  - New list-based limits format replacing the old dict format
  - Interactive UI with split metric/currency selection for cleaner dropdown menus
  - Support for all penalize_if modes: greater_than, less_than, outside_range, inside_range, auto
  - Optional stat aggregation: mean, min, max, std
  - Automatic legacy config conversion (old dict format → new list format)
  - Currency metrics with _usd/_btc suffix selection (default: usd)
  - Shared metrics (loss_profit_ratio, position_held_hours_max, etc.) without suffix
  - Edit/Delete limits directly from the table view
- **Suite Configuration**: Multi-scenario backtesting/optimization support
  - Evaluate configs across different coin sets, date ranges, exchanges, and parameter variations
  - Each scenario can override coins, dates, exchanges, and bot parameters
  - Results are aggregated across scenarios (mean, min, max, std)
  - Helps find robust configs that work across different market conditions
  - Interactive scenario editor with add/edit/delete functionality
- **UI Improvements**: Wider and taller help tooltips for better readability

## v1.38 (20-12-2025)
- Compatible with passivbot 7.5.8
- Added new Live parameters: recv_window_ms, order_match_tolerance_pct, balance_override, balance_hysteresis_snap_pct, max_warmup_minutes
- Added new Logging parameters: level, memory_snapshot_interval_minutes, volume_refresh_info_threshold_seconds
- Added new Backtest parameter: balance_sample_divider (controls resolution of balance/equity data to reduce file sizes)
- Added new Optimize parameter: pareto_max_size
- Improved Optimizer UI layout (4+4+1 row arrangement)
- Reordered bounds to match template.json alphabetical order
- Migrated use_btc_collateral to btc_collateral_cap and btc_collateral_ltv_cap
- PBData async for faster data fetching from exchanges
- Swap usage monitoring and memory-based bot restart
- Configurable server warning thresholds for memory, disk, swap and cpu
- Detect liquidation in backtest and display warning banner
- Enhanced websocket handling with backoff strategy and per-exchange client limits
- Price buffering and background writer for improved performance
- Dashboard: per-cell refresh intervals and auto-refresh functionality
- Income data edit/backup/restore functionality
- Enhanced VPS management: SSH key generation, firewall updates, role display
- Master VPS setup with OpenVPN and TOTP support
- Cleanup of pbgui_help.py (removed 185 duplicate help entries)
- Fixed KeyError issues in pareto view
- Many bugfixes

## v1.37 (19-10-2025)
- Compatible with passivbot 7.4.1
- Resize swap size on vps
- PBRemote optimized for more than 10 vps
- Update to Streamlit 1.50
- Save default sort options
- Display CMC Credits left for vps
- Dashboard new income as list
- Setup vps with privat key or user/pw
- Bugfixes

## v1.36 (07-08-2025)
- Added coin_overrides and removed old coin_flags
- Converted coin_flags to coin_overrides
- New ADG Dashboard
- Drawdown view for backtests
- Moved navigation to the top for more space on the left
- VPS cleanup to free up storage
- Option to skip installation or remove v6 bot from VPS
- Balance calculator
- Many bug fixes

## v1.35 (01-06-2025)
- Added new options from passivbot v7.3.13
  close_grid_markup_start, close_grid_markup_end
  mimic_backtest_1m_delay
  trailing_double_down_factor
- Config Archive
- Many small improvements
- A lot of bugfixes
- Update ccxt for work with okx

## v1.34 (21-04-2025)
- Compatible with passivbot v7.3.4
- Using fragments for speedup GUI
- Support for GateIO
- Send Telegram Messages on Bot errors
- logarithmic view of backtests
- TWE and WE view on backtests v7
- Update to streamlit v1.44
- hundreds of bug fixes


## v1.33 (30-12-2024)
- Filter for coins with warnings on CoinMarketCap
- Multi: Added only_cpt and apply_filter function
- Improved GridVis by Sephral
- Fetch notice from CoinMarketCap metadata for display warning messages
- Added preset manager for optimizer v7 by Sephral
- Small bugfixes

## v1.32 (26-12-2024)
- Converter for pb6 to pb7 configurations
- View Monitor for all VPS on one Page
- Small Bugfixes

## v1.31 (16-12-2024)
- Added coin_flags to pb7 run
- Reworked GridVisualizer
- Updated SYMBOLMAP for CoinData
- Change balance on bybit to totalWalletBalance
- Add install.sh for easy install PBGui, pb6 and pb7 on ubuntu
- Filters for all_results
- Added PBGui Logo
- Bugfix update VPS will no longer install all requirements.txt
- PBGui can now run without password
- Change password dialog
- pb7.2.10 multi exchange optimizer/backtester compatibility
- Much more Bugfixes

## v1.30 (03-12-2024)
- Added apply_filters for static symbol selection
- ignored_symbols will now always added to the dynamic_ignore
- Preview of dynamic_ignore, when enabling it
- Added copytrading only to dynamic_ignore filter
- Tags can be used as dynamic_ignore filter for running bots
- Add Tags filter from CoinMarketCap (example: memes, gaming, defi, layer-2 ... much more)
- Compare Backtests from All Results
- Show All Results on Backtest V7
- Update to Streamlit 1.40
- Add Position Value to Dashboard Positions
- Small Bugfixes
- Small cosmetic changes

## v1.29 (29-11-2024)
- Make PBGui passivbot v7.2.9 compatible
- Backtest V7 Compare results added
- Install requirements when updating vps or master
- Small bugfixes
- Added ko-fi for donations

## v1.28 (24-11-2024)
- VPS-Manager check for working rclone on Master before Setup a VPS
- Added GUI Settup for rclone buckets (Services PBRemote)
- Added Test Connection for rclone buckets
- VPS-Manager Install and Update rclone on Master
- Bugfix Optimize V7 corrupted results
- Higher verbosity Level when setup vps and select debug
- Disable ipv6 on VPS using grub
- Coindata Fix for NEIROETHUSDT on binance and correct marketcap
- Bugfix Hyperliquid import
- Expanded Settings when Setup an new VPS

## v1.27 (19-11-2024)
- Bugfix Results Backtest Single
- PBRemote: Added delete function for offline Remote Servers with cleanup remote storage
- VPS-Manager: Find new added VPS after a refresh of the Page
- VPS-Manager: View logfiles from VPS
- VPS-Manager: Don't allow add VPS with same names
- New P+L Dashboard (Sephral)
- New Navigation (Sephral)
- Added V7 Grid Visualizer (Sephral)
- Added optional notes to instances (Sephral)
- Imporved Titel & Page Headers (Sephral)
- VPS-Manager: Added update function for localhost (Master) for pbgui, pb6 and pb7
- A lot of small bugfixes
- More small improvements

## v1.26 (13-11-2024)
- VPS-Manager always build rust when update v7 passivbot
- VPS-Manager show status of PBGui Master
- VPS-Manager selectbox for easy switch between VPS
- VPS-Manager add option for delete a VPS
- VPS-Manager Bugfix for unknown vps hosts
- Bugfixes vol_mcap
- Select logfile size and view reverse for speed up big logfiles
- Bugfix API-Editor don't let you delete users that are used by pb7
- Bugfix Load v7 users wihtout need to have pb6 installed
- Show a OK when paths for pb6,pb7 and venvs are correct
- Remove /../.. from paths
- No longer need to restart passivbot 7 instances when dynamic_ignore select new coins
- More small bugfixes

## v1.25 (10-11-2024)
- Rewrite Config Module
- Bugfix for not saving selected coins
- Disable IPv6 on vps-setup
- Bugfix import config v7 on backtest and run

## v1.24 (09-11-2024)
- Added approved_coins_long and _short
- Added ignored_coins_long and short
- Added empty_means_all_approved option from v7.2.2
- Added compress_cache option on backtest_v7
- Bugfix for new passivbot v7.2.2
- Copy optimize_result before running analysis for not get locking errors
- Bugfix for PBRun create_parameters on pb6 single instances
- Bugfix for update pb6 and pb7 ignore error when no instance is running
- More small bugfixes

## v1.23 (07-11-2024)
- PBRun/PBRemote: Check if updates for Linux and reboot needed
- VPS-Manager: Overview of all VPS running versions and update/reboot status
- PBRemote: Compression for alive files, 75% less data usage
- VPS-Manager: Update only PBGui without pb6 and pb7
- PBRun/PBRemote: gater pbgui/pb6/pb7 versions and send them to master

## v1.22 (05-11-2024)
- VPS-Manager: UFW Firewall configuration
- VPS-Manager: Update pbgui, pb6 and pb7 and restart passivbot after update
- VPS-Manager: Update Linux
- VPS-Manager: Reboot VPS
- VPS-Manager: View Status and running passivbots
- Bugfix: Don't allow passwords with {{ or }}
- Add Master, Slave role, for using less traffic on PBRemote

## v1.21 (29-10-2024)
- VPS-Manager: Fully automate setup your VPS with PBGui, PB6 and PB7.
- starter.py for start, stop restart PBRun, PBRemote and PBCoinData.
- PBRemote: Finds new VPS without restart.
- Some small Bugfixes for new v7 functions.

## v1.20 (22-10-2024)
- V7: Added all latest config options to live and optimizer
- V7 Run: Added Dynamic filter for mcap and vol/mcap
- Multi: Filters for marketcap and vol/mcap added
- Multi: Dynamic filter for mcap and vol/mcap added
- PBRun: Dynamic filter update ignored_symbols
- PBCoinData: Update on start and every 24h symbols from all exchanges

## v1.19 (20-10-2024)
- CoinMarketCap integration
- V7 Run and Optimize filters for marketcap and vol/mcap added
- New Service PBCoinData fetch data from CoinMarketCap

## v1.18 (13-10-2024)
- Services: Show PNL Today, Yesterday from LogMonitor
- Services: Show Logmonitor Information Memory, CPU, Infos, Erros, Tracebacks
- PBRun/PBRemote: Monitor passivbot logs and send infos to master
- PBData: Reload User on every run for new added Users
- PBRun: Recompile rust if new pb7 version is installed
- PBData: Bugfix when removing User from API
- V7: Add Exchange and Time informations
- Bugfix: Update Symbols from Binance

## v1.17 (02-10-2024)
- V7: Run optimizer from a backtest result with -t starting_config
- Run V7 and Multi: Add All and Add CPT Symbols to approved_symbols
- Multi: lap, ucp and st in the Overview of Multi Run
- V7: Compile rust if needed
- V7: Show final_balance in backtests for easy sort them
- V7: refresh logfile fragment refresh for speed up

## v1.16 (28-09-2024)
- Run V7: First Version that can run passivbot v7
- PBRun: Can now start passivbot v7 instances
- PBRemote: Sync v7 added

## v1.15 (24-09-2024)
- Backtest V7: Added backtester for passivbot v7
- Bugfix OPtimizer autostart
- Optimizer V7: Added Name to results
- Bugfix venv for old passivbot

## v1.14 (19-09-2024)
- Optimize V7: Added optimizer for passivbot v7
- Add api-keys for passivbot version 7
- check for installed passivbot versions
- split venv pbgui and venv passivbot / Config Option for venv passivbot v6 and v7
- Dashboard: Added Timeframe to Order View and move the time left/right

## v1.13 (11-09-2024)
- Bugfix Multi Backtest Results, corrected time in View Results
- Removed PBShare, Live View, Grid Share for futures and removed old code from PBRun and PBRemote
- Speed Up when starting PBGui
- Bugfix Bitget Single

## v1.12 (31-08-2024)
- Dashboard: Bugfix Hyperliquid Price and Candlesticks timeframe
- Dashboard: Added Hyperliquid to PBData and Dashboard
- Multi and API-Editor: Added Hyperliquid
- Multi Added Button for Update Symbols from Exchange
- Bugfix for configparser. Under certain circumstances, configuration from other sections was being lost.

## v1.11 (27-08-2024)
- Dashboard: Change Bybit Income from positions_history to transactions for more accurate income history
- Dashboard: Kucoin added
- Dashboard: Move panels added
- Dashboard: Added 'ALL' to user selections

## v1.1 (20-08-2024)
- Dashboard: Added Dashboards for replacing the Live View in future versions of PBGui
- Dashboard: Added a SQLite database for fast view of the dashboards
- PBData: New scrapper for fetch balance, positions, orders, prices and income from exchanges

## v1.01 (23-07-2024)
- Optimize_Multi: Bugfix for object has no attribute 'hjson'
- Multi: Bugfix price_distance_threshold

## v1.0 (23-07-2024)
- Optimize_Multi: Generate Analysis from all_results added
- Optimize_Multi: Create Backtest from Analysis (Result)
- Optimize_Multi: Remove Results added
- Optimize_Multi: First running Version with multi optimizer

## Links:
- Telegram https://t.me/+kwyeyrmjQ-lkYTJk
- Passivbot https://www.passivbot.com/en/latest/
- Streamlit https://streamlit.io/

## Screenshots
![Alt text](docs/images/dashboard.png)
![Alt text](docs/images/run.png)
![Alt text](docs/images/backtest.png)
![Alt text](docs/images/optimize.png)
