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

I currently recommend [IONOS](https://aklam.io/CBA3zSaZ) and [netcup](https://www.netcup.com/server/vps-lite?ref=390177).
For IONOS open `Server` -> `vServer (VPS)` -> `Linux VPS`.
For normal VPS bots I currently suggest `VPS S+` with 2 vCores CPU, 2 GB RAM and 80 GB NVMe.
For a remote master I currently suggest `VPS M+` with 4 vCores CPU, 4 GB RAM and 120 GB NVMe.

Netcup also has very strong VPS offers:
- `VPS pico G11s`: 1 vCore, 1 GB RAM, 30 GB SSD and traffic included. This is a good low-cost VPS for several bot instances; around 6 bots or more can work depending on strategy load and market-data usage.
- `VPS Lite 1 G12s`: 2 vCore, 4 GB RAM, 80 GB SSD and traffic included. This is a good remote master option when you do not run optimizations on the master.

Netcup 5 EUR new-customer coupons (no domains): `36nc17835299729`, `36nc17835299728`, `36nc17835299727`, `36nc17835299726`, `36nc17835299725`, `36nc17835299724`, `36nc17835299723`, `36nc17835299722`, `36nc17835299721`, `36nc17835299720`.

### Support:
If you like to support pbgui, please join one of my copytradings:\
If you don't have an bybit account, please use my Referral Code: XZAJLZ https://www.bybit.com/invite?ref=XZAJLZ \
Here are all my copytradings and statistics of them: https://manicpt.streamlit.app/

## Installation

### Master Installer (Recommended)

Start the PBGui master installer from your local machine:

```
bash <(curl -fsSL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/master_installer.sh)
```

The command starts a local browser wizard at `http://127.0.0.1:8088/`. It can install a PBGui master either on a fresh remote VPS or on the local machine.

Remote Master VPS mode is the recommended production setup. It installs a fresh VPS over SSH, confirms the SSH host-key fingerprint before connecting, configures OpenVPN and TOTP, installs PBGui/PB7, creates systemd user services, and starts PBGui. The installer generates an individual PBGui web password; reveal and store it before starting the installation. PBGui is opened to the VPN network only; SSH can be restricted to specific IPs plus VPN, VPN-only, or explicitly left open with a warning. Keep the PBGui bind address at `0.0.0.0` for remote masters; the firewall limits the configured PBGui port to the OpenVPN network. If you install multiple remote masters, choose a different private OpenVPN CIDR for each one, for example `10.8.0.0/24`, `10.9.0.0/24`, or `10.10.0.0/24`. When importing the `.ovpn` profile with NetworkManager, enable `Use this connection only for resources on its network` and disable IPv6, or use the installer's NetworkManager import button to apply these settings automatically.

Local Master Install mode installs PBGui/PB7 under a configurable local parent directory, for example `~/software`. It creates systemd user services for PBGui. The installer checks local prerequisites first and only uses `apt`/`sudo` when required packages such as `git`, `curl`, `gcc`, `pkg-config`, or `python3.12-venv` are missing. If everything is already installed, no sudo password is needed.

Local Master Uninstall mode removes the local PBGui/PB7 checkouts, virtualenvs, and PBGui systemd user services under the selected install parent after an explicit safety confirmation.

For headless systems, use CLI mode:

```
bash <(curl -fsSL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/setup/master_installer.sh) --cli
```

To test an installer branch, set `PBGUI_INSTALLER_BRANCH` and fetch the same branch script:

```
PBGUI_INSTALLER_BRANCH=test-installer bash -c "$(curl -fsSL https://raw.githubusercontent.com/msei99/pbgui/refs/heads/test-installer/setup/master_installer.sh)"
```

OpenVPN and TOTP are mandatory for the remote master installer. Cluster Sync/PBCluster is the supported remote sync path.

### Ubuntu installer

There is an Ubuntu `install.sh` for PBGui + PB7. It works on Ubuntu 24.04 and only adds Deadsnakes when `python3.12-venv` is not available from the current distro repositories.
```
curl -L https://raw.githubusercontent.com/msei99/pbgui/refs/heads/main/install.sh | bash
```

### Docker (Any OS)
Want to use **Docker** instead? See the actively maintained community Docker project [dreamelite96/pbgui-docker](https://github.com/dreamelite96/pbgui-docker).

It is an independent Docker integration for current PBGui and Passivbot v7 releases and replaces the previous Docker link in this README.

## Running

Master installer setups run PBGui through systemd user services. Use the PBGui Services page for normal start/stop/restart actions.

Useful shell commands:

```
systemctl --user status pbgui-api.service
systemctl --user restart pbgui-api.service
journalctl --user -u pbgui-api.service -n 100 --no-pager
```

Installed service units:

```
pbgui-api.service
pbgui-pbcluster.service
pbgui-pbrun.service
pbgui-pbdata.service
pbgui-pbcoindata.service
```

Local Master installs open PBGui on `http://127.0.0.1:8000/` by default. Remote Master VPS installs are intended to be used through the generated OpenVPN profile, usually at `http://10.8.0.1:8000/` unless you selected a different OpenVPN CIDR.

New Master Installer runs generate an individual PBGui web password instead of using a shared default. Reveal and store the generated password in the browser wizard, or note the generated password printed by CLI mode. You can change it later on the Welcome page or in `data/auth/secrets.toml`.

Legacy or manual installations may still use the former default password. When PBGui detects that credential together with a wildcard bind address, the authenticated Welcome page shows a persistent warning. Verify that the API port is restricted to VPN or trusted networks and replace the legacy password when broader network access is possible.

For manual development runs only, you can still start the API directly from an activated PBGui virtualenv:

```
python PBApiServer.py
```

## PBRun Instance Manager

PBRun manages passivbot instances from the PBGui UI. Enable or restart it from `Services -> PBRun`. New installer setups manage it with `pbgui-pbrun.service`; no `start.sh` or crontab entry is needed.

## PBData Database for Dashboard

PBData fills the dashboard database. Enable or restart it from `Services -> PBData` when you want dashboard history collection. New installer setups manage it with `pbgui-pbdata.service` when enabled.

## PBCluster Sync

PBCluster replicates Cluster Sync operations and materializes assigned V7 configs/API keys on joined nodes. PBRun remains responsible for starting and stopping local bots from the materialized Cluster desired state.

Existing PBRemote/API Sync/V7 SSH Sync installations should follow `docs/help/40_cluster_migration.md` before joining production VPS runners.

## PBCoinData CoinMarketCap Filters

PBCoinData downloads CoinMarketCap data for symbols and helps maintain ignored symbols and ignored coins. It can filter low market-cap symbols or use volume/market-cap ratios to detect possible rug pulls early.

Configure the CoinMarketCap API key in PBGui after installation. The installer no longer asks for it. A minimal configuration looks like this:

```
[coinmarketcap]
api_key = <your_api_key>
fetch_limit = 1000
fetch_interval = 4
```

With these settings, PBCoinData fetches the top 1000 symbols every 4 hours. You need around 930 credits per month with this configuration. A Basic Free Plan from CoinMarketCap provides 10,000 credits per month, allowing one master and several VPS instances to share the same API key. New installer setups manage PBCoinData with `pbgui-pbcoindata.service`.

## Existing VPS and systemd migration

New VPS installs use systemd user services automatically. Existing VPS entries can be migrated from PBGui's VPS Manager with the systemd migration preview first, then the migration action. The migration removes legacy `start.sh`/crontab autostart only after systemd services verify successfully.

## Links:
- Telegram https://t.me/+kwyeyrmjQ-lkYTJk
- Passivbot https://www.passivbot.com/en/latest/
