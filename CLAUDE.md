# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PBGui is a Streamlit-based web interface for managing Passivbot trading bot instances. This fork is optimized for running on Windows 11 as a master node that manages remote Linux VPS instances. The application provides a complete lifecycle management system for cryptocurrency trading bots, including configuration, deployment, monitoring, backtesting, and optimization.

**Key capabilities:**
- Run, backtest, and optimize Passivbot v7 and v6 (single and multi-symbol modes)
- Manage multiple remote VPS instances via cloud storage sync (no open ports needed)
- Fully automate VPS setup, updates, and monitoring using Ansible
- Dashboard for real-time trading performance visualization
- CoinMarketCap integration for intelligent coin filtering
- Background services for instance management, data collection, and synchronization

## Development Commands

### Running the Application
```powershell
# Activate virtual environment (Windows)
.\venv_pbgui\Scripts\activate

# Run the main application
streamlit run pbgui.py
```

Default URL: http://localhost:8501
Default password: `PBGui$Bot!` (change in `.streamlit/secrets.toml`)

### Managing Background Services
```powershell
# Start/stop/restart services via the GUI
# Navigate to: System → Services

# Or use the command-line starter
python starter.py start pbrun    # Instance manager
python starter.py start pbremote # VPS synchronization
python starter.py start pbdata   # Dashboard data collector
python starter.py start pbcoin   # CoinMarketCap integration
python starter.py stop <service>
python starter.py restart <service>
```

Services store PIDs in `data/pid/` and logs in `data/logs/`.

### Installation & Setup
```powershell
# Install dependencies
pip install -r requirements.txt

# First-run configuration (via GUI):
# 1. Navigate to System → Services
# 2. Configure paths to passivbot v6/v7 directories and virtualenvs
# 3. Set pbname (master node identifier)
# 4. Configure rclone bucket for remote VPS sync (optional)
```

### Testing
No formal test suite currently exists. Manual testing via the GUI is standard practice.

## Architecture Overview

### Entry Point & Navigation
- **pbgui.py**: Minimal entry point that loads the Streamlit navigation framework
- **pbgui_func.py**: Core UI framework with `build_navigation()` that creates the top-level page structure
  - Handles authentication, session state, and navigation routing
  - Maps navigation keys to page files in the `navi/` directory
- **pbgui_purefunc.py**: Pure utility functions (INI management, path helpers, JSON validation)

### Page Structure
All UI pages are in `navi/`, organized by category:
- `system_*.py`: Login, API keys, services, VPS manager, debug log
- `info_*.py`: Dashboards, coin data
- `v7_*.py`: Passivbot v7 pages (run, backtest, optimize, grid visualizer, balance calculator)
- `v6_multi_*.py`, `v6_single_*.py`: Passivbot v6 pages

### Background Services Architecture

**Services.py** acts as the service manager for all background processes:

1. **PBRun.py** (Instance Manager) - CRITICAL SERVICE
   - Monitors `data/instances/`, `data/multi/`, `data/run_v7/` for bot configurations
   - Starts/stops/restarts passivbot processes via subprocess
   - Monitors logs for errors, PNL tracking, system resources
   - Manages dynamic coin filtering based on CoinMarketCap data
   - Automatically restarts crashed instances
   - Classes: `Monitor`, `DynamicIgnore`, `RunSingle`, `RunMulti`, `RunV7`

2. **PBRemote.py** (VPS Synchronization) - CRITICAL SERVICE
   - Master/slave architecture using rclone and cloud storage (Synology C2, AWS S3, etc.)
   - Master pushes configs to cloud, slaves pull and execute
   - Slaves push heartbeat files (alive_*.cmd) with status every ~60s
   - No direct network connection required (firewall-friendly)
   - Classes: `RemoteServer`, `PBRemote`

3. **PBData.py** (Dashboard Data Collector)
   - Fetches real-time data from exchanges (positions, orders, balance, income)
   - Stores in SQLite database (`data/pbgui.db`)
   - Async event loop for concurrent exchange API calls
   - Dashboard reads from this DB for fast rendering

4. **PBCoinData.py** (CoinMarketCap Integration)
   - Fetches top N coins from CoinMarketCap API
   - Maps exchange symbols to CMC data
   - Provides filtering by market cap, volume/mcap ratio, tags, warnings
   - Used by PBRun for dynamic_ignore functionality

5. **PBStat.py** (Statistics - spot trading legacy)
   - Collects statistics for spot trading performance

### Data Models & Storage

**Configuration System (Config.py):**
- `Config`: Base class for v6 configs (recursive grid, clock, neat grid)
- `ConfigV7`: Hierarchical v7 config with nested classes:
  - `Logging`, `Backtest`, `Bot` (with `Long`/`Short` sub-configs)
  - `ApprovedCoins`, `IgnoredCoins`, `Live`, `Optimize`, `Bounds`, `PBGui`
- Every config has a version number that increments on save
- v6 configs: JSON in `data/instances/{user}/{symbol}/config.json`
- v6 multi: HJSON in `data/multi/{user}/multi.hjson`
- v7 configs: JSON in `data/run_v7/{user}/config.json`

**Instance Management:**
- `Instance.py`: v6 single bot instances
- `Multi.py`: v6 multi-symbol instances
- `RunV7.py`: v7 instances (uses fragment-based UI for performance)
- `Status.py`: Tracks instance metadata (running state, enabled_on, activation time)
  - Status files: `status.json`, `status_single.json`, `status_v7.json`

**User & Exchange (User.py, Exchange.py):**
- `User`: Single user with API credentials
- `Users`: Collection manager loading from `{pbdir}/api-keys.json` and `{pb7dir}/api-keys.json`
- `Exchange`: CCXT wrapper for exchange interactions

**Database (Database.py):**
- SQLite at `data/pbgui.db`
- Tables: history, position, orders, prices, balances
- Backup/restore to `data/backup/db/`

### VPS Management System

**VPSManager.py** - Ansible-based automation:
- `VPS` class represents one VPS with hostname, IP, credentials
- Fully automates VPS lifecycle:
  1. **Initial Setup**: Create user, disable root, setup swap/OpenVPN/TOTP/firewall
  2. **Installation**: Clone repos (pbgui, pb6, pb7), create virtualenvs, build rust
  3. **Updates**: Pull latest code, install requirements, restart instances
  4. **Monitoring**: View status, pending updates, reboot management
- Uses Ansible playbooks stored in `data/vpsmanager/playbooks/`
- Dynamic inventory in `data/vpsmanager/inventory/`
- Note: Ansible only manages Linux targets (cannot manage local Windows)

### Passivbot Integration

**v6 (Legacy):**
- Single: `python passivbot.py {user} {symbol} {config_path}`
- Multi: `python passivbot_multi.py {multi_config_path}`

**v7 (Current):**
- Live: `python src/passivbot.py {config_path}`
- Backtest: `python src/backtest.py {config_path}`
- Optimize: `python src/optimize.py {config_path}`
- Requires passivbot-rust compilation (`maturin develop --release`)

**Backtesting & Optimization:**
- `BacktestV7.py`: Queue-based parallel backtesting, result comparison, config archives
- `OptimizeV7.py`: Parameter optimization with bounds, preset manager
- Similar structure for v6: `Backtest.py`, `BacktestMulti.py`, `Optimize.py`, `OptimizeMulti.py`

### Dashboard System (Dashboard.py)

Panels: Positions, Orders, Balance, Income, ADG (Average Daily Gain)
- Auto-refresh via Streamlit fragments
- Reads from SQLite DB populated by PBData
- Supports user filtering, timeframe selection, date ranges
- Chart visualization with Bokeh

## Critical Workflows

### Deploying a Bot Instance (v7)
1. Navigate to PBv7 → Run
2. Click "Add" to create new instance
3. Select user (auto-loads exchange)
4. Configure bot parameters (Long/Short settings, leverage, coin selection)
5. Setup dynamic filters (market cap, volume/mcap, CoinMarketCap tags)
6. Save config (version increments)
7. Select "Enabled on": localhost or remote VPS name
8. Click "Activate"
9. If local: PBRun detects and starts passivbot
10. If remote: PBRemote syncs to cloud → VPS pulls → VPS PBRun starts

### Managing Remote VPS
1. Navigate to System → VPS Manager
2. Add VPS with hostname, IP, credentials (password or SSH key)
3. Click "Init VPS" (creates user, sets up security, OpenVPN, firewall)
4. Click "Setup VPS" (installs pbgui, pb6, pb7, configures rclone)
5. VPS appears in Services → PBRemote as online
6. From any Run page, select VPS in "Enabled on" dropdown
7. Activate instances remotely - they sync via cloud storage

### Reading Logs & Debugging
- System → Debug Log: View all service logs
- VPS Manager: View remote VPS logs
- Each instance directory contains passivbot log files
- Monitor data stored in `monitor.json` per instance (PNL, errors, tracebacks)

## Configuration Files

**pbgui.ini** (main configuration):
```ini
[main]
pbdir = /path/to/pb6
pbvenv = /path/to/venv_pb6/bin/python
pb7dir = /path/to/pb7
pb7venv = /path/to/venv_pb7/bin/python
pbname = masternode_name

[pbremote]
bucket = rclone_bucket:

[coinmarketcap]
api_key = your_key
fetch_limit = 1000
fetch_interval = 4

[pbdata]
fetch_users = ['user1', 'user2']
```

**.streamlit/secrets.toml** (authentication):
```toml
password = "PBGui$Bot!"
```

## Directory Structure

```
pbgui/
├── pbgui.py                  # Entry point
├── pbgui_func.py             # Streamlit framework
├── pbgui_purefunc.py         # Pure utilities
├── Services.py               # Service manager
├── PBRun.py                  # Instance manager (background service)
├── PBRemote.py               # VPS sync (background service)
├── PBData.py                 # Data collector (background service)
├── PBCoinData.py             # CoinMarketCap (background service)
├── Database.py               # SQLite manager
├── VPSManager.py             # Ansible automation
├── Config.py                 # Configuration hierarchy
├── User.py, Exchange.py      # Models
├── Instance.py, Multi.py     # v6 instance types
├── RunV7.py                  # v7 instance type
├── Backtest*.py, Optimize*.py  # Testing tools
├── Dashboard.py              # Dashboard renderer
├── starter.py                # Service CLI
├── navi/                     # UI pages
│   ├── system_*.py           # System pages
│   ├── info_*.py             # Information pages
│   ├── v6_*.py               # Passivbot v6 pages
│   └── v7_*.py               # Passivbot v7 pages
└── data/                     # Runtime data (not in git)
    ├── instances/            # v6 single configs
    ├── multi/                # v6 multi configs
    ├── run_v7/               # v7 configs
    ├── cmd/                  # Remote commands for sync
    ├── remote/               # Synced status from VPS
    ├── pid/                  # Service PIDs
    ├── logs/                 # Service logs
    ├── backup/               # DB backups
    ├── vpsmanager/           # Ansible inventory/playbooks
    └── pbgui.db              # SQLite database
```

## Important Implementation Details

### Master/Slave Sync Pattern
- Master node (typically Windows 11 desktop) manages everything
- Slave VPS instances run PBRun + PBRemote in slave mode
- Communication via rclone and cloud storage buckets (S3, Synology C2, etc.)
- No direct network connection needed - firewall friendly
- Config changes: Master writes to `data/cmd/` → sync_up to cloud → Slave pulls → executes
- Status updates: Slave writes `alive_*.cmd` → sync_up → Master pulls → displays

### Version Management
- Every config has a version number
- Version increments on save
- Remote instances compare versions to detect changes
- Must click "Activate" after config changes to deploy

### Fragment System for Performance
- Streamlit fragments enable auto-refresh without full page reload
- Used extensively in v7 pages (`RunV7.py`, `BacktestV7.py`)
- Each input field can be a fragment with independent refresh

### Windows Compatibility
- Subprocess flags: `CREATE_NO_WINDOW`, `DETACHED_PROCESS` for background services
- Path handling via `pathlib` for cross-platform compatibility
- Note: Ansible features only manage remote Linux VPS, not local Windows machine

### Security Considerations
- Password authentication via `.streamlit/secrets.toml`
- SSH key management for VPS access
- Firewall configuration (UFW) via Ansible
- OpenVPN support for secure master node access
- TOTP (Google Authenticator) support for VPS SSH

### Error Handling & Monitoring
- Services log to individual files in `data/logs/`
- Debug log aggregator available at System → Debug Log
- PBRun's Monitor class tracks passivbot errors, tracebacks, PNL
- VPS errors and warnings shown on Services page
- Automatic instance restart on crashes

## Code Conventions

1. **INI File Management**: Use `pbgui_purefunc.py` functions (`load_ini`, `save_ini`)
2. **Path Helpers**: Use `pbdir()`, `pb7dir()`, `pbvenv()`, `pb7venv()` from `pbgui_purefunc.py`
3. **Streamlit Fragments**: Use `@st.fragment` decorator for auto-refresh components
4. **Config Changes**: Always increment version and require activation
5. **Status Files**: Use `InstancesStatus` class from `Status.py` for managing status JSON files
6. **Exchange Interactions**: Use `Exchange` class from `Exchange.py`, not raw CCXT
7. **Database Access**: Use `Database` class from `Database.py` for SQLite operations
8. **Service Management**: Services should store PID files, handle signals, and log to `data/logs/`

## Common Tasks

### Adding a New Background Service
1. Create service class with `start()`, `stop()`, `is_running()` methods
2. Add to `Services.py` service list
3. Create UI controls in `navi/system_services.py`
4. Store PID in `data/pid/` and logs in `data/logs/`

### Adding a New v7 Config Parameter
1. Update `ConfigV7` hierarchy in `Config.py`
2. Add UI input in `navi/v7_run.py` (or relevant page)
3. Update validation logic if needed
4. Test with passivbot to ensure compatibility

### Adding a New Exchange
1. Add to enums in `Exchange.py`
2. Add user filtering logic in `User.py`
3. Handle exchange-specific quirks in `Exchange.py`
4. Update symbol fetching logic
5. Test with PBData for dashboard compatibility

### Debugging Remote VPS Issues
1. Check VPS online status in Services → PBRemote
2. View `alive_*.cmd` files in `data/remote/` for last heartbeat
3. View VPS logs via VPS Manager → Show Details → Logs
4. Check rclone bucket sync status
5. Verify pbgui.ini bucket configuration on VPS
6. SSH to VPS and check PBRun logs: `tail -f data/logs/pbrun.log`

## Dependencies

Key Python packages (see `requirements.txt`):
- **streamlit 1.50.0**: Web UI framework
- **ccxt 4.4.85**: Exchange API library
- **ansible 8.7.0**: VPS automation (Linux targets only)
- **ansible-runner 2.4.0**: Programmatic Ansible execution
- **plotly 6.0.1**: Chart visualization
- **psutil 5.9.4**: System monitoring
- **paramiko 4.0.0**: SSH connections
- **hjson 3.1.0**: Human JSON for configs
- **python-telegram-bot 21.10**: Telegram notifications

External dependencies:
- **rclone**: Cloud storage sync (must be installed separately)
- **passivbot v6/v7**: Trading bot engine (separate git repos)
- **Python 3.10**: Required Python version

## References

- Original PBGui: https://github.com/msei99/pbgui
- Passivbot Documentation: https://www.passivbot.com/en/latest/
- Telegram Support: https://t.me/+kwyeyrmjQ-lkYTJk
- Streamlit Documentation: https://streamlit.io/