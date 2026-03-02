# VPS Manager

The **VPS Manager** page lets you add, configure, and maintain remote VPS servers that run Passivbot instances.
Each VPS is managed via Ansible playbooks executed from the Master (local) server.

---

## Overview table

The main view shows a table with all servers (Master + VPS) and their current status.

| Column | Description |
|--------|-------------|
| **Name** | Server hostname (Master shown as local) |
| **Online** | ✅ reachable / ❌ offline |
| **Role** | 🧠 Master / 💻 VPS |
| **Start** | Last boot time |
| **Reboot** | ✅ no reboot needed / ❌ reboot required |
| **Updates** | Pending Linux package updates |
| **PBGui / PBGui Branch / PBGui github** | Installed version, branch, and whether it matches GitHub origin |
| **PB6 / PB6 github** | PB6 version vs GitHub origin |
| **PB7 / PB7 Branch / PB7 github** | PB7 version, branch, and whether it matches GitHub origin |
| **API Sync** | ✅ API keys in sync with Master / ❌ out of sync |

Sidebar:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload all VPS status and version data |
| `:material/add_box:` | Add a new VPS |
| **Master (local)** button | Open the Master management view |
| **VPS hostname** buttons | Open the per-VPS management view |

At the bottom of the sidebar an **API sync status button** shows the current state:
- **🟢 API in sync** (green, disabled) — all online VPS servers have the current API keys
- **🔴 API not in sync** (red, clickable) — one or more servers are out of date; click to push keys to all of them; a live counter shows remaining servers (timeout: 180 s)

---

## Master management

Click the coloured **Master (local)** button in the sidebar to manage the local server.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload status |
| `:material/home:` | Back to Overview |
| **Update PBGui, PB6 and PB7** | Update all components |
| **Update PBGui** | Update only PBGui |
| **Update pb6 and pb7** | Update only PB6/PB7 |
| **Install rustup** | Install Rust toolchain (requires sudo password) |
| **Install rclone** | Install rclone (requires sudo password) |
| **Update PB7 venv** | Recreate PB7 Python 3.12 venv (requires sudo password) |
| **Install PBGui venv** | Recreate PBGui Python 3.12 venv (requires sudo password) |

The **Branch Management** expanders let you switch PBGui or PB7 to a different branch or commit without leaving the UI. Fork/custom-remote support is available via the optional *Custom remote* sub-expander.

---

## VPS management

Click a VPS hostname button in the sidebar to open its detail view.

Sidebar actions:

| Button | Action |
|--------|--------|
| `:material/refresh:` | Reload VPS status |
| `:material/home:` | Back to Overview |
| `:material/delete:` | Remove this VPS from PBGui |
| **Read settings from VPS** | Fetch current config from VPS via SSH |
| **Initialize** | Run initial VPS setup wizard |
| **Update PBGui** | Update PBGui on this VPS |
| **Update PBGui, PB6 and PB7** | Update all components |
| **Update PB7 venv** | Recreate PB7 Python 3.12 venv |
| **Update PBGui venv** | Recreate PBGui Python 3.12 venv |
| **Update Linux** | Run `apt upgrade` (optional reboot checkbox) |
| **Reboot VPS** | Restart the VPS |
| **Cleanup VPS** | Remove old packages and logs |
| **Resize Swap** | Resize swap file to configured size |
| **Update Firewall Settings** | Apply ufw firewall rules |
| **Update CoinData API** | Push updated CoinMarketCap API key |

The **VPS Setup Settings** expander contains the connection parameters (password, swap, rclone bucket, CoinMarketCap key, firewall).
Run **Setup VPS** once all parameters are filled in to complete the initial setup.

**Branch Management** expanders (PBGui and PB7) work the same as on the Master — switch branch or commit via Ansible without manual SSH.

The **Log viewer** at the bottom lets you fetch and display any log file from the VPS (PBRun, PBRemote, PBCoinData, etc.).

---

## Adding a new VPS

1. Click `:material/add_box:` in the sidebar.
2. Follow the 4-step wizard:
   - **Step 1** – Get a VPS (hosting recommendations)
   - **Step 2** – Install Ubuntu 24.04 on the VPS
   - **Step 3** – Add the VPS IP and hostname to your local `/etc/hosts`
   - **Step 4** – Enter credentials and click **Init VPS**
3. After initialisation succeeds, open the VPS detail view and click **Setup VPS**.

---

## Typical workflows

### Update all servers
1. Click **Master (local)** → **Update PBGui, PB6 and PB7** → wait for the log to show *successful*
2. For each VPS: click the hostname → **Update PBGui, PB6 and PB7**

### Switch to a feature branch
1. Open Master or VPS detail
2. Expand **Branch Management** → select the target branch → click **Switch Branch**

### Check API key sync
- Overview table column **API Sync**: ❌ means VPS keys are out of date
- Look at the bottom of the sidebar: a **🔴 API not in sync** button appears when keys are out of date — click it to push updated keys with a live progress counter
- Per-VPS: open the VPS detail → **Update CoinData API**
