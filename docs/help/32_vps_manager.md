# VPS Manager

The **VPS Manager** page lets you add, configure, and maintain remote VPS servers that run Passivbot instances.
Each VPS is managed via Ansible playbooks executed from the Master (local) server.

The default menu entry **System -> VPS Manager** opens the standalone **FastAPI** page.

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
| **PB7 / PB7 Branch / PB7 github** | PB7 version, branch, and whether it matches GitHub origin |
| **API Sync** | ✅ API keys in sync with Master / ❌ out of sync |

Left sidebar:

| Button | Action |
|--------|--------|
| **Add VPS** | Open the add / initialize form |
| **Refresh** | Reload all VPS status and version data |
| **Master** | Open the local Master management view |
| **API in sync / API not in sync** | Show API sync state and push API credentials to remotes that are out of sync |
| **Managed VPS** cards | Open the per-VPS management view |
| **Import Host** | Open the manual hostname import dialog; the hostname must already resolve via local `/etc/hosts` |

The overview uses the normal shared PBGui FastAPI shell. When you switch to **Master** or a specific **VPS**, the left sidebar changes into the view-specific action list just like the old page. The main overview area now stays focused on the table, while host import stays available from the sidebar as a manual hostname-based action.

The page keeps a live WebSocket connection for overview rows, progress logs, branch state, and API sync progress.

Live updates do not close the **VPS** selector anymore while you are choosing another host from the sidebar.

Live refreshes now update only the changed status regions, so typing in Add/Edit forms keeps the cursor in place and opened password reveal fields stay open while new monitor or progress data arrives.

---

## Master management

Open **Master** in the left control rail to manage the local server.

Sidebar actions:

| Button | Action |
|--------|--------|
| **Update PBGui and PB7** | Update all components |
| **Update PBGui** | Update only PBGui |
| **Update PB7** | Update only PB7 |
| **Install rustup** | Install Rust toolchain (requires sudo password) |
| **Install rclone** | Install rclone (requires sudo password) |
| **Update PB7 venv** | Recreate PB7 Python 3.12 venv (requires sudo password) |
| **Install PBGui venv** | Recreate PBGui Python 3.12 venv (requires sudo password) |

The **Master** content area also contains:
- a live status grid for PBRemote / CoinData / last command state
- **PBGui Branch Management** for branch or commit switches
- **PB7 Branch Management** with optional custom remote / fork URL support
- a **Monitor** section with server metrics plus PB7 activity data; if live monitor rows are missing, the page still lists running PB7 bot names from `status_v7.json`
- a **Progress** section with separate status buckets; when a sidebar action starts a master ansible task, the main pane switches to the shared **Command Log Viewer** for the full output, and **Home** returns to the normal master overview

---

## VPS management

Click a VPS card in the left rail to open its detail view.

Sidebar actions:

| Button | Action |
|--------|--------|
| **Read settings from VPS** | Fetch current config from VPS via SSH |
| **Initialize** | Run initial VPS setup wizard |
| **Save VPS** | Persist the current setup fields to the VPS Manager JSON entry |
| **Setup VPS** | Run the setup playbook with the current setup fields |
| **Delete VPS** | Remove this VPS from PBGui |
| **Update PBGui** | Update PBGui on this VPS |
| **Update PBGui and PB7** | Update all components |
| **Update PB7 venv** | Recreate PB7 Python 3.12 venv |
| **Update PBGui venv** | Recreate PBGui Python 3.12 venv |
| **Update Linux** | Run `apt upgrade` (optional reboot checkbox) |
| **Reboot VPS** | Restart the VPS |
| **Cleanup VPS** | Remove old packages and logs |
| **Resize Swap** | Resize swap file to configured size |
| **Update Firewall Settings** | Apply ufw firewall rules |
| **Task Logs** | Open the dedicated shared log-viewer screen for all stored VPS playbook logs and their history |
| **Host Logs** | Open the dedicated shared log-viewer screen for VPS service logs and file targets |
| **Update CoinData API** | Push updated CoinMarketCap API key |

The **VPS** content area also contains:
- a setup/config grid for password, swap, bucket, CoinMarketCap key and firewall fields
- **PBGui Branch Management** and **PB7 Branch Management** with the same switch / update workflow as the Master page
- a **Remote Monitor** section with server metrics plus PB7 activity data; if live monitor rows are missing, the page still lists running PB7 bot names from `status_v7.json`
- a **Progress** section with separate status buckets for init, setup and update runs; use the sidebar action buttons to open the shared **Command Log Viewer** whenever you need the full ansible output

The sidebar keeps the detailed log workflows separate from the normal host overview:
- utility actions such as **Task Logs**, **Host Logs**, **Read settings from VPS**, **Initialize**, or **Delete VPS** stay above a divider, while the executable ansible playbook buttons are grouped below it
- **Task Logs** opens a dedicated filtered viewer for all stored playbook logs of the selected VPS, including rotated history files
- actions such as **Initialize**, **Setup VPS**, **Update PBGui**, **Update PBGui and PB7**, **Update Linux**, **Cleanup VPS**, or **Update CoinData API** switch the main pane to the shared **Command Log Viewer** automatically
- **Host Logs** opens a dedicated **Host Log Viewer** screen for service logs, running bot logs, and file-style targets such as `sync.log`
- **Back to Host Overview** returns from either log screen to the normal VPS detail view without losing the selected host context
- every callable VPS Manager task now keeps its own current log plus rotated history entries in the shared viewer; the retention defaults to 10 history files and can be changed via `[vps_manager] task_log_history` in `pbgui.ini`
- when ansible output already contains terminal ANSI colors, the shared viewer now preserves those colors in the browser instead of relying only on text-pattern guesses
- ansible task logs with glued result markers or escaped payload control sequences like `\n` / `\r` are now expanded into readable separate display lines inside the shared viewer
- structured ansible result payloads with JSON bodies are now pretty-printed into multiline blocks, which makes nested metadata like `stat` results readable directly in the shared viewer

The status cards above the setup grid are live operator hints:
- **Update Ready** turns green as soon as a VPS user password is entered locally and shows how many Linux updates are pending.
- **CoinData Ready** shows the remaining CoinMarketCap credits when that value is available from PBRemote.
- Pending Linux updates and reboot-needed hints are refreshed from a live SSH package-status probe, so the cards no longer wait for the slower hourly `PBRemote` alive refresh.
- The detail page also includes a one-row summary table plus a remote server resource snapshot similar to the old Streamlit view.

Sensitive fields such as **VPS User Password** and **CoinMarketCap API Key** include an eye button so you can temporarily reveal the stored value while editing.

The reveal state is preserved during live updates, so opening an eye button does not immediately flip back to hidden when fresh WebSocket data arrives.

---

## Adding a new VPS

1. Click **Add VPS** in the left sidebar, or use **Import Host** to prefill the Add form from a hostname already mapped in local `/etc/hosts`.
2. Follow the step cards at the top of the page:
   - prepare an Ubuntu VPS
   - add the hostname to your local `/etc/hosts`
   - save the VPS record first
   - run **Init VPS**, then finish with **Setup VPS** from the detail page
3. Fill the **Step 4: Initial setup of your VPS** form and the **Save VPS Entry** defaults.
4. Click **Save VPS** to create or update the stored record.
5. Click **Init VPS** to start the bootstrap run.
6. After initialisation succeeds, open the VPS detail page and click **Setup VPS**.

---

## Typical workflows

### Update all servers
1. Click **Master (local)** → **Update PBGui and PB7** → wait for the log to show *successful*
2. For each VPS: click the hostname → **Update PBGui and PB7**

### Switch to a feature branch
1. Open Master or VPS detail
2. Expand **Branch Management** → select the target branch → click **Switch Branch**

### Check API key sync
- Overview table column **API Sync**: ❌ means VPS keys are out of date
- Look at the bottom of the sidebar: a **🔴 API not in sync** button appears when keys are out of date — click it to push updated keys with a live progress counter
- Per-VPS: open the VPS detail → **Update CoinData API**
