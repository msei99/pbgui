"""Browser wizard frontend for the PBGui master installer."""

from __future__ import annotations

import html
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import shutil
import subprocess
from pathlib import Path
import tempfile
import threading
import time
import urllib.parse
import webbrowser

from .core import (
    LocalMasterConfig,
    LocalMasterMaintenanceConfig,
    LocalUninstallConfig,
    TOTP_QR_BEGIN,
    TOTP_QR_END,
    RemoteInstallError,
    RemoteMasterConfig,
    default_local_install_dir,
    default_local_master_name,
    default_remote_install_dir,
    default_target_user,
    detect_public_ip,
    generate_pbgui_password,
    inspect_local_master_install,
    local_prerequisite_status,
    run_local_master_install,
    run_local_master_maintenance,
    run_local_master_uninstall,
    run_remote_master_install,
)
from .ssh import probe_ssh_host_key

_JOBS: dict[str, dict] = {}
_JOBS_LOCK = threading.Lock()


def _attach_totp_qr_text(result: dict) -> None:
    """Attach downloaded TOTP QR text to a job result for inline display."""
    path = Path(result.get("totp_qr_local") or "")
    if path.exists():
        result["totp_qr_text"] = path.read_text(encoding="utf-8", errors="replace")


def _preserve_streamed_qr(job_id: str, result: dict) -> None:
    """Keep QR text already streamed before the final result is stored."""
    with _JOBS_LOCK:
        existing_qr = (_JOBS.get(job_id) or {}).get("result", {}).get("totp_qr_text")
    if existing_qr and not result.get("totp_qr_text"):
        result["totp_qr_text"] = existing_qr


def _install_networkmanager_profile(job_id: str) -> dict:
    """Import the OpenVPN profile into NetworkManager as split tunnel."""
    if not shutil.which("nmcli"):
        raise RuntimeError("nmcli is not installed on this local machine.")
    with _JOBS_LOCK:
        result = dict((_JOBS.get(job_id) or {}).get("result") or {})
    ovpn_path = Path(result.get("ovpn_local") or "")
    if not ovpn_path.exists():
        raise RuntimeError("OpenVPN profile is not available yet.")
    openvpn_cidr = str(result.get("openvpn_cidr") or "").strip()
    if not openvpn_cidr:
        raise RuntimeError("OpenVPN CIDR is not available yet.")

    before_proc = subprocess.run(
        ["nmcli", "-t", "-f", "UUID", "connection", "show"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    before_uuids = (
        {line.strip() for line in before_proc.stdout.splitlines() if line.strip()}
        if before_proc.returncode == 0
        else set()
    )

    import_cmd = ["nmcli", "connection", "import", "type", "openvpn", "file", str(ovpn_path)]
    proc = subprocess.run(import_cmd, check=False, capture_output=True, text=True, timeout=30)
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        raise RuntimeError(output.strip() or "NetworkManager import failed.")

    after_proc = subprocess.run(
        ["nmcli", "-t", "-f", "UUID", "connection", "show"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    output += (after_proc.stdout or "") + (after_proc.stderr or "")
    after_uuids = (
        {line.strip() for line in after_proc.stdout.splitlines() if line.strip()}
        if before_proc.returncode == 0 and after_proc.returncode == 0
        else set()
    )
    new_uuids = sorted(after_uuids - before_uuids)
    connection_uuid = new_uuids[-1] if new_uuids else ""
    if connection_uuid:
        connection_selector = ["uuid", connection_uuid]
        name_proc = subprocess.run(
            ["nmcli", "-g", "connection.id", "connection", "show", "uuid", connection_uuid],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        output += (name_proc.stdout or "") + (name_proc.stderr or "")
        connection_name = name_proc.stdout.strip() or connection_uuid
    else:
        match = re.search(r"Connection ['\u2018\u2019\"]([^'\u2018\u2019\"]+)['\u2018\u2019\"]", output)
        connection_name = match.group(1) if match else ovpn_path.stem
        connection_selector = [connection_name]

    modify_cmd = [
        "nmcli",
        "connection",
        "modify",
        *connection_selector,
        "ipv4.never-default",
        "yes",
        "ipv4.ignore-auto-routes",
        "yes",
        "ipv6.never-default",
        "yes",
        "ipv6.method",
        "disabled",
        "ipv4.ignore-auto-dns",
        "yes",
        "ipv6.ignore-auto-dns",
        "yes",
        "ipv4.routes",
        openvpn_cidr,
    ]
    proc = subprocess.run(modify_cmd, check=False, capture_output=True, text=True, timeout=30)
    output += (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        raise RuntimeError(output.strip() or "NetworkManager split-tunnel update failed.")
    return {"connection": connection_name, "openvpn_cidr": openvpn_cidr, "output": output.strip()}


def _new_job(payload: dict) -> str:
    job_id = f"job-{int(time.time() * 1000)}"
    artifact_dir = Path(tempfile.mkdtemp(prefix="pbgui-installer-"))
    qr_capture: list[str] | None = None
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "running", "logs": [], "result": {}, "artifact_dir": str(artifact_dir)}

    def log(message: str) -> None:
        nonlocal qr_capture
        text = str(message).rstrip("\r")
        if text == TOTP_QR_BEGIN:
            qr_capture = []
            return
        if text == TOTP_QR_END:
            qr_text = "\n".join(qr_capture or [])
            if qr_text:
                with _JOBS_LOCK:
                    job = _JOBS.get(job_id)
                    if job is not None:
                        job.setdefault("result", {})["totp_qr_text"] = qr_text + "\n"
            qr_capture = None
            return
        if qr_capture is not None:
            qr_capture.append(text)
            return
        with _JOBS_LOCK:
            job = _JOBS.get(job_id)
            if job is not None:
                job.setdefault("logs", []).append(text)

    def worker() -> None:
        try:
            mode = str(payload.get("install_mode") or "local")
            if mode == "local-uninstall":
                cfg = LocalUninstallConfig.from_mapping(payload)
                result = run_local_master_uninstall(cfg, log, artifact_dir)
            elif mode == "local":
                cfg = LocalMasterConfig.from_mapping(payload)
                result = run_local_master_install(cfg, log, artifact_dir)
            elif mode in {"local-pb8", "local-update-all"}:
                cfg = LocalMasterMaintenanceConfig.from_mapping(payload)
                result = run_local_master_maintenance(cfg, log, artifact_dir)
            else:
                cfg = RemoteMasterConfig.from_mapping(payload)
                result = run_remote_master_install(cfg, log, artifact_dir)
                _attach_totp_qr_text(result)
                _preserve_streamed_qr(job_id, result)
            with _JOBS_LOCK:
                _JOBS[job_id]["status"] = "done"
                _JOBS[job_id]["result"] = result
        except RemoteInstallError as exc:
            log(f"ERROR: {exc}")
            result = dict(exc.result)
            _attach_totp_qr_text(result)
            _preserve_streamed_qr(job_id, result)
            with _JOBS_LOCK:
                _JOBS[job_id]["status"] = "error"
                _JOBS[job_id]["error"] = str(exc)
                _JOBS[job_id]["result"] = result
        except Exception as exc:
            log(f"ERROR: {exc}")
            with _JOBS_LOCK:
                _JOBS[job_id]["status"] = "error"
                _JOBS[job_id]["error"] = str(exc)

    threading.Thread(target=worker, daemon=True).start()
    return job_id


def _html() -> str:
    target_user = default_target_user()
    pbgui_password = generate_pbgui_password()
    prereqs = local_prerequisite_status()
    initial_local_status = inspect_local_master_install(default_local_install_dir())
    maintenance_default = bool(initial_local_status.get("installed"))
    missing_prereqs = [str(item) for item in prereqs.get("missing") or []]
    if missing_prereqs:
        local_prereq_status_html = "Missing local prerequisites: " + html.escape(", ".join(missing_prereqs))
        if prereqs.get("sudo_password_useful"):
            local_prereq_status_html += ". Enter your local sudo password or use an existing sudo session."
    else:
        local_prereq_status_html = "Local prerequisites are already available; sudo password is not needed."
    return r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>PBGui Master Installer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root { color-scheme: dark; --bg:#0f1724; --panel:#151f31; --line:#263247; --text:#e2e8f0; --muted:#94a3b8; --accent:#63b3ed; --danger:#f87171; --ok:#48bb78; }
    body { margin:0; font-family: Inter, system-ui, sans-serif; background:var(--bg); color:var(--text); }
    body { overflow-x:hidden; }
    main { max-width:1100px; margin:0 auto; padding:28px; display:grid; gap:20px; min-width:0; }
    h1 { margin:0; font-size:28px; }
    .panel { background:var(--panel); border:1px solid var(--line); border-radius:14px; padding:20px; box-shadow:0 18px 50px rgba(0,0,0,.25); min-width:0; }
    .grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }
    label { display:grid; gap:6px; font-size:13px; color:var(--muted); }
    input, select { height:38px; border-radius:8px; border:1px solid var(--line); background:#0b1220; color:var(--text); padding:0 10px; font-size:14px; }
    .password-wrap { display:flex; align-items:center; border:1px solid var(--line); border-radius:8px; background:#0b1220; overflow:hidden; }
    .password-wrap input { flex:1; min-width:0; border:0; border-radius:0; background:transparent; }
    .full { grid-column:1/-1; }
    .warning { border:1px solid rgba(248,113,113,.45); background:rgba(248,113,113,.08); color:#fecaca; padding:12px; border-radius:10px; display:none; }
    button { height:40px; border:0; border-radius:9px; padding:0 18px; font-weight:700; cursor:pointer; background:var(--accent); color:#07111f; }
    button:disabled { opacity:.55; cursor:not-allowed; }
    .password-toggle { width:42px; height:38px; border-left:1px solid var(--line); border-radius:0; padding:0; background:#111a2b; color:var(--muted); font-size:16px; }
    .password-toggle:hover, .password-toggle[aria-pressed="true"] { color:var(--accent); background:#172238; }
    pre { background:#090f1a; border:1px solid var(--line); border-radius:12px; padding:14px; min-height:280px; max-height:440px; overflow:auto; white-space:pre-wrap; overflow-wrap:anywhere; word-break:break-word; color:#cbd5e1; box-sizing:border-box; max-width:100%; }
    .qr-wrap { max-width:100%; overflow:auto; }
    .qr-code { min-height:0; max-height:none; display:block; width:max-content; font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size:7px; line-height:1; white-space:pre; overflow-wrap:normal; word-break:normal; }
    .ansi-red { color:#fca5a5; }
    .ansi-green { color:#86efac; }
    .ansi-yellow { color:#fde68a; }
    .ansi-cyan { color:#67e8f9; }
    .ansi-bold { font-weight:700; }
    .result { display:none; gap:10px; min-width:0; }
    .secondary { background:#1f2937; color:var(--text); }
    .recommendation { border:1px solid rgba(99,179,237,.35); background:rgba(99,179,237,.08); color:#bfdbfe; padding:12px; border-radius:10px; }
    .danger-panel { border:1px solid rgba(248,113,113,.55); background:rgba(248,113,113,.09); color:#fecaca; padding:12px; border-radius:10px; display:grid; gap:10px; }
    .danger-panel strong { color:#fee2e2; }
    .modal-backdrop { position:fixed; inset:0; display:none; align-items:center; justify-content:center; background:rgba(2,6,23,.76); padding:24px; z-index:20; }
    .modal { width:min(520px,100%); background:var(--panel); border:1px solid rgba(248,113,113,.55); border-radius:16px; box-shadow:0 22px 80px rgba(0,0,0,.5); padding:20px; display:grid; gap:14px; }
    .modal h3 { margin:0; font-size:20px; color:#fee2e2; }
    .modal p { margin:0; color:var(--muted); line-height:1.5; }
    .modal-actions { display:flex; gap:10px; justify-content:flex-end; flex-wrap:wrap; }
    .danger-button { background:var(--danger); color:#190606; }
    .path-preview { grid-column:1/-1; border:1px solid var(--line); background:#0b1220; color:var(--muted); padding:10px; border-radius:10px; font-size:13px; line-height:1.45; }
    .path-preview strong { color:var(--text); }
    .progress-wrap { display:grid; gap:8px; margin-bottom:12px; }
    .progress-row { display:flex; justify-content:space-between; gap:12px; color:var(--muted); font-size:13px; }
    .progress-stats { display:flex; gap:10px; white-space:nowrap; }
    .progress-track { height:12px; border-radius:999px; border:1px solid var(--line); background:#0b1220; overflow:hidden; }
    .progress-bar { height:100%; width:0%; background:linear-gradient(90deg,#2563eb,#63b3ed); transition:width .25s ease; }
    .progress-bar.done { background:linear-gradient(90deg,#16a34a,#86efac); }
    .progress-bar.error { background:linear-gradient(90deg,#dc2626,#fca5a5); }
    a { color:#93c5fd; }
  </style>
</head>
<body>
<main>
  <section>
    <h1>PBGui Master Installer</h1>
    <p style="color:var(--muted)">Install a fresh PBGui master or safely maintain an existing local master.</p>
  </section>
  <section class="panel">
    <h2 id="mode-title">Local Master Install</h2>
    <p class="recommendation remote-only">Recommended remote master VPS: <a href="https://aklam.io/CBA3zSaZ" target="_blank" rel="noopener noreferrer">IONOS VPS Linux M+</a> with 4 vCores CPU, 4 GB RAM, and 120 GB NVMe, or <a href="https://www.netcup.com/server/vps-lite?ref=390177" target="_blank" rel="noopener noreferrer">netcup VPS Lite 1 G12s</a> with 2 vCores CPU, 4 GB RAM, 80 GB SSD and traffic included. Use netcup for a remote master without optimization; customers can be assigned via the referral link or a 5 EUR new-customer coupon such as <code>36nc17835299729</code>.</p>
    <form id="install-form" class="grid">
      <label class="full">Install mode
        <select name="install_mode" id="install-mode">
          <option value="local-pb8" %%MAINTENANCE_SELECTED%%>Install/Update PB8</option>
          <option value="local-update-all">Update PBGui, PB7 and PB8</option>
          <option value="local" %%FRESH_SELECTED%%>Fresh/Reinstall Local Master</option>
          <option value="remote">Remote Master VPS</option>
          <option value="local-uninstall">Local Master Uninstall</option>
        </select>
      </label>
      <label class="remote-only">Initial login mode
        <select name="login_mode" id="login-mode">
          <option value="root">Fresh VPS / root login</option>
          <option value="sudo">Existing sudo user</option>
        </select>
      </label>
      <label class="remote-only">VPS IP or hostname <input name="remote_host" id="remote-host" required placeholder="1.2.3.4"></label>
      <label class="remote-only">SSH port <input name="ssh_port" id="ssh-port" type="number" value="22" min="1" max="65535"></label>
      <label class="remote-only">SSH username <input name="ssh_username" id="ssh-username" value="root"></label>
      <label class="remote-only">SSH password
        <span class="password-wrap">
          <input id="ssh-password" name="ssh_password" type="password" required autocomplete="current-password">
          <button class="password-toggle" type="button" data-target="ssh-password" aria-label="Show SSH password" aria-pressed="false">&#128065;</button>
        </span>
      </label>
      <label class="remote-only" id="root-password-wrap">New root password (optional)
        <span class="password-wrap">
          <input id="root-password" name="root_password" type="password" autocomplete="new-password" placeholder="Unchanged if empty">
          <button class="password-toggle" type="button" data-target="root-password" aria-label="Show new root password" aria-pressed="false">&#128065;</button>
        </span>
      </label>
      <label class="remote-only">Target PBGui user <input name="target_user" id="target-user" value="%%TARGET_USER%%"></label>
      <label class="remote-only" id="target-password-wrap">Target user password
        <span class="password-wrap">
          <input id="target-password" name="target_password" type="password" autocomplete="new-password">
          <button class="password-toggle" type="button" data-target="target-password" aria-label="Show target user password" aria-pressed="false">&#128065;</button>
        </span>
      </label>
      <label class="full">Install parent directory <input name="install_dir" id="install-dir" value="%%INSTALL_DIR%%" placeholder="/home/%%TARGET_USER%%/software"></label>
      <div class="path-preview" id="install-preview"></div>
      <div class="path-preview maintenance-only" id="local-master-status"></div>
      <div class="path-preview local-only" id="local-prereq-status">%%LOCAL_PREREQ_STATUS%%</div>
      <label class="local-only" id="local-sudo-wrap">Local sudo password (only if apt prerequisites are missing)
        <span class="password-wrap">
          <input id="local-sudo-password" name="local_sudo_password" type="password" autocomplete="current-password" placeholder="Only used for apt prerequisite installation">
          <button class="password-toggle" type="button" data-target="local-sudo-password" aria-label="Show local sudo password" aria-pressed="false">&#128065;</button>
        </span>
      </label>
      <label class="install-only">Master name <input name="hostname" id="master-name" value="pbgui-master"></label>
      <label class="remote-only">Swap size
        <select id="swap-size-select">
          <option value="4G">4 GB</option>
          <option value="6G" selected>6 GB</option>
          <option value="8G">8 GB</option>
          <option value="custom">Custom</option>
        </select>
      </label>
      <label class="remote-only" id="swap-custom-wrap" style="display:none">Custom swap size <input id="swap-custom" placeholder="10G"></label>
      <input type="hidden" name="swap_size" id="swap-size-value" value="6G">
      <label class="install-only">PBGui login password
        <span class="password-wrap">
          <input id="pbgui-password" name="pbgui_password" type="password" value="%%PBGUI_PASSWORD%%" autocomplete="new-password">
          <button class="password-toggle" type="button" data-target="pbgui-password" aria-label="Show PBGui login password" aria-pressed="false">&#128065;</button>
        </span>
        <span>Generated uniquely for this installation. Reveal and store it before starting.</span>
      </label>
      <label class="install-only">PBGui bind address <input name="pbgui_bind_host" id="pbgui-bind-host" value="0.0.0.0"></label>
      <label class="install-only">PBGui port <input name="pbgui_port" type="number" value="8000" min="1024" max="65535"></label>
      <label class="remote-only">OpenVPN network CIDR <input name="openvpn_cidr" value="10.8.0.0/24" placeholder="10.8.0.0/24"></label>
      <label class="remote-only">SSH firewall mode
        <select name="ssh_mode" id="ssh-mode">
          <option value="specific_ips_vpn">Specific IPs + VPN (Recommended)</option>
          <option value="vpn_only">VPN only (Most secure)</option>
          <option value="anywhere">Allow SSH from everywhere (Not secure, not recommended)</option>
        </select>
      </label>
      <label class="full remote-only" id="ssh-ips-wrap">Allowed SSH source IPs, comma-separated <input name="ssh_allowed_ips" id="ssh-allowed-ips" placeholder="detecting your public IP..."></label>
      <div class="warning full remote-only" id="ssh-warning">
        Not secure, not recommended. SSH will be reachable from the public internet. Use this only temporarily if you understand the risk.<br>
        <label style="margin-top:8px"><input type="checkbox" id="ssh-risk" style="height:auto"> I understand that public SSH access is not recommended.</label>
      </div>
      <div class="danger-panel full remote-only" id="fresh-host-warning">
        <strong>Remote Master install is intended for a fresh VPS.</strong>
        <span>It may change hostname, users, passwords, swap, firewall rules, OpenVPN, packages, and PBGui systemd services on the target.</span>
        <label><input type="checkbox" name="confirm_fresh_host" id="confirm-fresh-host" value="yes" style="height:auto"> I confirm this target is a fresh/disposable VPS prepared for PBGui Master install.</label>
        <input type="hidden" name="accept_unknown_host" id="accept-unknown-host" value="">
        <input type="hidden" name="accepted_host_key_fingerprint" id="accepted-host-key-fingerprint" value="">
      </div>
      <div class="danger-panel full fresh-local-only" id="fresh-local-warning">
        <strong>Fresh/Reinstall is disruptive when PBGui already exists.</strong>
        <span>It stops PBRun and PB7 processes and replaces PBGui configuration and authentication. Use a maintenance action above for an existing master.</span>
      </div>
      <div class="danger-panel full uninstall-only" id="uninstall-warning">
        <strong>Local uninstall removes PBGui/PB7/PB8 checkouts, virtualenvs, and PBGui systemd user services under the selected parent directory.</strong>
        <span>After clicking Uninstall Local Master, a safety dialog will ask for one final confirmation.</span>
      </div>
      <div class="full"><button id="start-btn" type="submit">Install Local Master</button></div>
    </form>
  </section>
  <section class="panel">
    <h2>Progress</h2>
    <div class="progress-wrap" id="progress-wrap">
      <div class="progress-row"><span id="progress-label">Waiting for installer input...</span><span class="progress-stats"><span id="progress-percent">0%</span><span id="progress-duration">Elapsed 0s</span></span></div>
      <div class="progress-track"><div class="progress-bar" id="progress-bar"></div></div>
    </div>
    <pre id="log">Waiting for installer input...</pre>
    <div class="result" id="result"></div>
  </section>
</main>
<div class="modal-backdrop" id="uninstall-modal" role="dialog" aria-modal="true" aria-labelledby="uninstall-modal-title">
  <div class="modal">
    <h3 id="uninstall-modal-title">Confirm Local Uninstall</h3>
    <p id="uninstall-modal-message"></p>
    <p>This removes the local PBGui/PB7/PB8 checkouts, virtualenvs, and PBGui systemd user services for the selected install parent.</p>
    <div class="modal-actions">
      <button type="button" class="secondary" id="uninstall-cancel-btn">Cancel</button>
      <button type="button" class="danger-button" id="uninstall-confirm-btn">Uninstall Local Master</button>
    </div>
  </div>
</div>
<div class="modal-backdrop" id="host-key-modal" role="dialog" aria-modal="true" aria-labelledby="host-key-modal-title">
  <div class="modal">
    <h3 id="host-key-modal-title">Confirm SSH Host Key</h3>
    <p id="host-key-modal-message"></p>
    <p>If this fingerprint does not match your VPS provider console, cancel and verify the target before installing.</p>
    <div class="modal-actions">
      <button type="button" class="secondary" id="host-key-cancel-btn">Cancel</button>
      <button type="button" class="danger-button" id="host-key-confirm-btn">Trust Host Key and Install</button>
    </div>
  </div>
</div>
<script>
const form = document.getElementById('install-form');
const logEl = document.getElementById('log');
const progressBar = document.getElementById('progress-bar');
const progressLabel = document.getElementById('progress-label');
const progressPercent = document.getElementById('progress-percent');
const progressDuration = document.getElementById('progress-duration');
const resultEl = document.getElementById('result');
const startBtn = document.getElementById('start-btn');
const installMode = document.getElementById('install-mode');
const modeTitle = document.getElementById('mode-title');
const sshMode = document.getElementById('ssh-mode');
const sshWarning = document.getElementById('ssh-warning');
const sshRisk = document.getElementById('ssh-risk');
const uninstallModal = document.getElementById('uninstall-modal');
const uninstallModalMessage = document.getElementById('uninstall-modal-message');
const uninstallCancelBtn = document.getElementById('uninstall-cancel-btn');
const uninstallConfirmBtn = document.getElementById('uninstall-confirm-btn');
const localSudoWrap = document.getElementById('local-sudo-wrap');
const hostKeyModal = document.getElementById('host-key-modal');
const hostKeyModalMessage = document.getElementById('host-key-modal-message');
const hostKeyCancelBtn = document.getElementById('host-key-cancel-btn');
const hostKeyConfirmBtn = document.getElementById('host-key-confirm-btn');
const remoteHost = document.getElementById('remote-host');
const sshPort = document.getElementById('ssh-port');
const sshIpsWrap = document.getElementById('ssh-ips-wrap');
const sshAllowedIps = document.getElementById('ssh-allowed-ips');
const loginMode = document.getElementById('login-mode');
const sshUsername = document.getElementById('ssh-username');
const targetUser = document.getElementById('target-user');
const targetPasswordWrap = document.getElementById('target-password-wrap');
const rootPasswordWrap = document.getElementById('root-password-wrap');
const installDir = document.getElementById('install-dir');
const installPreview = document.getElementById('install-preview');
const masterName = document.getElementById('master-name');
const pbguiBindHost = document.getElementById('pbgui-bind-host');
const swapSizeSelect = document.getElementById('swap-size-select');
const swapCustomWrap = document.getElementById('swap-custom-wrap');
const swapCustom = document.getElementById('swap-custom');
const swapSizeValue = document.getElementById('swap-size-value');
const confirmFreshHost = document.getElementById('confirm-fresh-host');
const localMasterStatusEl = document.getElementById('local-master-status');
const acceptUnknownHost = document.getElementById('accept-unknown-host');
const acceptedHostKeyFingerprint = document.getElementById('accepted-host-key-fingerprint');
const defaultTargetUser = %%TARGET_USER_JSON%%;
const defaultLocalInstallDir = %%LOCAL_INSTALL_DIR_JSON%%;
const defaultLocalMasterName = %%LOCAL_MASTER_NAME_JSON%%;
const localSudoPasswordUseful = %%LOCAL_SUDO_PASSWORD_USEFUL_JSON%%;
let localMasterStatus = %%LOCAL_MASTER_STATUS_JSON%%;
let localStatusGeneration = 0;
let pollTimer = null;
let progressTimer = null;
let progressStartedAt = 0;
let progressStoppedElapsed = 0;
let currentJobId = '';
let currentJobMode = 'local';
let lastLogCount = 0;
let logHasContent = false;
let installDirTouched = false;
let masterNameTouched = false;
let bindHostTouched = false;
let pendingUninstallConfirm = null;
let pendingHostKeyConfirm = null;
function defaultInstallDir() {
  if (installMode.value !== 'remote') return defaultLocalInstallDir;
  const user = (targetUser.value || defaultTargetUser || 'pbgui').trim() || 'pbgui';
  return '/home/' + user + '/software';
}
function defaultMasterName() { return installMode.value === 'remote' ? 'pbgui-master' : defaultLocalMasterName; }
function defaultBindHost() { return installMode.value === 'remote' ? '0.0.0.0' : '127.0.0.1'; }
function syncInstallDir() {
  if (!installDirTouched) installDir.value = defaultInstallDir();
  syncInstallPreview();
}
function joinPath(parent, child) {
  return String(parent || '').replace(/\/+$/, '') + '/' + child;
}
function syncInstallPreview() {
  const parent = (installDir.value || defaultInstallDir()).trim();
  const valid = parent.startsWith('/') || parent === '~' || parent.startsWith('~/');
  const action = installMode.value === 'local-uninstall' ? 'Will remove ' : (installMode.value.startsWith('local-') ? 'Existing ' : '');
  installPreview.innerHTML = (valid ? '' : '<div style="color:var(--danger)">Install parent directory must be an absolute path or start with ~.</div>')
    + '<div><strong>' + action + 'PBGui:</strong> ' + escapeHtml(joinPath(parent, 'pbgui')) + '</div>'
    + '<div><strong>' + action + 'PB7:</strong> ' + escapeHtml(joinPath(parent, 'pb7')) + '</div>'
    + '<div><strong>' + action + 'PB8:</strong> ' + escapeHtml(joinPath(parent, 'pb8')) + '</div>'
    + '<div><strong>' + action + 'Venvs:</strong> ' + escapeHtml(joinPath(parent, 'venv_pbgui')) + ', ' + escapeHtml(joinPath(parent, 'venv_pb7')) + ', ' + escapeHtml(joinPath(parent, 'venv_pb8')) + '</div>'
    + (installMode.value === 'local-uninstall' ? '<div><strong>Will remove systemd user units:</strong> pbgui-api, pbgui-pbcluster, pbgui-pbrun, pbgui-pbdata, pbgui-pbcoindata, obsolete pbgui-pbremote if present</div>' : '');
}
function isMaintenanceMode() { return installMode.value === 'local-pb8' || installMode.value === 'local-update-all'; }
function renderLocalMasterStatus() {
  if (!localMasterStatusEl) return;
  const installed = !!localMasterStatus.installed;
  const pb8Installed = !!localMasterStatus.pb8_installed;
  const errors = installMode.value === 'local-update-all' ? (localMasterStatus.update_all_errors || []) : (localMasterStatus.maintenance_errors || []);
  localMasterStatusEl.innerHTML = installed
    ? '<div><strong>Existing local master detected.</strong></div><div>PBGui: ' + escapeHtml(localMasterStatus.pbgui_dir || '') + '</div><div>PB8: ' + (pb8Installed ? 'installed' : 'not installed') + '</div>'
      + (errors.length ? '<div style="color:var(--danger)">' + errors.map(escapeHtml).join('<br>') + '</div>' : '<div style="color:var(--ok)">Maintenance preflight passed.</div>')
    : '<div style="color:var(--danger)">No existing local PBGui master was detected under this install parent.</div>';
  const pb8Option = installMode.querySelector('option[value="local-pb8"]');
  if (pb8Option) pb8Option.textContent = pb8Installed ? 'Update PB8' : 'Install PB8';
  if (installMode.value === 'local-pb8') {
    modeTitle.textContent = pb8Installed ? 'Update PB8' : 'Install PB8';
    startBtn.textContent = pb8Installed ? 'Update PB8' : 'Install PB8';
  }
  if (isMaintenanceMode() && !currentJobId) startBtn.disabled = !installed || errors.length > 0;
}
let statusTimer = null;
function refreshLocalMasterStatus() {
  if (statusTimer) clearTimeout(statusTimer);
  const generation = ++localStatusGeneration;
  statusTimer = setTimeout(() => {
    const parent = (installDir.value || defaultLocalInstallDir).trim();
    fetch('/api/local-master-status?install_dir=' + encodeURIComponent(parent))
      .then(r => r.json().then(data => ({ok:r.ok, data})))
      .then(({ok, data}) => {
        if (generation !== localStatusGeneration) return;
        localMasterStatus = ok ? data : {installed:false, maintenance_errors:[data.error || 'Inspection failed'], update_all_errors:[data.error || 'Inspection failed']};
        renderLocalMasterStatus();
      })
      .catch(err => {
        if (generation !== localStatusGeneration) return;
        localMasterStatus = {installed:false, maintenance_errors:[String(err)], update_all_errors:[String(err)]};
        renderLocalMasterStatus();
      });
  }, 250);
}
function syncMode() {
  if (installMode.value !== 'remote') {
    sshWarning.style.display = 'none';
    sshIpsWrap.style.display = 'none';
    return;
  }
  sshWarning.style.display = sshMode.value === 'anywhere' ? 'block' : 'none';
  sshIpsWrap.style.display = sshMode.value === 'specific_ips_vpn' ? 'grid' : 'none';
}
function syncLogin() {
  if (installMode.value !== 'remote') {
    syncInstallDir();
    return;
  }
  if (loginMode.value === 'root') {
    sshUsername.value = 'root';
    targetUser.value = targetUser.value || defaultTargetUser;
    rootPasswordWrap.style.display = 'grid';
    targetPasswordWrap.style.display = 'grid';
  } else {
    if (sshUsername.value === 'root') sshUsername.value = '';
    targetUser.value = sshUsername.value || targetUser.value;
    rootPasswordWrap.style.display = 'none';
    targetPasswordWrap.style.display = 'none';
  }
  syncInstallDir();
}
function syncInstallMode() {
  const isRemote = installMode.value === 'remote';
  const isUninstall = installMode.value === 'local-uninstall';
  const isMaintenance = isMaintenanceMode();
  const isFreshInstall = installMode.value === 'local' || isRemote;
  if (!isMaintenance && !currentJobId) startBtn.disabled = false;
  document.querySelectorAll('.remote-only').forEach(el => {
    el.style.display = isRemote ? '' : 'none';
    el.querySelectorAll('input,select,button,textarea').forEach(ctrl => { ctrl.disabled = !isRemote; });
  });
  document.querySelectorAll('.install-only').forEach(el => {
    el.style.display = isFreshInstall ? '' : 'none';
    el.querySelectorAll('input,select,button,textarea').forEach(ctrl => { ctrl.disabled = !isFreshInstall; });
  });
  document.querySelectorAll('.uninstall-only').forEach(el => {
    el.style.display = isUninstall ? 'grid' : 'none';
    el.querySelectorAll('input,select,button,textarea').forEach(ctrl => { ctrl.disabled = !isUninstall; });
  });
  document.querySelectorAll('.local-only').forEach(el => {
    el.style.display = installMode.value === 'local' ? '' : 'none';
    el.querySelectorAll('input,select,button,textarea').forEach(ctrl => { ctrl.disabled = installMode.value !== 'local'; });
  });
  document.querySelectorAll('.maintenance-only').forEach(el => {
    el.style.display = isMaintenance ? '' : 'none';
  });
  document.querySelectorAll('.fresh-local-only').forEach(el => {
    el.style.display = installMode.value === 'local' ? 'grid' : 'none';
  });
  if (localSudoWrap) {
    const showSudo = installMode.value === 'local' && localSudoPasswordUseful;
    localSudoWrap.style.display = showSudo ? '' : 'none';
    localSudoWrap.querySelectorAll('input,select,button,textarea').forEach(ctrl => { ctrl.disabled = !showSudo; });
  }
  modeTitle.textContent = isUninstall ? 'Local Master Uninstall' : (isRemote ? 'Remote Master VPS' : (installMode.value === 'local-pb8' ? (localMasterStatus.pb8_installed ? 'Update PB8' : 'Install PB8') : (installMode.value === 'local-update-all' ? 'Update Local Master' : 'Fresh/Reinstall Local Master')));
  startBtn.textContent = isUninstall ? 'Uninstall Local Master' : (isRemote ? 'Install Remote Master' : (installMode.value === 'local-pb8' ? (localMasterStatus.pb8_installed ? 'Update PB8' : 'Install PB8') : (installMode.value === 'local-update-all' ? 'Update PBGui, PB7 and PB8' : 'Fresh/Reinstall Local Master')));
  if (!masterNameTouched && !isUninstall) masterName.value = defaultMasterName();
  if (!bindHostTouched && !isUninstall) pbguiBindHost.value = defaultBindHost();
  syncMode();
  syncLogin();
  syncInstallDir();
  renderLocalMasterStatus();
  if (isMaintenance) refreshLocalMasterStatus();
}
function syncSwapSize() {
  const isCustom = swapSizeSelect.value === 'custom';
  swapCustomWrap.style.display = isCustom ? 'grid' : 'none';
  swapSizeValue.value = isCustom ? (swapCustom.value.trim() || '6G') : swapSizeSelect.value;
}
installMode.addEventListener('change', syncInstallMode); sshMode.addEventListener('change', syncMode); loginMode.addEventListener('change', syncLogin); sshUsername.addEventListener('input', () => { if (loginMode.value === 'sudo') { targetUser.value = sshUsername.value; syncInstallDir(); } }); targetUser.addEventListener('input', syncInstallDir); installDir.addEventListener('input', () => { installDirTouched = true; syncInstallPreview(); if (isMaintenanceMode()) refreshLocalMasterStatus(); }); masterName.addEventListener('input', () => { masterNameTouched = true; }); pbguiBindHost.addEventListener('input', () => { bindHostTouched = true; }); swapSizeSelect.addEventListener('change', syncSwapSize); swapCustom.addEventListener('input', syncSwapSize);
syncInstallMode();
syncSwapSize();
document.querySelectorAll('.password-toggle').forEach(btn => {
  btn.addEventListener('click', () => {
    const input = document.getElementById(btn.dataset.target || '');
    if (!input) return;
    const show = input.type === 'password';
    input.type = show ? 'text' : 'password';
    btn.setAttribute('aria-pressed', show ? 'true' : 'false');
    const labels = {'ssh-password':'SSH password', 'root-password':'new root password', 'target-password':'target user password', 'local-sudo-password':'local sudo password', 'pbgui-password':'PBGui login password'};
    const label = labels[input.id] || 'password';
    btn.setAttribute('aria-label', (show ? 'Hide ' : 'Show ') + label);
  });
});
fetch('/api/public-ip').then(r => r.json()).then(data => {
  if (data.ip && sshAllowedIps && !sshAllowedIps.value.trim()) {
    sshAllowedIps.value = data.ip;
    sshAllowedIps.placeholder = data.ip;
  }
}).catch(() => {});
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function cleanLogChunk(value) {
  return escapeHtml(String(value).replace(/\r/g, '\n').replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, ''));
}
function formatElapsed(ms) {
  const total = Math.max(0, Math.floor(Number(ms || 0) / 1000));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = total % 60;
  if (hours > 0) return hours + 'h ' + String(minutes).padStart(2, '0') + 'm ' + String(seconds).padStart(2, '0') + 's';
  if (minutes > 0) return minutes + 'm ' + String(seconds).padStart(2, '0') + 's';
  return seconds + 's';
}
function updateElapsedDisplay() {
  const elapsed = progressStartedAt ? (progressStoppedElapsed || (Date.now() - progressStartedAt)) : 0;
  progressDuration.textContent = 'Elapsed ' + formatElapsed(elapsed);
}
function stopProgressTimer() {
  if (progressTimer) clearInterval(progressTimer);
  progressTimer = null;
  if (progressStartedAt && !progressStoppedElapsed) progressStoppedElapsed = Date.now() - progressStartedAt;
  updateElapsedDisplay();
}
function startProgressTimer() {
  if (progressTimer) clearInterval(progressTimer);
  progressStartedAt = Date.now();
  progressStoppedElapsed = 0;
  updateElapsedDisplay();
  progressTimer = setInterval(updateElapsedDisplay, 1000);
}
function stopPolling() {
  if (pollTimer) clearTimeout(pollTimer);
  pollTimer = null;
}
function ansiToHtml(text) {
  const colorMap = {31:'ansi-red', 32:'ansi-green', 33:'ansi-yellow', 36:'ansi-cyan'};
  const re = /\x1b\[([0-9;]*)m/g;
  let html = '';
  let last = 0;
  let open = false;
  let match;
  while ((match = re.exec(String(text))) !== null) {
    html += cleanLogChunk(String(text).slice(last, match.index));
    if (open) { html += '</span>'; open = false; }
    const codes = (match[1] || '0').split(';').filter(Boolean);
    if (!codes.includes('0')) {
      const classes = [];
      if (codes.includes('1')) classes.push('ansi-bold');
      codes.forEach(code => { if (colorMap[code]) classes.push(colorMap[code]); });
      if (classes.length) { html += '<span class="' + classes.join(' ') + '">'; open = true; }
    }
    last = re.lastIndex;
  }
  html += cleanLogChunk(String(text).slice(last));
  if (open) html += '</span>';
  return html;
}
function selectionInsideLog() {
  const sel = window.getSelection ? window.getSelection() : null;
  return !!(sel && !sel.isCollapsed && logEl.contains(sel.anchorNode) && logEl.contains(sel.focusNode));
}
function appendLogs(lines) {
  lines = Array.isArray(lines) ? lines : [];
  if (!lines.length) {
    if (!logHasContent) logEl.textContent = 'No logs yet...';
    return;
  }
  if (lines.length < lastLogCount) {
    logEl.innerHTML = '';
    lastLogCount = 0;
    logHasContent = false;
  }
  if (lines.length === lastLogCount) return;
  const nearBottom = logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 48;
  const nextLines = lines.slice(lastLogCount);
  if (!logHasContent) {
    logEl.innerHTML = '';
    logHasContent = true;
  }
  const prefix = lastLogCount > 0 ? '\n' : '';
  logEl.insertAdjacentHTML('beforeend', ansiToHtml(prefix + nextLines.join('\n')));
  lastLogCount = lines.length;
  if (nearBottom && !selectionInsideLog()) logEl.scrollTop = logEl.scrollHeight;
}
const progressPhases = [
  { pct: 5, label: 'Starting installer', re: /Starting installation|Starting update|Connecting to|Using local install parent directory|Existing local PBGui master detected/ },
  { pct: 15, label: 'Running maintenance preflight', re: /Maintenance preflight|existing local PBGui master detected/i },
  { pct: 30, label: 'Updating PBGui and PB7', re: /Updating PBGui and pinned PB7|master-update-pb\.yml/ },
  { pct: 12, label: 'Installing prerequisites', re: /Ensuring local installer prerequisites|Installing system packages|apt-get install/ },
  { pct: 22, label: 'Preparing repositories', re: /Cloning PBGui|Updating existing checkout|git clone|Using current PBGui checkout/ },
  { pct: 38, label: 'Creating virtualenvs', re: /Creating Python virtualenvs|python3\.12 -m venv/ },
  { pct: 52, label: 'Installing Python dependencies', re: /pip install -r|requirements\.txt|pip install maturin/ },
  { pct: 68, label: 'Building passivbot-rust', re: /Building passivbot-rust|maturin develop|Rust source stamp updated/ },
  { pct: 76, label: 'Installing Passivbot v8', re: /Installing Passivbot v8 full profile|pb8\[full\]|passivbot --help|CONFIG_SCHEMA_VERSION/ },
  { pct: 82, label: 'Writing configuration', re: /Writing PBGui configuration|secrets\.toml|pbgui\.ini/ },
  { pct: 82, label: 'Removing local install', re: /Uninstalling local PBGui master|Removed PBGui|Removed PB7|Removed PB8|Removed PBGui venv|Removed PB7 venv|Removed PB8 venv/ },
  { pct: 87, label: 'Configuring remote access', re: /Setting up OpenVPN|Configuring firewall|TOTP|OpenVPN/ },
  { pct: 93, label: 'Installing systemd services', re: /Installing PBGui systemd user services|setup_systemd|Enabled pbgui-/ },
  { pct: 97, label: 'Checking PBGui API', re: /Checking PBGui API service|PBGui API is listening/ },
];
function setProgress(pct, label, state) {
  const value = Math.max(0, Math.min(100, Math.round(pct || 0)));
  progressBar.style.width = value + '%';
  progressBar.className = 'progress-bar' + (state ? ' ' + state : '');
  progressLabel.textContent = label || 'Waiting for installer input...';
  progressPercent.textContent = value + '%';
}
function updateProgress(job) {
  const status = job.status || 'running';
  const mode = (job.result || {}).mode || currentJobMode;
  const maintenance = mode === 'local-pb8' || mode === 'local-update-all';
  if (status === 'done') { setProgress(100, mode === 'local-uninstall' ? 'Uninstall complete' : (maintenance ? 'Update complete' : 'Installation complete'), 'done'); return; }
  if (status === 'error') { setProgress(100, mode === 'local-uninstall' ? 'Uninstall failed' : (maintenance ? 'Update failed' : 'Installation failed'), 'error'); return; }
  const text = (job.logs || []).join('\n');
  let pct = text ? 5 : 0;
  let label = text ? 'Starting installer' : 'Waiting for installer input...';
  progressPhases.forEach(phase => {
    if (phase.re.test(text) && phase.pct >= pct) {
      pct = phase.pct;
      label = phase.label;
    }
  });
  setProgress(pct, label, '');
}
function renderResult(job) {
  currentJobId = job.id || currentJobId;
  const r = job.result || {};
  const isUninstall = r.mode === 'local-uninstall' || currentJobMode === 'local-uninstall';
  const isMaintenance = ['local-pb8', 'local-update-all'].includes(r.mode || currentJobMode);
  if (job.status !== 'done' && job.status !== 'error' && !r.totp_qr_text) return;
  const vpnUrl = escapeHtml(r.vpn_url || '');
  const vpnHref = escapeHtml(r.vpn_url || '#');
  const localUrl = escapeHtml(r.local_url || '');
  const localHref = escapeHtml(r.local_url || '#');
  const qrText = escapeHtml(r.totp_qr_text || '');
  resultEl.style.display = 'grid';
  resultEl.innerHTML = (job.status === 'done' && r.mode === 'local-uninstall'
    ? '<strong style="color:var(--ok)">Local uninstall complete.</strong><div style="color:var(--muted)">Removed install parent targets under: ' + escapeHtml(r.install_dir || '') + '</div>'
    : job.status === 'done' && isMaintenance
    ? '<strong style="color:var(--ok)">Local master update complete.</strong><div style="color:var(--muted)">PBGui: ' + escapeHtml(r.pbgui_dir || '') + '<br>PB7: ' + escapeHtml(r.pb7_dir || '') + '<br>PB8: ' + escapeHtml(r.pb8_dir || '') + '</div>'
    : job.status === 'done' && r.mode === 'local'
    ? '<strong style="color:var(--ok)">Local installation complete.</strong><div>Open PBGui: <a href="' + localHref + '" target="_blank">' + localUrl + '</a></div><div style="color:var(--muted)">PBGui: ' + escapeHtml(r.pbgui_dir || '') + '<br>PB7: ' + escapeHtml(r.pb7_dir || '') + '<br>PB8: ' + escapeHtml(r.pb8_dir || '') + '<br>Python: ' + escapeHtml(r.pbgui_python || '') + '</div>'
    : job.status === 'done'
    ? '<strong style="color:var(--ok)">Installation complete.</strong><div>Connect OpenVPN, then open: <a href="' + vpnHref + '" target="_blank">' + vpnUrl + '</a></div><div style="color:var(--muted)">Use the NetworkManager button to import the profile as split tunnel. If you import it manually, enable <strong>Use this connection only for resources on its network</strong> so the VPN does not become your default internet route.</div>'
    : job.status === 'error'
      ? '<strong style="color:var(--danger)">' + (isUninstall ? 'Uninstall' : (isMaintenance ? 'Update' : 'Installation')) + ' failed: ' + escapeHtml(job.error || 'unknown error') + '</strong>'
      : '<strong style="color:var(--ok)">TOTP QR code is ready.</strong><div>Scan this now. The installation is still running.</div>')
    + (r.ovpn_local ? '<div><a href="/download/ovpn?job=' + encodeURIComponent(job.id) + '">Download OpenVPN profile</a> <button class="secondary" type="button" id="nm-install-btn">Install in NetworkManager as split tunnel</button></div><div id="nm-install-result" style="color:var(--muted)"></div>' : '')
    + (qrText ? '<div><strong>TOTP QR code</strong><div class="qr-wrap"><pre class="qr-code">' + qrText + '</pre></div></div>' : '');
  const nmBtn = document.getElementById('nm-install-btn');
  if (nmBtn) nmBtn.addEventListener('click', installNetworkManagerProfile);
}
function installNetworkManagerProfile() {
  const box = document.getElementById('nm-install-result');
  const btn = document.getElementById('nm-install-btn');
  if (!currentJobId || !box || !btn) return;
  btn.disabled = true;
  box.textContent = 'Importing profile into NetworkManager...';
  fetch('/api/networkmanager/import', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({job: currentJobId}) })
    .then(r => r.json().then(data => ({ok:r.ok, data})))
    .then(({ok, data}) => {
      btn.disabled = false;
      if (!ok) { box.innerHTML = '<span style="color:var(--danger)">' + escapeHtml(data.error || 'NetworkManager import failed') + '</span>'; return; }
      box.innerHTML = '<span style="color:var(--ok)">NetworkManager profile "' + escapeHtml(data.connection || '') + '" imported with split-tunnel routing for ' + escapeHtml(data.openvpn_cidr || '') + '.</span>';
    })
    .catch(err => { btn.disabled = false; box.innerHTML = '<span style="color:var(--danger)">' + escapeHtml(err) + '</span>'; });
}
function poll(jobId) {
  fetch('/api/status?job=' + encodeURIComponent(jobId)).then(r => r.json()).then(job => {
    updateProgress(job);
    appendLogs(job.logs || []);
    renderResult(job);
    if (job.status === 'done' || job.status === 'error') {
      startBtn.disabled = false;
      currentJobId = '';
      if (isMaintenanceMode()) refreshLocalMasterStatus();
      stopProgressTimer();
      stopPolling();
      return;
    }
    pollTimer = setTimeout(() => poll(jobId), 1500);
  }).catch(err => {
    startBtn.disabled = false;
    stopProgressTimer();
    stopPolling();
    resultEl.style.display = 'grid';
    resultEl.innerHTML = '<strong style="color:var(--danger)">Installer status polling failed: ' + escapeHtml(err) + '</strong>';
  });
}
function openUninstallConfirmModal(onConfirm) {
  pendingUninstallConfirm = onConfirm;
  uninstallModalMessage.innerHTML = 'Install parent: <strong>' + escapeHtml((installDir.value || defaultInstallDir()).trim()) + '</strong>';
  uninstallModal.style.display = 'flex';
  uninstallCancelBtn.focus();
}
function closeUninstallConfirmModal() {
  uninstallModal.style.display = 'none';
  pendingUninstallConfirm = null;
  startBtn.focus();
}
uninstallCancelBtn.addEventListener('click', closeUninstallConfirmModal);
uninstallConfirmBtn.addEventListener('click', () => {
  const onConfirm = pendingUninstallConfirm;
  uninstallModal.style.display = 'none';
  pendingUninstallConfirm = null;
  if (onConfirm) onConfirm();
});
function showStartError(message) {
  startBtn.disabled = false;
  resultEl.style.display = 'grid';
  resultEl.innerHTML = '<strong style="color:var(--danger)">' + escapeHtml(message) + '</strong>';
}
function resetHostKeyAcceptance() {
  acceptUnknownHost.value = '';
  acceptedHostKeyFingerprint.value = '';
}
function openHostKeyConfirmModal(info, onConfirm) {
  pendingHostKeyConfirm = onConfirm;
  hostKeyModalMessage.innerHTML = 'Unknown SSH host key for <strong>' + escapeHtml(info.host || '') + ':' + escapeHtml(info.port || '') + '</strong><br>Key type: <strong>' + escapeHtml(info.key_type || '') + '</strong><br>Fingerprint: <strong>' + escapeHtml(info.fingerprint || '') + '</strong>';
  hostKeyModal.style.display = 'flex';
  hostKeyCancelBtn.focus();
}
function closeHostKeyConfirmModal() {
  hostKeyModal.style.display = 'none';
  pendingHostKeyConfirm = null;
  startBtn.focus();
}
hostKeyCancelBtn.addEventListener('click', closeHostKeyConfirmModal);
hostKeyConfirmBtn.addEventListener('click', () => {
  const onConfirm = pendingHostKeyConfirm;
  hostKeyModal.style.display = 'none';
  pendingHostKeyConfirm = null;
  if (onConfirm) onConfirm();
});
function preflightRemoteHostKey(confirmedUninstall) {
  const host = (remoteHost.value || '').trim();
  const port = (sshPort.value || '22').trim();
  if (!host) { showStartError('Remote host is required.'); return; }
  resetHostKeyAcceptance();
  startBtn.disabled = true;
  resultEl.style.display = 'grid';
  resultEl.innerHTML = '<span style="color:var(--muted)">Checking SSH host key...</span>';
  fetch('/api/ssh-host-key?host=' + encodeURIComponent(host) + '&port=' + encodeURIComponent(port))
    .then(r => r.json().then(data => ({ok:r.ok, data})))
    .then(({ok, data}) => {
      startBtn.disabled = false;
      if (!ok) { showStartError(data.error || 'Could not read SSH host key.'); return; }
      if (data.mismatch) {
        showStartError('SSH host key mismatch. Refusing to connect until known_hosts is fixed intentionally. Presented key: ' + (data.key_type || '') + ' ' + (data.fingerprint || ''));
        return;
      }
      if (!data.known) {
        openHostKeyConfirmModal(data, () => {
          acceptUnknownHost.value = 'yes';
          acceptedHostKeyFingerprint.value = data.fingerprint || '';
          startJob(confirmedUninstall, true);
        });
        return;
      }
      startJob(confirmedUninstall, true);
    })
    .catch(err => { showStartError('Could not read SSH host key: ' + err); });
}
function startJob(confirmedUninstall, confirmedHostKey) {
  if (installMode.value === 'remote' && sshMode.value === 'anywhere' && !sshRisk.checked) {
    showStartError('Please confirm the public SSH warning before continuing.');
    return;
  }
  if (installMode.value === 'remote' && !confirmFreshHost.checked) {
    showStartError('Please confirm this is a fresh/disposable VPS before continuing.');
    return;
  }
  if (installMode.value === 'remote' && !confirmedHostKey) {
    preflightRemoteHostKey(confirmedUninstall);
    return;
  }
  if (installMode.value === 'local-uninstall' && !confirmedUninstall) {
    openUninstallConfirmModal(() => startJob(true));
    return;
  }
  currentJobMode = installMode.value;
  const maintenance = currentJobMode === 'local-pb8' || currentJobMode === 'local-update-all';
  const startText = currentJobMode === 'local-uninstall' ? 'Starting uninstall...' : (maintenance ? 'Starting update...' : 'Starting installation...');
  stopPolling();
  startProgressTimer();
  startBtn.disabled = true; resultEl.style.display = 'none'; logEl.textContent = startText;
  lastLogCount = 0; logHasContent = false; setProgress(3, startText, '');
  const payload = Object.fromEntries(new FormData(form).entries());
  if (currentJobMode === 'local-uninstall') payload.uninstall_confirm = 'yes';
  fetch('/api/install', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) })
    .then(r => r.json()).then(data => poll(data.job_id))
    .catch(err => { startBtn.disabled = false; stopProgressTimer(); logEl.textContent = 'Failed to start: ' + err; });
}
form.addEventListener('submit', ev => {
  ev.preventDefault();
  startJob(false, false);
});
</script>
</body>
</html>
""".replace("%%TARGET_USER%%", html.escape(target_user, quote=True)).replace("%%TARGET_USER_JSON%%", json.dumps(target_user)).replace("%%INSTALL_DIR%%", html.escape(default_remote_install_dir(target_user), quote=True)).replace("%%LOCAL_INSTALL_DIR_JSON%%", json.dumps(default_local_install_dir())).replace("%%LOCAL_MASTER_NAME_JSON%%", json.dumps(default_local_master_name())).replace("%%LOCAL_PREREQ_STATUS%%", local_prereq_status_html).replace("%%LOCAL_SUDO_PASSWORD_USEFUL_JSON%%", json.dumps(bool(prereqs.get("sudo_password_useful")))).replace("%%LOCAL_MASTER_STATUS_JSON%%", json.dumps(initial_local_status).replace("</", "<\\/")).replace("%%MAINTENANCE_SELECTED%%", "selected" if maintenance_default else "").replace("%%FRESH_SELECTED%%", "" if maintenance_default else "selected").replace("%%PBGUI_PASSWORD%%", html.escape(pbgui_password, quote=True))


class InstallerHandler(BaseHTTPRequestHandler):
    """HTTP handler for the local installer wizard."""

    def _send_json(self, payload: dict, status: int = 200) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_text(self, data: bytes, *, filename: str, content_type: str = "text/plain") -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            body = _html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/status":
            job_id = urllib.parse.parse_qs(parsed.query).get("job", [""])[0]
            with _JOBS_LOCK:
                job = dict(_JOBS.get(job_id) or {"status": "missing", "logs": []})
            job["id"] = job_id
            self._send_json(job)
            return
        if parsed.path == "/api/public-ip":
            self._send_json({"ip": detect_public_ip()})
            return
        if parsed.path == "/api/local-master-status":
            install_dir = urllib.parse.parse_qs(parsed.query).get("install_dir", [default_local_install_dir()])[0]
            try:
                self._send_json(inspect_local_master_install(str(install_dir)))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/ssh-host-key":
            query = urllib.parse.parse_qs(parsed.query)
            host = str(query.get("host", [""])[0]).strip()
            try:
                port = int(query.get("port", ["22"])[0] or 22)
            except ValueError:
                self._send_json({"error": "SSH port must be a number."}, status=HTTPStatus.BAD_REQUEST)
                return
            if not (1 <= port <= 65535):
                self._send_json({"error": "SSH port must be between 1 and 65535."}, status=HTTPStatus.BAD_REQUEST)
                return
            if not host:
                self._send_json({"error": "Remote host is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            try:
                self._send_json(probe_ssh_host_key(host, port))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if parsed.path in {"/download/ovpn", "/download/totp"}:
            job_id = urllib.parse.parse_qs(parsed.query).get("job", [""])[0]
            with _JOBS_LOCK:
                result = dict((_JOBS.get(job_id) or {}).get("result") or {})
            key = "ovpn_local" if parsed.path.endswith("ovpn") else "totp_qr_local"
            path = Path(result.get(key) or "")
            if not path.exists():
                self.send_error(404)
                return
            self._send_text(path.read_bytes(), filename=path.name)
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/api/networkmanager/import":
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            try:
                result = _install_networkmanager_profile(str(payload.get("job") or ""))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(result)
            return
        if self.path != "/api/install":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        try:
            job_id = _new_job(payload)
        except Exception as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json({"job_id": job_id})

    def log_message(self, fmt: str, *args) -> None:
        return


def run_server(*, host: str = "127.0.0.1", port: int = 8088) -> int:
    """Run the local browser wizard."""
    server = ThreadingHTTPServer((host, port), InstallerHandler)
    url = f"http://{host}:{server.server_port}/"
    print(f"PBGui master installer running at {url}")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping installer.")
    finally:
        server.server_close()
    return 0
