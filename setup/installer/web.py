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
    TOTP_QR_BEGIN,
    TOTP_QR_END,
    RemoteInstallError,
    RemoteMasterConfig,
    default_local_install_dir,
    default_local_master_name,
    default_remote_install_dir,
    default_target_user,
    detect_public_ip,
    run_local_master_install,
    run_remote_master_install,
)

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
            if str(payload.get("install_mode") or "remote") == "local":
                cfg = LocalMasterConfig.from_mapping(payload)
                result = run_local_master_install(cfg, log, artifact_dir)
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
    .path-preview { grid-column:1/-1; border:1px solid var(--line); background:#0b1220; color:var(--muted); padding:10px; border-radius:10px; font-size:13px; line-height:1.45; }
    .path-preview strong { color:var(--text); }
    a { color:#93c5fd; }
  </style>
</head>
<body>
<main>
  <section>
    <h1>PBGui Master Installer</h1>
    <p style="color:var(--muted)">Install a fresh PBGui master either on a remote VPS or on this local machine.</p>
  </section>
  <section class="panel">
    <h2 id="mode-title">Remote Master VPS</h2>
    <p class="recommendation remote-only">Recommended VPS: <a href="https://aklam.io/CBA3zSaZ" target="_blank" rel="noopener noreferrer">IONOS VPS Linux M+</a> with 4 vCores CPU, 4 GB RAM, and 120 GB NVMe.</p>
    <form id="install-form" class="grid">
      <label class="full">Install mode
        <select name="install_mode" id="install-mode">
          <option value="remote">Remote Master VPS</option>
          <option value="local">Local Master Install</option>
        </select>
      </label>
      <label class="remote-only">Initial login mode
        <select name="login_mode" id="login-mode">
          <option value="root">Fresh VPS / root login</option>
          <option value="sudo">Existing sudo user</option>
        </select>
      </label>
      <label class="remote-only">VPS IP or hostname <input name="remote_host" required placeholder="1.2.3.4"></label>
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
      <label>Master name <input name="hostname" id="master-name" value="pbgui-master"></label>
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
      <label>PBGui login password
        <span class="password-wrap">
          <input id="pbgui-password" name="pbgui_password" type="password" value="PBGui$Bot!" autocomplete="new-password">
          <button class="password-toggle" type="button" data-target="pbgui-password" aria-label="Show PBGui login password" aria-pressed="false">&#128065;</button>
        </span>
      </label>
      <label>PBGui bind address <input name="pbgui_bind_host" id="pbgui-bind-host" value="0.0.0.0"></label>
      <label>PBGui port <input name="pbgui_port" type="number" value="8000" min="1024" max="65535"></label>
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
      <div class="full"><button id="start-btn" type="submit">Install Remote Master</button></div>
    </form>
  </section>
  <section class="panel">
    <h2>Progress</h2>
    <pre id="log">Waiting for installer input...</pre>
    <div class="result" id="result"></div>
  </section>
</main>
<script>
const form = document.getElementById('install-form');
const logEl = document.getElementById('log');
const resultEl = document.getElementById('result');
const startBtn = document.getElementById('start-btn');
const installMode = document.getElementById('install-mode');
const modeTitle = document.getElementById('mode-title');
const sshMode = document.getElementById('ssh-mode');
const sshWarning = document.getElementById('ssh-warning');
const sshRisk = document.getElementById('ssh-risk');
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
const defaultTargetUser = %%TARGET_USER_JSON%%;
const defaultLocalInstallDir = %%LOCAL_INSTALL_DIR_JSON%%;
const defaultLocalMasterName = %%LOCAL_MASTER_NAME_JSON%%;
let pollTimer = null;
let currentJobId = '';
let installDirTouched = false;
let masterNameTouched = false;
let bindHostTouched = false;
function defaultInstallDir() {
  if (installMode.value === 'local') return defaultLocalInstallDir;
  const user = (targetUser.value || defaultTargetUser || 'pbgui').trim() || 'pbgui';
  return '/home/' + user + '/software';
}
function defaultMasterName() { return installMode.value === 'local' ? defaultLocalMasterName : 'pbgui-master'; }
function defaultBindHost() { return installMode.value === 'local' ? '127.0.0.1' : '0.0.0.0'; }
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
  installPreview.innerHTML = (valid ? '' : '<div style="color:var(--danger)">Install parent directory must be an absolute path or start with ~.</div>')
    + '<div><strong>PBGui:</strong> ' + escapeHtml(joinPath(parent, 'pbgui')) + '</div>'
    + '<div><strong>PB7:</strong> ' + escapeHtml(joinPath(parent, 'pb7')) + '</div>'
    + '<div><strong>Venvs:</strong> ' + escapeHtml(joinPath(parent, 'venv_pbgui')) + ', ' + escapeHtml(joinPath(parent, 'venv_pb7')) + '</div>';
}
function syncMode() {
  if (installMode.value === 'local') {
    sshWarning.style.display = 'none';
    sshIpsWrap.style.display = 'none';
    return;
  }
  sshWarning.style.display = sshMode.value === 'anywhere' ? 'block' : 'none';
  sshIpsWrap.style.display = sshMode.value === 'specific_ips_vpn' ? 'grid' : 'none';
}
function syncLogin() {
  if (installMode.value === 'local') {
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
  const isLocal = installMode.value === 'local';
  document.querySelectorAll('.remote-only').forEach(el => {
    el.style.display = isLocal ? 'none' : '';
    el.querySelectorAll('input,select,button,textarea').forEach(ctrl => { ctrl.disabled = isLocal; });
  });
  modeTitle.textContent = isLocal ? 'Local Master Install' : 'Remote Master VPS';
  startBtn.textContent = isLocal ? 'Install Local Master' : 'Install Remote Master';
  if (!masterNameTouched) masterName.value = defaultMasterName();
  if (!bindHostTouched) pbguiBindHost.value = defaultBindHost();
  syncMode();
  syncLogin();
  syncInstallDir();
}
function syncSwapSize() {
  const isCustom = swapSizeSelect.value === 'custom';
  swapCustomWrap.style.display = isCustom ? 'grid' : 'none';
  swapSizeValue.value = isCustom ? (swapCustom.value.trim() || '6G') : swapSizeSelect.value;
}
installMode.addEventListener('change', syncInstallMode); sshMode.addEventListener('change', syncMode); loginMode.addEventListener('change', syncLogin); sshUsername.addEventListener('input', () => { if (loginMode.value === 'sudo') { targetUser.value = sshUsername.value; syncInstallDir(); } }); targetUser.addEventListener('input', syncInstallDir); installDir.addEventListener('input', () => { installDirTouched = true; syncInstallPreview(); }); masterName.addEventListener('input', () => { masterNameTouched = true; }); pbguiBindHost.addEventListener('input', () => { bindHostTouched = true; }); swapSizeSelect.addEventListener('change', syncSwapSize); swapCustom.addEventListener('input', syncSwapSize);
syncInstallMode();
syncSwapSize();
document.querySelectorAll('.password-toggle').forEach(btn => {
  btn.addEventListener('click', () => {
    const input = document.getElementById(btn.dataset.target || '');
    if (!input) return;
    const show = input.type === 'password';
    input.type = show ? 'text' : 'password';
    btn.setAttribute('aria-pressed', show ? 'true' : 'false');
    const labels = {'ssh-password':'SSH password', 'root-password':'new root password', 'target-password':'target user password', 'pbgui-password':'PBGui login password'};
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
function appendLogs(lines) { logEl.innerHTML = lines.length ? ansiToHtml(lines.join('\n')) : 'No logs yet...'; logEl.scrollTop = logEl.scrollHeight; }
function renderResult(job) {
  currentJobId = job.id || currentJobId;
  const r = job.result || {};
  if (job.status !== 'done' && job.status !== 'error' && !r.totp_qr_text) return;
  const vpnUrl = escapeHtml(r.vpn_url || '');
  const vpnHref = escapeHtml(r.vpn_url || '#');
  const localUrl = escapeHtml(r.local_url || '');
  const localHref = escapeHtml(r.local_url || '#');
  const qrText = escapeHtml(r.totp_qr_text || '');
  resultEl.style.display = 'grid';
  resultEl.innerHTML = (job.status === 'done' && r.mode === 'local'
    ? '<strong style="color:var(--ok)">Local installation complete.</strong><div>Open PBGui: <a href="' + localHref + '" target="_blank">' + localUrl + '</a></div><div style="color:var(--muted)">PBGui: ' + escapeHtml(r.pbgui_dir || '') + '<br>PB7: ' + escapeHtml(r.pb7_dir || '') + '<br>Python: ' + escapeHtml(r.pbgui_python || '') + '</div>'
    : job.status === 'done'
    ? '<strong style="color:var(--ok)">Installation complete.</strong><div>Connect OpenVPN, then open: <a href="' + vpnHref + '" target="_blank">' + vpnUrl + '</a></div><div style="color:var(--muted)">Use the NetworkManager button to import the profile as split tunnel. If you import it manually, enable <strong>Use this connection only for resources on its network</strong> so the VPN does not become your default internet route.</div>'
    : job.status === 'error'
      ? '<strong style="color:var(--danger)">Installation failed: ' + escapeHtml(job.error || 'unknown error') + '</strong>'
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
    appendLogs(job.logs || []);
    renderResult(job);
    if (job.status === 'done' || job.status === 'error') {
      startBtn.disabled = false;
      clearTimeout(pollTimer);
      return;
    }
    pollTimer = setTimeout(() => poll(jobId), 1500);
  });
}
form.addEventListener('submit', ev => {
  ev.preventDefault();
  if (installMode.value !== 'local' && sshMode.value === 'anywhere' && !sshRisk.checked) {
    resultEl.style.display = 'grid';
    resultEl.innerHTML = '<strong style="color:var(--danger)">Please confirm the public SSH warning before continuing.</strong>';
    return;
  }
  startBtn.disabled = true; resultEl.style.display = 'none'; logEl.textContent = 'Starting installation...';
  const payload = Object.fromEntries(new FormData(form).entries());
  fetch('/api/install', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) })
    .then(r => r.json()).then(data => poll(data.job_id))
    .catch(err => { startBtn.disabled = false; logEl.textContent = 'Failed to start: ' + err; });
});
</script>
</body>
</html>
""".replace("%%TARGET_USER%%", html.escape(target_user, quote=True)).replace("%%TARGET_USER_JSON%%", json.dumps(target_user)).replace("%%INSTALL_DIR%%", html.escape(default_remote_install_dir(target_user), quote=True)).replace("%%LOCAL_INSTALL_DIR_JSON%%", json.dumps(default_local_install_dir())).replace("%%LOCAL_MASTER_NAME_JSON%%", json.dumps(default_local_master_name()))


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
