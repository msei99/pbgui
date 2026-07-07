#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-}"
if [[ -z "$CONFIG_PATH" || ! -f "$CONFIG_PATH" ]]; then
  echo "Config JSON path is required." >&2
  exit 2
fi

info() { printf '\033[36m[INFO]\033[0m %s\n' "$*"; }
success() { printf '\033[32m[ OK ]\033[0m %s\n' "$*"; }
warn() { printf '\033[33m[WARN]\033[0m %s\n' "$*"; }
err() { printf '\033[31m[ERR ]\033[0m %s\n' "$*" >&2; }

eval "$(python3 - "$CONFIG_PATH" <<'PY'
import json, shlex, sys
cfg = json.load(open(sys.argv[1], encoding='utf-8'))
for key, value in cfg.items():
    if isinstance(value, bool):
        value = '1' if value else '0'
    elif isinstance(value, list):
        value = ','.join(str(item) for item in value)
    elif value is None:
        value = ''
    print(f"{key.upper()}={shlex.quote(str(value))}")
PY
)"

LOGIN_MODE="${LOGIN_MODE:-root}"
TARGET_USER="${TARGET_USER:-pbgui}"
TARGET_PASSWORD="${TARGET_PASSWORD:-}"
ROOT_PASSWORD="${ROOT_PASSWORD:-}"
HOSTNAME="${HOSTNAME:-pbgui-master}"
SWAP_SIZE="${SWAP_SIZE:-6G}"
PBG_PASSWORD="${PBGUI_PASSWORD:-PBGui\$Bot!}"
PBG_BIND="${PBGUI_BIND_HOST:-0.0.0.0}"
PBG_PORT="${PBGUI_PORT:-8000}"
OPENVPN_CIDR="${OPENVPN_CIDR:-10.8.0.0/24}"
SSH_MODE="${SSH_MODE:-specific_ips_vpn}"
SSH_ALLOWED_IPS="${SSH_ALLOWED_IPS:-}"
INSTALL_DIR="${INSTALL_DIR:-/home/${TARGET_USER}/software}"
RESULT_PATH="${RESULT_PATH:-/tmp/pbgui_remote_master_result.json}"
LOCAL_PUBLIC_KEY="${LOCAL_PUBLIC_KEY:-}"
UPLOADED_SETUP_SYSTEMD_PATH="${UPLOADED_SETUP_SYSTEMD_PATH:-}"
INSTALLER_BRANCH="${INSTALLER_BRANCH:-}"
export COINMARKETCAP_API_KEY="${COINMARKETCAP_API_KEY:-}"

eval "$(python3 - "$INSTALL_DIR" <<'PY'
import shlex
import sys
import re
from pathlib import PurePosixPath

raw = sys.argv[1].strip()
if not raw:
    raise SystemExit("Install parent directory is required.")
if any(ch in raw for ch in ("\x00", "\n", "\r")):
    raise SystemExit("Install parent directory contains invalid control characters.")
if "{{" in raw or "}}" in raw:
    raise SystemExit("Install parent directory contains invalid template markers.")
if not re.fullmatch(r"[A-Za-z0-9._~/-]+", raw):
    raise SystemExit("Install parent directory may only contain letters, numbers, '/', '.', '_', '-' and '~'.")
path = PurePosixPath(raw)
if not path.is_absolute():
    raise SystemExit("Install parent directory must be an absolute path.")
if "." in path.parts or ".." in path.parts:
    raise SystemExit("Install parent directory cannot contain '.' or '..' path segments.")
normalized = str(path)
if normalized == "/":
    raise SystemExit("Install parent directory cannot be '/'.")
print(f"INSTALL_DIR={shlex.quote(normalized)}")
PY
)"

eval "$(python3 - "$OPENVPN_CIDR" <<'PY'
import ipaddress, shlex, sys
network = ipaddress.ip_network(sys.argv[1], strict=True)
if network.version != 4 or not network.is_private or network.prefixlen > 30:
    raise SystemExit("Invalid OpenVPN CIDR")
gateway = next(network.hosts())
print(f"OVPN_NETWORK={shlex.quote(str(network.network_address))}")
print(f"OVPN_NETMASK={shlex.quote(str(network.netmask))}")
print(f"OVPN_GATEWAY={shlex.quote(str(gateway))}")
PY
)"

if [[ "$(id -u)" -ne 0 ]]; then
  err "Remote bootstrap must run as root."
  exit 1
fi

if [[ ! -f /etc/os-release ]] || ! grep -qi '^ID=ubuntu' /etc/os-release; then
  warn "This installer is tested on Ubuntu. Continuing anyway."
fi

info "Configuring hostname and target user..."
hostnamectl set-hostname "$HOSTNAME" || true
python3 - "$HOSTNAME" <<'PY'
from pathlib import Path
import sys

hostname = sys.argv[1].strip()
path = Path("/etc/hosts")
lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
updated = []
has_localhost = False
has_hostname = False

for line in lines:
    stripped = line.strip()
    if stripped.startswith("127.0.0.1"):
        has_localhost = True
    if stripped.startswith("127.0.1.1"):
        if not has_hostname:
            updated.append(f"127.0.1.1\t{hostname}")
            has_hostname = True
        continue
    updated.append(line)

if not has_localhost:
    updated.insert(0, "127.0.0.1\tlocalhost")
if not has_hostname:
    updated.append(f"127.0.1.1\t{hostname}")

tmp = path.with_suffix(".tmp")
tmp.write_text("\n".join(updated) + "\n", encoding="utf-8")
tmp.replace(path)
PY
if [[ "$LOGIN_MODE" == "root" && -n "$ROOT_PASSWORD" ]]; then
  info "Changing root password before root login is locked..."
  printf 'root:%s\n' "$ROOT_PASSWORD" | chpasswd
  unset ROOT_PASSWORD
fi
if ! id "$TARGET_USER" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "$TARGET_USER"
  usermod -aG sudo "$TARGET_USER"
fi
if [[ -n "$TARGET_PASSWORD" ]]; then
  printf '%s:%s\n' "$TARGET_USER" "$TARGET_PASSWORD" | chpasswd
fi

TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6)"
if [[ -z "$TARGET_HOME" ]]; then
  err "Could not determine target user home."
  exit 1
fi
TARGET_GROUP="$(id -gn "$TARGET_USER")"
info "Using install parent directory: $INSTALL_DIR"

if [[ -n "$LOCAL_PUBLIC_KEY" ]]; then
  info "Installing SSH public key for $TARGET_USER..."
  install -d -m 700 -o "$TARGET_USER" -g "$TARGET_GROUP" "$TARGET_HOME/.ssh"
  auth_keys="$TARGET_HOME/.ssh/authorized_keys"
  touch "$auth_keys"
  chown "$TARGET_USER:$TARGET_GROUP" "$auth_keys"
  chmod 600 "$auth_keys"
  if ! grep -Fqx "$LOCAL_PUBLIC_KEY" "$auth_keys"; then
    printf '%s\n' "$LOCAL_PUBLIC_KEY" >> "$auth_keys"
  fi
fi

info "Installing system packages..."
export DEBIAN_FRONTEND=noninteractive
APT_OPTS=(-o Dpkg::Use-Pty=0 -o APT::Color=0)
apt-get "${APT_OPTS[@]}" update
apt-get "${APT_OPTS[@]}" install -y software-properties-common sudo curl ca-certificates git ufw openvpn easy-rsa qrencode libpam-google-authenticator oathtool python3 python3-pip gcc build-essential pkg-config sshpass
if apt-cache policy python3.12-venv | grep -Eq 'Candidate:\s+\(none\)'; then
  add-apt-repository ppa:deadsnakes/ppa -y
  apt-get "${APT_OPTS[@]}" update
fi
apt-get "${APT_OPTS[@]}" install -y python3.12-venv

if [[ -n "$INSTALLER_BRANCH" ]]; then
  if ! git check-ref-format --branch "$INSTALLER_BRANCH" >/dev/null 2>&1; then
    err "Invalid PBGui installer branch: $INSTALLER_BRANCH"
    exit 1
  fi
  info "Using PBGui branch: $INSTALLER_BRANCH"
fi

info "Creating swapfile ($SWAP_SIZE) if needed..."
if [[ -f /swapfile ]]; then
  swapoff /swapfile >/dev/null 2>&1 || true
  rm -f /swapfile
fi
fallocate -l "$SWAP_SIZE" /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
grep -q '^/swapfile ' /etc/fstab || printf '/swapfile none swap sw 0 0\n' >> /etc/fstab
sysctl -w vm.swappiness=10 >/dev/null || true
sysctl -w vm.vfs_cache_pressure=50 >/dev/null || true

run_as_user() {
  sudo -H -u "$TARGET_USER" bash -lc "$1"
}

install -d -o "$TARGET_USER" -g "$TARGET_GROUP" "$INSTALL_DIR"

info "Cloning PBGui and Passivbot..."
branch_arg=""
branch_refspec=""
branch_remote_ref=""
if [[ -n "$INSTALLER_BRANCH" ]]; then
  branch_arg="$(printf '%q' "$INSTALLER_BRANCH")"
  branch_refspec="$(printf '%q' "${INSTALLER_BRANCH}:refs/remotes/origin/${INSTALLER_BRANCH}")"
  branch_remote_ref="$(printf '%q' "refs/remotes/origin/${INSTALLER_BRANCH}")"
fi
if [[ -d "$INSTALL_DIR/pbgui/.git" ]]; then
  if [[ -n "$branch_arg" ]]; then
    run_as_user "cd '$INSTALL_DIR/pbgui' && git fetch origin $branch_refspec && git checkout -B $branch_arg $branch_remote_ref && git pull --ff-only origin $branch_arg"
  else
    run_as_user "cd '$INSTALL_DIR/pbgui' && git pull --ff-only"
  fi
else
  if [[ -n "$branch_arg" ]]; then
    run_as_user "git clone --branch $branch_arg --single-branch https://github.com/msei99/pbgui.git '$INSTALL_DIR/pbgui'"
  else
    run_as_user "git clone https://github.com/msei99/pbgui.git '$INSTALL_DIR/pbgui'"
  fi
fi
if [[ -d "$INSTALL_DIR/pb7/.git" ]]; then
  run_as_user "cd '$INSTALL_DIR/pb7' && git pull --ff-only"
else
  run_as_user "git clone https://github.com/enarjord/passivbot.git '$INSTALL_DIR/pb7'"
fi
if [[ -n "$UPLOADED_SETUP_SYSTEMD_PATH" && -f "$UPLOADED_SETUP_SYSTEMD_PATH" ]]; then
  info "Installing local systemd setup helper into PBGui checkout..."
  install -D -m 755 -o "$TARGET_USER" -g "$TARGET_GROUP" "$UPLOADED_SETUP_SYSTEMD_PATH" "$INSTALL_DIR/pbgui/setup/setup_systemd.sh"
fi

info "Creating Python virtualenvs..."
PB7_REQUIREMENTS="$INSTALL_DIR/pb7/requirements.txt"
PBGUI_REQUIREMENTS="$INSTALL_DIR/pbgui/requirements.txt"
[[ -f "$PBGUI_REQUIREMENTS" ]] || PBGUI_REQUIREMENTS="$INSTALL_DIR/pbgui/requirements_vps.txt"
run_as_user "python3.12 -m venv '$INSTALL_DIR/venv_pb7'"
run_as_user "'$INSTALL_DIR/venv_pb7/bin/python' -m pip install --upgrade pip"
run_as_user "'$INSTALL_DIR/venv_pb7/bin/python' -m pip install -r '$PB7_REQUIREMENTS'"
run_as_user "'$INSTALL_DIR/venv_pb7/bin/python' -m pip install maturin"
run_as_user "python3.12 -m venv '$INSTALL_DIR/venv_pbgui'"
run_as_user "'$INSTALL_DIR/venv_pbgui/bin/python' -m pip install --upgrade pip"
run_as_user "'$INSTALL_DIR/venv_pbgui/bin/python' -m pip install -r '$PBGUI_REQUIREMENTS'"

info "Building passivbot-rust..."
run_as_user "if ! command -v rustup >/dev/null 2>&1; then curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal; fi"
run_as_user "source '$TARGET_HOME/.cargo/env' && rustup toolchain install 1.90.0 && rustup default 1.90.0"
run_as_user "source '$TARGET_HOME/.cargo/env' && source '$INSTALL_DIR/venv_pb7/bin/activate' && cd '$INSTALL_DIR/pb7/passivbot-rust' && maturin develop --release"
run_as_user "source '$INSTALL_DIR/venv_pb7/bin/activate' && cd '$INSTALL_DIR/pb7' && python -c \"import sys; sys.path.insert(0, 'src'); from rust_utils import stamp_compiled_extensions, source_fingerprint; stamp_compiled_extensions(source_fingerprint()); print('Rust source stamp updated.')\""

info "Writing PBGui configuration..."
run_as_user "mkdir -p '$INSTALL_DIR/pbgui/data/auth'"
python3 - "$INSTALL_DIR/pbgui/pbgui.ini" "$HOSTNAME" "$TARGET_USER" "$INSTALL_DIR" "$PBG_BIND" "$PBG_PORT" <<'PY'
import configparser, os, sys
path, hostname, user, install_dir, bind_host, port = sys.argv[1:]
cfg = configparser.ConfigParser()
cfg['main'] = {
    'pbname': hostname,
    'pb7dir': f'{install_dir}/pb7',
    'pb7venv': f'{install_dir}/venv_pb7/bin/python',
    'role': 'master',
}
cfg['api_server'] = {'host': bind_host, 'port': port}
cfg['coinmarketcap'] = {'api_key': os.environ.get('COINMARKETCAP_API_KEY', ''), 'fetch_limit': '1000', 'fetch_interval': '4'}
tmp = path + '.tmp'
with open(tmp, 'w', encoding='utf-8') as handle:
    cfg.write(handle)
os.replace(tmp, path)
PY
chown "$TARGET_USER:$TARGET_GROUP" "$INSTALL_DIR/pbgui/pbgui.ini"
PBG_PASSWORD_ENV="$PBG_PASSWORD" python3 - "$INSTALL_DIR/pbgui/data/auth/secrets.toml" <<'PY'
import os, pathlib, sys
path = pathlib.Path(sys.argv[1])
path.parent.mkdir(parents=True, exist_ok=True)
password = os.environ.get('PBG_PASSWORD_ENV', 'PBGui$Bot!').replace('\\', '\\\\').replace('"', '\\"')
tmp = path.with_suffix('.tmp')
tmp.write_text(f'password = "{password}"\n', encoding='utf-8')
tmp.replace(path)
PY
chown -R "$TARGET_USER:$TARGET_GROUP" "$INSTALL_DIR/pbgui/data/auth"

info "Setting up OpenVPN and TOTP..."
SERVER_NAME="$HOSTNAME"
CLIENT_NAME="$TARGET_USER"
PROFILE_NAME="${SERVER_NAME//[!A-Za-z0-9._-]/_}"
[[ -n "$PROFILE_NAME" ]] || PROFILE_NAME="pbgui-master"
OVPN_DIR="/etc/openvpn/server"
EASYRSA_DIR="/etc/openvpn/easy-rsa"
CLIENT_KEYS_DIR="/etc/openvpn/client_keys"
CLIENT_OVPN_DIR="$TARGET_HOME/${PROFILE_NAME}_client"
CLIENT_FILE="$CLIENT_OVPN_DIR/${PROFILE_NAME}.ovpn"

rm -rf "$EASYRSA_DIR"
mkdir -p "$EASYRSA_DIR"
cp -r /usr/share/easy-rsa/* "$EASYRSA_DIR"
cd "$EASYRSA_DIR"
EASYRSA_BATCH=1 EASYRSA_REQ_CN="$SERVER_NAME" ./easyrsa init-pki
EASYRSA_BATCH=1 EASYRSA_REQ_CN="$SERVER_NAME" ./easyrsa build-ca nopass
EASYRSA_BATCH=1 EASYRSA_REQ_CN="$CLIENT_NAME" ./easyrsa gen-req "$CLIENT_NAME" nopass
printf 'yes\n' | EASYRSA_BATCH=1 ./easyrsa sign-req client "$CLIENT_NAME"
EASYRSA_BATCH=1 ./easyrsa gen-req server nopass
printf 'yes\n' | EASYRSA_BATCH=1 ./easyrsa sign-req server server
mkdir -p "$OVPN_DIR"
cp pki/ca.crt pki/issued/server.crt pki/private/server.key pki/issued/${CLIENT_NAME}.crt pki/private/${CLIENT_NAME}.key "$OVPN_DIR/"
openssl ecparam -name prime256v1 -out "$OVPN_DIR/ecdh.pem"
openvpn --genkey secret "$OVPN_DIR/ta.key"

cat > "$OVPN_DIR/server.conf" <<EOF
port 1194
proto udp
dev tun

# Drop privileges
user nobody
group nogroup
persist-key
persist-tun

# Certificates
ca ca.crt
cert server.crt
key server.key
dh none
ecdh-curve prime256v1

# TLS security
tls-server
tls-version-min 1.3
tls-ciphersuites TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256
tls-crypt ta.key
auth SHA256
cipher AES-256-GCM
ncp-ciphers AES-256-GCM:CHACHA20-POLY1305
data-ciphers AES-256-GCM:CHACHA20-POLY1305
data-ciphers-fallback AES-256-GCM

# VPN network
server $OVPN_NETWORK $OVPN_NETMASK
topology subnet

# PAM + Google Authenticator MFA
plugin /usr/lib/x86_64-linux-gnu/openvpn/plugins/openvpn-plugin-auth-pam.so openvpn
verify-client-cert require
username-as-common-name
auth-nocache

# Keepalive & connection options
keepalive 10 120
explicit-exit-notify 1
client-to-client
duplicate-cn
reneg-sec 3600
auth-gen-token 604800

# Logging
status /var/log/openvpn-status.log
log-append /var/log/openvpn.log
verb 3
mute 10

# Security
script-security 2
capath /etc/ssl/certs
remote-cert-eku "TLS Web Client Authentication"
EOF

mkdir -p /dev/net || true
mknod -m 600 /dev/net/tun c 10 200 || true
chown -R root:root "$OVPN_DIR"
chmod 600 "$OVPN_DIR/server.key" "$OVPN_DIR/ta.key"
chmod 644 "$OVPN_DIR/server.crt" "$OVPN_DIR/ca.crt" "$OVPN_DIR/${CLIENT_NAME}.crt"
mkdir -p /etc/systemd/system/openvpn-server@server.service.d
cat > /etc/systemd/system/openvpn-server@server.service.d/override.conf <<EOF
[Service]
ExecStart=
ExecStart=/usr/sbin/openvpn --config /etc/openvpn/server/server.conf --writepid /run/openvpn/server.pid
EOF

mkdir -p /etc/openvpn/google-auth
chmod 700 /etc/openvpn/google-auth
cat > /etc/pam.d/openvpn <<'EOF'
auth required pam_google_authenticator.so secret=/etc/openvpn/google-auth/${USER} user=root
account required pam_unix.so
EOF
GA_FILE="$TARGET_HOME/GA-QR.txt"
GA_SECRET_FILE="$TARGET_HOME/.google_authenticator"
SECRET="$(head -c 10 /dev/urandom | base32 | tr -d '=' | tr -d '\n' | cut -c1-16)"
ACCOUNT="$TARGET_USER@$(hostname)"
cat > "$GA_SECRET_FILE" <<EOF
$SECRET
" RESETTING_TIME_SKEW
" RATE_LIMIT 3 30
" WINDOW_SIZE 17
" DISALLOW_REUSE
" TOTP_AUTH
EOF
chown "$TARGET_USER:$TARGET_GROUP" "$GA_SECRET_FILE"
chmod 600 "$GA_SECRET_FILE"
qrencode -t ASCII "otpauth://totp/$ACCOUNT?secret=$SECRET&issuer=OpenVPN" > "$GA_FILE"
chown "$TARGET_USER:$TARGET_GROUP" "$GA_FILE"
chmod 600 "$GA_FILE"
printf '__PBGUI_TOTP_QR_BEGIN__\n'
cat "$GA_FILE"
printf '__PBGUI_TOTP_QR_END__\n'
cp "$GA_SECRET_FILE" "/etc/openvpn/google-auth/$TARGET_USER"
chown root:root "/etc/openvpn/google-auth/$TARGET_USER"
chmod 600 "/etc/openvpn/google-auth/$TARGET_USER"

systemctl daemon-reload
systemctl enable openvpn-server@server.service
systemctl restart openvpn-server@server.service

EXTERNAL_IP="$(curl -fsSL https://api.ipify.org || true)"
[[ -z "$EXTERNAL_IP" ]] && EXTERNAL_IP="$HOSTNAME"
mkdir -p "$CLIENT_OVPN_DIR"
cat > "$CLIENT_FILE" <<EOF
client
dev tun
proto udp
remote $EXTERNAL_IP 1194
resolv-retry infinite
nobind
user nobody
group nogroup
persist-key
persist-tun
auth SHA256
cipher AES-256-GCM
key-direction 1
remote-cert-tls server
auth-user-pass
auth-retry interact
verb 3

<ca>
$(cat "$OVPN_DIR/ca.crt")
</ca>

<cert>
$(cat "$CLIENT_KEYS_DIR/${CLIENT_NAME}.crt" 2>/dev/null || cat "$OVPN_DIR/${CLIENT_NAME}.crt")
</cert>

<key>
$(cat "$OVPN_DIR/${CLIENT_NAME}.key")
</key>

<tls-crypt>
$(cat "$OVPN_DIR/ta.key")
</tls-crypt>
EOF
chown -R "$TARGET_USER:$TARGET_GROUP" "$CLIENT_OVPN_DIR"
chmod 600 "$CLIENT_FILE"

info "Disabling direct root SSH login..."
passwd -l root || true
shopt -s nullglob
for ssh_config_file in /etc/ssh/sshd_config /etc/ssh/sshd_config.d/*.conf; do
  [[ -f "$ssh_config_file" ]] || continue
  if grep -Eq '^[#[:space:]]*PermitRootLogin' "$ssh_config_file"; then
    sed -i 's/^[#[:space:]]*PermitRootLogin.*/PermitRootLogin no/' "$ssh_config_file"
  fi
done
mkdir -p /etc/ssh/sshd_config.d
printf 'PermitRootLogin no\n' > /etc/ssh/sshd_config.d/99-pbgui-disable-root-login.conf
if /usr/sbin/sshd -t; then
  systemctl reload sshd >/dev/null 2>&1 || systemctl reload ssh >/dev/null 2>&1 || true
else
  err "SSH configuration validation failed after disabling root login."
  exit 1
fi

info "Configuring firewall..."
VPN_CIDR="$OPENVPN_CIDR"
ufw --force reset
ufw allow 1194/udp comment 'OpenVPN UDP'
if [[ "$SSH_MODE" == "vpn_only" ]]; then
  ufw allow from "$VPN_CIDR" to any port 22 proto tcp comment "SSH from VPN"
elif [[ "$SSH_MODE" == "specific_ips_vpn" ]]; then
  IFS=',' read -r -a ips <<< "$SSH_ALLOWED_IPS"
  for src in "${ips[@]}"; do
    src="$(echo "$src" | xargs)"
    [[ -n "$src" ]] && ufw allow from "$src" to any port 22 proto tcp comment "SSH from $src"
  done
  ufw allow from "$VPN_CIDR" to any port 22 proto tcp comment "SSH from VPN"
else
  warn "Allowing SSH from everywhere. This is not secure and not recommended."
  ufw allow 22/tcp comment "SSH from everywhere"
fi
ufw allow from "$VPN_CIDR" to any port "$PBG_PORT" proto tcp comment "PBGui via VPN"
ufw default deny incoming
ufw default allow outgoing
ufw --force enable

info "Installing PBGui systemd user services..."
bash "$INSTALL_DIR/pbgui/setup/setup_systemd.sh" --user "$TARGET_USER" --pbgui-dir "$INSTALL_DIR/pbgui" --python "$INSTALL_DIR/venv_pbgui/bin/python" --enable api,pbrun,pbdata,pbcoindata,monitor-agent

info "Checking PBGui API service..."
SERVICE_UID="$(id -u "$TARGET_USER")"
API_CHECK_HOST="127.0.0.1"
if [[ "$PBG_BIND" != "0.0.0.0" && "$PBG_BIND" != "::" ]]; then
  API_CHECK_HOST="$PBG_BIND"
fi
run_user_systemctl() {
  sudo -H -u "$TARGET_USER" env XDG_RUNTIME_DIR="/run/user/$SERVICE_UID" systemctl --user "$@"
}
run_user_journalctl() {
  sudo -H -u "$TARGET_USER" env XDG_RUNTIME_DIR="/run/user/$SERVICE_UID" journalctl --user "$@"
}
api_ready=false
for _attempt in $(seq 1 30); do
  if python3 - "$API_CHECK_HOST" "$PBG_PORT" <<'PY'
import socket
import sys

sock = socket.socket()
sock.settimeout(1)
try:
    sock.connect((sys.argv[1], int(sys.argv[2])))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
  then
    api_ready=true
    break
  fi
  sleep 2
done
if [[ "$api_ready" != true ]]; then
  err "PBGui API did not start or is not listening on $API_CHECK_HOST:$PBG_PORT."
  run_user_systemctl status pbgui-api.service --no-pager || true
  run_user_journalctl -u pbgui-api.service -n 120 --no-pager || true
  exit 1
fi
success "PBGui API is listening on $API_CHECK_HOST:$PBG_PORT."

python3 - "$RESULT_PATH" "$CLIENT_FILE" "$GA_FILE" "$PBG_PORT" "$OVPN_GATEWAY" <<'PY'
import json, sys
path, ovpn, qr, port, gateway = sys.argv[1:]
payload = {
    'ok': True,
    'ovpn_path': ovpn,
    'totp_qr_path': qr,
    'vpn_url': f'http://{gateway}:{port}/',
}
with open(path, 'w', encoding='utf-8') as handle:
    json.dump(payload, handle, indent=2)
PY

success "Remote PBGui master installation complete."
