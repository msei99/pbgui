"""Offline regressions for LF checkouts and root-safe standalone setup scripts."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _run(argv, *, env=None, input="", cwd=None, binary=False):
    """Bound a synthetic command and reap its entire process group on timeout."""
    proc = subprocess.Popen(
        argv, cwd=cwd or ROOT, env=env, stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=not binary, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(input.encode() if binary else input, timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.communicate()
        pytest.fail(f"Synthetic command timed out: {argv[0]}")
    return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)


# This dispatcher has no route to a real account, package, firewall or service command.
DISPATCHER = r'''import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ['SANDBOX'])
state_file = root / 'state.json'
state = json.loads(state_file.read_text())
with (root / 'calls').open('a') as log:
    log.write(json.dumps([name, args, {k: os.environ.get(k) for k in ('HOME','USER','LOGNAME','XDG_RUNTIME_DIR','DEBIAN_FRONTEND')}]) + '\n')
def save():
    state_file.write_text(json.dumps(state))
if name == 'id':
    target = next((x for x in args if not x.startswith('-')), None)
    exists = state.get('exists', False)
    if target and not exists:
        sys.exit(1)
    if '-nG' in args:
        print('admin sudo' if state.get('admin', False) else 'admin')
    elif '-gn' in args:
        print('admin')
    elif '-g' in args:
        print(1000)
    elif '-u' in args:
        print(state.get('target_uid', 1000) if target else state.get('uid', 0))
    else:
        print('uid=1000(admin)')
elif name == 'getent':
    print('admin:x:1000:1000::' + os.environ['HOME_FIXTURE'] + ':/bin/bash')
elif name == 'whoami':
    print('admin')
elif name == 'awk':
    print(sys.stdin.read().split()[0])
elif name == 'hostname':
    print('test-host')
elif name == 'lsb_release':
    print(state.get('fallback_os', 'Ubuntu'))
elif name == 'apt':
    pass
elif name == 'apt-get':
    action = 'install' if 'install' in args else 'update'
    if state.get('apt_failure') == action:
        sys.exit(9)
    if action == 'install' and state.get('install_sudo', True):
        (root / 'bin' / 'sudo').symlink_to(root / 'dispatcher')
elif name in ('sudo', 'runuser'):
    if '-u' in args:
        pos = args.index('-u')
        args = args[pos + 2:]
        if args and args[0] == '--':
            args = args[1:]
    elif args in (['-v'], ['-n', 'true']):
        sys.exit(0)
    os.execvpe(args[0], args, os.environ)
elif name == 'useradd':
    state['exists'] = True
    save()
elif name == 'usermod':
    state['admin'] = not state.get('group_failure', False)
    save()
elif name == 'chpasswd':
    sys.stdin.read()
elif name == 'ufw':
    if args == ['status', 'numbered']:
        print('[ 2] 8501/tcp ALLOW IN Anywhere')
elif name == 'qrencode':
    (root / 'qr-input').write_text(sys.stdin.read())
    print('synthetic QR')
elif name == 'systemctl':
    if '--user' in args:
        if 'show-environment' in args and state.get('legacy', False):
            sys.exit(1)
        if 'show' in args:
            print('active')
    sys.exit(state.get('service_error', 0))
elif name == 'pgrep':
    sys.exit(0)
elif name in ('hostnamectl', 'passwd', 'chown', 'loginctl', 'sleep'):
    pass
elif name == 'probe':
    sys.exit(state.get('probe_error', 0))
else:
    raise SystemExit('Unexpected synthetic command: ' + name)
'''


class Sandbox:
    """Run rewritten fixture scripts with an allowlisted, synthetic command PATH."""

    def __init__(self, root: Path, **state):
        """Create temporary OS, home, SSH and command fixtures."""
        self.root = root
        self.bin = root / 'bin'
        self.bin.mkdir()
        self.home = root / "home 'quoted' $literal"
        self.home.mkdir()
        self.os_release = root / 'os-release'
        self.os_release.write_text('ID=ubuntu\n')
        (root / 'sshd_config').write_text('PermitRootLogin yes\n')
        (root / 'state.json').write_text(json.dumps(state))
        dispatcher = root / 'dispatcher'
        dispatcher.write_text(f'#!{sys.executable}\n' + DISPATCHER)
        dispatcher.chmod(0o755)
        for name in ('id', 'getent', 'whoami', 'hostname', 'apt', 'apt-get', 'useradd',
                     'usermod', 'chpasswd', 'hostnamectl', 'passwd', 'chown',
                     'loginctl', 'systemctl', 'ufw', 'qrencode', 'pgrep', 'sleep', 'probe', 'awk'):
            (self.bin / name).symlink_to(dispatcher)
        # Only these real utilities run, against fixture files. No host PATH fallback.
        for name in ('bash', 'env', 'cat', 'cut', 'tr', 'grep', 'sed', 'head',
                     'base32', 'mkdir', 'chmod', 'cp', 'tee', 'realpath', 'dirname',
                     'basename', 'stat', 'cmp', 'mktemp', 'mv', 'rm', 'ln', 'readlink'):
            path = Path('/usr/bin') / name
            assert path.exists(), name
            (self.bin / name).symlink_to(path)
        self.env = {
            'PATH': str(self.bin), 'HOME': str(self.home), 'USER': 'root', 'LOGNAME': 'root',
            'SANDBOX': str(root), 'HOME_FIXTURE': str(self.home),
            'LC_ALL': 'C', 'PROMPTS': str(root / 'prompts'),
        }

    def tool(self, name):
        """Expose an optional synthetic executable."""
        (self.bin / name).symlink_to(self.root / 'dispatcher')

    def source(self, path):
        """Redirect hardcoded OS/SSH paths only in a temporary script copy."""
        source = (ROOT / path).read_text()
        source = source.replace('/etc/os-release', str(self.os_release))
        source = source.replace('/etc/ssh/sshd_config', str(self.root / 'sshd_config'))
        source = source.replace('/etc/openvpn', str(self.root / 'openvpn'))
        source = source.replace('/etc/pam.d', str(self.root / 'pam.d'))
        source = source.replace('read -s -p', 'printf "prompt\\n" >> "$PROMPTS"; read -s -p')
        return source

    def run(self, path, args=(), *, source=None, input='fixture-password\nfixture-password\n'):
        """Execute only the fixture copy, never the production installer."""
        script = self.root / 'fixture.sh'
        script.write_text(self.source(path) if source is None else source)
        return _run(['/bin/bash', str(script), *args], env=self.env, input=input)

    def calls(self):
        """Read synthetic command metadata without input secrets."""
        path = self.root / 'calls'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def test_shell_lf_and_windows_git_conversion(tmp_path):
    """Use a temporary index and stdout filters without altering the checkout."""
    git_dir = tmp_path / 'git-metadata'
    assert _run(['git', 'init', '--bare', str(git_dir)]).returncode == 0
    env = {**os.environ, 'GIT_DIR': str(git_dir), 'GIT_WORK_TREE': str(ROOT),
           'GIT_INDEX_FILE': str(tmp_path / 'index'),
           'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1'}
    assert _run(['git', 'read-tree', '--empty'], env=env).returncode == 0
    # The runner snapshot deliberately has no .git. Use only temporary metadata
    # and objects, with attributes from the existing source; never create a checkout.
    files = sorted([*ROOT.glob('*.sh'), *(ROOT / 'setup').rglob('*.sh')])
    assert files
    for file in files:
        path = str(file.relative_to(ROOT))
        content = file.read_bytes()
        assert b'\r' not in content, path
        result = _run(['git', '-c', 'core.autocrlf=true', 'check-attr', 'text', 'eol', '--', path], env=env)
        assert ': text: set' in result.stdout and ': eol: lf' in result.stdout
        object_id = _run(['git', 'hash-object', '-w', '--stdin'], env=env, input=content.decode()).stdout.strip()
        result = _run(['git', '-c', 'core.autocrlf=true', 'cat-file', '--filters',
                       f'--path={path}', object_id], env=env, binary=True)
        assert result.returncode == 0, result.stderr
        assert result.stdout == content
        assert _run(['/bin/bash', '-n', str(file)]).returncode == 0

    for path in ('install.sh', 'setup/nested/example.sh'):
        normalized = _run(['git', '-c', 'core.autocrlf=true', 'hash-object', f'--path={path}', '--stdin'],
                          env=env, input='#!/bin/bash\r\nexit 0\r\n')
        expected = _run(['git', 'hash-object', '--stdin'], env=env, input='#!/bin/bash\nexit 0\n')
        assert normalized.stdout == expected.stdout


@pytest.mark.parametrize('path', ['install.sh', 'setup/setup_openvpn.sh', 'setup/setup_firewall.sh',
                                 'setup/setup_totp.sh', 'setup/setup_swap.sh'])
@pytest.mark.parametrize('uid,sudo', [(0, False), (0, True), (1000, True), (1000, False)])
def test_privilege_prefix_and_shell_contexts(tmp_path, path, uid, sudo):
    """Exercise each real privilege preflight with empty arrays and shell contexts."""
    box = Sandbox(tmp_path, uid=uid, exists=True)
    if sudo:
        box.tool('sudo')
    source = box.source(path)
    # Execute the actual preflight, then harmless operations in all affected forms.
    end = source.index('\nfi\n', source.index('SUDO=()')) + len('\nfi\n')
    source = source[:end] + r'''
printf '%s' 'quoted value' | "${SUDO[@]}" tee "$HOME/pipe"
"${SUDO[@]}" tee "$HOME/heredoc" >/dev/null <<EOF
fixture
EOF
value="$("${SUDO[@]}" cat "$HOME/pipe")"
[[ "$value" == 'quoted value' ]]
"${SUDO[@]}" bash -c '[[ "$1" == "a b" && "$2" == "quote\"" ]]' -- 'a b' 'quote"'
"${SUDO[@]}" probe 'a b' 'literal;$HOME'
'''
    result = box.run(path, source=source)
    assert result.returncode == (1 if uid and not sudo else 0), result.stderr
    if uid and not sudo:
        assert 'Run as root' in result.stderr
        assert not (box.home / 'pipe').exists()
    else:
        assert (box.home / 'heredoc').read_text() == 'fixture\n'
        calls = box.calls()
        assert any(c[:2] == ['probe', ['a b', 'literal;$HOME']] for c in calls)
        assert any(c[0] == 'sudo' for c in calls) == bool(uid)


@pytest.mark.parametrize('path', ['install.sh', 'setup/master_vps_init.sh'])
@pytest.mark.parametrize('os_id,fallback,expected', [('ubuntu', False, 0), ('debian', True, 1),
                                                  ('', True, 0), ('', False, 1)])
def test_os_detection_without_lsb_release(tmp_path, path, os_id, fallback, expected):
    """Test actual OS code against temporary metadata, never /etc overrides."""
    box = Sandbox(tmp_path)
    box.os_release.write_text(f'ID={os_id}\n')
    if fallback:
        box.tool('lsb_release')
    source = box.source(path)
    start = source.index('# Read distribution metadata')
    end = source.index('\nfi\n', source.index('if [[ "$(ubuntu_id)"', start)) + len('\nfi\n')
    result = box.run(path, source='set -euo pipefail\n' + source[start:end])
    assert result.returncode == expected, result.stderr
    if os_id == 'debian':
        assert not any(c[0] == 'lsb_release' for c in box.calls())


@pytest.mark.parametrize('failure', ['apt_missing', 'update', 'install', 'still_missing', 'none'])
def test_master_sudo_bootstrap_order_and_failures(tmp_path, failure):
    """sudo readiness must precede prompts, account changes and root lockout."""
    box = Sandbox(tmp_path, uid=0, apt_failure=failure, install_sudo=failure != 'still_missing')
    if failure == 'apt_missing':
        (box.bin / 'apt-get').unlink()
    result = box.run('setup/master_vps_init.sh', ['test-host', 'admin'])
    names = [c[0] for c in box.calls()]
    if failure != 'none':
        assert result.returncode != 0
        assert not (tmp_path / 'prompts').exists()
        assert not set(names) & {'hostnamectl', 'useradd', 'chpasswd', 'usermod', 'passwd', 'systemctl'}
        assert (tmp_path / 'sshd_config').read_text() == 'PermitRootLogin yes\n'
    else:
        assert result.returncode == 0, result.stderr
        assert names.index('apt-get') < names.index('hostnamectl') < names.index('passwd')
        assert (tmp_path / 'prompts').read_text().count('prompt') == 2
        assert (tmp_path / 'sshd_config').read_text() == 'PermitRootLogin no\n'
        assert 'sudo' not in names  # Root commands still execute directly.
        apt_calls = [c for c in box.calls() if c[0] == 'apt-get']
        assert len(apt_calls) == 2
        assert apt_calls[1][1][-3:] == ['install', '-y', 'sudo']
        assert all(c[2]['DEBIAN_FRONTEND'] == 'noninteractive' for c in apt_calls)


@pytest.mark.parametrize('case', ['existing_admin', 'existing_nonadmin', 'uid_zero_alias',
                                  'new_group_failure', 'nonroot_no_sudo', 'nonroot_sudo', 'sudo_present'])
def test_master_accounts_and_existing_sudo(tmp_path, case):
    """Reject unsafe target accounts and verify new membership before root locking."""
    box = Sandbox(tmp_path, uid=1000 if case.startswith('nonroot') else 0,
                  exists=case.startswith('existing') or case == 'uid_zero_alias',
                  admin=case == 'existing_admin', target_uid=0 if case == 'uid_zero_alias' else 1000,
                  group_failure=case == 'new_group_failure')
    if case in ('sudo_present', 'nonroot_sudo'):
        box.tool('sudo')
    result = box.run('setup/master_vps_init.sh', ['test-host', 'admin'])
    names = [c[0] for c in box.calls()]
    success = case in ('existing_admin', 'sudo_present', 'nonroot_sudo')
    assert (result.returncode == 0) == success, result.stderr
    if not success:
        assert 'passwd' not in names and 'systemctl' not in names
        assert (tmp_path / 'sshd_config').read_text() == 'PermitRootLogin yes\n'
        if case != 'new_group_failure':
            assert 'apt-get' not in names and not (tmp_path / 'prompts').exists()
    if case in ('sudo_present', 'nonroot_sudo'):
        assert 'apt-get' not in names
    if case == 'existing_admin':
        assert 'usermod' not in names and 'useradd' not in names


@pytest.mark.parametrize('hostname,user,os_id', [('bad/host', 'admin', 'ubuntu'),
    ('-host', 'admin', 'ubuntu'), ('a..b', 'admin', 'ubuntu'), ('host', '-admin', 'ubuntu'),
    ('host', 'root', 'ubuntu'), ('host', 'bad\nuser', 'ubuntu'), ('host', 'admin', 'debian')])
def test_master_invalid_inputs_precede_bootstrap(tmp_path, hostname, user, os_id):
    """Invalid names or OS must not install packages, prompt or mutate the host."""
    box = Sandbox(tmp_path)
    box.os_release.write_text(f'ID={os_id}\n')
    result = box.run('setup/master_vps_init.sh', [hostname, user])
    assert result.returncode != 0
    assert not any(c[0] in ('apt-get', 'hostnamectl', 'useradd', 'passwd') for c in box.calls())
    assert not (tmp_path / 'prompts').exists()


@pytest.mark.parametrize('tool,target_uid', [('sudo', 1000), ('runuser', 1000), (None, 0), (None, 1000)])
@pytest.mark.parametrize('path', ['setup/setup_systemd.sh', 'setup/vps_service_control.sh'])
def test_service_identity_environment_and_early_failure(tmp_path, tool, target_uid, path):
    """Check numeric root aliases, target environment, quoting and error propagation."""
    box = Sandbox(tmp_path, uid=0, exists=True, target_uid=target_uid, probe_error=7)
    if tool:
        box.tool(tool)
    source = box.source(path)
    if path.endswith('setup_systemd.sh'):
        start = source.index('# Resolve the switch')
        end = source.index('unit_dir=', start)
        function_start = source.index('  run_user_systemctl() {', source.index('if [[ "$RUNNING_AS_ROOT" == true ]]; then\n  loginctl'))
        function_end = source.index('\nelse\n', function_start)
        source = ('set -euo pipefail\nerr() { echo "$*" >&2; }\nRUNNING_AS_ROOT=true\n'
                  + f'TARGET_USER=alias\nTARGET_UID={target_uid}\nuid={target_uid}\nTARGET_HOME={shlex.quote(str(box.home))}\n'
                  + source[start:end] + source[function_start:function_end].replace('systemctl --user', 'probe')
                  + '\nrun_user_systemctl "a b" \'literal;$HOME\'\n')
    else:
        start = source.index('# Check once')
        end = source.index('\nunit_for()', start)
        source = ('set -euo pipefail\n' + f'target_user=alias\ntarget_uid={target_uid}\n'
                  + f'target_home={shlex.quote(str(box.home))}\nXDG_RUNTIME_DIR=/wrong\n'
                  + source[start:end] + '\nrun_as_service_user probe "a b" \'literal;$HOME\'\n')
    result = box.run(path, source=source)
    calls = box.calls()
    if tool is None and target_uid:
        assert result.returncode == 1
        assert 'sudo or runuser' in result.stderr
        assert not any(c[0] == 'probe' for c in calls)
    else:
        assert result.returncode == 7, result.stderr
        probe = next(c for c in calls if c[0] == 'probe')
        assert probe[1] == ['a b', 'literal;$HOME']
        assert probe[2] == {'HOME': str(box.home), 'USER': 'alias', 'LOGNAME': 'alias',
                            'XDG_RUNTIME_DIR': f'/run/user/{target_uid}', 'DEBIAN_FRONTEND': None}


def test_totp_stdin_quoting_and_file_modes(tmp_path):
    """A quoted home stays literal; QR data travels over stdin, never argv."""
    box = Sandbox(tmp_path, uid=0, exists=True)
    (tmp_path / 'pam.d').mkdir()
    result = box.run('setup/setup_totp.sh')
    assert result.returncode == 0, result.stderr
    secret_file = box.home / '.google_authenticator'
    lines = secret_file.read_text().splitlines()
    assert len(lines[0]) == 16 and set(lines[0]) <= set('ABCDEFGHIJKLMNOPQRSTUVWXYZ234567')
    assert lines[1:] == ['" RESETTING_TIME_SKEW', '" RATE_LIMIT 3 30', '" WINDOW_SIZE 17',
                        '" DISALLOW_REUSE', '" TOTP_AUTH']
    assert secret_file.stat().st_mode & 0o777 == 0o600
    qr = box.home / 'GA-QR.txt'
    assert qr.read_text() == 'synthetic QR\n' and qr.stat().st_mode & 0o777 == 0o644
    qr_call = next(c for c in box.calls() if c[0] == 'qrencode')
    assert qr_call[1] == ['-t', 'ASCII']
    assert (tmp_path / 'qr-input').read_text() == f'otpauth://totp/admin@test-host?secret={lines[0]}&issuer=OpenVPN'
    assert (tmp_path / 'openvpn/google-auth/admin').read_bytes() == secret_file.read_bytes()
    assert (tmp_path / 'openvpn/google-auth/admin').stat().st_mode & 0o777 == 0o600
    assert not any(c[0] == 'sudo' for c in box.calls())


@pytest.mark.parametrize('uid,sudo', [(0, False), (1000, False), (1000, True)])
def test_ufw_dry_run_without_writes(tmp_path, uid, sudo):
    """Run the real UFW helper only; root must inspect rules without sudo."""
    box = Sandbox(tmp_path, uid=uid)
    if sudo:
        box.tool('sudo')
    source = box.source('setup/cleanup_streamlit_master.sh')
    start = source.index('cleanup_ufw_8501() {')
    end = source.index('\ncleanup_venv_packages()', start)
    source = ('set -euo pipefail\ndry_run=1\ninfo() { :; }; warn() { :; }; '
              'error() { :; }; success() { :; }\n' + source[start:end] + '\ncleanup_ufw_8501\n')
    result = box.run('setup/cleanup_streamlit_master.sh', source=source)
    assert result.returncode == 0, result.stderr
    ufw = [c for c in box.calls() if c[0] == 'ufw']
    assert len(ufw) == (2 if uid == 0 or sudo else 0)
    assert all(c[1] == ['status', 'numbered'] for c in ufw)


@pytest.mark.parametrize('tool,target_uid', [('sudo', 1000), ('runuser', 1000), (None, 0), (None, 1000)])
def test_full_service_control_starter_fallback(tmp_path, tool, target_uid):
    """The actual service loop must switch the starter fallback or fail before it."""
    box = Sandbox(tmp_path, uid=0, exists=True, target_uid=target_uid, legacy=True, probe_error=7)
    if tool:
        box.tool(tool)
    app = tmp_path / 'pbgui'
    app.mkdir()
    starter = app / 'starter.py'
    starter.write_text('# Synthetic marker; the probe never executes Python source.\n')
    box.env.update(PBGUI_USER='alias', PBGUI_DIR=str(app), PBGUI_PYTHON=str(box.bin / 'probe'))
    result = box.run('setup/vps_service_control.sh', ['start', 'PBCluster'])
    probe = [c for c in box.calls() if c[0] == 'probe']
    if tool is None and target_uid:
        assert result.returncode == 1
        assert not probe and not any(c[0] == 'systemctl' for c in box.calls())
    else:
        assert result.returncode == 7, result.stderr
        assert len(probe) == 1 and probe[0][1] == [str(starter), '-s', 'PBCluster']
        assert probe[0][2]['USER'] == probe[0][2]['LOGNAME'] == 'alias'
        assert probe[0][2]['HOME'] == str(box.home)
        assert probe[0][2]['XDG_RUNTIME_DIR'] == f'/run/user/{target_uid}'


@pytest.mark.parametrize('tool,target_uid', [('sudo', 1000), ('runuser', 1000), (None, 0), (None, 1000)])
def test_full_systemd_preflight_and_target_environment(tmp_path, tool, target_uid):
    """Missing switch tools must fail before writing units, including preserve-state."""
    box = Sandbox(tmp_path, uid=0, exists=True, target_uid=target_uid)
    if tool:
        box.tool(tool)
    app = tmp_path / 'pbgui'
    app.mkdir()
    result = box.run('setup/setup_systemd.sh', ['--user', 'alias', '--pbgui-dir', str(app),
                    '--python', str(box.bin / 'probe'), '--enable', 'monitor-agent', '--preserve-state'])
    units = box.home / '.config/systemd/user'
    if tool is None and target_uid:
        assert result.returncode == 1
        assert not units.exists()
        assert not any(c[0] in ('systemctl', 'loginctl', 'chown') for c in box.calls())
    else:
        assert result.returncode == 0, result.stderr
        assert (units / 'pbgui-monitor-agent.service').is_file()
        calls = [c for c in box.calls() if c[0] == 'systemctl' and '--user' in c[1]]
        assert calls
        assert all(c[2]['USER'] == c[2]['LOGNAME'] == 'alias' for c in calls)
        assert all(c[2]['HOME'] == str(box.home) for c in calls)
        assert all(c[2]['XDG_RUNTIME_DIR'] == f'/run/user/{target_uid}' for c in calls)
        assert not any(set(c[1]) & {'enable', 'disable', 'restart', 'stop'} for c in calls)
