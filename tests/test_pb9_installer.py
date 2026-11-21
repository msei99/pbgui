"""PB9 installer isolation, shared artifact contracts and guarded dispatch."""
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import zipfile

import pytest
import yaml
from jinja2 import Environment, StrictUndefined

import pb8_wheels as wheels
import pb8_live_stop
from passivbot_install_paths import validate_install_paths
from pb9_backtest_runtime import require_runtime_available
from setup.installer import core
from test_master_installer_maintenance import _existing_master, _init_checkout
from vps_manager_service import _pb8_remote_action_status
import vps_manager_service as service_mod
import pb7_guard


@pytest.mark.parametrize('scope', ['master', 'vps'])
@pytest.mark.parametrize('existing', [False, True], ids=['new', 'existing'])
@pytest.mark.parametrize('product', ['pb8', 'pb9'])
def test_official_guard_fetches_generation_branch(tmp_path, monkeypatch, scope, existing, product):
    """Execute rendered installer argv against local master/v9 refs, without any host action."""
    env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1', GIT_TERMINAL_PROMPT='0')
    monkeypatch.setenv('GIT_CONFIG_GLOBAL', os.devnull)
    monkeypatch.setenv('GIT_CONFIG_NOSYSTEM', '1')

    def git(*args):
        """Use only temporary fixture repositories and hermetic author configuration."""
        return subprocess.run(['git', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                               *map(str, args)], env=env, check=True, capture_output=True, text=True).stdout.strip()

    upstream = tmp_path/'upstream'
    git('init', '-q', '--initial-branch=master', upstream)
    (upstream/'src/config').mkdir(parents=True)
    commits = {}
    for branch, version, schema in [('master', '8.2.0.dev0', 'v8.6.0'), ('v9', '9.0.0.dev0', 'v9.0.0')]:
        if branch != 'master':
            git('-C', upstream, 'checkout', '-q', '-b', branch)
        (upstream/'src/passivbot_version.py').write_text(f'__version__ = "{version}"\n')
        (upstream/'src/config/schema.py').write_text(f'CONFIG_SCHEMA_VERSION = "{schema}"\n')
        git('-C', upstream, 'add', 'src')
        git('-C', upstream, 'commit', '-qm', branch)
        commits[branch] = git('-C', upstream, 'rev-parse', 'HEAD')
    git('-C', upstream, 'checkout', '-q', 'master')
    checkout = tmp_path/product
    git('clone', *([] if existing else ['--no-checkout']), upstream, checkout)
    before = (git('-C', checkout, 'rev-parse', 'HEAD'), git('-C', checkout, 'status', '--porcelain'))

    project = Path(__file__).resolve().parents[1]
    play = yaml.safe_load((project/f'{scope}-update-pb8.yml').read_text())[0]
    render = Environment(undefined=StrictUndefined)
    context = {'pb8dir': str(checkout), 'pbgdir': str(project),
               'pb8_guard_script': str(project/'pb7_guard.py'), 'pbgui_python': sys.executable}
    if product == 'pb9':
        wrapper = yaml.safe_load((project/f'{scope}-update-pb9.yml').read_text())[0]
        context.update(wrapper['vars'])
    for key in ('passivbot_product', 'passivbot_major', 'passivbot_default_branch'):
        context[key] = render.from_string(play['vars'][key]).render(context)
    suffix = 'for an existing checkout' if existing else 'for a new checkout'
    task = next(task for task in play['tasks']
                if task.get('name', '').startswith('Fetch and verify latest official Passivbot')
                and task['name'].endswith(suffix))
    argv = [render.from_string(str(value)).render(context) for value in task['command']['argv']]
    assert argv[argv.index('--fetch-url')+1] == 'https://github.com/enarjord/passivbot.git'
    argv[argv.index('--fetch-url')+1] = str(upstream)
    assert '--checkout' not in argv and '--stop-processes' not in argv
    result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    branch = 'v9' if product == 'pb9' else 'master'
    ref = f'refs/remotes/pbgui-pb7-pin/{branch}'
    assert argv[argv.index('--ref')+1] == ref
    assert git('-C', checkout, 'rev-parse', ref) == commits[branch]
    assert (git('-C', checkout, 'rev-parse', 'HEAD'), git('-C', checkout, 'status', '--porcelain')) == before


@pytest.mark.parametrize('schema,accepted', [('v9.0.0',True),('v8.6.0',False),('v10.0.0',False)])
def test_pb9_install_guard_requires_v9_schema(tmp_path, schema, accepted):
    """Check generation before an installer may mutate its checkout."""
    source = tmp_path/'src';(source/'config').mkdir(parents=True)
    (source/'passivbot_version.py').write_text('__version__ = "9.0.0.dev0"\n')
    (source/'config/schema.py').write_text(f'CONFIG_SCHEMA_VERSION = "{schema}"\n')
    if accepted:
        assert pb7_guard.verify_passivbot_major(tmp_path,9) == '9.0.0.dev0'
    else:
        with pytest.raises(RuntimeError,match='expected Passivbot v9 config schema'):
            pb7_guard.verify_passivbot_major(tmp_path,9)


def test_pb9_uninstall_targets_stay_below_selected_parent(tmp_path):
    """Include the optional slot without touching any real installation."""
    targets = core._local_install_targets(tmp_path)
    assert targets['PB9'] == tmp_path/'pb9'
    assert targets['PB9 venv'] == tmp_path/'venv_pb9'
    assert all(path.parent == tmp_path for path in targets.values())


@pytest.mark.parametrize('role,custom,allowed', [('master', False, True), ('slave', False, False), ('master', True, False)])
def test_pb9_master_dispatch_requires_role_and_fixed_variables(monkeypatch, role, custom, allowed):
    """Capture a requested playbook without executing an update."""
    service = object.__new__(service_mod.VPSManagerService)
    calls = []
    service.vpsmanager = SimpleNamespace(update_master=lambda **kw: calls.append(kw))
    monkeypatch.setattr(service_mod, 'load_ini', lambda *_: role)
    args = dict(command='master-update-pb9', command_text='fixture', extra_vars={'pb8dir':'/bad'} if custom else None)
    if allowed:
        service.run_master_command(**args)
        assert calls[0]['command'] == 'master-update-pb9'
        assert calls[0]['extra_vars'] is None
    else:
        with pytest.raises(ValueError):
            service.run_master_command(**args)
        assert not calls


@pytest.mark.parametrize('fresh,custom,allowed', [(True, False, True), (False, False, False), (True, True, False)])
def test_pb9_remote_dispatch_checks_before_launch(fresh, custom, allowed):
    """Use synthetic telemetry and credentials; never contact a host."""
    service = object.__new__(service_mod.VPSManagerService)
    vps = SimpleNamespace(hostname='fixture', user_pw=None, command_run_id='fixture')
    vps._task_log_path = lambda *args: Path('fixture.log')
    calls = []
    service.vpsmanager = SimpleNamespace(update_vps=lambda *args, **kw: calls.append(kw))
    service._require_vps = lambda _: vps
    service._apply_session_secrets_to_vps = lambda *args: None
    service._get_monitor_state = lambda: {}
    service._get_host_telemetry = lambda *args: {'meta':{'role':'slave'},'system':{'disk_free':4*1024**3}}
    service._host_telemetry_fresh = lambda _: fresh
    service._credential_playbook_vars = lambda *args: {}
    service._sync_vps_config_from_host_meta = lambda *args: None
    service._raise_if_vps_task_active = lambda *args: None
    args = dict(token='fixture',hostname='fixture',command='vps-update-pb9',command_text='fixture',extra_vars={'pb8dir':'/bad'} if custom else None)
    if allowed:
        result = service.run_vps_command(**args)
        assert result['command'] == calls[0]['command'] == 'vps-update-pb9'
        assert calls[0]['extra_vars'] == {'pb8_min_free_bytes':service_mod.PB8_MIN_FREE_DISK_BYTES}
    else:
        with pytest.raises(ValueError):
            service.run_vps_command(**args)
        assert not calls


@pytest.mark.parametrize('source', ['pbgui/pb9', 'pb8', 'pb8/nested', 'pb7'])
def test_pb9_install_rejects_source_overlap(tmp_path, source):
    """Refuse equal or nested slots before any update action."""
    with pytest.raises(ValueError, match='separate'):
        validate_install_paths('pb9', tmp_path/'pbgui', tmp_path/source, tmp_path/'venv_pb9')


def test_pb9_install_accepts_separate_venvs_with_one_system_python(tmp_path):
    """Python symlink targets do not identify an environment."""
    import sys
    for name in ['venv_pb8', 'venv_pb9']:
        path = tmp_path/name/'bin/python'; path.parent.mkdir(parents=True)
        path.symlink_to(sys.executable)
    validate_install_paths('pb9', tmp_path/'pbgui', tmp_path/'pb9', tmp_path/'venv_pb9')
    with pytest.raises(ValueError, match='separate'):
        validate_install_paths('pb9', tmp_path/'pbgui', tmp_path/'pb9', tmp_path/'venv_pb8')


def test_pb9_maintenance_is_independent_of_dirty_pb8(tmp_path, monkeypatch):
    """A PB8 development checkout cannot block the separate PB9 install."""
    root = _existing_master(tmp_path, monkeypatch)
    _init_checkout(root/'pb8')
    tracked = next(path for path in (root/'pb8').iterdir() if path.is_file())
    tracked.write_text('changed')
    status = core.inspect_local_master_install(str(root))
    assert status['can_maintain_pb9']
    assert not status['can_maintain_pb8']
    assert status['pb9_dir'] == str(root/'pb9')
    assert status['pb9_venv'] == str(root/'venv_pb9')


@pytest.mark.parametrize('action,configured,expected', [
    ('local-pb9', False, ['master-update-pb9.yml']),
    ('local-update-all', False, ['master-update-pb.yml', 'master-update-pb8.yml']),
    ('local-update-all', True, ['master-update-pb.yml', 'master-update-pb8.yml', 'master-update-pb9.yml']),
])
def test_pb9_local_dispatch_preserves_other_slots(tmp_path, monkeypatch, action, configured, expected):
    """Update All includes PB9 only when it is already configured."""
    root = tmp_path/'software'; gui = root/'pbgui'; gui.mkdir(parents=True)
    for product in ['pb8', 'pb9']:
        (gui/f'master-update-{product}.yml').write_text('---\n')
    status = {'installed': True, 'maintenance_errors': [], 'pb9_maintenance_errors': [], 'update_all_errors': [],
        'pbgui_dir': str(gui), 'pbgui_python': '/fixture/bin/python', 'ansible_playbook': '/fixture/bin/ansible-playbook',
        'pb7_dir': str(root/'pb7'), 'pb7_venv': str(root/'venv_pb7'),
        'pb9_dir': str(root/'pb9'), 'pb9_venv': str(root/'venv_pb9'), 'pb9_configured': configured}
    calls = []
    def dispatch(argv, _log, **kwargs):
        """Capture only private fixture variables, never execute Ansible."""
        values = json.loads(Path(str(argv[argv.index('--extra-vars')+1])[1:]).read_text())
        calls.append((Path(argv[-1]).name, values))
    monkeypatch.setattr(core, 'inspect_local_master_install', lambda _: dict(status))
    monkeypatch.setattr(core, 'acquire_master_update_lock', lambda _: nullcontext())
    monkeypatch.setattr(core, '_run_streaming_command', dispatch)
    core.run_local_master_maintenance(core.LocalMasterMaintenanceConfig(str(root), action), lambda _: None, tmp_path/'artifacts')
    assert [name for name, _ in calls] == expected
    if 'master-update-pb9.yml' in expected:
        assert calls[-1][1]['pb8dir'] == str(root/'pb9')
        assert calls[-1][1]['pb8venv'] == str(root/'venv_pb9')


def test_pb9_combined_wheel_is_verified_and_installed_once(tmp_path, monkeypatch):
    """A separate Rust distribution must not shadow PB9's RECORD-owned binary."""
    bundle=tmp_path/'bundle';bundle.mkdir()
    wheel='passivbot-9.0.0.dev0-cp312-cp312-manylinux_2_35_x86_64.whl'
    binary=b'synthetic native extension'
    with zipfile.ZipFile(bundle/wheel,'w') as archive:
        archive.writestr('passivbot_rust.cpython-312-x86_64-linux-gnu.so',binary)
    (bundle/'Cargo.lock').write_text('tracked lock\n')
    manifest={'format':1,'product':'pb9','commit':'a'*40,'source_fingerprint':'b'*64,
        'extension_sha256':hashlib.sha256(binary).hexdigest(),
        'artifacts':{path.name:wheels.digest(path) for path in bundle.iterdir()}}
    (bundle/'manifest.json').write_text(json.dumps(manifest))
    verified=wheels.verify_bundle(bundle,'a'*40,product='pb9')
    assert verified['python_wheel']==verified['rust_wheel']==wheel
    commands=[]
    monkeypatch.setattr(core,'_run_command',lambda argv,*args,**kwargs:commands.append([str(a) for a in argv]) or '')
    monkeypatch.setattr(core,'pb8_gpu_extras',lambda _: 'full')
    repo=tmp_path/'pb9';(repo/'passivbot-rust').mkdir(parents=True)
    core._install_master_wheel('pb9',repo,tmp_path/'venv_pb9',tmp_path/'pbgui','a'*40,bundle,verified,lambda _:None)
    install=next(argv for argv in commands if '--force-reinstall' in argv)
    assert install.count(str(bundle/wheel))==1
    with pytest.raises(ValueError):
        wheels.verify_bundle(bundle,'a'*40,product='pb8')


def test_pb9_stops_only_owned_pb9_live_instances(tmp_path):
    """Share PB8 ownership machinery while selecting only run_v9."""
    for runtime in ['v8','v9']:
        folder=tmp_path/'data'/('run_'+runtime)/'fixture';folder.mkdir(parents=True)
        (folder/'config.json').write_text('{}')
    stopped=[]
    class Runner:
        """Synthetic exact-owner runner; no real process is inspected or killed."""
        def pid(self):
            """Verify only the selected generation was reached."""
            assert 'run_v9' in self.path
            assert self.pb9dir==str(tmp_path/'pb9')
            return SimpleNamespace(pid=123)
        def stop(self, process):
            """Record the fixture process only."""
            stopped.append(process.pid)
    assert pb8_live_stop.stop_managed_live_processes(tmp_path,tmp_path/'pb9',tmp_path/'venv_pb9/bin/python',runtime='pb9',runner_class=Runner)==[123]
    assert stopped==[123]


def test_pb9_invalid_marker_blocks_launches_and_preserves_pb8(tmp_path):
    """A failed update cannot run, even if source and native hashes still match."""
    root=tmp_path/'data/locks';root.mkdir(parents=True)
    (root/'pb8-runtime-invalid').write_text('blocked')
    require_runtime_available(tmp_path)
    (root/'pb9-runtime-invalid').write_text('blocked')
    with pytest.raises(RuntimeError,match='PB9 installation'):
        require_runtime_available(tmp_path)


@pytest.mark.parametrize('fresh,role,disk,allowed',[(True,'slave',4*1024**3,True),(True,'master',4*1024**3,True),(False,'slave',4*1024**3,False),(True,'other',4*1024**3,False),(True,'slave',1,False)])
def test_pb9_remote_preflight_shares_role_and_capacity_rules(fresh,role,disk,allowed):
    """No remote write is possible with stale or insufficient telemetry."""
    state={'meta':{'role':role},'system':{'disk_free':disk}}
    result=_pb8_remote_action_status(state,telemetry_fresh=fresh,product='pb9')
    assert result['allowed'] is allowed
    assert result['profile']==('full' if role=='master' else 'live')


@pytest.mark.parametrize('scope',['master','vps'])
def test_pb9_playbooks_are_thin_profiles_of_verified_pb8_installer(scope):
    """All installation and validation ordering remains in the existing shared playbooks."""
    wrapper=yaml.safe_load(Path(f'{scope}-update-pb9.yml').read_text())[0]
    assert wrapper['import_playbook']==f'{scope}-update-pb8.yml'
    assert wrapper['vars']['runtime_product']=='pb9'
    shared=Path(f'{scope}-update-pb8.yml').read_text()
    assert "'v9' if passivbot_product == 'pb9' else 'master'" in shared
    assert 'pb9-runtime-invalid' in shared
    assert shared.index('Prepare central') < shared.index('Mark {{ passivbot_label }} runtime unavailable')
    assert shared.index('Validate centrally built') < shared.index('Save {{ passivbot_label }} runtime paths')
