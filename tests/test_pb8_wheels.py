"""Offline artifact integrity, compatibility, source and cache regression checks."""
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
from types import SimpleNamespace
import tarfile
import zipfile

import pytest
import yaml

import pb8_wheels as wheels

COMMIT = 'a' * 40
TARGET = {'system': 'Linux', 'machine': 'x86_64', 'python': [3, 12], 'libc': ['glibc', '2.35']}
PYTHON_WHEEL = 'passivbot-8.2.0.dev0-cp312-cp312-manylinux_2_28_x86_64.whl'
RUST_WHEEL = 'passivbot_rust-0.1.0-cp312-abi3-manylinux_2_28_x86_64.whl'


def make_bundle(root):
    """Create minimal fake wheel bytes and an internally consistent manifest."""
    root.mkdir(exist_ok=True)
    with zipfile.ZipFile(root / PYTHON_WHEEL, 'w') as archive:
        archive.writestr('main.py', '# test package')
    with zipfile.ZipFile(root / RUST_WHEEL, 'w') as archive:
        archive.writestr('passivbot_rust/passivbot_rust.abi3.so', b'test compiled extension')
    (root / 'Cargo.lock').write_text('# pinned dependencies\n')
    manifest = {'format': 1, 'commit': COMMIT, 'source_fingerprint': 'b' * 64,
                'extension_sha256': hashlib.sha256(b'test compiled extension').hexdigest(),
                'artifacts': {p.name: wheels.digest(p) for p in root.iterdir()}}
    (root / 'manifest.json').write_text(json.dumps(manifest))
    return manifest


@pytest.mark.parametrize('changes', [
    {'machine': 'aarch64'}, {'python': [3, 14]}, {'system': 'Darwin'},
    {'libc': ['glibc', '2.27']}, {'libc': ['musl', '1.2']}, {'libc': []},
])
def test_unsupported_targets_fail_before_any_build(changes):
    """ARM and other ABIs fail explicitly instead of compiling on the live VPS."""
    with pytest.raises(ValueError, match='no remote Rust build'):
        wheels.require_target({**TARGET, **changes})


@pytest.mark.parametrize('libc', ['2.28', '2.35', '2.39'])
def test_supported_linux_baselines(libc):
    """One manylinux build serves Ubuntu hosts with newer compatible libc."""
    wheels.require_target({**TARGET, 'libc': ['glibc', libc]})


def test_integrity_and_commit_are_required(tmp_path):
    """Wrong provenance and corrupted binaries cannot pass installation preflight."""
    bundle = tmp_path / 'bundle'
    make_bundle(bundle)
    assert wheels.verify_bundle(bundle, COMMIT)['rust_wheel'] == RUST_WHEEL
    with pytest.raises(ValueError, match='commit mismatch'):
        wheels.verify_bundle(bundle, 'c' * 40)
    with (bundle / RUST_WHEEL).open('ab') as handle:
        handle.write(b'corruption')
    with pytest.raises(ValueError, match='checksum mismatch'):
        wheels.verify_bundle(bundle, COMMIT)


@pytest.mark.parametrize('name', ['../outside', '/outside', 'bad.whl', 'passivbot-1-cp314-cp314-linux_x86_64.whl'])
def test_manifest_artifact_paths_and_platform_are_not_trusted(tmp_path, name):
    """Persisted manifest filenames remain subject to validation at every read."""
    bundle = tmp_path / 'bundle'
    manifest = make_bundle(bundle)
    manifest['artifacts'][name] = manifest['artifacts'].pop(PYTHON_WHEEL)
    (bundle / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        wheels.verify_bundle(bundle, COMMIT)


def test_artifact_symlink_is_rejected(tmp_path):
    """A correct digest does not authorize following an artifact symlink."""
    bundle = tmp_path / 'bundle'
    make_bundle(bundle)
    data = (bundle / 'Cargo.lock').read_bytes()
    (bundle / 'Cargo.lock').unlink()
    other = tmp_path / 'other'
    other.write_bytes(data)
    (bundle / 'Cargo.lock').symlink_to(other)
    with pytest.raises(ValueError, match='checksum mismatch'):
        wheels.verify_bundle(bundle, COMMIT)


@pytest.mark.parametrize('name,kind', [('../escape', tarfile.REGTYPE), ('/escape', tarfile.REGTYPE),
                                     ('linked', tarfile.SYMTYPE), ('linked', tarfile.LNKTYPE)])
def test_source_archives_cannot_escape_or_link(tmp_path, name, kind):
    """Source exports are parsed as untrusted data before entering the builder."""
    path = tmp_path / 'source.tar'
    with tarfile.open(path, 'w') as archive:
        member = tarfile.TarInfo(name)
        member.type = kind
        member.linkname = '/etc/passwd'
        archive.addfile(member)
    with pytest.raises(ValueError, match='Unsafe'):
        wheels.unpack_source(path, tmp_path / 'source')


def test_build_reuses_verified_cache_and_pins_each_run(tmp_path, monkeypatch):
    """Two requests share one build and receive independent checked run bundles."""
    source = tmp_path / 'source.tar'
    with tarfile.open(source, 'w') as archive:
        for name in ('setup.py', 'src/rust_utils.py', 'passivbot-rust/Cargo.toml'):
            member = tarfile.TarInfo(name)
            member.size = 1
            archive.addfile(member, io.BytesIO(b'x'))
    calls = []

    def compile_fake(source, artifacts, identity, env):
        """Create packages without a compiler, network or production cache."""
        calls.append(source)
        make_bundle(artifacts)
        (artifacts / 'manifest.json').unlink()
        (artifacts / 'source-fingerprint').write_text('b' * 64)

    monkeypatch.setattr(wheels, 'builder_identity', lambda target: ({'mode': 'test'}, {}))
    monkeypatch.setattr(wheels, 'compile_wheels', compile_fake)
    first = wheels.build(source, COMMIT, TARGET, tmp_path / 'cache', tmp_path / 'first')
    second = wheels.build(source, COMMIT, TARGET, tmp_path / 'cache', tmp_path / 'second')
    assert len(calls) == 1
    assert first['artifacts'] == second['artifacts']
    (tmp_path / 'first' / RUST_WHEEL).unlink()
    assert wheels.verify_bundle(tmp_path / 'second', COMMIT)


def test_source_archive_is_fetched_on_controller_from_exact_commit(tmp_path, monkeypatch):
    """The controller archives the selected exact commit, never target bytes."""
    repo = tmp_path / 'upstream'
    repo.mkdir()
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    (repo / 'setup.py').write_text('trusted source\n')
    subprocess.run(['git', '-C', str(repo), 'add', 'setup.py'], check=True)
    subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Test',
                    '-c', 'user.email=test@example.invalid', 'commit', '-qm', 'source'], check=True)
    commit = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
    subprocess.run(['git', '-C', str(repo), 'branch', '-M', 'master'], check=True)
    monkeypatch.setattr(wheels, '_source_url', lambda _url: str(repo))
    monkeypatch.setattr(wheels, '_source_env', lambda: os.environ.copy())
    assert wheels.resolve_source_head(str(repo), 'master') == {'commit': commit}
    archive = tmp_path / 'staging' / 'source.tar'
    result = wheels.export_trusted_source(commit, archive, str(repo))
    assert result['commit'] == commit
    with tarfile.open(archive) as packed:
        assert packed.extractfile('setup.py').read() == b'trusted source\n'
    with pytest.raises(ValueError, match='must be absent'):
        wheels.export_trusted_source(commit, archive, str(repo))


def test_controller_resolves_exact_branch_head_without_target_input(monkeypatch):
    """Only a named remote branch head, not a fetchable fork SHA, selects normal PB8."""
    calls = []

    def fake_run(argv, **kwargs):
        """Return one official master ref for the controller lookup."""
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, f'{COMMIT}\trefs/heads/master\n', '')

    monkeypatch.setattr(wheels.subprocess, 'run', fake_run)
    assert wheels.resolve_source_head(wheels.PASSIVBOT_UPSTREAM, 'master') == {'commit': COMMIT}
    assert calls[0][0][-2:] == [wheels.PASSIVBOT_UPSTREAM, 'refs/heads/master']
    with pytest.raises(ValueError, match='credential-free'):
        wheels.resolve_source_head('file:///tmp/fork', 'master')
    with pytest.raises(ValueError, match='query'):
        wheels.resolve_source_head('https://github.com/enarjord/passivbot.git?token=secret', 'master')


def test_pb8_wheel_commit_is_controller_selected_and_target_confirmed():
    """PB8 normal and branch updates cannot build a target-reported commit alone."""
    tasks = yaml.safe_load(Path('tasks/pb8_live_wheels_prepare.yml').read_text())
    by_name = {task['name']: task for task in tasks}
    resolve = by_name['Resolve immutable PB8 wheel source commit on controller']
    assert resolve['delegate_to'] == 'localhost'
    assert 'resolve' in resolve['command']['argv']
    assert 'pb8_target_commit' in str(by_name['Pin explicitly selected PB8 wheel source commit'])
    assert 'pb8_resolved_commit' not in str(by_name['Pin explicitly selected PB8 wheel source commit'])
    assert 'pb8_resolved_commit' not in str(by_name['Pin resolved PB8 wheel source commit'])
    confirm = by_name['Require target PB8 commit to match controller selection']
    names = list(by_name)
    assert names.index('Require target PB8 commit to match controller selection') < names.index('Fetch selected PB8 source on the controller')
    assert 'pb8_target_head.stdout' in str(confirm['assert']['that'])
    assert 'pb8_resolved_commit.stdout' in str(confirm['assert']['that'])
    source = by_name['Fetch selected PB8 source on the controller']
    assert source['delegate_to'] == 'localhost'
    for path in ('vps-update-pb8.yml', 'master-update-pb8.yml'):
        content = Path(path).read_text()
        assert '"{{ pb8_wheel_commit.stdout }}"' in content


def test_corrupt_cache_entry_is_rebuilt(tmp_path, monkeypatch):
    """A damaged keyed entry cannot permanently block native wheel preparation."""
    source = tmp_path / 'source.tar'
    with tarfile.open(source, 'w') as archive:
        for name in ('setup.py', 'src/rust_utils.py', 'passivbot-rust/Cargo.toml'):
            member = tarfile.TarInfo(name)
            member.size = 1
            archive.addfile(member, io.BytesIO(b'x'))
    calls = []

    def compile_fake(_source, artifacts, _identity, _env):
        """Write deterministic isolated wheel fixtures."""
        calls.append(1)
        make_bundle(artifacts)
        (artifacts / 'manifest.json').unlink()
        (artifacts / 'source-fingerprint').write_text('b' * 64)

    monkeypatch.setattr(wheels, 'builder_identity', lambda _target: ({'mode': 'test'}, {}))
    monkeypatch.setattr(wheels, 'compile_wheels', compile_fake)
    cache = tmp_path / 'cache'
    wheels.build(source, COMMIT, TARGET, cache, tmp_path / 'first')
    entry = next(path for path in cache.iterdir() if path.is_dir())
    (entry / 'manifest.json').write_text('{broken')
    wheels.build(source, COMMIT, TARGET, cache, tmp_path / 'second')
    assert len(calls) == 2
    assert wheels.verify_bundle(tmp_path / 'second', COMMIT)


def test_receipt_preflight_rejects_symlink_without_changing_directory_mode(tmp_path):
    """Unsafe receipt paths fail before bot stop and ordinary modes remain intact."""
    state = tmp_path / 'data' / 'state'
    state.mkdir(parents=True)
    state.chmod(0o755)
    receipt = state / 'pb7-wheel.json'
    assert wheels.check_receipt_location(receipt)['ready']
    assert stat.S_IMODE(state.stat().st_mode) == 0o755
    linked = tmp_path / 'linked'
    linked.symlink_to(state, target_is_directory=True)
    with pytest.raises(ValueError, match='Symlink'):
        wheels.check_receipt_location(linked / 'pb7-wheel.json')
    receipt.mkdir()
    with pytest.raises(ValueError, match='regular file'):
        wheels.check_receipt_location(receipt)
    assert wheels.check_receipt_location(tmp_path / 'data' / 'new-state' / 'pb7-wheel.json')['ready']


@pytest.mark.parametrize('product', ['pb7', 'pb8'])
@pytest.mark.parametrize('existing_state', [False, True])
def test_receipt_preflight_creates_private_state_before_runtime_changes(tmp_path, product, existing_state):
    """The receipt command secures missing or group-writable legacy state directories."""
    import sys

    data = tmp_path / 'pbgui' / 'data'
    data.mkdir(parents=True)
    data.chmod(0o775)
    if existing_state:
        state = data / 'state'
        state.mkdir()
        state.chmod(0o775)
    receipt = data / 'state' / f'{product}-wheel.json'
    result = subprocess.run(
        [sys.executable, 'pb8_wheels.py', 'receipt', '--receipt', str(receipt)],
        capture_output=True, text=True, check=True,
    )
    assert json.loads(result.stdout)['ready'] is True
    assert stat.S_IMODE(receipt.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(data.stat().st_mode) == 0o775


@pytest.mark.parametrize('playbook', ['vps-update-pb8.yml', 'master-update-pb8.yml'])
def test_playbook_prepares_binaries_before_runtime_guard_and_never_compiles(playbook):
    """Masters and VPS targets install checked binaries before releasing their guard."""
    tasks = yaml.safe_load(Path(playbook).read_text())[0]['tasks']
    names = [task['name'] for task in tasks]
    assert names.index('Prepare central PB8 wheels before runtime changes') < names.index('Mark PB8 runtime unavailable until validation completes')
    assert not any('install rustup' in name.lower() or 'Rebuild' in name for name in names)
    install = next(task for task in tasks if task['name'] == 'Install verified PB8 wheels')
    assert '--only-binary=:all:' in install['command']['argv']
    assert '-e' not in install['command']['argv']
    assert 'when' not in install
    assert names.index('Validate centrally built PB8 runtime') < names.index('Mark validated PB8 runtime available')
    prep = yaml.safe_load(Path('tasks/pb8_live_wheels_prepare.yml').read_text())
    download = next(task for task in prep if task['name'].startswith('Download binary PB8 dependencies'))
    assert '--only-binary=:all:' in download['command']['argv']
    assert '{{ pb8_wheel_extras }}' in download['command']['argv'][-1]
    seed = next(task for task in prep if task['name'] == 'Seed reusable wheel cache on target masters')
    assert seed['when'] == "pb8_install_profile == 'full'"
    assert prep[-2]['name'].startswith('Check unpacked binary installation capacity')
    assert prep[-1]['name'] == 'Check PB8 receipt path before runtime changes'


@pytest.mark.parametrize('bad', ['commit', 'source', 'binary', None])
def test_runtime_stamps_only_matching_installed_extension(tmp_path, monkeypatch, bad):
    """Validation never stamps a stale binary or invokes compilation to repair it."""
    import sys
    bundle = tmp_path / 'bundle'
    manifest = make_bundle(bundle)
    repo = tmp_path / 'pb8'
    (repo / 'passivbot-rust').mkdir(parents=True)
    (repo / 'passivbot-rust/Cargo.lock').write_bytes((bundle / 'Cargo.lock').read_bytes())
    binary = tmp_path / 'installed.abi3.so'
    binary.write_bytes(b'wrong extension' if bad == 'binary' else b'test compiled extension')
    stamps = []
    requirements = []
    helper = SimpleNamespace(
        source_fingerprint=lambda: 'c' * 64 if bad == 'source' else manifest['source_fingerprint'],
        prune_shadowing_local_extensions=lambda: None,
        _compiled_path_from_loaded_module=lambda: binary,
        write_source_stamp=lambda *args: stamps.append(args),
        verify_loaded_runtime_extension=lambda **kwargs: {'verified': True})
    monkeypatch.setattr(wheels, 'probe', lambda: TARGET)
    monkeypatch.setattr(wheels.subprocess, 'check_output', lambda *a, **kw: ('c' * 40 if bad == 'commit' else COMMIT) + '\n')
    monkeypatch.setattr(wheels.importlib.util, 'spec_from_file_location', lambda *a: SimpleNamespace(loader=SimpleNamespace(exec_module=lambda module: None)))
    monkeypatch.setattr(wheels.importlib.util, 'module_from_spec', lambda spec: helper)
    monkeypatch.setattr(wheels.importlib.util, 'find_spec', lambda name: SimpleNamespace(origin=str(tmp_path / 'procedures.py')))
    monkeypatch.setattr(wheels, 'install_live_requirements', lambda *args: requirements.append(args))
    monkeypatch.setitem(sys.modules, 'passivbot_rust', SimpleNamespace())
    monkeypatch.chdir(tmp_path)
    if bad:
        with pytest.raises(ValueError):
            wheels.validate_runtime(bundle, repo, COMMIT, receipt=tmp_path / 'receipt.json')
        assert not stamps
        assert not requirements
        assert not (tmp_path / 'receipt.json').exists()
    else:
        assert wheels.validate_runtime(bundle, repo, COMMIT, receipt=tmp_path / 'receipt.json') == {'verified': True}
        assert len(requirements) == 1
        assert stamps == [(binary, manifest['source_fingerprint'])]
        assert json.loads((tmp_path / 'receipt.json').read_text())['extension_sha256'] == manifest['extension_sha256']


def test_live_requirements_come_from_verified_commit(tmp_path):
    """PB8's installed procedures receives the committed CCXT pin, not working-tree edits."""
    repo = tmp_path / 'repo'
    repo.mkdir()
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    source = repo / 'requirements-live.txt'
    source.write_text('ccxt==4.5.66\n', encoding='utf-8')
    subprocess.run(['git', '-C', str(repo), 'add', 'requirements-live.txt'], check=True)
    subprocess.run([
        'git', '-C', str(repo), '-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
        'commit', '-qm', 'Pin ccxt',
    ], check=True)
    commit = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
    source.write_text('ccxt==9.9.9\n', encoding='utf-8')
    venv = tmp_path / 'venv'
    site = venv / 'lib' / 'python3.12' / 'site-packages'
    site.mkdir(parents=True)
    procedures = site / 'procedures.py'
    procedures.write_text('# installed wheel module\n', encoding='utf-8')

    destination = wheels.install_live_requirements(repo, commit, procedures, venv)

    assert destination == venv / 'lib' / 'python3.12' / 'requirements-live.txt'
    assert destination.read_text(encoding='utf-8') == 'ccxt==4.5.66\n'
    assert wheels.install_live_requirements(repo, commit, procedures, venv) == destination


def test_pb7_bundle_cannot_be_used_as_pb8(tmp_path):
    """Runtime identity is bound into the signed checks even for equal source names."""
    bundle = tmp_path / 'bundle'
    manifest = make_bundle(bundle)
    manifest['product'] = 'pb7'
    (bundle / 'manifest.json').write_text(json.dumps(manifest))
    assert wheels.verify_bundle(bundle, COMMIT, TARGET, 'pb7')['product'] == 'pb7'
    with pytest.raises(ValueError, match='manifest'):
        wheels.verify_bundle(bundle, COMMIT, TARGET, 'pb8')


@pytest.mark.parametrize('free,allowed', [(1, False), (1024**3, True)])
def test_install_capacity_includes_unpacked_files_and_reserve(tmp_path, monkeypatch, free, allowed):
    """A small download must not mask insufficient space for the unpacked runtime."""
    bundle = tmp_path / 'bundle'
    make_bundle(bundle)
    monkeypatch.setattr(wheels.shutil, 'disk_usage', lambda path: SimpleNamespace(free=free))
    if allowed:
        assert wheels.require_install_capacity(bundle, tmp_path)['required_bytes'] >= 128 * 1024**2
    else:
        with pytest.raises(ValueError, match='Active runtime was not changed'):
            wheels.require_install_capacity(bundle, tmp_path)


@pytest.mark.parametrize('name', ['cache,other', 'cache\nother'])
def test_staging_paths_reject_delimiters(tmp_path, name):
    """Staging paths reject control characters and ambiguous delimiters."""
    with pytest.raises(ValueError, match='Invalid staging'):
        wheels.private_directory(tmp_path / name)


def test_builder_failure_removes_partial_cache(tmp_path, monkeypatch):
    """Failed native builds cannot leave a reusable partial cache entry."""
    source = tmp_path / 'source.tar'
    with tarfile.open(source, 'w') as archive:
        for name in ('setup.py', 'src/rust_utils.py', 'passivbot-rust/Cargo.toml'):
            member = tarfile.TarInfo(name)
            member.size = 1
            archive.addfile(member, io.BytesIO(b'x'))

    def fail(*args):
        """Simulate a compiler failure after private staging was created."""
        raise ValueError('compile failed')

    monkeypatch.setattr(wheels, 'builder_identity', lambda target: ({'mode': 'test'}, {}))
    monkeypatch.setattr(wheels, 'compile_wheels', fail)
    with pytest.raises(ValueError, match='compile failed'):
        wheels.build(source, COMMIT, TARGET, tmp_path / 'cache', tmp_path / 'out')
    assert not any(p.is_dir() for p in (tmp_path / 'cache').iterdir())


def test_received_master_cache_can_serve_other_hosts_without_build_tools(tmp_path, monkeypatch):
    """Master A -> master B -> VPS reuses identical verified source artifacts."""
    source = tmp_path / 'source.tar'
    source.write_bytes(b'exact Git archive')
    bundle = tmp_path / 'received'
    manifest = make_bundle(bundle)
    manifest['source_archive_sha256'] = wheels.digest(source)
    (bundle / 'manifest.json').write_text(json.dumps(manifest))
    monkeypatch.setattr(wheels, 'probe', lambda: TARGET)
    wheels.import_bundle(bundle, tmp_path / 'master_b_cache')

    def no_builder(*args):
        """A receiving master has no compiler and must never try to use one."""
        pytest.fail('Imported cache attempted to build')

    monkeypatch.setattr(wheels, 'builder_identity', no_builder)
    result = wheels.build(source, COMMIT, TARGET, tmp_path / 'master_b_cache', tmp_path / 'vps_transfer')
    assert result['artifacts'] == manifest['artifacts']
    assert wheels.verify_bundle(tmp_path / 'vps_transfer', COMMIT, TARGET)


def test_wheel_cannot_be_reused_on_older_target(tmp_path):
    """A valid newer ABI tag must not be passed off as an older compatible build."""
    bundle = tmp_path / 'bundle'
    manifest = make_bundle(bundle)
    for name in list(manifest['artifacts']):
        if name.endswith('.whl'):
            newer = name.replace('manylinux_2_28', 'manylinux_2_39')
            (bundle / name).rename(bundle / newer)
            manifest['artifacts'][newer] = manifest['artifacts'].pop(name)
    (bundle / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='newer glibc'):
        wheels.verify_bundle(bundle, COMMIT, TARGET)
    assert wheels.verify_bundle(bundle, COMMIT, {**TARGET, 'libc': ['glibc', '2.39']})


@pytest.mark.parametrize('result', ['failed', 'timeout', 'interrupted'])
def test_native_builder_reaps_owned_process_groups(monkeypatch, result):
    """Timeouts, compiler errors and termination must clean up spawned compilers."""
    waits = []
    killed = []

    def wait(**kwargs):
        """Raise once like an interrupted native compiler, then allow reaping."""
        waits.append(kwargs)
        if len(waits) == 1:
            if result == 'timeout':
                raise wheels.subprocess.TimeoutExpired(['compiler'], 2400)
            if result == 'interrupted':
                raise KeyboardInterrupt
        return 1

    def start(argv, **kwargs):
        """Capture ownership without spawning a real process."""
        assert kwargs['start_new_session'] is True
        return SimpleNamespace(pid=123456, wait=wait)

    monkeypatch.setattr(wheels.subprocess, 'Popen', start)
    monkeypatch.setattr(wheels.os, 'killpg', lambda pid, sig: killed.append((pid, sig)))
    error = {'failed': wheels.subprocess.CalledProcessError,
             'timeout': wheels.subprocess.TimeoutExpired, 'interrupted': KeyboardInterrupt}[result]
    with pytest.raises(error):
        wheels.run(['compiler'])
    assert killed == [(123456, wheels.signal.SIGKILL)]
    assert len(waits) == 2


def test_native_build_uses_private_environment_and_audited_platform(tmp_path, monkeypatch):
    """Build settings cannot inherit native CPU flags, tokens or a user's Cargo cache."""
    source = tmp_path / 'source'
    artifacts = tmp_path / 'artifacts'
    (source / 'passivbot-rust').mkdir(parents=True)
    artifacts.mkdir()
    (source / 'passivbot-rust/Cargo.lock').write_text('lock')
    calls = []
    monkeypatch.setenv('GITHUB_TOKEN', 'must-not-reach-builder')
    monkeypatch.setenv('RUSTFLAGS', '-C target-cpu=native')

    def run(argv, **kwargs):
        """Create raw artifacts after the mocked build, recording each invocation."""
        calls.append((argv, kwargs))
        if 'maturin' in argv:
            (tmp_path / 'raw' / RUST_WHEEL).write_bytes(b'wheel')
        if 'wheel' in argv and '-m' in argv:
            (tmp_path / 'raw' / PYTHON_WHEEL).write_bytes(b'wheel')

    monkeypatch.setattr(wheels, 'run', run)
    monkeypatch.setattr(wheels.subprocess, 'check_output', lambda *a, **kw: 'b' * 64)
    wheels.compile_wheels(source, artifacts, {'platform': 'manylinux_2_35_x86_64'},
                          {'PATH': '/usr/bin', 'RUSTUP_HOME': '/existing/toolchain'})
    repairs = [argv for argv, kw in calls if 'auditwheel' in argv]
    assert len(repairs) == 2
    assert all('manylinux_2_35_x86_64' in argv for argv in repairs)
    for argv, kwargs in calls:
        env = kwargs['env']
        assert env['RUSTFLAGS'] == '-C target-cpu=x86-64'
        assert env['CARGO_HOME'] == str(tmp_path / 'cargo')
        assert env['HOME'] == str(tmp_path / 'home')
        assert 'GITHUB_TOKEN' not in env
        assert 'docker' not in argv


def test_master_import_cache_is_bounded(tmp_path, monkeypatch):
    """Repeated received revisions retain only four verified wheel cache entries."""
    bundle = tmp_path / 'bundle'
    manifest = make_bundle(bundle)
    monkeypatch.setattr(wheels, 'probe', lambda: TARGET)
    for index in range(6):
        manifest['source_archive_sha256'] = f'{index:064x}'
        (bundle / 'manifest.json').write_text(json.dumps(manifest))
        wheels.import_bundle(bundle, tmp_path / 'cache')
    entries = [p for p in (tmp_path / 'cache').iterdir() if p.is_dir()]
    assert len(entries) == 4
    assert all(wheels.verify_bundle(p, COMMIT, TARGET) for p in entries)
