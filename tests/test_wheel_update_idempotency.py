"""Content-based PB7/PB8/PB9 update gates use isolated wheel/venv fixtures."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import venv
import zipfile

import pytest
import yaml

import pb_wheel_status as status
from setup.installer import core

COMMIT = 'a' * 40
FINGERPRINT = 'b' * 64


@pytest.fixture
def installed_bundle(tmp_path, monkeypatch):
    """Construct real wheel archives and site files without running bot code."""
    def make(product, requires=''):
        root = tmp_path / product
        root.mkdir()
        environment = root / 'venv'
        venv.EnvBuilder(with_pip=False).create(environment)
        python = environment / 'bin/python'
        site = environment / 'lib/python3.12/site-packages'
        site.mkdir(parents=True, exist_ok=True)
        bundle = root / 'bundle'
        bundle.mkdir()
        contents = {
            'passivbot-1.whl': {'procedures.py': 'raise RuntimeError("must not execute")\n',
                               'passivbot-1.dist-info/METADATA': 'Metadata-Version: 2.1\nName: passivbot\nVersion: 1\n' + requires,
                               'passivbot-1.dist-info/RECORD': 'wheel-owned RECORD'},
        }
        binary_name = ('passivbot_rust.cpython-312-x86_64-linux-gnu.so' if product == 'pb9'
                       else 'passivbot_rust/passivbot_rust.abi3.so')
        if product == 'pb9':
            contents['passivbot-1.whl'][binary_name] = 'binary'
        else:
            contents['passivbot-1.whl']['passivbot_rust.cpython-312-x86_64-linux-gnu.so'] = 'legacy-shadow'
            contents['passivbot_rust-1.whl'] = {binary_name: 'binary',
                'passivbot_rust-1.dist-info/METADATA': 'Metadata-Version: 2.1\nName: passivbot_rust\nVersion: 1\n'}
        artifacts = {}
        for name, members in contents.items():
            with zipfile.ZipFile(bundle / name, 'w') as archive:
                for member, value in members.items():
                    archive.writestr(member, value)
                    if product != 'pb9' and '/' not in member and member.endswith('.so'):
                        continue  # The real installer prunes this shadow binary.
                    path = site / member
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(value if not member.endswith('/RECORD') else 'pip-rewritten RECORD')
            artifacts[name] = hashlib.sha256((bundle / name).read_bytes()).hexdigest()
        (bundle / 'Cargo.lock').write_text('locked\n')
        artifacts['Cargo.lock'] = hashlib.sha256((bundle / 'Cargo.lock').read_bytes()).hexdigest()
        (bundle / 'manifest.json').write_text(json.dumps({'format': 1, 'product': product,
            'commit': COMMIT, 'source_fingerprint': FINGERPRINT, 'artifacts': artifacts}))
        binary = site / binary_name
        binary.with_name(binary.name + '.rust-src-sha256').write_text(FINGERPRINT)
        cli = python.parent / 'passivbot'
        cli.write_text('#!/bin/sh\nexit 99\n')
        cli.chmod(0o700)
        repo = root / 'repo'
        (repo / 'passivbot-rust').mkdir(parents=True)
        (repo / 'passivbot-rust/Cargo.lock').write_text('locked\n')
        pbgdir = root / 'pbgui'
        pbgdir.mkdir()
        (pbgdir / 'pbgui.ini').write_text(f'[main]\n{product}dir={repo}\n{product}venv={python}\n')
        real_output = status.subprocess.check_output
        def output(argv, **kwargs):
            """Use the real isolated interpreter; simulate only fixture Git metadata."""
            if argv[0] == 'git':
                return '' if 'status' in argv else COMMIT
            return real_output(argv, **kwargs)
        monkeypatch.setattr(status.subprocess, 'check_output', output)
        return pbgdir, product, repo, python, bundle, COMMIT
    return make


@pytest.mark.parametrize('product', ['pb7', 'pb8', 'pb9'])
def test_identical_wheels_need_no_receipt_or_bot_execution(installed_bundle, product):
    """A missing legacy receipt is not a reason to force reinstall identical files."""
    args = installed_bundle(product)
    assert status.matching_wheel_installation(*args) == {'wheels_match': True, 'runtime_current': True}
    # Source/binary marker fixtures would fail immediately if imported or executed.
    assert not (args[0] / 'data/state').exists()


@pytest.mark.parametrize('product', ['pb7', 'pb8', 'pb9'])
@pytest.mark.parametrize('part', ['python', 'rust', 'metadata'])
def test_changed_wheel_contents_require_installation(installed_bundle, product, part):
    """Same package versions do not hide changed Python, Rust or wheel metadata."""
    args = installed_bundle(product)
    site = args[3].parent.parent / 'lib/python3.12/site-packages'
    target = {'python': 'procedures.py', 'rust': ('passivbot_rust.cpython-312-x86_64-linux-gnu.so'
              if product == 'pb9' else 'passivbot_rust/passivbot_rust.abi3.so'),
              'metadata': 'passivbot-1.dist-info/METADATA'}[part]
    (site / target).write_text('changed')
    assert status.matching_wheel_installation(*args) == {'wheels_match': False, 'runtime_current': False}


@pytest.mark.parametrize('fault', ['commit', 'dirty', 'invalid_marker', 'stamp', 'cli', 'path', 'shadow'])
def test_incomplete_runtime_is_not_silently_declared_current(installed_bundle, monkeypatch, fault):
    """Identical artifacts cannot bypass checkout, provenance or launch-path validation."""
    args = installed_bundle('pb9')
    pbgdir, _, repo, python, _, _ = args
    site = python.parent.parent / 'lib/python3.12/site-packages'
    if fault in {'commit', 'dirty'}:
        real = status.subprocess.check_output
        monkeypatch.setattr(status.subprocess, 'check_output', lambda argv, **kw:
            (' M src.py' if fault == 'dirty' and 'status' in argv else 'c' * 40)
            if argv[0] == 'git' else real(argv, **kw))
    elif fault == 'invalid_marker':
        (pbgdir / 'data/locks').mkdir(parents=True)
        (pbgdir / 'data/locks/pb9-runtime-invalid').touch()
    elif fault == 'stamp':
        (site / 'passivbot_rust.cpython-312-x86_64-linux-gnu.so.rust-src-sha256').write_text('old')
    elif fault == 'cli':
        (python.parent / 'passivbot').unlink()
    elif fault == 'path':
        (pbgdir / 'pbgui.ini').write_text('[main]\n')
    else:
        (site / 'passivbot_rust.abi3.so').write_text('shadow')
    assert not status.matching_wheel_installation(*args)['runtime_current']


def test_missing_full_dependencies_does_not_replace_matching_wheels(installed_bundle):
    """Changing to full research dependencies must not force reinstall the bot wheel."""
    args = installed_bundle('pb8', 'Requires-Dist: definitely-missing-test-package>=1; extra == "full"\n')
    assert status.matching_wheel_installation(*args)['runtime_current']
    assert status.matching_wheel_installation(*args, extras='full') == {
        'wheels_match': True, 'runtime_current': False}


@pytest.mark.parametrize('fault', ['checksum', 'symlink', 'traversal'])
def test_unverified_or_unsafe_wheels_cannot_skip_installation(installed_bundle, fault):
    """Comparison rejects tampered bundles and symlinked installed source files."""
    args = installed_bundle('pb7')
    if fault == 'checksum':
        (args[4] / 'passivbot-1.whl').write_bytes(b'tampered')
        with pytest.raises(ValueError, match='unverified'):
            status.matching_wheel_installation(*args)
    elif fault == 'symlink':
        file = args[3].parent.parent / 'lib/python3.12/site-packages/procedures.py'
        file.unlink()
        file.symlink_to(args[4] / 'Cargo.lock')
        assert not status.matching_wheel_installation(*args)['wheels_match']
    else:
        wheel = args[4] / 'passivbot-1.whl'
        with zipfile.ZipFile(wheel, 'a') as archive:
            archive.writestr('../escape.py', 'bad')
        manifest = json.loads((args[4] / 'manifest.json').read_text())
        manifest['artifacts'][wheel.name] = hashlib.sha256(wheel.read_bytes()).hexdigest()
        (args[4] / 'manifest.json').write_text(json.dumps(manifest))
        assert not status.matching_wheel_installation(*args)['wheels_match']


@pytest.mark.parametrize('filename', ['master-update-pb7.yml', 'vps-update-pb7.yml',
                                     'master-update-pb8.yml', 'vps-update-pb8.yml'])
def test_update_tasks_gate_install_and_restart_before_any_runtime_changes(filename):
    """Normal play completion retains cleanup and the next imported runtime's update."""
    play = yaml.safe_load(Path(filename).read_text())[0]
    tasks = play['tasks']
    product = 'pb7' if 'pb7' in filename else 'pb8'
    preparation = next(i for i, t in enumerate(tasks) if str(t.get('include_tasks', '')).endswith('_wheels_prepare.yml'))
    for task in tasks[preparation + 1:]:
        assert product + '_wheel_match.' in str(task.get('when', '')), task['name']
    assert not any(t.get('meta') == 'end_host' for t in tasks)
    assert any('Clean' in h['name'] for h in play['handlers'])


@pytest.mark.parametrize('product', ['pb7', 'pb8', 'pb9'])
def test_local_installer_skips_all_install_commands_for_matching_runtime(installed_bundle, monkeypatch, product):
    """Local maintenance uses the same artifact gate and does not invoke pip or a bot CLI."""
    args = installed_bundle(product)
    pbgdir, _, repo, python, bundle, commit = args
    monkeypatch.setattr(core, 'pb8_gpu_extras', lambda _repo: '')
    commands = []
    monkeypatch.setattr(core, '_run_command', lambda *a, **kw: commands.append(a))
    core._install_master_wheel(product, repo, python.parent.parent, pbgdir, commit, bundle, {}, lambda _message: None)
    assert commands == []


@pytest.mark.parametrize('state', ['unchanged', 'changed', 'dependencies_only'])
def test_real_ansible_conditions_skip_matching_wheels_and_continue_next_runtime(tmp_path, state):
    """Run real update conditions with debug-only actions, never real service/install commands."""
    executable = Path(sys.executable).parent / 'ansible-playbook'
    if not executable.is_file():
        pytest.skip('Ansible is not installed in this test environment')
    imports = []
    for product in ('pb7', 'pb8', 'pb9'):
        prefix = 'pb7' if product == 'pb7' else 'pb8'
        play = yaml.safe_load(Path(f'vps-update-{prefix}.yml').read_text())[0]
        task_source = play['tasks']
        if prefix == 'pb7':
            task_source += yaml.safe_load(Path('tasks/pb7_wheels_install.yml').read_text())
        selected = [t for t in task_source if t['name'].startswith(('Stop PBRun', 'Install verified', 'Restart managed'))]
        actions = []
        for task in selected:
            action = {'name': task['name'], 'debug': {'msg': 'EXECUTED_' + product + '_' + task['name']},
                      'changed_when': True, 'when': task['when']}
            if task['name'].startswith('Stop PBRun'):
                action['notify'] = 'Fixture PBRun recovery'
            actions.append(action)
        actions.append({'name': 'Next runtime stays reachable', 'debug': {'msg': 'REACHED_' + product}})
        spec = [{'hosts': 'localhost', 'gather_facts': False, 'vars': {
            prefix + '_wheel_match': {'runtime_current': state == 'unchanged',
                                     'wheels_match': state != 'changed'},
            'passivbot_label': product.upper(), 'pb8_branch_switch': False,
            'pb8_git': {'stat': {'exists': True}},
        }, 'tasks': actions, 'handlers': [{'name': 'Fixture PBRun recovery',
                                          'debug': {'msg': 'RECOVERED_' + product}}]}]
        path = tmp_path / (product + '.yml')
        path.write_text(yaml.safe_dump(spec, sort_keys=False))
        imports.append({'import_playbook': str(path)})
    wrapper = tmp_path / 'update.yml'
    wrapper.write_text(yaml.safe_dump(imports))
    import os
    env = dict(os.environ, ANSIBLE_LOCAL_TEMP=str(tmp_path / 'ansible'), ANSIBLE_NOCOLOR='1')
    completed = subprocess.run([str(executable), '-i', 'localhost,', '-c', 'local', str(wrapper)],
                               env=env, capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stdout[-5000:] + completed.stderr[-2000:]
    for product in ('pb7', 'pb8', 'pb9'):
        assert f'REACHED_{product}' in completed.stdout
    if state == 'unchanged':
        assert 'EXECUTED_' not in completed.stdout
        assert 'RECOVERED_' not in completed.stdout
    elif state == 'dependencies_only':
        assert 'EXECUTED_' in completed.stdout
        assert not any('"msg": "EXECUTED_' in line and 'Install verified' in line
                       for line in completed.stdout.splitlines())
    else:
        for product in ('pb7', 'pb8', 'pb9'):
            assert any('"msg": "EXECUTED_' + product + '_' in line and 'Install verified' in line
                       for line in completed.stdout.splitlines())


def test_local_dependency_repair_keeps_identical_bot_wheels(installed_bundle, monkeypatch):
    """Missing research extras install dependencies without forcing the same bot wheels again."""
    args = installed_bundle('pb8', 'Requires-Dist: definitely-missing-test-package>=1; extra == "full"\n')
    pbgdir, product, repo, python, bundle, commit = args
    monkeypatch.setattr(core, 'pb8_gpu_extras', lambda _repo: 'full')
    commands = []
    monkeypatch.setattr(core, '_run_command', lambda argv, *_a, **_kw: commands.append([str(x) for x in argv]))
    manifest = {'python_wheel': 'passivbot-1.whl', 'rust_wheel': 'passivbot_rust-1.whl'}
    core._install_master_wheel(product, repo, python.parent.parent, pbgdir, commit, bundle, manifest, lambda _m: None)
    assert any('install' in command for command in commands)
    assert not any('--force-reinstall' in command for command in commands)
    assert any('runtime' in command for command in commands)


@pytest.mark.parametrize('filename', ['vps-update-pb8.yml', 'master-update-pb8.yml'])
@pytest.mark.parametrize('product', ['pb8', 'pb9'])
def test_entire_unchanged_update_skips_before_reading_missing_task_results(tmp_path, filename, product):
    """A skipped stat/probe leaves no result fields; every dependent action must skip first."""
    executable = Path(sys.executable).parent / 'ansible-playbook'
    if not executable.is_file():
        pytest.skip('Ansible is not installed in this test environment')
    play = yaml.safe_load(Path(filename).read_text())[0]
    start = next(i for i, task in enumerate(play['tasks'])
                 if task.get('include_tasks') == 'tasks/pb8_live_wheels_prepare.yml')
    actions = []
    for task in play['tasks'][start + 1:]:
        action = {'name': task['name'], 'debug': {'msg': 'UNEXPECTED_EXECUTION'},
                  'when': task['when']}
        if 'register' in task:
            action['register'] = task['register']
        actions.append(action)
    actions.append({'name': 'Next imported runtime remains reachable', 'debug': {'msg': 'NEXT_RUNTIME'}})
    spec = [{'hosts': 'localhost', 'gather_facts': False, 'vars': {
        'passivbot_label': product.upper(), 'pb8_branch_switch': False,
        'pb8_wheel_match': {'runtime_current': True, 'wheels_match': True},
    }, 'tasks': actions}]
    path = tmp_path / 'unchanged.yml'
    path.write_text(yaml.safe_dump(spec, sort_keys=False))
    import os
    env = dict(os.environ, ANSIBLE_LOCAL_TEMP=str(tmp_path / 'ansible'), ANSIBLE_NOCOLOR='1')
    completed = subprocess.run([str(executable), '-i', 'localhost,', '-c', 'local', str(path)],
                               env=env, capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stdout[-5000:] + completed.stderr[-2000:]
    assert 'UNEXPECTED_EXECUTION' not in completed.stdout
    assert 'NEXT_RUNTIME' in completed.stdout
