"""Offline regression coverage for bounded monitor software subprocess facts."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import monitor_agent


def namespace(monkeypatch, cache=None, clock=100.0):
    """Load only the embedded script's definitions, never host collection."""
    monkeypatch.setenv('PBGUI_CACHED_SOFTWARE_PROBES', json.dumps(cache or {}))
    script = monitor_agent._embedded_monitor_script('HOST_META_SCRIPT')
    prefix = script.split("<<'PY'\n", 1)[1].split('\ncfg = configparser.ConfigParser()', 1)[0]
    ns = {}
    exec(compile(prefix, '<monitor-software-probes>', 'exec'), ns)
    ns['time'] = SimpleNamespace(monotonic=lambda: clock)
    return ns


def git_tree(tmp_path):
    """Create synthetic ref files without using a real checkout or Git process."""
    root = tmp_path / 'repo' / '.git'
    (root / 'refs' / 'heads').mkdir(parents=True)
    (root / 'HEAD').write_text('ref: refs/heads/main\n')
    (root / 'refs' / 'heads' / 'main').write_text('a' * 40)
    return root


@pytest.mark.parametrize('changed', ['HEAD', 'refs/heads/main', 'packed-refs', 'config', 'shallow', 'info/grafts'])
def test_git_changes_invalidate_before_ttl(monkeypatch, tmp_path, changed):
    """Loose refs, packing, branch/config and shallow history updates stay current."""
    root = git_tree(tmp_path)
    first = namespace(monkeypatch)
    first['run'] = lambda *a, **kw: 'before'
    assert first['git_value'](str(root), ['log']) == 'before'
    cache = first['_next_software_probes']
    second = namespace(monkeypatch, cache, 101.0)
    second['run'] = lambda *a, **kw: pytest.fail('unchanged software must not spawn Git')
    assert second['git_value'](str(root), ['log']) == 'before'
    path = root / changed
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('b' * 40)
    third = namespace(monkeypatch, cache, 102.0)
    third['run'] = lambda *a, **kw: 'after'
    assert third['git_value'](str(root), ['log']) == 'after'


def test_git_shared_worktree_ref_changes(monkeypatch, tmp_path):
    """A linked worktree's shared loose ref is part of the invalidation signature."""
    common = git_tree(tmp_path)
    worktree = tmp_path / 'worktree'
    worktree.mkdir()
    (worktree / 'commondir').write_text(str(common))
    (worktree / 'HEAD').write_text('ref: refs/heads/main\n')
    first = namespace(monkeypatch)
    first['run'] = lambda *a, **kw: 'old'
    assert first['git_value'](str(worktree), ['log']) == 'old'
    (common / 'refs/heads/main').write_text('c' * 40)
    second = namespace(monkeypatch, first['_next_software_probes'], 101)
    second['run'] = lambda *a, **kw: 'new'
    assert second['git_value'](str(worktree), ['log']) == 'new'


@pytest.mark.parametrize('change', ['executable', 'symlink', 'venv_config'])
def test_python_update_invalidates(monkeypatch, tmp_path, change):
    """Executable replacement, symlink retarget and venv changes invalidate facts."""
    venv = tmp_path / 'venv'
    (venv / 'bin').mkdir(parents=True)
    target = tmp_path / 'python-a'
    target.write_text('test executable')
    exe = venv / 'bin/python'
    exe.symlink_to(target)
    first = namespace(monkeypatch)
    first['run'] = lambda *a, **kw: '3.12'
    assert first['python_version'](str(exe)) == '3.12'
    if change == 'executable':
        replacement = tmp_path / 'replacement'
        replacement.write_text('test executable')
        replacement.replace(target)
    elif change == 'symlink':
        new_target = tmp_path / 'python-b'
        new_target.write_text('test executable')
        exe.unlink()
        exe.symlink_to(new_target)
    else:
        (venv / 'pyvenv.cfg').write_text('version = 3.14')
    second = namespace(monkeypatch, first['_next_software_probes'], 101)
    second['run'] = lambda *a, **kw: '3.14'
    assert second['python_version'](str(exe)) == '3.14'


@pytest.mark.parametrize('age', [-1, 59, 60, 1000])
def test_ttl_is_bounded_and_hits_do_not_extend_it(monkeypatch, tmp_path, age):
    """Even unchanged signatures expire; clock rollback never certifies a hit."""
    root = git_tree(tmp_path)
    first = namespace(monkeypatch)
    first['run'] = lambda *a, **kw: 'old'
    first['git_value'](str(root), ['log'])
    second = namespace(monkeypatch, first['_next_software_probes'], 100 + age)
    second['run'] = lambda *a, **kw: 'fresh'
    assert second['git_value'](str(root), ['log']) == ('old' if age == 59 else 'fresh')
    if age == 59:
        assert list(second['_next_software_probes'].values())[0][1] == 100


def test_failure_and_raced_update_are_not_cached(monkeypatch, tmp_path):
    """Transient failures and file updates during subprocess execution retry next time."""
    root = git_tree(tmp_path)
    ns = namespace(monkeypatch)
    ns['run'] = lambda *a, **kw: ''
    assert ns['git_value'](str(root), ['log'], 'unknown') == 'unknown'
    assert ns['_next_software_probes'] == {}

    def update(*args, **kwargs):
        """Simulate a concurrent checkout while Git is running."""
        (root / 'HEAD').write_text('d' * 40)
        return 'raced'

    ns['run'] = update
    assert ns['git_value'](str(root), ['log']) == 'raced'
    assert ns['_next_software_probes'] == {}


def test_probe_work_and_output_parity(monkeypatch, tmp_path):
    """Six 10-second cohorts reduce 18 child calls to three with identical facts."""
    root = git_tree(tmp_path)
    exe = tmp_path / 'python'
    exe.write_text('test executable')
    cache, results, calls = {}, [], []
    for cycle in range(6):
        ns = namespace(monkeypatch, cache, 100 + cycle * 10)

        def probe(argv, **kwargs):
            """Return deterministic non-secret child output and count calls."""
            calls.append(argv)
            return '3.12' if argv[0] == str(exe) else ('main' if 'rev-parse' in argv else 'a' * 40)

        ns['run'] = probe
        results.append((ns['git_value'](str(root), ['log']),
                        ns['git_value'](str(root), ['rev-parse']),
                        ns['python_version'](str(exe))))
        cache = ns['_next_software_probes']
    assert len(calls) == 3
    assert results == [('a' * 40, 'main', '3.12')] * 6


def test_agent_keeps_probe_cache_private_and_dynamic_state_current(monkeypatch, tmp_path):
    """The collector passes RAM facts forward without publishing them or caching live state."""
    monkeypatch.setattr(monitor_agent, '_SOFTWARE_PROBE_CACHE', {})
    monkeypatch.setattr(monitor_agent, '_LEGACY_CRON_CACHE', None)
    monkeypatch.setattr(monitor_agent, 'DATA_DIR', tmp_path)
    monkeypatch.setattr(monitor_agent, '_embedded_monitor_script', lambda _: 'script')
    monkeypatch.setattr(monitor_agent, '_script_env', lambda extra: extra)
    monkeypatch.setattr(monitor_agent, '_local_credential_capability', lambda: {'credential_active': True})
    environments = []

    def collect(script, env, timeout):
        """Return varying live metadata and the script-owned private cache."""
        environments.append(env)
        return {'boot': len(environments), 'pb8blocked': len(environments) == 2,
                '_software_probe_cache': {'test': ['signature', 100.0, '3.12']}}

    monkeypatch.setattr(monitor_agent, '_run_shell_script', collect)
    for cycle in (1, 2):
        monitor_agent._run_host_meta()
        payload = json.loads((tmp_path / 'host_meta.json').read_text())
        assert '_software_probe_cache' not in payload
        assert payload['boot'] == cycle
        assert payload['pb8blocked'] == (cycle == 2)
        assert payload['credential_active'] is True
    assert json.loads(environments[0]['PBGUI_CACHED_SOFTWARE_PROBES']) == {}
    assert json.loads(environments[1]['PBGUI_CACHED_SOFTWARE_PROBES'])['test'][2] == '3.12'


def test_probe_cache_has_a_hard_entry_limit(monkeypatch, tmp_path):
    """Even unexpected extra probe keys cannot grow the cross-cohort RAM payload."""
    source = tmp_path / 'source'
    source.write_text('fact')
    ns = namespace(monkeypatch)
    for index in range(100):
        assert ns['cached_software_probe'](str(index), [source], lambda: '3.12') == '3.12'
    assert len(ns['_next_software_probes']) == 32


@pytest.mark.parametrize('entry', [None, [], ['x', 'invalid', 'value'], ['x', 100.0, {}]])
def test_malformed_cache_entries_are_not_certified(monkeypatch, tmp_path, entry):
    """Bad optional cache state cannot replace a current subprocess observation."""
    source = tmp_path / 'source'
    source.write_text('fact')
    ns = namespace(monkeypatch, {'key': entry})
    assert ns['cached_software_probe']('key', [source], lambda: 'fresh') == 'fresh'
    assert ns['_next_software_probes']['key'][2] == 'fresh'
