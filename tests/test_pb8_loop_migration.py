"""Offline migration of captured Loop bundles, without jobs or production IO."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from fastapi import HTTPException

import pb8_config
import pb8_loop_backend


def legacy_bundle():
    """Return an old disabled HSL input with a reviewed sparse override."""
    return {'config': {'config_version': 'v8.4.0',
                      'bot': {side: {'hsl': {'enabled': False,
                               'restart_after_red_policy': 'threshold',
                               'orange_tier_mode': 'tp_only_with_active_entry_cancellation'}}
                              for side in ('long', 'short')},
                      'coin_overrides': {'BTC': {'override_config_path': 'BTC.json'}},
                      'pbgui': {'note': 'Keep this captured source'}},
            'override_configs': {'BTC.json': {'bot': {'long': {'risk': {'n_positions': 1}}}}}}


@pytest.fixture
def native_migration(monkeypatch):
    """Stub only the native boundary; retain actual temporary bundle IO."""
    calls = []
    monkeypatch.setattr(pb8_config, '_cache_config', lambda *args: None)

    def migrate(path, choices):
        """Read the staged captured input and return its native-shaped result."""
        path = Path(path)
        config = json.loads(path.read_text())
        assert choices == {}
        assert json.loads((path.parent / 'BTC.json').read_text())['bot']['long']['risk']['n_positions'] == 1
        calls.append(path)
        config['config_version'] = 'v8.6.0'
        for side in ('long', 'short'):
            config['bot'][side]['hsl'].pop('orange_tier_mode', None)
            config['bot'][side]['hsl']['restart_after_red_policy'] = None
        return {'config': config}

    monkeypatch.setattr(pb8_config, 'preview_pb8_hsl_migration', migrate)
    return calls


def test_captured_migration_preserves_overrides_source_and_temp_cleanup(native_migration):
    """A fresh native call migrates the copy and preserves its captured override."""
    source = legacy_bundle()
    before = copy.deepcopy(source)
    result = pb8_loop_backend.migrate_loop_bundle(source)
    assert source == before
    assert result['config']['config_version'] == 'v8.6.0'
    assert result['config']['pbgui'] == before['config']['pbgui']
    assert result['override_configs'] == before['override_configs']
    assert set(result) == {'config', 'override_configs'}
    assert all(not path.parent.exists() for path in native_migration)
    # Even an already-normalized snapshot is validated through the native tool.
    assert pb8_loop_backend.migrate_loop_bundle(result) == result
    assert len(native_migration) == 2


@pytest.mark.parametrize('filename', ['../BTC.json', '/tmp/BTC.json', '.BTC.json', 'optimize.json'])
def test_captured_migration_rejects_unsafe_override_filenames(monkeypatch, filename):
    """Persisted bundle references cannot select a file outside their new sandbox."""
    source = legacy_bundle()
    source['config']['coin_overrides']['BTC']['override_config_path'] = filename
    source['override_configs'] = {filename: source['override_configs']['BTC.json']}
    monkeypatch.setattr(pb8_config, 'preview_pb8_hsl_migration', lambda *args: pytest.fail('Native migration must not run'))
    with pytest.raises(HTTPException) as exc:
        pb8_loop_backend.migrate_loop_bundle(source)
    assert exc.value.status_code == 422


@pytest.mark.parametrize('retryable', [False, True])
def test_native_migration_errors_keep_expected_request_status(monkeypatch, retryable):
    """Policy errors are explicit; temporary runtime unavailability remains retryable."""
    monkeypatch.setattr(pb8_config, '_cache_config', lambda *args: None)
    calls = []

    def rejected(path, choices):
        """Reject the copy at the native migration boundary."""
        calls.append(Path(path))
        error = pb8_config.PB8RuntimeBusyError('PB8 update in progress') if retryable else pb8_config.PB8ConfigurationError('HSL requires an explicit choice of always or never')
        raise error

    monkeypatch.setattr(pb8_config, 'preview_pb8_hsl_migration', rejected)
    source = legacy_bundle()
    before = copy.deepcopy(source)
    with pytest.raises(HTTPException if retryable else ValueError) as exc:
        pb8_loop_backend.migrate_loop_bundle(source)
    if retryable:
        assert exc.value.status_code == 503
    else:
        assert 'explicit choice' in str(exc.value)
    assert source == before
    assert not calls[0].parent.exists()


def test_unresolved_editor_draft_never_selects_a_restart_policy(monkeypatch):
    """Save must not turn a nullable migration draft into an authored policy."""
    source = legacy_bundle()
    source['migration_unresolved'] = ['bot.long.hsl.restart_after_red_policy']
    monkeypatch.setattr(pb8_config, 'preview_pb8_hsl_migration', lambda *args: pytest.fail('Do not choose a policy'))
    with pytest.raises(ValueError, match='explicit HSL choice'):
        pb8_loop_backend.migrate_loop_bundle(source)


def test_vast_old_worker_is_rejected_before_runtime_probes_or_launch(tmp_path, monkeypatch):
    """A migrated schema cannot be queued against the known immutable old worker."""
    from api import optimize_v8 as opt
    import vast_jobs
    from pb8_loop_store import LoopStore

    snapshot = legacy_bundle()
    snapshot['config']['config_version'] = 'v8.6.0'
    monkeypatch.setattr(pb8_loop_backend, 'migrate_loop_bundle', lambda value: copy.deepcopy(value))
    monkeypatch.setattr(opt, '_config_file', lambda name: tmp_path / 'source.json')
    monkeypatch.setattr(opt, 'pb8_runtime_status', lambda: pytest.fail('Do not probe or launch a job'))
    monkeypatch.setattr(vast_jobs, 'REVISION', '7b639e1180fa6bfe02e110429d4f931933c73089')
    backend = pb8_loop_backend.LoopBackend(tmp_path, LoopStore(tmp_path / 'loops'))
    with pytest.raises(ValueError, match='Vast.ai worker supports schema v8.4.0'):
        backend.initial('source', {'execution': 'vast'}, snapshot)
