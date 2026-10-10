"""Isolated export/rental/claim regression tests; no provider or runtime I/O."""
import copy
import json
from pathlib import Path

import pytest
from starlette.responses import Response

import vast_queue
from secure_files import ensure_private_directory
from vast_jobs import JobStore, IMAGE, REVISION, SUPPORTED_RENTAL_IMAGE_REVISIONS, digest, native_job_config, write_json
from vast_provider import VastError
from vast_queue import CloudQueue, preflight_gpu_snapshots, worker_step
from vast_direction_fixtures import direction_config


def unstable_config():
    """A fully specified activatable Short search space with an inactive seed."""
    config = direction_config()
    config['live']['approved_coins']['short'] = ['BTC']
    config['optimize']['bounds'].update(short_total_wallet_exposure_limit=[0, 1], short_n_positions=[0, 1])
    return config


def prepared(store, config, *, kind='job'):
    """Persist a complete frozen synthetic snapshot and immutable bundle."""
    row = store.create_preparation('synthetic', 512, 4, False)
    directory = store.directory(row['id'])
    folder = ensure_private_directory(directory / 'input')
    write_json(folder / 'optimize.json', config)
    write_json(folder / 'manifest.json', {'config_sha256': digest(folder / 'optimize.json'), 'files': []})
    archive = directory / 'input.tar.gz'
    archive.write_bytes(b'synthetic input archive')
    archive.chmod(0o600)
    write_json(directory / 'intent.json', dict(id=row['id'], image=IMAGE, pb8_revision=REVISION,
        source_config_sha256='synthetic-source', bundle_sha256=digest(archive), bundle_bytes=archive.stat().st_size,
        results_root=str(store.root / 'results')))
    return store.update(row['id'], status='ready', kind=kind, exchanges=['binance'], input_bytes=1,
        input_progress={'stage': 'complete', 'files_completed': 0, 'files_total': 0, 'bytes_completed': 0, 'bytes_total': 0})


@pytest.fixture
def queue(tmp_path, monkeypatch):
    """Make provider and metadata boundaries explicit mocks."""
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    queue.calls = []
    monkeypatch.setattr(vast_queue, 'preflight_local_metadata',
        lambda store, jobs, *args: queue.calls.append(('metadata', [row['id'] for row in jobs])))
    monkeypatch.setattr(queue.store, 'start',
        lambda identifier, *args: queue.calls.append(('rent', identifier)) or queue.store.read(identifier))
    return queue


def worker(queue, image=IMAGE, revision=REVISION):
    """Create an active paid lease with a valid synthetic transfer budget."""
    identifier = 'a' * 32
    directory = ensure_private_directory(queue.root / 'jobs' / identifier)
    write_json(directory / 'state.json', dict(id=identifier, kind='worker', status='running', rental_state='active',
        workers=4, deadline=10000, created_at=0, idle_seconds=-1))
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    write_json(directory / 'intent.json', dict(image=image, pb8_revision=revision, transfer_reserve_usd=1,
        offer={'download_gb_usd': .01, 'upload_gb_usd': .01}))
    queue.update(worker_id=identifier)
    return identifier


def test_editor_and_export_reject_without_changing_source():
    """The unsaved editor reports the same actionable error as export."""
    from api.vast import ValidateConfigRequest, validate_config
    config = unstable_config()
    original = copy.deepcopy(config)
    response = Response()
    result = validate_config(ValidateConfigRequest(config=config), response, None)
    assert not result['valid'] and result['warnings'] == []
    error = result['errors'][0]
    assert error['path'] == 'bot.short.risk.total_wallet_exposure_limit'
    assert 'Short must stay on or off' in error['message']
    assert 'Long-only' in error['suggestions'][0] and 'Long + Short' in error['suggestions'][1]
    assert response.headers['Cache-Control'] == 'no-store'
    with pytest.raises(VastError, match='Short must stay on or off'):
        native_job_config(config, 512, 4, False)
    assert config == original


def test_collapsed_issue_countercase_remains_exportable():
    """Positive scenario exposure alone is not proof of the historical error."""
    config = direction_config()
    config['live']['approved_coins']['short'] = ['BTC']
    config['optimize']['bounds']['short_n_positions'] = [0, 1]
    config['backtest'].update(suite_enabled=True, scenarios=[
        {'label': f'exposure-{value}', 'overrides': {'bot.short.risk.total_wallet_exposure_limit': value}}
        for value in (.5, 1, 1.5, 2)])
    assert native_job_config(config, 512, 4, False)['optimize']['backend'] == 'gpu'


def test_invalid_queue_job_never_reaches_metadata_or_rental(queue):
    """Saved Altjobs are filtered before either metadata preflight."""
    row = prepared(queue.store, unstable_config())
    with pytest.raises(VastError, match='Queue a cloud'):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert queue.calls == []
    assert queue.store.read(row['id'])['status'] == 'failed'
    assert 'Short must stay on or off' in queue.store.read(row['id'])['error']


def test_invalid_job_does_not_block_valid_queue_candidates(queue):
    """Only eligible jobs reach both preflights and the single rental."""
    bad = prepared(queue.store, unstable_config())
    good = prepared(queue.store, direction_config())
    queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert queue.calls[:2] == [('metadata', [good['id']]), ('metadata', [good['id']])]
    assert queue.calls[2][0] == 'rent'
    assert queue.store.read(bad['id'])['status'] == 'failed'


@pytest.mark.parametrize('snapshot', [None, {}, {'unreadable': True}])
def test_uncheckable_input_is_deferred_not_failed(queue, snapshot):
    """Missing/incomplete inputs do not permanently mark a ready job failed."""
    row = prepared(queue.store, direction_config())
    path = queue.store.directory(row['id']) / 'input/optimize.json'
    if snapshot is None:
        path.unlink()
    else:
        write_json(path, snapshot)
    with pytest.raises(VastError):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    state = queue.store.read(row['id'])
    assert state['status'] == 'ready' and 'cannot be checked' in state['error']
    assert not queue.calls


@pytest.mark.parametrize('invalid', [False, True])
def test_changed_snapshot_cannot_reuse_first_preflight(queue, monkeypatch, invalid):
    """A changed input aborts before the second metadata check or rental."""
    row = prepared(queue.store, direction_config())
    def change(store, jobs, *args):
        """Simulate a concurrent edit while first-candle preparation is unlocked."""
        queue.calls.append(('metadata', [item['id'] for item in jobs]))
        config = unstable_config() if invalid else direction_config()
        config['optimize']['iters'] = 1024
        write_json(store.directory(row['id']) / 'input/optimize.json', config)
    monkeypatch.setattr(vast_queue, 'preflight_local_metadata', change)
    with pytest.raises(VastError, match='changed during local metadata'):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert queue.calls == [('metadata', [row['id']])]


@pytest.mark.parametrize('calibration', [False, True])
def test_current_claim_filters_incompatible_snapshots(queue, calibration):
    """Neither optimizer nor calibration is dispatched to the current worker."""
    row = prepared(queue.store, unstable_config(), kind='calibration' if calibration else 'job')
    identifier = worker(queue)
    if calibration:
        queue.store.update(identifier, calibration_job_id=row['id'])
    assert worker_step(queue, identifier, now=100) is None
    assert queue.store.read(row['id'])['status'] == 'failed'
    assert not queue.store.read(identifier).get('active_job')


@pytest.mark.parametrize('image,revision', [(i, r) for i, r in SUPPORTED_RENTAL_IMAGE_REVISIONS.items() if i != IMAGE])
def test_supported_older_claims_preserve_behavior(queue, image, revision, monkeypatch):
    """Older paid workers are deliberately outside the new current-pin check."""
    row = prepared(queue.store, unstable_config())
    identifier = worker(queue, image, revision)
    monkeypatch.setattr(vast_queue, 'preflight_gpu_snapshots', lambda *args, **kwargs: pytest.fail('Old lease was revalidated'))
    assert worker_step(queue, identifier, now=100) == row['id']


@pytest.mark.parametrize('image,revision', [(IMAGE, 'bad'), ('bad', REVISION), (None, None)])
def test_invalid_image_revision_pairs_are_not_exempt(queue, image, revision):
    """Both components must match a supported intent before new claims."""
    row = prepared(queue.store, direction_config())
    identifier = worker(queue, image, revision)
    with pytest.raises(VastError, match='Unsupported worker'):
        worker_step(queue, identifier, now=100)
    assert queue.store.read(row['id'])['status'] == 'ready'


def test_running_job_is_not_revalidated(queue, monkeypatch):
    """A job already dispatched on a lease continues unchanged."""
    row = prepared(queue.store, unstable_config())
    identifier = worker(queue)
    queue.store.update(row['id'], status='running', lease_id=identifier)
    queue.store.update(identifier, active_job=row['id'])
    monkeypatch.setattr(vast_queue, 'preflight_gpu_snapshots', lambda *args, **kwargs: pytest.fail('Running job was checked'))
    assert worker_step(queue, identifier, now=100) == row['id']


def test_calibration_start_checks_before_metadata(queue):
    """The calibration_id branch cannot bypass the pre-rental gate."""
    row = prepared(queue.store, unstable_config(), kind='calibration')
    with pytest.raises(VastError, match='Short must stay on or off'):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 0, calibration_id=row['id'])
    assert queue.calls == []


def test_create_calibration_checks_source_before_linking(queue):
    """An invalid saved job never creates a calibration input copy."""
    from vast_calibration import configurable_calibration_plan
    source = prepared(queue.store, unstable_config())
    before = {row['id'] for row in queue.store.list()}
    with pytest.raises(VastError, match='Short must stay on or off'):
        queue.store.create_calibration(source['id'], configurable_calibration_plan(4096, 512, 8192, .03))
    assert {row['id'] for row in queue.store.list()} == before


def test_requeue_clone_is_filtered_before_rental(queue, monkeypatch):
    """clone_prepared stays unchanged; its ready result is checked at start."""
    source = prepared(queue.store, unstable_config())
    queue.store.update(source['id'], status='failed')
    monkeypatch.setattr('pb8_config.load_pb8_config', lambda path: {'optimize': {'iters': 512, 'n_cpus': 4}})
    clone = queue.store.clone_prepared(source['id'], 'synthetic', 'synthetic-source', queue.root / 'results', 512, 4, False)
    assert clone and clone['status'] == 'ready'
    with pytest.raises(VastError):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert queue.store.read(clone['id'])['status'] == 'failed'
    assert queue.calls == []


def test_unknown_coin_count_hint_does_not_block_rental(queue):
    """Prepared dataset uncertainty is not a new hard configuration error."""
    config = direction_config()
    config['live']['approved_coins']['long'] = ['BTC', 'ETH', 'SOL']
    config['optimize']['bounds']['long_n_positions'] = [1, 3, 1]
    prepared(queue.store, config)
    queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert queue.calls[-1][0] == 'rent'


@pytest.mark.parametrize('key,value', [('_fine_tune_anchor_plan', {'anchors': [{}]}), ('enable_overrides', ['mirror_short_from_long'])])
def test_unsupported_current_features_are_blocked_in_saved_inputs(queue, key, value):
    """Old ready snapshots cannot bypass the current Cloud feature restrictions."""
    config = direction_config()
    (config if key.startswith('_') else config['optimize'])[key] = value
    row = prepared(queue.store, config)
    assert preflight_gpu_snapshots(queue.store, [row]) == []
    assert queue.store.read(row['id'])['status'] == 'failed'


@pytest.mark.parametrize('preset', ['canonical', 'small', 'medium', 'large'])
def test_generated_calibrations_keep_disabled_short_valid(preset):
    """Generated inputs, rather than templates, receive the direction contract."""
    from vast_calibration import canonical_calibration_config, preset_calibration_config
    from vast_direction_validation import validate_gpu_directions
    template = direction_config()
    template['optimize']['gpu'] = {}
    metadata = {
        'strategies': ['ema_anchor'],
        'strategy_defaults': {side: {'ema_anchor': {}} for side in ('long', 'short')},
        'active_bounds': {'ema_anchor': {
            'long': {'risk': {'total_wallet_exposure_limit': [1, 1]}},
            'short': {'risk': {'total_wallet_exposure_limit': [0, 0]}},
        }},
    }
    config = (canonical_calibration_config(template, optimize_metadata=metadata) if preset == 'canonical'
              else preset_calibration_config(template, preset, optimize_metadata=metadata))
    assert config['optimize']['fixed_params'] == []
    assert config['optimize']['fixed_runtime_overrides'] == {}
    errors, hints = validate_gpu_directions(config)
    assert not errors
    assert hints  # The actual prepared count is intentionally not certified locally.
    assert native_job_config(config, 512, 4, False)


@pytest.mark.parametrize('invalid', [False, True])
def test_second_metadata_preflight_cannot_change_approved_input(queue, monkeypatch, invalid):
    """Recheck immediately before creating the rental record, including calibration."""
    row = prepared(queue.store, direction_config())
    def change_on_second(store, jobs, *args):
        """Change a gate inside the last metadata preparation stage."""
        queue.calls.append(('metadata', [item['id'] for item in jobs]))
        if len(queue.calls) == 2:
            config = unstable_config() if invalid else direction_config()
            config['optimize']['iters'] = 1024
            write_json(store.directory(row['id']) / 'input/optimize.json', config)
    monkeypatch.setattr(vast_queue, 'preflight_local_metadata', change_on_second)
    with pytest.raises(VastError, match='changed during local metadata'):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert len(queue.calls) == 2
    assert not queue.workers()


@pytest.mark.parametrize('value', ['1', float('nan'), float('inf')])
def test_definitely_invalid_runtime_gate_is_failed_not_deferred(queue, value):
    """Invalid numeric pins are proven errors rather than missing input information."""
    config = direction_config()
    config['optimize']['fixed_runtime_overrides']['bot.short.risk.n_positions'] = value
    row = prepared(queue.store, direction_config())
    # Deliberately corrupt isolated persisted input; production writers reject NaN.
    (queue.store.directory(row['id']) / 'input/optimize.json').write_text(json.dumps(config))
    assert preflight_gpu_snapshots(queue.store, [row]) == []
    assert queue.store.read(row['id'])['status'] == 'failed'


@pytest.mark.parametrize('bounds', [[], [2, 1], [0, 1, 1, 1]])
def test_malformed_frozen_bound_cannot_crash_or_rent(queue, bounds):
    """A malformed old snapshot is handled before metadata or a provider call."""
    config = direction_config()
    config['optimize']['bounds']['short_n_positions'] = bounds
    row = prepared(queue.store, config)
    assert preflight_gpu_snapshots(queue.store, [row]) == []
    assert queue.store.read(row['id'])['status'] == 'failed'


def test_checked_config_must_match_the_frozen_manifest(queue):
    """A stable edited local config cannot validate a different immutable bundle."""
    row = prepared(queue.store, unstable_config())
    write_json(queue.store.directory(row['id']) / 'input/optimize.json', direction_config())
    with pytest.raises(VastError):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)
    assert queue.calls == []
    state = queue.store.read(row['id'])
    assert state['status'] == 'ready'
    assert 'Frozen optimizer input changed' in state['error']


def test_snapshot_symlink_is_deferred_without_reading_target(queue, tmp_path):
    """No frozen optimizer input can redirect the bounded reader outside the store."""
    row = prepared(queue.store, direction_config())
    target = tmp_path / 'outside.json'
    write_json(target, direction_config())
    path = queue.store.directory(row['id']) / 'input/optimize.json'
    path.unlink()
    path.symlink_to(target)
    assert preflight_gpu_snapshots(queue.store, [row]) == []
    assert queue.store.read(row['id'])['status'] == 'ready'


def test_reusing_older_rental_does_not_apply_current_direction_rule(queue, monkeypatch):
    """start() returning an existing old worker preserves that worker's pending jobs."""
    row = prepared(queue.store, unstable_config())
    image, revision = next((i, r) for i, r in SUPPORTED_RENTAL_IMAGE_REVISIONS.items() if i != IMAGE)
    identifier = worker(queue, image, revision)
    monkeypatch.setattr(vast_queue, 'preflight_gpu_snapshots', lambda *args, **kwargs: pytest.fail('Older rental was checked'))
    assert queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300)['id'] == identifier
    assert queue.store.read(row['id'])['status'] == 'ready'
    assert queue.calls == [('metadata', [row['id']])]
