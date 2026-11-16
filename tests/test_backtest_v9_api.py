"""PB9 native queue, immutable provenance and artifact boundaries with temporary data."""
import copy
import datetime as dt
import hashlib
import json
from pathlib import Path
import time
import uuid

import numpy as np
import pytest
from fastapi import HTTPException

from api import backtest_v9 as bt
from pb9_config import PB9CancelSignal


@pytest.fixture
def native(tmp_path, monkeypatch):
    """Replace only native execution; all file/lock boundaries use private tmp_path data."""
    metadata = json.loads((Path(__file__).parent / 'fixtures/pb9_run_metadata.json').read_text())
    source = copy.deepcopy(metadata['template'])
    status = {'detected':True, 'commit':'a'*40, 'config_schema':'v9.0.0', 'package_version':'8.2.0.dev0'}
    monkeypatch.setattr(bt,'PBGDIR',tmp_path)
    from api import backtest_v7
    monkeypatch.setattr(backtest_v7, '_archive_retests_dir', lambda: tmp_path/'archive-retest-state')
    monkeypatch.setattr(bt,'_pbgui_data_path',lambda:str(tmp_path/'data/ohlcv'))
    import logging_helpers
    monkeypatch.setattr(logging_helpers,'LOG_ROOT',tmp_path/'data/logs')
    import backtest_autostart
    monkeypatch.setattr(backtest_autostart,'PBGDIR',tmp_path)
    monkeypatch.setattr(backtest_autostart,'_managed_processes',lambda: {})
    import backtest_result_index
    monkeypatch.setattr(backtest_result_index,'PBGDIR',tmp_path)
    monkeypatch.setattr(bt,'_runtime',lambda cancel=None:(copy.deepcopy(status),{'pb9dir':str(tmp_path/'source'),'pb9venv':str(tmp_path/'env/bin/python')}))
    monkeypatch.setattr(bt,'_paths',lambda:{'pb9dir':str(tmp_path/'source'),'pb9venv':str(tmp_path/'env/bin/python')})
    def validate(config, *args, **kwargs):
        """Require native identity while retaining the caller's exact sparse values."""
        if config.get('config_version') != 'v9.0.0':
            raise HTTPException(422,'Expected v9 config')
        return {'source':status}
    monkeypatch.setattr(bt,'validate_pb9_backtest_config',validate)
    import pb9_config_storage
    def document(config,*args,**kwargs):
        """Exercise the explicit shared config gateway without starting a helper."""
        assert kwargs.get('validation_target') == 'backtest'
        validate(config)
        return {'config':copy.deepcopy(config),'pbgui':None,'source':status}
    monkeypatch.setattr(pb9_config_storage,'validate_pb9_document',document)
    monkeypatch.setattr(bt,'load_ini_section',lambda section:{})
    monkeypatch.setattr(bt,'save_ini_section',lambda *args,**kwargs:pytest.fail('Unexpected settings write'))
    bt._drafts.clear()
    yield source,status,tmp_path
    bt._drafts.clear()


def operation(action,body=None,**args):
    """Call the same owned-worker handler used by authenticated routes."""
    payload=json.dumps({'args':args,'body':body}).encode()
    return bt._operation('bt9_'+action,PB9CancelSignal(),payload)


def test_ohlcv_dispatch_retains_native_config_and_cancellation(native, monkeypatch):
    """Both tools use the PB9 adapter, never the legacy preparation endpoint."""
    from api import pb9_ohlcv_tools as tools
    source, _, _ = native
    calls=[]
    monkeypatch.setattr(tools,'preflight',lambda config,cancel:calls.append((copy.deepcopy(config),cancel)) or {'summary':{'headline':'Ready'}})
    monkeypatch.setattr(tools,'start',lambda config,owner:calls.append((copy.deepcopy(config),owner)) or {'job_id':'0123456789ab'})
    assert operation('ohlcv_preflight',{'config':source})['summary']['headline']=='Ready'
    assert calls[0][0]==source and isinstance(calls[0][1],PB9CancelSignal)
    assert operation('ohlcv_start',{'config':source},owner='alice')['job_id']=='0123456789ab'
    assert calls[1]==(source,'alice')
    assert not bt._root('queue').exists()


def test_ohlcv_rejects_other_generation_before_job_admission(native, monkeypatch):
    """A PB8 config at the same package version cannot enter the native lookup."""
    from api import pb9_ohlcv_tools as tools
    source, _, _ = native;source['config_version']='v8.6.0'
    monkeypatch.setattr(tools,'start',lambda *args:pytest.fail('foreign config admitted'))
    with pytest.raises(HTTPException) as error:
        operation('ohlcv_start',{'config':source},owner='alice')
    assert error.value.status_code==422


@pytest.mark.parametrize('action',['ohlcv_preflight','ohlcv_start'])
def test_ohlcv_requires_bounded_config_envelope(native,action):
    """Missing config input fails before source inspection or creating a helper."""
    with pytest.raises(HTTPException) as error:
        operation(action,{'config':None},owner='alice')
    assert error.value.status_code==422


def test_queue_snapshot_is_separate_immutable_and_preserves_sparse_source(native):
    """Queue writes never edit the Run document or PB8 roots."""
    source,status,root=native
    original=copy.deepcopy(source)
    result=operation('enqueue',{'name':'sample','config':source,'operation_id':'once'})
    job=result['filename']
    saved=bt._read(bt._job_path(job))
    snapshot=bt._read(bt._job_path(job,'.config.json'))
    assert source == original
    assert snapshot['bot']['long'] == original['bot']['long']
    assert snapshot['bot']['short'] == original['bot']['short']
    assert saved['commit'] == status['commit']
    assert saved['schema'] == 'v9.0.0'
    assert saved['snapshot_sha256'] == hashlib.sha256(bt._job_path(job,'.config.json').read_bytes()).hexdigest()
    assert Path(snapshot['backtest']['base_dir']).is_relative_to(root/'data/bt_v9_results')
    assert not (root/'data/bt_v8').exists()
    assert operation('enqueue',{'name':'sample','config':source,'operation_id':'once'})['filename'] == job
    assert len(operation('queue')['queue']) == 1


def test_config_gateway_save_and_load_preserves_native_side_overrides(native):
    """Saved backtests use the shared config facade and never acquire PB8 fields."""
    source,_,_=native
    operation('save',{'config':source},name='sample')
    loaded=operation('load',name='sample')['config']
    expected=copy.deepcopy(source)
    expected['backtest']['base_dir']=loaded['backtest']['base_dir']
    assert loaded == expected
    source['backtest']['starting_balance']=1234
    operation('save',{'config':source,'expected_revision':operation('load',name='sample')['revision']},name='sample')
    assert operation('load',name='sample')['config']['backtest']['starting_balance'] == 1234


@pytest.mark.parametrize('shared,long,short,expected', [
    ([], ['HYPE'], ['HYPE'], 1),
    (['BTC'], None, ['HYPE'], 2),
    (['BTC'], [], [], 0),
    ([], 'all', [], 'all'),
    (['BTC'], None, None, 1),
])
def test_config_list_counts_native_side_universes(native, shared, long, short, expected):
    """The real summary operation resolves inheritance without inventing a zero count."""
    source, _, _ = native
    source['bot']['global']['universe']['approved_coins'] = shared
    for side, selected in [('long', long), ('short', short)]:
        source['bot'][side].setdefault('universe', {})['approved_coins'] = selected
    operation('save', {'config': source}, name='sample')
    row = operation('configs')['configs'][0]
    assert row['coins'] == expected
    assert row['modified'].endswith('+00:00')
    assert dt.datetime.fromisoformat(row['modified']).timestamp() == pytest.approx(bt._cfg_path('sample').stat().st_mtime)


def test_draft_is_owned_bounded_and_expires(native,monkeypatch):
    """Another authenticated user cannot retrieve a handoff even with its ID."""
    source,_,_=native
    token=operation('draft_create',{'config':source},owner='alice')['draft_id']
    expected=copy.deepcopy(source);expected['backtest']['ohlcv_source_dir']=bt._pbgui_data_path()
    assert operation('draft_get',id=token,owner='alice')['config']==expected
    with pytest.raises(HTTPException) as error:
        operation('draft_get',id=token,owner='bob')
    assert error.value.status_code==404
    bt._drafts[token]['created']-=601
    with pytest.raises(HTTPException):
        operation('draft_get',id=token,owner='alice')


@pytest.mark.parametrize('name',['../outside','a/b','a\\b','', '.', '..','x\n', '/tmp/other'])
def test_untrusted_names_cannot_escape_managed_roots(native,name):
    """Path validation applies before any config write."""
    source,_,root=native
    with pytest.raises(HTTPException):
        operation('save',{'config':source},name=name)
    assert not (root/'outside').exists()


def test_enqueue_rejects_pb8_source_at_identical_package_version(native):
    """Package version alone never selects the native config generation."""
    source,_,_=native
    source['config_version']='v8.6.0'
    with pytest.raises(HTTPException) as error:
        operation('enqueue',{'name':'wrong','config':source})
    assert error.value.status_code==422
    assert operation('queue')['queue']==[]


def test_start_refuses_a_changed_runtime_before_launch(native,monkeypatch):
    """An old snapshot cannot silently execute against a newer native checkout."""
    source,status,_=native
    job=operation('enqueue',{'name':'sample','config':source})['filename']
    status['commit']='b'*40
    monkeypatch.setattr(bt.subprocess,'Popen',lambda *a,**k:pytest.fail('must not launch'))
    with pytest.raises(HTTPException) as error:
        operation('start',id=job)
    assert error.value.status_code==409


def test_start_refuses_a_modified_snapshot_before_launch(native,monkeypatch):
    """Queued configs are immutable even when edits retain the same schema."""
    source,_,_=native
    job=operation('enqueue',{'name':'sample','config':source})['filename']
    path=bt._job_path(job,'.config.json')
    altered=json.loads(path.read_text());altered['backtest']['starting_balance']=1
    path.write_text(json.dumps(altered))
    monkeypatch.setattr(bt.subprocess,'Popen',lambda *a,**k:pytest.fail('must not launch'))
    with pytest.raises(HTTPException) as error:
        operation('start',id=job)
    assert error.value.status_code==409


def test_foreign_process_record_is_never_signalled(native,monkeypatch):
    """A PID file pointing at the current test process does not establish ownership."""
    import psutil,os
    source,_,_=native
    job=operation('enqueue',{'name':'sample','config':source})['filename']
    bt._write(bt._job_path(job,'.pid.json'),{'pid':os.getpid(),'create_time':psutil.Process().create_time()})
    monkeypatch.setattr(psutil.Process,'terminate',lambda self:pytest.fail('foreign process signalled'))
    operation('stop',id=job)
    assert operation('queue')['queue'][0]['status']=='stopped'


def test_native_npz_equity_preserves_real_timestamps_and_quantities(native):
    """NPZ millisecond clocks are converted to ISO times, never interpreted as minutes."""
    _,_,_=native
    directory=bt._root('results')/'sample'/'job'/'binance'/'result'
    directory.mkdir(parents=True)
    np.savez(directory/'balance_and_equity.npz',schema_version=np.int64(1),
             timestamp_ms=np.array([1735689600000,1735689660000],dtype=np.int64),
             usd_total_balance=np.array([1000.,1001.]),usd_total_equity=np.array([1000.,1002.]))
    csv=operation('equity',path=str(directory.relative_to(bt._root('results'))))['csv']
    assert '2025-01-01T00:00:00+00:00,1000.0,1000.0' in csv
    assert '2025-01-01T00:01:00+00:00,1001.0,1002.0' in csv
    assert 'usd_total_equity' in csv


def test_results_cannot_follow_links_outside_native_root(native,tmp_path):
    """A managed result symlink cannot expose unrelated local files."""
    root=bt._root('results');root.mkdir(parents=True)
    outside=tmp_path/'outside';outside.mkdir();(outside/'analysis.json').write_text('{"secret":true}')
    (root/'link').symlink_to(outside,target_is_directory=True)
    with pytest.raises(HTTPException):
        operation('analysis',path='link')


def test_stop_keeps_a_completed_native_state(native):
    """A late Stop request cannot replace an already successful result with an error."""
    source,_,_=native
    job=operation('enqueue',{'name':'sample','config':source})['filename']
    state={'returncode':0,'completed_at':time.time()}
    bt._write(bt._job_path(job,'.state.json'),state)
    operation('stop',id=job)
    assert bt._read(bt._job_path(job,'.state.json')) == state
    assert operation('queue')['queue'][0]['status'] == 'complete'


def test_gui_preferences_survive_every_handoff_without_native_pollution(native):
    """Safe filters follow drafts, saved configs and results, not the native input."""
    from pb9_backtest_runtime import publish_result_source
    source, _, _ = native
    prefs = {'market_cap': 25, 'vol_mcap': 5.5, 'tags': ['defi'], 'only_cpt': True,
             'notices_ignore': False, 'note': 'review me'}
    source['pbgui'] = {**prefs, 'enabled_on': 'remote', 'runtime': 'pb9'}
    assert operation('prepare', {'config': source})['config']['pbgui'] == prefs
    draft = operation('draft_create', {'config': source}, owner='alice')['draft_id']
    assert operation('draft_get', id=draft, owner='alice')['config']['pbgui'] == prefs
    operation('save', {'config': source}, name='filters')
    assert operation('load', name='filters')['config']['pbgui'] == prefs
    job = operation('enqueue', {'name': 'filters'})['filename']
    assert bt._read(bt._job_path(job))['gui_preferences'] == prefs
    snapshot = bt._read(bt._job_path(job, '.config.json'))
    assert 'pbgui' not in snapshot
    result = Path(snapshot['backtest']['base_dir']) / 'combined' / 'result'
    result.mkdir(parents=True)
    bt._write(result / 'analysis.json', {})
    publish_result_source(bt._job_path(job, '.config.json'))
    assert operation('result_config', path=str(result))['pbgui'] == prefs


@pytest.mark.parametrize('prefs', [{'market_cap': -1}, {'tags': ['bad\n']}, {'only_cpt': 1}, {'note': []}])
def test_invalid_gui_preferences_fail_before_saving_or_enqueue(native, prefs):
    """Malformed sidecar values never become native or persisted GUI inputs."""
    source, _, _ = native
    source['pbgui'] = prefs
    for action, body, args in [('prepare', {'config': source}, {}),
                               ('save', {'config': source}, {'name': 'bad'}),
                               ('enqueue', {'name': 'bad', 'config': source}, {})]:
        with pytest.raises(HTTPException) as error:
            operation(action, body, **args)
        assert error.value.status_code == 422
    assert not bt._cfg_path('bad').exists()
    assert operation('queue')['queue'] == []


def test_stop_is_reported_as_stopped_and_cleanup_does_not_follow_links(native):
    """Deleting queue caches cannot remove results or the linked public data."""
    source, _, root = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    workspace = bt._working_dir(job); workspace.mkdir(parents=True)
    public = root / 'public'; public.mkdir(); (public / 'keep').write_text('candles')
    (workspace / 'metadata').symlink_to(public, target_is_directory=True)
    result = Path(bt._read(bt._job_path(job, '.config.json'))['backtest']['base_dir'])
    result.mkdir(parents=True); (result / 'keep').write_text('result')
    operation('stop', id=job)
    assert operation('queue')['queue'][0]['status'] == 'stopped'
    assert operation('clear_finished')['removed'] == 1
    assert not workspace.exists() and not bt._job_path(job).exists()
    assert (public / 'keep').read_text() == 'candles'
    assert (result / 'keep').read_text() == 'result'


def test_restart_uses_new_output_and_keeps_original_result(native, monkeypatch):
    """A retry uses the frozen source while retaining the old run's evidence."""
    source, _, _ = native
    source['pbgui'] = {'tags': ['defi']}
    old = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    original = bt._read(bt._job_path(old, '.config.json'))
    result = Path(original['backtest']['base_dir']); result.mkdir(parents=True)
    (result / 'keep').write_text('original')
    bt._write(bt._job_path(old, '.state.json'), {'returncode': 1})
    launched = []
    monkeypatch.setattr(bt, '_start', lambda job, cancel: launched.append(job) or {'ok': True})
    new = operation('restart', id=old)['filename']
    assert launched == [new] and old != new
    snapshot = bt._read(bt._job_path(new, '.config.json'))
    assert snapshot['bot'] == original['bot']
    assert snapshot['backtest']['base_dir'] != original['backtest']['base_dir']
    assert bt._read(bt._job_path(new))['gui_preferences'] == {'tags': ['defi']}
    assert (result / 'keep').read_text() == 'original'
    assert bt._read(bt._job_path(old, '.state.json')) == {'returncode': 1}


@pytest.mark.parametrize('remove_results', [True, False])
def test_config_delete_honors_result_choice_recoverably(native, remove_results):
    """The shared deletion dialog has the same effect for native PB9 configs."""
    source, _, _ = native
    operation('save', {'config': source}, name='sample')
    results = bt._safe(bt._root('results'), 'sample'); results.mkdir(parents=True)
    (results / 'keep').write_text('result')
    operation('delete_config', name='sample', remove_results=remove_results)
    assert not bt._cfg_path('sample').exists()
    assert results.exists() is not remove_results
    trashed = list(bt._root('trash').iterdir())
    assert len(trashed) == (2 if remove_results else 1)
    assert any((directory / 'config.json').exists() for directory in trashed)


@pytest.mark.parametrize('exit_code,expected', [(0,'complete'), (1,'error'), (-15,'stopped')])
def test_stop_distinguishes_concurrent_native_exit_from_requested_signal(native, monkeypatch, exit_code, expected):
    """A natural exit keeps its state; termination cannot be displayed as failure."""
    from types import SimpleNamespace
    source, _, _ = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    active = [True]
    original_status = bt._job_status
    record = {'pid': 123}
    monkeypatch.setattr(bt, '_job_status', lambda job: ('running',record) if active[0] else original_status(job))
    def terminate():
        """Simulate the race without creating or signalling any actual process."""
        active[0] = False
        bt._write(bt._job_path(job, '.state.json'), {'returncode': exit_code})
    process = SimpleNamespace(children=lambda recursive: [], terminate=terminate)
    monkeypatch.setattr(bt.psutil, 'Process', lambda pid: process)
    monkeypatch.setattr(bt.psutil, 'wait_procs', lambda processes, timeout: (processes, []))
    operation('stop', id=job)
    assert operation('queue')['queue'][0]['status'] == expected
    assert bt._read(bt._job_path(job, '.state.json')).get('stopped', False) == (exit_code < 0)


def test_config_delete_rolls_back_when_result_move_fails(native, monkeypatch):
    """A failed combined deletion retains both original user-visible data roots."""
    source, _, _ = native
    operation('save', {'config': source}, name='sample')
    results = bt._safe(bt._root('results'), 'sample'); results.mkdir(parents=True)
    (results / 'keep').write_text('original')
    replace = bt.os.replace
    def fail_result(old, new):
        """Fail only the result move; allow the config rollback to complete."""
        if old == results:
            raise OSError('synthetic result move failure')
        return replace(old, new)
    monkeypatch.setattr(bt.os, 'replace', fail_result)
    with pytest.raises(OSError, match='synthetic'):
        operation('delete_config', name='sample', remove_results=True)
    assert operation('load', name='sample')['config']['config_version'] == 'v9.0.0'
    assert (results / 'keep').read_text() == 'original'
    assert list(bt._root('trash').iterdir()) == []


@pytest.mark.parametrize('change', ['runtime','snapshot'])
def test_restart_rejects_changed_identity_without_creating_a_new_job(native, change):
    """Restart cannot bless a different runtime or altered frozen input."""
    source, status, _ = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    bt._write(bt._job_path(job, '.state.json'), {'returncode': 0})
    if change == 'runtime':
        status['commit'] = 'b' * 40
    else:
        snapshot = bt._read(bt._job_path(job, '.config.json'))
        snapshot['backtest']['starting_balance'] += 1
        bt._write(bt._job_path(job, '.config.json'), snapshot)
    with pytest.raises(HTTPException) as error:
        operation('restart', id=job)
    assert error.value.status_code == 409
    assert len(operation('queue')['queue']) == 1
    assert bt._read(bt._job_path(job, '.state.json')) == {'returncode': 0}


def test_stale_save_revision_is_rejected_without_overwrite(native):
    """Only the exact revision opened by the editor authorizes an update."""
    source,_,_=native
    first=operation('save',{'config':source},name='sample')['revision']
    changed=copy.deepcopy(source);changed['backtest']['starting_balance']=2000
    operation('save',{'config':changed,'expected_revision':first},name='sample')
    with pytest.raises(HTTPException) as error:
        operation('save',{'config':source,'expected_revision':first},name='sample')
    assert error.value.status_code == 409
    assert operation('load',name='sample')['config']['backtest']['starting_balance'] == 2000


def test_native_results_index_accepts_absolute_shared_result_identity(native):
    """The shared result index and the result API must agree on canonical path keys."""
    source,_,_=native
    directory=bt._root('results')/'sample'/str(uuid.uuid4())/'binance'/'result'
    directory.mkdir(parents=True)
    bt._write(directory/'config.json',source)
    bt._write(directory/'analysis.json',{'drawdown_max_fraction_strategy_equity':.02,'sharpe_ratio_strategy_equity':1.5})
    np.savez(directory/'balance_and_equity.npz',schema_version=np.int64(1),
             timestamp_ms=np.array([1735689600000,1735776000000],dtype=np.int64),
             usd_total_balance=np.array([np.nan,1001.]),usd_total_equity=np.array([1000.,1002.]))
    result=operation('results',offset=0,limit=100)['results']
    assert len(result)==1
    assert result[0]['path']==str(directory)
    assert result[0]['final_balance']==1001
    assert result[0]['final_equity']==1002
    assert result[0]['drawdown_worst']==.02
    assert result[0]['adg']==pytest.approx(.002)
    assert len(operation('results',offset=0,limit=100)['results'])==1
    raw=operation('equity',path=str(directory))['csv']
    assert '2025-01-01T00:00:00+00:00,,1000.0' in raw


def test_non_monotonic_native_clock_is_rejected(native):
    """Charts cannot silently reorder or reinterpret a corrupt native clock."""
    _,_,_=native
    directory=bt._root('results')/'bad';directory.mkdir(parents=True)
    np.savez(directory/'balance_and_equity.npz',schema_version=np.int64(1),
             timestamp_ms=np.array([2,1]),usd_total_balance=np.array([1.,1.]),usd_total_equity=np.array([1.,1.]))
    with pytest.raises(HTTPException) as error:
        operation('equity',path='bad')
    assert error.value.status_code==422


@pytest.fixture
def migration(native, monkeypatch):
    """Use real PB8 path/snapshot rules and an isolated synthetic native proposal."""
    from api import pb9_migration as migration, backtest_v8 as pb8
    source, status, root = native
    monkeypatch.setattr(pb8, 'PBGDIR', root)
    monkeypatch.setattr(pb8, '_results_root', lambda:root/'results8')
    config = {'config_version':'v8.6.0','pbgui':{'note':'source untouched'},
              'backtest':{'start_date':'2023-01-01','end_date':'2023-01-02'}}
    path = root/'data/bt_v8/example/backtest.json'; path.parent.mkdir(parents=True)
    path.write_text(json.dumps(config))
    calls = []
    def convert(bundle, *args, **kwargs):
        """Preserve inputs for inspection without invoking any installed runtime."""
        calls.append(copy.deepcopy(bundle))
        return {'source':status, 'migration':{'status':'ready','config':copy.deepcopy(source),
                'report':{'mode':'best_effort','changes':[{'reason':'review'}],'warnings':['Risk memory changes']}}}
    monkeypatch.setattr(migration,'migrate_pb8_config_to_pb9',convert)
    migration.shutdown()
    yield migration, path, config, calls
    migration.shutdown()


def test_migration_review_to_unsaved_draft_leaves_pb8_and_queue_unchanged(native, migration):
    """Preview/adoption never persist a config, enqueue a job or edit the PB8 source."""
    service,path,config,calls = migration
    raw = path.read_bytes()
    review = operation('migrate_preview',{'source_type':'config','source':'example'},owner='one')
    assert 'pbgui' not in calls[0]['config']
    assert review['report']['warnings']
    with pytest.raises(HTTPException) as wrong_owner:
        operation('migrate_accept',{'review_id':review['review_id']},owner='two')
    assert wrong_owner.value.status_code == 404
    accepted = operation('migrate_accept',{'review_id':review['review_id']},owner='one')
    assert accepted['suggested_name'] == 'example_v9'
    draft = operation('draft_get',id=accepted['draft_id'],owner='one')
    assert draft['config']['config_version'] == 'v9.0.0'
    assert draft['config']['pbgui']['note'] == 'source untouched'
    assert draft['migration_report'] == review['report']
    assert path.read_bytes() == raw
    assert not bt._root('configs').exists()
    assert not bt._root('queue').exists()


@pytest.mark.parametrize('changed', ['source','override','converter','expired'])
def test_migration_rejects_changes_after_review(native,migration,changed):
    """Source/override edits, converter changes and expiry cannot reuse stale proposals."""
    service,path,config,_ = migration
    config['coin_overrides']={'BTC':{'override_config_path':'BTC.json'}}
    path.write_text(json.dumps(config)); override=path.parent/'BTC.json';override.write_text('{}')
    preview=operation('migrate_preview',{'source_type':'config','source':'example'},owner='one')
    if changed == 'source': path.write_text(json.dumps({**config,'note':'edited'}))
    elif changed == 'override': override.write_text('{"bot": {}}')
    elif changed == 'converter': native[1]['commit']='b'*40
    else: service._reviews[preview['review_id']]['created'] -= 601
    with pytest.raises(HTTPException) as error:
        operation('migrate_accept',{'review_id':preview['review_id']},owner='one')
    assert error.value.status_code == (404 if changed == 'expired' else 409)


def test_migration_result_prefers_authored_original_and_snapshots_local_overrides(native,migration):
    """A result conversion uses its original config instead of runtime-expanded output."""
    service,_,config,calls=migration
    result=native[2]/'results8/example/binance/run'; result.mkdir(parents=True)
    (result/'analysis.json').write_text('{}')
    (result/'config.json').write_text('{"config_version":"v9.0.0"}')
    config['coin_overrides']={'BTC':{'override_config_path':'BTC.json'}}
    (result/'config.original.json').write_text(json.dumps(config))
    (result/'BTC.json').write_text('{"bot":{"long":{}}}')
    response=operation('migrate_preview',{'source_type':'result','source':str(result)},owner='one')
    assert response['status']=='ready'
    assert calls[0]['config']['config_version']=='v8.6.0'
    assert calls[0]['files']=={'BTC.json':{'bot':{'long':{}}}}


@pytest.mark.parametrize('broken',['traversal','symlink','missing','unknown_schema','secret','oversized'])
def test_migration_source_boundaries(native,migration,broken):
    """Invalid source files fail before the native converter sees their contents."""
    service,path,config,calls=migration
    if broken in {'traversal','symlink','missing'}:
        filename='../outside.json' if broken=='traversal' else 'BTC.json'
        config['coin_overrides']={'BTC':{'override_config_path':filename}}
        if broken=='symlink':
            outside=native[2]/'outside.json'; outside.write_text('{}');(path.parent/filename).symlink_to(outside)
    elif broken=='unknown_schema':config['config_version']='v8.2.0'
    elif broken=='secret':config['live']={'api_key':'sensitive'}
    path.write_text(json.dumps(config))
    if broken=='oversized':path.write_bytes(b' '*(1024*1024+1))
    with pytest.raises(HTTPException):
        operation('migrate_preview',{'source_type':'config','source':'example'},owner='one')
    assert calls==[]


def test_migration_review_capacity_does_not_evict_other_owners(native,migration):
    """One user's repeated previews cannot remove another user's accepted context."""
    service,_,_,_=migration
    previews={}
    for index in range(20):
        owner=str(index)
        previews[owner]=operation('migrate_preview',{'source_type':'config','source':'example'},owner=owner)['review_id']
    with pytest.raises(HTTPException) as busy:
        operation('migrate_preview',{'source_type':'config','source':'example'},owner='new')
    assert busy.value.status_code==429
    assert set(previews.values())==set(service._reviews)
    assert operation('migrate_accept',{'review_id':previews['0']},owner='0')['draft_id']


def test_migration_rejects_deep_source_with_diagnostic(native,migration):
    """Deep but parseable JSON produces an actionable 422 rather than recursion failure."""
    _,path,config,_=migration
    value={}
    for _ in range(70):value={'nested':value}
    config['nested']=value;path.write_text(json.dumps(config))
    with pytest.raises(HTTPException) as error:
        operation('migrate_preview',{'source_type':'config','source':'example'},owner='one')
    assert error.value.status_code==422
    assert '64 levels' in error.value.detail


@pytest.mark.parametrize('chosen_source',[None,'/custom/ohlcv'])
def test_new_backtest_and_run_handoff_default_to_same_pbgui_source(native,monkeypatch,chosen_source):
    """Only new configs/handoffs acquire the default; saved/imported source choices survive."""
    import pb9_config
    source,_,_=native
    monkeypatch.setattr(pb9_config,'load_pb9_template',lambda *args,**kwargs:{'config':copy.deepcopy(source)})
    created=operation('new_config')['config']
    assert created['backtest']['ohlcv_source_dir']==bt._pbgui_data_path()
    # A deliberate Passivbot-native choice stays null on validation, save and enqueue.
    source['backtest']['ohlcv_source_dir']=chosen_source
    prepared=operation('prepare',{'config':source})['config']
    assert prepared['backtest']['ohlcv_source_dir']==chosen_source
    operation('save',{'config':source},name='native-data')
    assert operation('load',name='native-data')['config']['backtest']['ohlcv_source_dir']==chosen_source
    job=operation('enqueue',{'name':'native-data','config':source})['filename']
    assert bt._read(bt._job_path(job,'.config.json'))['backtest']['ohlcv_source_dir']==chosen_source


def test_missing_offline_metadata_leaves_queue_and_snapshot_unchanged(native,monkeypatch):
    """Readiness failures return a useful conflict and do not consume a process slot."""
    source,_,_=native
    source['backtest'].update(offline=True,exchanges=['bybit'])
    job=operation('enqueue',{'name':'offline','config':source})['filename']
    raw=bt._job_path(job,'.config.json').read_bytes()
    monkeypatch.setattr(bt,'claim_backtest_slot',lambda *args,**kwargs:pytest.fail('metadata must be checked first'))
    with pytest.raises(HTTPException) as error:operation('start',id=job)
    assert error.value.status_code==409 and 'Coin Data' in error.value.detail
    assert operation('queue')['queue'][0]['status']=='queued'
    assert bt._job_path(job,'.config.json').read_bytes()==raw


@pytest.mark.parametrize('value',[None,[]])
def test_run_handoff_invalid_backtest_reaches_native_validation(native,monkeypatch,value):
    """Defaults must not dereference a malformed section before native rejection."""
    source,_,_=native;source['backtest']=value
    def reject(config,cancel):
        """Represent the native schema validator, not an attribute access failure."""
        assert config['backtest']==value
        raise HTTPException(422,'Invalid native backtest section')
    monkeypatch.setattr(bt,'_native',reject)
    with pytest.raises(HTTPException) as error:operation('draft_create',{'config':source},owner='alice')
    assert error.value.status_code==422


def test_price_overlay_reuses_shared_reader_with_result_owned_dates(native,monkeypatch):
    """Native result charts use the same bounded price reader as PB7/PB8, without config imports."""
    source,_,_=native
    source['backtest'].update(exchanges=['bybit','hyperliquid'],start_date='2025-07-01',end_date='2025-07-02')
    directory=bt._root('results')/'sample'/'job'/'combined'/'result';directory.mkdir(parents=True)
    bt._write(directory/'analysis.json',{});bt._write(directory/'config.json',source)
    calls=[]
    def reader(config,**kwargs):
        """Record the result config boundary without reading production candles."""
        calls.append((config,kwargs))
        return {'available':True,'time':['2025-07-01T00:00:00Z'],'close':[42.0]}
    monkeypatch.setattr(bt,'build_market_price_payload',reader)
    result=operation('price',path=str(directory),exchange='bybit',coin='HYPE',max_points=6000)
    assert result['close']==[42.0]
    assert calls==[(source,{'exchange':'bybit','coin':'HYPE','max_points':6000})]
    with pytest.raises(HTTPException):operation('price',path='../outside',exchange='bybit',coin='HYPE',max_points=6000)
    assert len(calls)==1


def test_price_overlay_retains_shared_exchange_validation(native,monkeypatch):
    """A requested venue outside the result cannot cause arbitrary filesystem lookup."""
    source,_,_=native;source['backtest']['exchanges']=['bybit']
    directory=bt._root('results')/'sample'/'job'/'combined'/'result';directory.mkdir(parents=True)
    bt._write(directory/'analysis.json',{});bt._write(directory/'config.json',source)
    with pytest.raises(HTTPException) as error:
        operation('price',path=str(directory),exchange='hyperliquid',coin='HYPE',max_points=6000)
    assert error.value.status_code==400 and 'not part' in error.value.detail


def test_migrated_result_reference_survives_save_queue_and_result_publication(native,migration,monkeypatch):
    """Reviewed PB8 provenance reaches results without entering native bot parameters."""
    from api import backtest_v8 as pb8
    from pb9_backtest_runtime import publish_result_source
    service,_,config,_=migration
    source8=native[2]/'results8/example/binance/original';source8.mkdir(parents=True)
    (source8/'analysis.json').write_text('{}');(source8/'config.original.json').write_text(json.dumps(config))
    original=(source8/'config.original.json').read_bytes()
    preview=operation('migrate_preview',{'source_type':'result','source':str(source8)},owner='one')
    draft=operation('migrate_accept',{'review_id':preview['review_id']},owner='one')
    config9=operation('draft_get',id=draft['draft_id'],owner='one')['config']
    reference=config9['pbgui']['comparison_reference']
    assert reference['path']==str(source8) and reference['version']=='v8'
    assert operation('prepare',{'config':config9})['config']['pbgui']['comparison_reference']==reference
    operation('save',{'config':config9},name='converted')
    assert operation('load',name='converted')['config']['pbgui']['comparison_reference']==reference
    job=operation('enqueue',{'name':'converted'})['filename']
    record=bt._read(bt._job_path(job));snapshot=bt._read(bt._job_path(job,'.config.json'))
    assert record['comparison_reference']==reference
    assert 'pbgui' not in snapshot
    result9=Path(snapshot['backtest']['base_dir'])/'combined'/'native';result9.mkdir(parents=True)
    bt._write(result9/'config.json',snapshot);bt._write(result9/'analysis.json',{})
    publish_result_source(bt._job_path(job,'.config.json'))
    assert bt._read(result9/'pbgui.provenance.json')['comparison_reference']==reference
    operation('delete_job',id=job)
    assert not bt._job_path(job).exists()
    indexed=operation('results',offset=0,limit=100)['results']
    assert indexed[0]['comparison_reference']==reference
    assert operation('result_config',path=str(result9))['pbgui']['comparison_reference']==reference
    monkeypatch.setattr(pb8,'_list_results',lambda paths:[{'path':str(source8),'backtest_version':'v8'}])
    pair=operation('compare_source',path=str(result9))['results']
    assert [item['backtest_version'] for item in pair]==['v8','v9']
    assert (source8/'config.original.json').read_bytes()==original
    (source8/'analysis.json').write_text('{"changed":true}')
    with pytest.raises(HTTPException) as changed:operation('compare_source',path=str(result9))
    assert changed.value.status_code==409


def test_comparison_reference_is_not_inferred_from_names(native):
    """Older and config-only conversions require manual Compare, never fuzzy matching."""
    source,_,_=native
    result=bt._root('results')/'same_name_v9'/'job'/'combined'/'native';result.mkdir(parents=True)
    bt._write(result/'analysis.json',{});bt._write(result/'config.json',source)
    with pytest.raises(HTTPException) as missing:operation('compare_source',path=str(result))
    assert missing.value.status_code==404 and 'no PB8 source' in missing.value.detail


@pytest.mark.parametrize('patch',[{'version':'v7'},{'path':'../outside'},{'path':'/tmp/bad\npath'},
                                  {'config_fingerprint':'not-a-hash'},{'extra':'unexpected'}])
def test_invalid_persisted_reference_is_rejected_before_save(native,patch):
    """Reference JSON is a validated navigation boundary, not an arbitrary file-read argument."""
    source,_,_=native
    source['pbgui']={'comparison_reference':{'version':'v8','path':'/approved/result',
                      'config_fingerprint':'a'*64,'analysis_sha256':'b'*64,**patch}}
    with pytest.raises(HTTPException) as error:operation('save',{'config':source},name='invalid')
    assert error.value.status_code==422
    assert not bt._root('configs').exists()


@pytest.mark.parametrize('size',[2*1024*1024,9*1024*1024])
def test_large_analysis_does_not_prevent_config_migration(native,migration,size):
    """Optional comparison metadata cannot turn an otherwise valid conversion into an error."""
    service,_,config,_=migration
    directory=native[2]/'results8/example/binance/result';directory.mkdir(parents=True)
    (directory/'config.json').write_text(json.dumps(config))
    (directory/'analysis.json').write_bytes(b'{"padding":"'+b'x'*size+b'"}')
    preview=operation('migrate_preview',{'source_type':'result','source':str(directory)},owner='one')
    assert preview['status']=='ready'
    record=service._reviews[preview['review_id']]
    if size<8*1024*1024:assert record['comparison_reference']
    else:
        assert record['comparison_reference'] is None
        assert any('manual Compare' in warning for warning in preview['report']['warnings'])


def test_invalid_optional_reference_does_not_hide_native_results(native):
    """A damaged GUI sidecar leaves the result list and manual comparison available."""
    source,_,_=native
    directory=bt._root('results')/'sample'/'job'/'combined'/'native';directory.mkdir(parents=True)
    bt._write(directory/'analysis.json',{});bt._write(directory/'config.json',source)
    bt._write(directory/'pbgui.provenance.json',{'comparison_reference':{'version':'v7'}})
    data=operation('results',offset=0,limit=100)['results']
    assert len(data)==1 and data[0]['comparison_reference_error']
    assert 'comparison_reference' not in data[0]
    with pytest.raises(HTTPException) as rejected:operation('compare_source',path=str(directory))
    assert rejected.value.status_code==422


def test_queue_policy_defaults_and_separate_atomic_persistence(native, monkeypatch):
    """PB9 starts disabled and its locked INI writes preserve legacy queue settings."""
    import pbgui_purefunc as pure
    _, _, root = native
    path = root / 'test.ini'
    path.write_text('[backtest_v7]\nautostart = True\ncpu = 3\n')
    monkeypatch.setattr(pure, 'pbgui_ini_path', lambda: path)
    monkeypatch.setattr(bt, 'load_ini_section', pure.load_ini_section)
    monkeypatch.setattr(bt, 'save_ini_section', pure.save_ini_section)
    monkeypatch.setattr(bt.os, 'cpu_count', lambda: 4)
    assert bt.settings()['autostart'] is False
    assert bt.settings()['cpu'] == 1
    assert bt.settings()['capabilities']['autostart'] is True
    bt.update_settings({'autostart': True, 'cpu': 99})
    assert bt.settings()['autostart'] is True
    assert bt.settings()['cpu'] == bt.settings()['cpu_max'] == 4
    assert pure.load_ini_section('backtest_v7') == {'autostart': 'True', 'cpu': '3'}
    bt.update_settings({'autostart': False})
    assert bt.settings()['cpu'] == 4
    assert bt.settings()['autostart'] is False


@pytest.mark.parametrize('body', [{'cpu': True}, {'cpu': '2'}, {'autostart': 'true'},
                                  {'autostart': 1}, {'hlcvs_cleanup_enabled': True}])
def test_queue_settings_reject_invalid_types_without_write(native, body):
    """Only supported typed controls can modify the native queue section."""
    with pytest.raises(HTTPException) as error:
        bt.update_settings(body)
    assert error.value.status_code == 422


def test_auto_pause_during_runtime_preflight_prevents_spawn(native, monkeypatch):
    """A freshly acknowledged pause wins over a scan that already selected a job."""
    source, _, _ = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    policy = {'autostart': 'True'}
    monkeypatch.setattr(bt, 'load_ini_section', lambda _: policy.copy())
    monkeypatch.setattr(bt, 'prepare_market_metadata', lambda *args: policy.update(autostart='False'))
    monkeypatch.setattr(bt, 'claim_backtest_slot', lambda *args, **kwargs: pytest.fail('must not reserve'))
    with pytest.raises(HTTPException, match='paused'):
        bt._start(job, PB9CancelSignal(), automatic=True)
    assert operation('queue')['queue'][0]['status'] == 'queued'
    assert not bt._job_path(job, '.state.json').exists()


def test_manual_start_uses_configured_shared_capacity_when_autostart_disabled(native, monkeypatch):
    """Manual execution remains available and uses the same cross-version slot limit."""
    source, _, _ = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    monkeypatch.setattr(bt, 'prepare_market_metadata', lambda *args: None)
    monkeypatch.setattr(bt, 'load_ini_section', lambda _: {'cpu': '2'})
    monkeypatch.setattr(bt.os, 'cpu_count', lambda: 4)
    claims = []
    monkeypatch.setattr(bt, 'claim_backtest_slot', lambda *args, **kwargs: claims.append(args) or False)
    with pytest.raises(HTTPException, match='slots'):
        operation('start', id=job)
    assert claims == [('v9', job, 2)]
    assert operation('queue')['queue'][0]['status'] == 'queued'


def test_worker_obeys_disabled_policy_capacity_and_pause_between_dispatches(native, monkeypatch):
    """Automatic selection uses fresh policy for each dispatch and ignores completed jobs."""
    import asyncio
    worker = bt.BacktestV9Worker(); worker._running = True
    policy = {'autostart': False, 'cpu': 2}
    launches = []
    monkeypatch.setattr(bt, '_queue_settings', lambda: policy.copy())
    monkeypatch.setattr(bt, '_queue', lambda: pytest.fail('disabled worker must not scan'))
    asyncio.run(worker._cycle())
    monkeypatch.setattr(bt, '_queue', lambda: [
        {'filename': 'done', 'status': 'complete'}, {'filename': 'running', 'status': 'running'},
        {'filename': 'first', 'status': 'queued'}, {'filename': 'second', 'status': 'queued'}])
    policy['autostart'] = True
    monkeypatch.setattr(worker, '_dispatch', lambda job: launches.append(job))
    asyncio.run(worker._cycle())
    assert launches == ['first']
    launches.clear(); policy['cpu'] = 3
    def dispatch(job):
        """Simulate a second client pausing the durable queue after the first launch."""
        launches.append(job); policy['autostart'] = False
    monkeypatch.setattr(worker, '_dispatch', dispatch)
    asyncio.run(worker._cycle())
    assert launches == ['first']


@pytest.mark.parametrize('phase', ['scan', 'launch'])
def test_worker_shutdown_drains_its_work_without_stopping_published_jobs(native, monkeypatch, phase):
    """Repeated shutdown requests join one executor owner and never signal native jobs."""
    import asyncio
    import threading
    entered, release, finished = (threading.Event() for _ in range(3))
    worker = bt.BacktestV9Worker()
    monkeypatch.setattr(bt, '_queue_settings', lambda: {'autostart': True, 'cpu': 1})
    def slow():
        """Keep an owned operation alive until shutdown has started."""
        entered.set()
        assert release.wait(5)
        finished.set()
        return [{'filename': 'one', 'status': 'queued'}]
    if phase == 'scan':
        monkeypatch.setattr(bt, '_queue', slow)
        monkeypatch.setattr(worker, '_dispatch', lambda *_: pytest.fail('late launch'))
    else:
        monkeypatch.setattr(bt, '_queue', lambda: [{'filename': 'one', 'status': 'queued'}])
        monkeypatch.setattr(worker, '_dispatch', lambda *_: slow())
    async def exercise():
        """Stop stays pending until work completes; concurrent callers share the owner."""
        worker.start(); task = worker._task
        assert await asyncio.to_thread(entered.wait, 2)
        stopping = asyncio.create_task(worker.stop())
        await asyncio.sleep(.02)
        assert worker._cancel.is_set() and not stopping.done()
        again = asyncio.create_task(worker.stop())
        await asyncio.sleep(.02)
        release.set()
        await asyncio.gather(stopping, again)
        assert finished.is_set() and task.done() and worker._task is None
        worker.start()
        assert not worker._cancel.is_set()
        await worker.stop()
    try:
        asyncio.run(exercise())
    finally:
        release.set()


def test_native_automatic_launch_publishes_one_slot_and_survives_controller_stop(native, monkeypatch):
    """One synthetic detached launch is published once and is not stopped by the controller."""
    import asyncio
    import shutil
    source, status, root = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    monkeypatch.setattr(bt, '_runtime', lambda cancel=None: ({**status, 'cli_file': root/'cli'},
                        {'pb9dir': str(root/'source'), 'pb9venv': str(root/'env/bin/python')}))
    monkeypatch.setattr(bt, 'prepare_market_metadata', lambda *args: None)
    monkeypatch.setattr(bt, 'load_ini_section', lambda _: {'autostart': 'True', 'cpu': '1'})
    monkeypatch.setattr(shutil, 'which', lambda *_: None)
    claims, published, launches = [], [], []
    monkeypatch.setattr(bt, 'claim_backtest_slot', lambda *args, **kwargs: claims.append(args) or True)
    monkeypatch.setattr(bt, 'publish_backtest_process', lambda *args: published.append(args))
    class Process:
        """A synthetic independent runner: real subprocess creation is prohibited."""
        def poll(self):
            """Remain running after ready publication."""
            return None
        def terminate(self):
            """Published jobs must not be signalled by API controller stop."""
            pytest.fail('published child was stopped')
    def launch(command, **kwargs):
        """Publish the same runner handshake as production, entirely in tmp_path."""
        launches.append((command, kwargs))
        bt._write(Path(command[6]), {'pid': 12345, 'create_time': 1.25})
        Path(command[7]).write_text('12345')
        return Process()
    monkeypatch.setattr(bt.subprocess, 'Popen', launch)
    worker = bt.BacktestV9Worker()
    assert worker._dispatch(job) == {'ok': True, 'pid': 12345}
    asyncio.run(worker.stop())
    assert claims == [('v9', job, 1)]
    assert len(launches) == len(published) == 1
    assert published[0][:4] == ('v9', job, 12345, 1.25)
    assert launches[0][1]['start_new_session'] is True
    assert not bt._job_path(job, '.state.json').exists()


def test_automatic_queue_orders_oldest_first(native, monkeypatch):
    """UUID filename ordering cannot overtake a previously queued snapshot."""
    source, _, _ = native
    first = operation('enqueue', {'name': 'first', 'config': source})['filename']
    second = operation('enqueue', {'name': 'second', 'config': source})['filename']
    for job, created in [(first, 1), (second, 2)]:
        record = bt._read(bt._job_path(job)); record['created_at'] = created
        bt._write(bt._job_path(job), record)
    assert [item['filename'] for item in bt._queue()] == [first, second]


def test_worker_stops_cycle_at_full_capacity_and_throttles_unchanged_deferrals(native, monkeypatch):
    """One failed global reservation cannot trigger preflight for the whole queue."""
    import asyncio
    worker = bt.BacktestV9Worker(); worker._running = True
    monkeypatch.setattr(bt, '_queue_settings', lambda: {'autostart': True, 'cpu': 2})
    monkeypatch.setattr(bt, '_queue', lambda: [{'filename': str(i), 'status': 'queued'} for i in range(5)])
    launches, logs = [], []
    def full(job):
        """Simulate PB8 consuming every shared slot."""
        launches.append(job); raise bt._QueueCapacityUnavailable(409, 'full')
    monkeypatch.setattr(worker, '_dispatch', full)
    asyncio.run(worker._cycle())
    assert launches == ['0']
    def deferred(job):
        """A durable mismatch stays actionable without writing five lines every poll."""
        raise HTTPException(409, 'Source changed')
    monkeypatch.setattr(worker, '_dispatch', deferred)
    monkeypatch.setattr(bt, '_log', lambda *args, **kwargs: logs.append(args))
    asyncio.run(worker._cycle()); asyncio.run(worker._cycle())
    assert len(logs) == 5


def test_automatic_start_does_not_wait_for_a_manually_owned_job_lock(native, monkeypatch):
    """An automatic scan never waits on a manual start or another lock-stripe owner."""
    import concurrent.futures
    source, _, _ = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    monkeypatch.setattr(bt, '_runtime', lambda *_: pytest.fail('busy job entered preflight'))
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        with bt._job_lock(job):
            future = pool.submit(bt._start, job, PB9CancelSignal(), True)
            with pytest.raises(BlockingIOError):
                future.result(timeout=2)


def test_cancelled_unpublished_start_returns_to_queue_after_confirmed_cleanup(native, monkeypatch):
    """An API restart during readiness leaves an unstarted snapshot retryable."""
    import shutil
    source, status, root = native
    job = operation('enqueue', {'name': 'sample', 'config': source})['filename']
    monkeypatch.setattr(bt, '_runtime', lambda cancel=None: ({**status, 'cli_file': root/'cli'},
                        {'pb9dir': str(root/'source'), 'pb9venv': str(root/'env/bin/python')}))
    monkeypatch.setattr(bt, 'prepare_market_metadata', lambda *args: None)
    monkeypatch.setattr(bt, 'load_ini_section', lambda _: {'autostart': 'True'})
    monkeypatch.setattr(shutil, 'which', lambda *_: None)
    cancel = PB9CancelSignal(); stopped = []
    class Process:
        """A not-yet-ready synthetic runner owned by the cancelled launch."""
        def poll(self):
            """Remain alive until cancellation cleanup."""
            return None
        def terminate(self):
            """Record cleanup without touching a real process."""
            stopped.append('terminate')
        def wait(self, timeout=None):
            """Confirm that the synthetic launch is stopped."""
            return 0
    def launch(*args, **kwargs):
        """Cancel immediately after admission, before any ready handshake."""
        cancel.set(); return Process()
    monkeypatch.setattr(bt.subprocess, 'Popen', launch)
    with pytest.raises(bt.PB9ConfigError):
        bt._start(job, cancel, automatic=True)
    assert stopped == ['terminate']
    assert bt._job_status(job)[0] == 'queued'
    assert not bt._job_path(job, '.state.json').exists()
    assert not any(key in bt._read(bt._job_path(job)) for key in ['started_at', 'launch_command', 'status_override'])
    assert not (root/'data/locks/backtest-autostart.json').exists()


@pytest.fixture
def archived_native(native, monkeypatch):
    """Create a native result and its real archive copy using temporary data only."""
    from api import backtest_v7, archive_helpers
    source, _, root = native
    from api import pb8_ohlcv_tools
    (root/'source').mkdir(exist_ok=True)
    monkeypatch.setattr(pb8_ohlcv_tools, '_pbgui_market_data_root', lambda: root/'data/ohlcv')
    backtest_v7._invalidate_archive_cache()
    source['bot']['long'] = {'enabled': True, 'risk': {'max_positions': 2, 'total_wallet_exposure_limit_ratio': 3},
                             'universe': {'approved_coins': ['HYPE']}}
    source['backtest']['starting_balance'] = 1000
    job = operation('enqueue', {'name': 'archive-sample', 'config': source})['filename']
    config = bt._read(bt._job_path(job, '.config.json'))
    local = Path(config['backtest']['base_dir']) / 'combined' / 'native-run'
    local.mkdir(parents=True)
    bt._write(local/'config.json', config)
    bt._write(local/'pbgui.source.json', config)
    bt._write(local/'analysis.json', {'sharpe_ratio_strategy_equity': 2.5,
        'drawdown_max_fraction_strategy_equity': .2, 'liquidated': False})
    np.savez(local/'balance_and_equity.npz', schema_version=np.int64(1),
             timestamp_ms=np.array([1735689600000,1735776000000], dtype=np.int64),
             usd_total_balance=np.array([1000.,1200.]), usd_total_equity=np.array([1000.,1300.]))
    archive_root = root/'data/config_archives'
    monkeypatch.setattr(backtest_v7, '_archives_dir', lambda: archive_root)
    archive = archive_root/'own'
    copied = archive_helpers.copy_backtest_result_to_archive(local, archive)
    yield local, Path(copied['path']), archive, source
    backtest_v7._invalidate_archive_cache()


def test_archive_native_projection_matches_local_values_and_keeps_raw_artifacts(archived_native):
    """Archive copy keeps native bytes and shares final balance/equity and risk formulas."""
    from api import archive_helpers
    local, archived, archive, _ = archived_native
    before = bt._summaries([local/'analysis.json'])[0]
    after = archive_helpers.summarize_backtest_result(archived, archive)
    for key in ['gain','adg','final_balance','final_equity','sharpe_ratio','drawdown_worst',
                'config_name','strategy','coins','twe_long','pos_long']:
        assert after[key] == before[key], key
    assert after['config_family'] == 'pb9'
    assert after['backtest_version'] == 'v9'
    assert after['config_name'] == 'archive-sample'
    assert after['gain'] == 1.3 and after['final_balance'] == 1200
    for file in local.iterdir():
        assert (archived/file.name).read_bytes() == file.read_bytes()
    analysis = bt._read(archived/'analysis.json')
    config = bt._read(archived/'config.json')
    assert archive_helpers.detect_liquidation(analysis, config) == (False, '')
    assert 'v9.0.0/backtests/archive-sample' in str(archived)


def test_archived_native_reads_are_version_owned_and_never_allow_local_delete(archived_native):
    """Read-only archive support cannot widen native result-delete ownership."""
    from api import backtest_v7
    _, archived, archive, _ = archived_native
    path = str(archived)
    assert operation('result_config', path=path)['config_version'] == 'v9.0.0'
    assert operation('analysis', path=path)['sharpe_ratio_strategy_equity'] == 2.5
    assert '1300.0' in operation('equity', path=path)['csv']
    config, version, overrides = backtest_v7._load_archived_backtest_config(archived, archive)
    assert version == 'v9' and overrides == {} and config['config_version'] == 'v9.0.0'
    with pytest.raises(HTTPException):
        operation('delete_result', path=path)
    assert (archived/'config.json').is_file()
    foreign = archive/'foreign'; foreign.mkdir()
    bt._write(foreign/'config.json', {'config_version': 'v8.6.0'})
    with pytest.raises(HTTPException, match='PB9'):
        operation('result_config', path=str(foreign))


def test_archive_native_requeue_uses_only_native_queue_and_explicit_overrides(archived_native, monkeypatch):
    """Reusing an archived result preserves sparse native params and queues a new snapshot."""
    from api import backtest_v7
    _, archived, _, _ = archived_native
    monkeypatch.setattr(backtest_v7, 'add_to_queue', lambda *_a, **_k: pytest.fail('PB7 queue called'))
    before = (archived/'config.json').read_bytes()
    result = operation('archive_requeue', {'paths': [str(archived)], 'overrides': {
        'start_date': '2025-01-01', 'end_date': '2025-01-02', 'starting_balance': 2000,
        'exchanges': ['bybit'], 'use_pbgui_market_data': True}}, name='own')
    assert result['queued'] == 1 and result['queue_items'][0]['backtest_version'] == 'v9'
    snapshot = bt._read(bt._job_path(result['queue_items'][0]['filename'], '.config.json'))
    assert snapshot['config_version'] == 'v9.0.0'
    assert snapshot['backtest']['starting_balance'] == 2000
    assert snapshot['backtest']['exchanges'] == ['bybit']
    assert snapshot['backtest']['ohlcv_source_dir'] == bt._pbgui_data_path()
    assert (archived/'config.json').read_bytes() == before


def test_native_archive_cannot_fall_back_to_legacy_retest_or_another_archive(archived_native):
    """Unconnected replacement routes reject native configs before any legacy conversion."""
    from api import backtest_v7
    _, archived, archive, _ = archived_native
    with pytest.raises(HTTPException, match='PB9'):
        backtest_v7.rebacktest_archive_results('own', {'paths': [str(archived)]})
    with pytest.raises(HTTPException, match='owned native endpoint'):
        backtest_v7._queue_archive_retest_run('own', archive, archived, {})
    with pytest.raises(HTTPException):
        operation('archive_requeue', {'paths': [str(archived)]}, name='other')


def test_archive_native_reader_rejects_equity_symlink(archived_native, tmp_path):
    """An archived artifact cannot expose arbitrary files through the native NPZ reader."""
    _, archived, _, _ = archived_native
    target = tmp_path/'outside.npz'; target.write_bytes(b'not a numeric archive')
    equity = archived/'balance_and_equity.npz'; equity.unlink(); equity.symlink_to(target)
    with pytest.raises(HTTPException):
        operation('equity', path=str(archived))
    assert target.read_bytes() == b'not a numeric archive'


def test_native_archive_rename_keeps_artifact_bytes_and_updates_display_group(archived_native, monkeypatch):
    """Renaming the archive group must not rewrite native results or authored provenance."""
    from api import backtest_v7, archive_helpers
    _, archived, archive, _ = archived_native
    monkeypatch.setattr(backtest_v7, '_own_archive_name', lambda: 'own')
    before = {file.name:file.read_bytes() for file in archived.iterdir()}
    response = backtest_v7.rename_archive_backtest_config('own', {'path':str(archived),'new_name':'renamed'})
    renamed = Path(response['path'])
    assert renamed.is_dir() and not archived.exists()
    assert {file.name:file.read_bytes() for file in renamed.iterdir()} == before
    assert archive_helpers.summarize_backtest_result(renamed, archive)['config_name'] == 'renamed'


def test_native_archive_browse_read_only_never_migrates_or_writes_manifest(archived_native, monkeypatch):
    """Opening PB9 archive results is a read, even for the configured own archive."""
    from api import backtest_v7
    _, archived, archive, _ = archived_native
    monkeypatch.setattr(backtest_v7, '_get_cached_archive_results', lambda *_: None)
    monkeypatch.setattr(backtest_v7, 'maybe_migrate_own_archive', lambda *_a, **_k: pytest.fail('read migrated archive'))
    monkeypatch.setattr(backtest_v7, 'rebuild_archive_manifest', lambda *_: pytest.fail('read rewrote archive'))
    monkeypatch.setattr(backtest_v7, 'archive_migration_status', lambda *_a, **_k: {})
    response = backtest_v7.list_archive_results('own', read_only=True)
    assert len(response['results']) == 1
    assert response['results'][0]['backtest_version'] == 'v9'
    assert not (archive/'pbgui/archive_manifest.json').exists()


def test_archive_source_json_symlink_is_rejected_before_read(archived_native, tmp_path):
    """The native archive resolver rejects source JSON links, not just linked NPZ files."""
    _, archived, _, _ = archived_native
    outside = tmp_path/'private.json'; outside.write_text('{"secret":"fixture-only"}')
    source = archived/'pbgui.source.json'; source.unlink(); source.symlink_to(outside)
    with pytest.raises(HTTPException):
        operation('result_config', path=str(archived))
    assert outside.read_text() == '{"secret":"fixture-only"}'


def test_malformed_native_archive_result_does_not_hide_valid_results(archived_native):
    """A bad foreign NPZ is skipped per result without dropping the shared archive list."""
    from api import archive_helpers
    _, archived, archive, _ = archived_native
    bad = archived.parent/'broken'; bad.mkdir()
    bt._write(bad/'config.json', bt._read(archived/'config.json'))
    bt._write(bad/'analysis.json', {})
    (bad/'balance_and_equity.npz').write_bytes(b'not a zip archive')
    rows = archive_helpers.list_archive_backtest_results(archive)
    assert len(rows) == 1 and rows[0]['path'] == str(archived)


def test_archive_requeue_rejects_foreign_data_paths_before_native_import(archived_native, monkeypatch):
    """A repository cannot steer native preparation outside approved market/runtime roots."""
    _, archived, _, _ = archived_native
    source = bt._read(archived/'pbgui.source.json')
    source['backtest']['hlcvs_data_dir'] = '/outside-approved-roots/dataset'
    bt._write(archived/'pbgui.source.json', source)
    monkeypatch.setattr(bt, '_native', lambda *_a: pytest.fail('unapproved source reached native code'))
    with pytest.raises(HTTPException, match='Archived data path'):
        operation('archive_requeue', {'paths':[str(archived)]}, name='own')


def test_native_npz_rejects_forged_array_shape_before_numpy_allocation(archived_native, monkeypatch):
    """An untrusted NPY header cannot allocate beyond the verified ZIP member storage."""
    import io
    import zipfile
    _, archived, _, _ = archived_native
    path = archived/'balance_and_equity.npz'
    with zipfile.ZipFile(path) as archive:
        values = {name:archive.read(name) for name in archive.namelist()}
    forged = io.BytesIO()
    np.lib.format.write_array_header_1_0(forged, {'descr':'<i8','fortran_order':False,'shape':(10**12,)})
    forged.write(b'\0'*8)
    values['timestamp_ms.npy'] = forged.getvalue()
    with zipfile.ZipFile(path, 'w') as archive:
        for name, value in values.items():
            archive.writestr(name, value)
    monkeypatch.setattr(np, 'load', lambda *_a, **_k: pytest.fail('forged shape reached numpy allocation'))
    with pytest.raises(HTTPException, match='Invalid native PB9 equity artifact'):
        operation('equity', path=str(archived))


def test_archive_requeue_lost_response_retry_reuses_native_snapshot(archived_native):
    """One user operation remains idempotent after an uncertain HTTP response."""
    _, archived, _, _ = archived_native
    body = {'paths':[str(archived)], 'operation_id':'fixture-retry-id', 'overrides':{'exchanges':['bybit']}}
    before = len(bt._queue())
    first = operation('archive_requeue', body, name='own')
    retry = operation('archive_requeue', body, name='own')
    assert first['queue_items'][0]['filename'] == retry['queue_items'][0]['filename']
    assert len(bt._queue()) == before + 1


def test_native_archive_websocket_releases_watcher_subscription(native):
    """Only a live native websocket may retain its shared archive-notification sink."""
    import asyncio
    from types import SimpleNamespace
    from fastapi import WebSocketDisconnect
    from api import backtest_v7
    before = set(backtest_v7._archive_ws_clients)
    class Owner:
        """Provide a queue snapshot without touching any configured runtime."""
        async def run(self, operation):
            """Return the owned native queue/settings contract."""
            assert operation == 'bt9_queue'
            return {'queue':[], 'settings':{'autostart':False,'cpu':1}}
    class Socket:
        """Disconnect after the first native push, exercising the endpoint's finally."""
        app = SimpleNamespace(state=SimpleNamespace(pb9_requests=Owner()))
        async def send_json(self, payload):
            """Verify subscription exists only while its endpoint is active."""
            assert self in backtest_v7._archive_ws_clients
            assert payload['type'] == 'queue_update'
            raise WebSocketDisconnect()
    asyncio.run(bt.websocket.__wrapped__(Socket()))
    assert backtest_v7._archive_ws_clients == before


@pytest.fixture
def native_retest(archived_native, monkeypatch):
    """Queue one real native retest with mocked native validation and private data roots."""
    from api import backtest_v7
    _, archived, archive, _ = archived_native
    monkeypatch.setattr(backtest_v7, '_own_archive_name', lambda:'own')
    monkeypatch.setattr(bt.subprocess, 'Popen', lambda *_a, **_k:pytest.fail('a retest enqueue started a process'))
    response = backtest_v7._native_archive_retest('own', {'paths':[str(archived)]}, PB9CancelSignal())
    assert response['queued']==1
    return response['runs'][0], archived, archive


def finish_native_retest(run, *, liquidated=False, balance=1200., equity=1300., state=True):
    """Publish native completion exactly as the runner does, without running a backtest."""
    from pb9_backtest_runtime import publish_result_source
    snapshot=bt._job_path(run['queue_filename'],'.config.json')
    config=bt._read(snapshot)
    config['backtest']['starting_balance']=config['backtest'].get('starting_balance') or 1000
    root=Path(config['backtest']['base_dir'])
    directory=root/'combined'/'native-retest-completed'; directory.mkdir(parents=True)
    bt._write(directory/'config.json',config)
    bt._write(directory/'analysis.json', {'liquidated':liquidated,'sharpe_ratio_strategy_equity':2.5})
    start=float(config['backtest']['starting_balance'])
    np.savez(directory/'balance_and_equity.npz', schema_version=np.int64(1),
        timestamp_ms=np.array([1735689600000,1735776000000],dtype=np.int64),
        usd_total_balance=np.array([start,balance]), usd_total_equity=np.array([start,equity]))
    publish_result_source(snapshot)
    if state:
        bt._write(bt._job_path(run['queue_filename'],'.state.json'), {'returncode':0})
    return directory


def test_native_retest_queue_is_native_and_private_control_metadata_never_enters_bot_input(native_retest):
    """Date/exchange policy is shared, but native input and control provenance stay separate."""
    from api import backtest_v7
    run, original, _=native_retest
    record=bt._read(bt._job_path(run['queue_filename']))
    config=bt._read(bt._job_path(run['queue_filename'],'.config.json'))
    assert run['backtest_version']=='v9' and run['config_family']=='pb9'
    assert record['archive_retest']['id']==run['id']
    assert run['snapshot_sha256']==record['snapshot_sha256']
    assert config['backtest']['end_date']==run['date']['end_date']
    assert 'pbgui' not in config and 'archive_retest' not in config
    assert original.exists()
    with pytest.raises(HTTPException,match='already pending'):
        backtest_v7._native_archive_retest('own',{'paths':[str(original)]},PB9CancelSignal())
    assert len(backtest_v7._load_archive_retest_runs())==1


def test_successful_native_retest_replaces_archive_with_immutable_artifacts_and_recoverable_original(native_retest):
    """Completion copies verified native output, updates the shared run and retires only its job."""
    from api import backtest_v7, archive_helpers
    run, original, archive=native_retest
    before={file.name:file.read_bytes() for file in original.iterdir()}
    output=finish_native_retest(run)
    produced={file.name:file.read_bytes() for file in output.iterdir()}
    backtest_v7._process_archive_retest_completions()
    stored=backtest_v7._load_archive_retest_runs()[0]
    assert stored['status']=='complete'
    new=Path(stored['result']['new_path'])
    assert new.is_dir() and not original.exists()
    assert {file.name:file.read_bytes() for file in new.iterdir()}==produced
    backup=Path(stored['result']['backup'])
    assert {file.name:file.read_bytes() for file in backup.iterdir()}==before
    assert archive_helpers.summarize_backtest_result(new,archive)['config_name']=='archive-sample'
    assert not bt._job_path(run['queue_filename']).exists()
    assert stored['result']['cleanup']['complete'] is True


@pytest.mark.parametrize('failure',['failed','liquidated','changed_source','changed_provenance','changed_original','ambiguous'])
def test_native_retest_failures_keep_the_original_archive_unchanged(native_retest,failure):
    """No failed, altered or ambiguous native run can replace the queued original."""
    from api import backtest_v7
    run, original, _=native_retest
    output=finish_native_retest(run,liquidated=failure=='liquidated')
    if failure=='failed':
        bt._write(bt._job_path(run['queue_filename'],'.state.json'),{'returncode':1})
    elif failure=='changed_source':
        source=bt._read(output/'pbgui.source.json');source['backtest']['starting_balance']+=1
        bt._write(output/'pbgui.source.json',source)
    elif failure=='changed_provenance':
        proof=bt._read(output/'pbgui.provenance.json');proof['snapshot_sha256']='f'*64
        bt._write(output/'pbgui.provenance.json',proof)
    elif failure=='changed_original':
        (original/'note.txt').write_text('user change after enqueue')
    elif failure=='ambiguous':
        import shutil
        shutil.copytree(output,output.with_name('another-result'))
    before={file.name:file.read_bytes() for file in original.iterdir()}
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='error'
    assert {file.name:file.read_bytes() for file in original.iterdir()}==before


def test_native_retest_recovery_reconstructs_run_without_guessing_other_jobs(native_retest):
    """A crash between immutable enqueue and shared state save cannot orphan the retest."""
    from api import backtest_v7
    from pb9_archive_retests import recover_runs
    run, _, _=native_retest
    backtest_v7._save_archive_retest_runs([])
    recover_runs(); recover_runs()
    recovered=backtest_v7._load_archive_retest_runs()
    assert len(recovered)==1 and recovered[0]['id']==run['id']
    assert recovered[0]['queue_filename']==run['queue_filename']


def test_native_retest_checkpoint_recovers_after_archive_move_and_local_cleanup(native_retest,monkeypatch):
    """A restart after the replacement is copied/finalized does not lose successful completion."""
    from api import backtest_v7, archive_helpers
    from pb9_archive_retests import replace_result
    run, original, _=native_retest
    finish_native_retest(run)
    result=replace_result(run)
    assert not original.exists() and not bt._job_path(run['queue_filename']).exists()
    # Shared runs still say queued, as if the API died before its final state write.
    backtest_v7._process_archive_retest_completions()
    restored=backtest_v7._load_archive_retest_runs()[0]
    assert restored['status']=='complete'
    assert restored['result']['new_relative_path']==result['new_relative_path']
    assert Path(restored['result']['new_path']).is_dir()


def test_native_schedule_creation_run_completion_and_target_relink_use_existing_engine(archived_native,monkeypatch):
    """Daily/weekly controls create native queued jobs and follow replacement paths."""
    from api import backtest_v7
    _, original, _, _=archived_native
    monkeypatch.setattr(backtest_v7,'_own_archive_name',lambda:'own')
    created=backtest_v7.create_archive_retest_schedule('own',{'paths':[str(original)],'cadence':'weekly','weekday':2,'time':'04:10'})['schedule']
    assert created['backtest_versions']==['v9'] and created['cadence']=='weekly'
    response=backtest_v7._native_schedule_run('own',created['id'],PB9CancelSignal())
    run=response['runs'][0]
    assert run['schedule_id']==created['id']
    finish_native_retest(run)
    backtest_v7._process_archive_retest_completions()
    schedule=backtest_v7._load_archive_retest_schedules()[0]
    assert schedule['targets'][0]['relative_path']!=run['source_relative_path']
    assert schedule['last_status']=='complete'
    backtest_v7.toggle_archive_retest_schedule('own',created['id'],{'enabled':False})
    assert backtest_v7._load_archive_retest_schedules()[0]['enabled'] is False
    backtest_v7.delete_archive_retest_schedule('own',created['id'])
    assert backtest_v7._load_archive_retest_schedules()==[]


def test_native_due_schedule_is_supervised_and_does_not_start_a_backtest(archived_native,monkeypatch):
    """The existing schedule tick only enqueues a native snapshot and advances its period."""
    from api import backtest_v7
    _, original, _, _=archived_native
    monkeypatch.setattr(backtest_v7,'_own_archive_name',lambda:'own')
    monkeypatch.setattr(bt.subprocess,'Popen',lambda *_a,**_k:pytest.fail('schedule launched a bot'))
    schedule=backtest_v7.create_archive_retest_schedule('own',{'paths':[str(original)]})['schedule']
    schedule['next_run_at']='2000-01-01T00:00:00'
    backtest_v7._save_archive_retest_schedules([schedule])
    backtest_v7._queue_due_archive_retest_schedules(PB9CancelSignal())
    assert len(backtest_v7._load_archive_retest_runs())==1
    assert backtest_v7._load_archive_retest_schedules()[0]['next_run_at']!='2000-01-01T00:00:00'
    backtest_v7._queue_due_archive_retest_schedules(PB9CancelSignal())
    assert len(backtest_v7._load_archive_retest_runs())==1


def test_native_retest_requires_job_level_completion_marker(native_retest):
    """Partial publications never retire the original, even with a terminal queue state."""
    from api import backtest_v7
    run, original, _=native_retest
    result=finish_native_retest(run)
    (result.parents[1]/'pbgui.completion.json').unlink()
    backtest_v7._process_archive_retest_completions()
    assert original.is_dir()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='error'


def test_native_due_cycle_resumes_remaining_targets_after_cancellation(archived_native,monkeypatch):
    """A partially queued daily cycle resumes without duplicating its first target."""
    import shutil
    from api import backtest_v7
    _, original, _, _=archived_native
    other=original.with_name('second-original');shutil.copytree(original,other)
    monkeypatch.setattr(backtest_v7,'_own_archive_name',lambda:'own')
    schedule=backtest_v7.create_archive_retest_schedule('own',{'paths':[str(original),str(other)]})['schedule']
    schedule['next_run_at']='2000-01-01T00:00:00'
    backtest_v7._save_archive_retest_schedules([schedule])
    cancel=PB9CancelSignal();queue=backtest_v7._queue_archive_retest_run
    def interrupt(*args,**kwargs):
        """Cancel after one persisted native enqueue, as during API shutdown."""
        run=queue(*args,**kwargs);cancel.set();return run
    monkeypatch.setattr(backtest_v7,'_queue_archive_retest_run',interrupt)
    backtest_v7._queue_due_archive_retest_schedules(cancel)
    assert len(backtest_v7._load_archive_retest_runs())==1
    assert backtest_v7._load_archive_retest_schedules()[0]['next_run_at']==schedule['next_run_at']
    backtest_v7.toggle_archive_retest_schedule('own',schedule['id'],{'enabled':False})
    backtest_v7.toggle_archive_retest_schedule('own',schedule['id'],{'enabled':True})
    assert backtest_v7._load_archive_retest_schedules()[0]['next_run_at']==schedule['next_run_at']
    monkeypatch.setattr(backtest_v7,'_queue_archive_retest_run',queue)
    backtest_v7._queue_due_archive_retest_schedules(PB9CancelSignal())
    runs=backtest_v7._load_archive_retest_runs()
    assert len(runs)==2 and len({run['schedule_target_id'] for run in runs})==2
    assert len({run['schedule_cycle'] for run in runs})==1


def test_archive_retest_worker_stop_joins_preparation_and_resets_cancellation(native,monkeypatch):
    """API shutdown signals and joins preparation without touching detached native jobs."""
    import asyncio
    import threading
    from api import backtest_v7
    entered=threading.Event();exited=threading.Event()
    worker=backtest_v7.ArchiveRetestWorker()
    monkeypatch.setattr(backtest_v7,'_process_archive_retest_completions',lambda:None)
    def prepare(cancel):
        """Model a native helper that cooperatively exits on its owner's signal."""
        entered.set()
        deadline=time.monotonic()+3
        while not cancel.is_set() and time.monotonic()<deadline:
            time.sleep(.01)
        assert cancel.is_set();exited.set()
    monkeypatch.setattr(backtest_v7,'_queue_due_archive_retest_schedules',prepare)
    async def scenario():
        """Run two lifetimes with separate cancellation signals."""
        worker.start()
        assert await asyncio.to_thread(entered.wait,3)
        first=worker._cancel
        await worker.stop()
        assert exited.is_set() and first.is_set() and worker._task is None
        entered.clear();exited.clear();worker.start()
        assert worker._cancel is not first and not worker._cancel.is_set()
        assert await asyncio.to_thread(entered.wait,3)
        await worker.stop()
    asyncio.run(scenario())


def test_concurrent_native_schedule_creates_preserve_both_updates(archived_native,monkeypatch):
    """The shared RMW transaction serializes competing API schedule creations."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from api import backtest_v7
    _, original, _, _=archived_native
    monkeypatch.setattr(backtest_v7,'_own_archive_name',lambda:'own')
    entered=threading.Event();release=threading.Event();save=backtest_v7._save_archive_retest_schedules
    def hold_first(schedules):
        """Hold the first write while another request attempts its transaction."""
        if not entered.is_set():
            entered.set();assert release.wait(3)
        save(schedules)
    monkeypatch.setattr(backtest_v7,'_save_archive_retest_schedules',hold_first)
    with ThreadPoolExecutor(2) as pool:
        first=pool.submit(backtest_v7.create_archive_retest_schedule,'own',{'paths':[str(original)],'time':'01:00'})
        assert entered.wait(3)
        second=pool.submit(backtest_v7.create_archive_retest_schedule,'own',{'paths':[str(original)],'time':'03:00'})
        release.set();first.result(5);second.result(5)
    schedules=backtest_v7._load_archive_retest_schedules()
    assert len(schedules)==2 and {item['time'] for item in schedules}=={'01:00','03:00'}


@pytest.mark.parametrize('failure',[RuntimeError('manifest unavailable'),HTTPException(409,'busy'),OSError('disk unavailable')])
def test_native_replacement_finalization_errors_retry_without_losing_success(native_retest,monkeypatch,failure):
    """Any transient finalization failure after oldmove retains the checkpoint for retry."""
    from api import archive_helpers,backtest_v7
    run,original,_=native_retest
    finish_native_retest(run)
    rebuild=archive_helpers.rebuild_archive_manifest
    def fail(*args,**kwargs):
        """Inject a finalization failure after the original was retained in backup."""
        raise failure
    monkeypatch.setattr(archive_helpers,'rebuild_archive_manifest',fail)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='processing'
    assert not original.exists()
    monkeypatch.setattr(archive_helpers,'rebuild_archive_manifest',rebuild)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='complete'


def test_native_recovery_failure_does_not_block_existing_completions(native_retest,monkeypatch):
    """A damaged recovery scan cannot stop the shared completion controller."""
    from api import backtest_v7
    import pb9_archive_retests as adapter
    run,_,_=native_retest;finish_native_retest(run)
    def fail():
        """Model an unavailable recovery root without touching a real path."""
        raise OSError('temporary recovery scan failure')
    monkeypatch.setattr(adapter,'recover_runs',fail)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='complete'


def test_native_schedule_continues_after_bad_target_and_run_now_preserves_cycle(archived_native,monkeypatch):
    """One rejected target cannot drop the rest of a mixed schedule batch."""
    import shutil
    from api import backtest_v7
    _,original,_,_=archived_native
    other=original.with_name('second-original');shutil.copytree(original,other)
    monkeypatch.setattr(backtest_v7,'_own_archive_name',lambda:'own')
    schedule=backtest_v7.create_archive_retest_schedule('own',{'paths':[str(original),str(other)]})['schedule']
    schedule['targets'][0]['backtest_version']='v8'
    before=schedule['next_run_at'];backtest_v7._save_archive_retest_schedules([schedule])
    result=backtest_v7._native_schedule_run('own',schedule['id'],PB9CancelSignal())
    assert result['queued']==1 and len(result['errors'])==1
    saved=backtest_v7._load_archive_retest_schedules()[0]
    assert saved['last_status']=='partial' and saved['next_run_at']==before


def test_native_cleanup_interrupted_companion_removal_retains_terminal_record(native_retest,monkeypatch):
    """Crash recovery can still identify cleanup after terminal state was already removed."""
    from api import backtest_v7
    run,_,_=native_retest;finish_native_retest(run)
    unlink=Path.unlink
    def fail_ready(path,*args,**kwargs):
        """Fail after companion state deletion but before removing the queue record."""
        if path==bt._job_path(run['queue_filename'],'.ready'):
            raise OSError('interrupted cleanup')
        return unlink(path,*args,**kwargs)
    monkeypatch.setattr(Path,'unlink',fail_ready)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='processing'
    assert bt._job_path(run['queue_filename']).exists()
    assert bt._job_status(run['queue_filename'])[0]=='stopped'
    monkeypatch.setattr(Path,'unlink',unlink)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='complete'
    assert not bt._job_path(run['queue_filename']).exists()


def test_native_oldmove_failure_rolls_back_only_verified_new_copy(native_retest,monkeypatch):
    """A failure before retiring the original leaves no duplicate replacement in the archive."""
    from api import backtest_v7
    import pb9_archive_retests as adapter
    run,original,archive=native_retest;finish_native_retest(run)
    replace=adapter.os.replace
    def fail_oldmove(source,destination,*args,**kwargs):
        """Reject exactly the old-result move, preserving all other atomic writes."""
        if Path(source)==original:
            raise OSError('oldmove failed')
        return replace(source,destination,*args,**kwargs)
    monkeypatch.setattr(adapter.os,'replace',fail_oldmove)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='error'
    assert original.exists() and list(archive.rglob('analysis.json'))==[original/'analysis.json']
    assert not adapter.has_replacement(run)


def test_completed_native_retest_cleanup_failure_never_downgrades_success(native_retest,monkeypatch):
    """Late cleanup errors retain completed replacement status and retry separately."""
    from api import backtest_v7
    import pb9_archive_retests as adapter
    run,_,_=native_retest;finish_native_retest(run)
    cleanup=adapter.cleanup
    monkeypatch.setattr(adapter,'cleanup',lambda *args:{'complete':False,'errors':['waiting']})
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['status']=='complete'
    def fail(*args):
        """Model a late queue cleanup failure after successful archive replacement."""
        raise HTTPException(409,'temporarily busy')
    monkeypatch.setattr(adapter,'cleanup',fail)
    backtest_v7._process_archive_retest_completions()
    saved=backtest_v7._load_archive_retest_runs()[0]
    assert saved['status']=='complete' and saved['result']['cleanup']['complete'] is False
    monkeypatch.setattr(adapter,'cleanup',cleanup)
    backtest_v7._process_archive_retest_completions()
    assert backtest_v7._load_archive_retest_runs()[0]['result']['cleanup']['complete'] is True
