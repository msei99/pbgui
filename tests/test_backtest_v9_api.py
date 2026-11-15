"""PB9 native queue, immutable provenance and artifact boundaries with temporary data."""
import copy
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
    import logging_helpers
    monkeypatch.setattr(logging_helpers,'LOG_ROOT',tmp_path/'data/logs')
    import backtest_autostart
    monkeypatch.setattr(backtest_autostart,'PBGDIR',tmp_path)
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
    bt._drafts.clear()
    yield source,status,tmp_path
    bt._drafts.clear()


def operation(action,body=None,**args):
    """Call the same owned-worker handler used by authenticated routes."""
    payload=json.dumps({'args':args,'body':body}).encode()
    return bt._operation('bt9_'+action,PB9CancelSignal(),payload)


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


def test_draft_is_owned_bounded_and_expires(native,monkeypatch):
    """Another authenticated user cannot retrieve a handoff even with its ID."""
    source,_,_=native
    token=operation('draft_create',{'config':source},owner='alice')['draft_id']
    assert operation('draft_get',id=token,owner='alice')['config']==source
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
    assert operation('queue')['queue'][0]['status']=='error'


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
