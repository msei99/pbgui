"""PB9 source provenance and environment rules for the shared independent runner."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

from pb9_backtest_runtime import launch_environment, publish_result_source, source_identity


@pytest.fixture
def source(tmp_path):
    """Create a hermetic native source identity without importing fixture modules."""
    root=tmp_path/'source';(root/'src/config').mkdir(parents=True)
    (root/'src/config/schema.py').write_text('CONFIG_SCHEMA_VERSION = "v9.0.0"\n')
    (root/'src/passivbot_version.py').write_text('__version__ = "8.2.0.dev0"\n')
    (root/'src/other.py').write_text('value = 1\n')
    env={key:value for key,value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_CONFIG_GLOBAL=os.devnull,GIT_CONFIG_NOSYSTEM='1')
    def git(*args):
        """Never use a user's signing or repository overrides in this fixture."""
        return subprocess.run(['git','-C',str(root),'-c','user.name=Test','-c','user.email=test@example.invalid',
                               '-c','commit.gpgsign=false',*args],env=env,check=True,capture_output=True)
    git('init');git('add','.');git('commit','-m','fixture')
    return root


def test_full_source_changes_cannot_keep_a_clean_commit_identity(source):
    """A modified non-metadata file invalidates exact backtest source provenance."""
    assert source_identity(source)['generation']=='pb9'
    (source/'.api-keys.json.pbgui-merge.lock').write_text('')
    assert source_identity(source)['commit']
    (source/'src/other.py').write_text('value = 2\n')
    with pytest.raises(RuntimeError,match='local changes'):
        source_identity(source)


def test_launch_environment_drops_credentials_and_python_injection(monkeypatch):
    """Native backtests receive only explicitly allowed locale/runtime variables."""
    for key in ('PYTHONPATH','PYTHONHOME','PYTHONUSERBASE','ANTHROPIC_API_KEY','EXCHANGE_API_SECRET'):
        monkeypatch.setenv(key,'must-not-propagate')
    env=launch_environment('/separate/venv/bin/python')
    assert all(key not in env for key in ('PYTHONPATH','PYTHONHOME','PYTHONUSERBASE','ANTHROPIC_API_KEY','EXCHANGE_API_SECRET'))
    assert env['PATH'].startswith('/separate/venv/bin'+os.pathsep)


def test_result_source_publication_preserves_exact_authored_snapshot(tmp_path):
    """Result-owned source remains available after the queue records are cleared."""
    queue=tmp_path/'data/bt_v9_queue';queue.mkdir(parents=True)
    result=tmp_path/'data/bt_v9_results/sample/job/binance/result';result.mkdir(parents=True)
    (result/'analysis.json').write_text('{}')
    config={'config_version':'v9.0.0','bot':{'long':{'enabled':True},'short':{}},
            'backtest':{'base_dir':str(tmp_path/'data/bt_v9_results/sample/job')}}
    raw=(json.dumps(config,indent=4)+'\n').encode()
    snapshot=queue/'job.config.json';snapshot.write_bytes(raw)
    record={'runtime':'pb9','schema':'v9.0.0','commit':'a'*40,'package_version':'8.2.0.dev0',
            'snapshot_sha256':hashlib.sha256(raw).hexdigest(),'filename':'job'}
    (queue/'job.json').write_text(json.dumps(record))
    publish_result_source(snapshot)
    snapshot.unlink();(queue/'job.json').unlink()
    assert (result/'pbgui.source.json').read_bytes()==raw
    assert json.loads((result/'pbgui.provenance.json').read_text())==record
    assert (result/'pbgui.source.json').stat().st_mode&0o777==0o600


def test_changed_snapshot_cannot_publish_false_result_provenance(tmp_path):
    """Publication must recheck the hash after native execution, not only before it."""
    queue=tmp_path/'data/bt_v9_queue';queue.mkdir(parents=True)
    snapshot=queue/'job.config.json';snapshot.write_text('{}')
    (queue/'job.json').write_text(json.dumps({'snapshot_sha256':'a'*64}))
    with pytest.raises(RuntimeError,match='snapshot changed'):
        publish_result_source(snapshot)


def test_offline_market_metadata_references_shared_files_without_copy(tmp_path):
    """PBGui public metadata is read through a link; source bytes/inodes stay unchanged."""
    from pb9_backtest_runtime import prepare_market_metadata
    data=tmp_path/'coindata';(data/'bybit').mkdir(parents=True)
    path=data/'bybit/ccxt_markets.json';path.write_text(json.dumps({'HYPE':{'symbol':'HYPE/USDT:USDT'}}))
    before=path.read_bytes();inode=path.stat().st_ino
    work=tmp_path/'work';work.mkdir()
    prepare_market_metadata(work,{'backtest':{'offline':True,'exchanges':['bybit'],'ohlcv_source_dir':'/shared/ohlcv'}},data,tmp_path/'no-bot')
    linked=work/'caches/bybit/markets.json'
    assert linked.is_symlink() and linked.samefile(path)
    assert path.read_bytes()==before and path.stat().st_ino==inode
    assert not (tmp_path/'no-bot').exists()


def test_offline_listing_metadata_uses_one_existing_versioned_bundle(tmp_path):
    """Native listing proof files are referenced together, not fabricated from local candle dates."""
    from pb9_backtest_runtime import prepare_market_metadata
    native=tmp_path/'native';cache=native/'caches';(cache/'bybit').mkdir(parents=True)
    (cache/'bybit/markets.json').write_text(json.dumps({'HYPE':{'symbol':'HYPE/USDT:USDT'}}))
    names=['first_ohlcv_timestamps_unified.version','first_ohlcv_timestamps_unified.json',
           'first_ohlcv_timestamps_unified_exchange_specific.json','first_ohlcv_timestamps_unified_exchange_specific_symbols.json']
    for name in names:(cache/name).write_text('2' if name.endswith('.version') else '{}')
    work=tmp_path/'work';work.mkdir()
    prepare_market_metadata(work,{'backtest':{'offline':True,'exchanges':['bybit']}},tmp_path/'no-coindata',native)
    for name in names:assert (work/'caches'/name).samefile(cache/name)


@pytest.mark.parametrize('online', [False,True])
def test_metadata_links_are_never_given_to_online_writers(tmp_path,online):
    """Online jobs obtain their own cache; unavailable offline metadata fails before launch."""
    from pb9_backtest_runtime import prepare_market_metadata
    work=tmp_path/'work';work.mkdir()
    config={'backtest':{'offline':not online,'exchanges':['bybit']}}
    if online:prepare_market_metadata(work,config,tmp_path/'missing',tmp_path/'native')
    else:
        with pytest.raises(ValueError,match='Offline market metadata missing for bybit'):
            prepare_market_metadata(work,config,tmp_path/'missing',tmp_path/'native')
    assert not (work/'caches').exists()


def test_shared_metadata_links_and_traversal_are_rejected(tmp_path):
    """Only server-selected regular cache files and canonical exchange names are accepted."""
    from pb9_backtest_runtime import prepare_market_metadata
    source=tmp_path/'coindata';(source/'bybit').mkdir(parents=True)
    outside=tmp_path/'outside.json';outside.write_text(json.dumps({'HYPE':{'symbol':'HYPE/USDT:USDT'}}))
    (source/'bybit/ccxt_markets.json').symlink_to(outside)
    work=tmp_path/'work';work.mkdir()
    for exchange in ['../bybit','bybit']:
        with pytest.raises(ValueError):prepare_market_metadata(work,{'backtest':{'offline':True,'exchanges':[exchange]}},source,tmp_path/'native')
