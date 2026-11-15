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
