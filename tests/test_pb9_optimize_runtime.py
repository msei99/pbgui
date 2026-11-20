"""Native optimizer runs reuse the installed bot's durable cache like PB8."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import pb8_optimize_runner as runner
import pb9_backtest_runtime as runtime


def test_native_runs_reuse_the_same_bot_cache(tmp_path,monkeypatch):
    """Separate launches keep the inherited bot CWD, cached candles and saved proofs."""
    repo=tmp_path/'pb9';repo.mkdir()
    cache=repo/'caches/ohlcvs';cache.mkdir(parents=True)
    (cache/'catalog.fixture').write_text('existing proof')
    seen=[]
    async def native_main():
        """Act as the native optimizer, using its ordinary relative cache path."""
        assert Path.cwd()==repo
        assert Path('caches/ohlcvs/catalog.fixture').read_text()=='existing proof'
        marker=Path('caches/ohlcvs/candle.fixture')
        seen.append(marker.exists())
        marker.write_text('retained raw candle')
    original_import=runner.importlib.import_module
    monkeypatch.setattr(runner.importlib,'import_module',lambda name: SimpleNamespace(main=native_main)
                        if name=='optimize' else original_import(name))
    monkeypatch.setattr(runner,'acquire_master_runtime_lock',lambda *_:SimpleNamespace(release=lambda:None))
    monkeypatch.setattr(runtime,'source_identity',lambda *_:{'commit':'a'*40,'config_schema':'v9.0.0'})
    monkeypatch.setattr(runtime,'verify_native_extension',lambda path:None)
    monkeypatch.chdir(repo)
    for index in range(2):
        launch=tmp_path/f'launch-{index}';launch.mkdir()
        config=launch/'config.json';config.write_text('{}')
        options=launch/'options.json'
        options.write_text(json.dumps({'runtime_identity':{'runtime':'pb9','commit':'a'*40,'schema':'v9.0.0',
            'launch_sha256':hashlib.sha256(config.read_bytes()).hexdigest()}}))
        assert runner.main(['optimize',str(launch/'state.json'),str(launch/'owner.json'),
            str(launch/'ready'),str(repo/'src/optimize.py'),str(repo),str(config),str(options)],runtime='v9')==0
        assert not (launch/'work').exists()
    assert seen==[False,True]


def test_native_polish_argv_preserves_fraction_and_legacy_contract():
    """Only the native option spelling differs; stored fractions must not be divided again."""
    from pb8_optimize_runner import _optimizer_argv
    options={'polish_percentage':0.2,'polish_bounds_mode':'override-tunable'}
    native=_optimizer_argv('optimize.py','input.json',options,runtime='v9')
    legacy=_optimizer_argv('optimize.py','input.json',options)
    assert native==['optimize.py','input.json','--polish-bounds-fraction','0.2','--polish-bounds-mode','override-tunable']
    assert legacy==['optimize.py','input.json','--polish-pct','0.2','--polish-bounds-mode','override-tunable']
