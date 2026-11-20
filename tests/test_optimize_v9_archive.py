"""Native configs reuse the shared archive transaction and collision workflow."""
from pathlib import Path

from api import backtest_v7, optimize_v9
from test_optimize_v9_api import native_optimizer


def test_native_optimize_archive_roundtrip(native_optimizer,monkeypatch,tmp_path):
    """Export/view/import preserve sparse native policy and select only the v9 slot."""
    engine,metadata,runtime=native_optimizer
    monkeypatch.setattr(optimize_v9,'engine',engine)
    archives=tmp_path/'archives';archive=archives/'mine';archive.mkdir(parents=True)
    monkeypatch.setattr(backtest_v7,'_archives_dir',lambda:archives)
    monkeypatch.setattr(backtest_v7,'_own_archive_name',lambda:'mine')
    monkeypatch.setattr(backtest_v7,'maybe_migrate_own_archive',lambda *args,**kwargs:{'status':{'status':'current'}})
    engine.save_config('source',{'config':metadata['template']},session=None)
    exported=backtest_v7._add_optimize_config_to_archive_sync('mine','source','v9')
    path=Path(exported['path'])
    assert path.relative_to(archive).parts[:4]==('pbgui','configs','v9.0.0','optimize')
    assert backtest_v7.list_archive_optimize('mine',version='v8',session=None)['configs']==[]
    assert len(backtest_v7.list_archive_optimize('mine',version='v9',session=None)['configs'])==1
    source=backtest_v7.get_archive_optimize_config('mine',str(path),version='v9',session=None)
    assert source['bot']==metadata['template']['bot']
    imported=backtest_v7.import_archive_optimize_config('mine',{'path':str(path),'name':'restored','optimize_version':'v9'},session=None)
    assert imported['optimize_version']=='v9'
    assert engine.native.load(engine._config_file('restored'))['bot']==source['bot']
