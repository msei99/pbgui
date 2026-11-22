"""Native optimizer isolation and shared engine contracts with temporary data only."""

import copy
import hashlib
import json
from pathlib import Path
from time import perf_counter

from fastapi import HTTPException
import pytest

from api import optimize_v9
from api.optimize_engine import OptimizeEngine
from optimize_profile import OptimizeProfile
from pb9_config import PB9CancelSignal


@pytest.fixture
def native_optimizer(tmp_path, monkeypatch):
    """Use real shared persistence/locks and replace only the native interpreter boundary."""
    import pb9_optimize_adapter as adapter
    import pb9_config_storage as storage
    metadata=json.loads((Path(__file__).parent/'fixtures/pb9_optimize_metadata.json').read_text())
    engine=optimize_v9.NativeOptimizeEngine();engine.PBGDIR=tmp_path
    monkeypatch.setattr(optimize_v9,'engine',engine)
    paths={'pb9dir':str(tmp_path/'source'),'pb9venv':str(tmp_path/'env/bin/python')}
    monkeypatch.setattr(engine.native,'paths',lambda:paths)
    monkeypatch.setattr(engine.native,'metadata',lambda **kwargs:copy.deepcopy(metadata))
    engine.get_pb8_optimize_metadata=engine.native.metadata
    runtime={**paths,'ready':True,'detected':True,'commit':'a'*40,'config_schema':'v9.0.0','version':'8.2.0.dev0',
             'source_clean':True,'source_error':''}
    monkeypatch.setattr(engine.native,'runtime',lambda **_kwargs:copy.deepcopy(runtime))
    engine.pb8_runtime_status=engine.native.runtime
    def validate(document,*args,**kwargs):
        """Require the explicit native Optimize target and preserve sparse authored values."""
        assert kwargs['validation_target']=='optimize'
        value=copy.deepcopy(document)
        assert value['config_version']=='v9.0.0'
        gui=value.pop('pbgui',None)
        return {'config':value,'pbgui':gui,'source':runtime}
    monkeypatch.setattr(adapter,'validate_pb9_document',validate)
    monkeypatch.setattr(storage,'validate_pb9_document',validate)
    monkeypatch.setattr(engine.subprocess,'Popen',lambda *args,**kwargs:pytest.fail('native execution during offline test'))
    monkeypatch.setattr(engine,'load_ini_section',lambda section:{})
    monkeypatch.setattr(engine,'save_ini_section',lambda *args:pytest.fail('unexpected settings write'))
    return engine,metadata,runtime


def test_native_and_legacy_engines_share_implementation_without_runtime_state(native_optimizer):
    """Sharing code never shares mutable caches, workers, paths or native hooks."""
    native,_,_=native_optimizer
    legacy=OptimizeEngine();legacy.PBGDIR=native.PBGDIR
    assert native._configs_dir()!=legacy._configs_dir()
    assert native._queue_dir()!=legacy._queue_dir()
    assert native._dash_sessions is not legacy._dash_sessions
    assert native._backtest_count_scans is not legacy._backtest_count_scans
    assert native._worker is not legacy._worker
    assert native._worker.engine is native and legacy._worker.engine is legacy
    assert native._QUEUE_SETTINGS_SECTION=='optimize_v9'
    assert legacy._QUEUE_SETTINGS_SECTION=='optimize_v7'
    assert native.profile.text('/ws/opt8')=='/ws/opt9'


def test_native_config_list_is_subsecond_without_runtime_or_result_work(native_optimizer, monkeypatch):
    """List cold config summaries without native helpers, scans or a configured bot."""
    engine, metadata, _ = native_optimizer
    for index in range(20):
        engine.save_config(f'fixture-{index:02}', {'config': copy.deepcopy(metadata['template'])}, session=None)
    engine._config_summary_cache.clear()
    def forbidden(*args, **kwargs):
        """Fail if lightweight browsing starts unrelated expensive work."""
        pytest.fail('config list attempted native runtime/result work')
    for method in ('metadata', 'runtime', 'load', 'prepare', 'effective'):
        monkeypatch.setattr(engine.native, method, forbidden)
    monkeypatch.setattr(engine, '_list_results', forbidden)
    monkeypatch.setattr(engine, '_backtests_root', forbidden)
    started = perf_counter()
    rows = engine.list_configs(None, include_result_summary=False, include_backtest_counts=False)['configs']
    assert perf_counter() - started < 1.0
    assert len(rows) == 20
    assert all(row['backtest_count_status'] == 'pending' for row in rows)


@pytest.mark.parametrize('version', ['v8', 'v9'])
def test_shared_config_list_never_reads_private_transaction_directories(tmp_path, monkeypatch, version):
    """Concurrent atomic-save stages/backups are not user configs or validation errors."""
    from optimize_profile import OptimizeProfile
    engine = OptimizeEngine(OptimizeProfile(version)) if version == 'v8' else optimize_v9.NativeOptimizeEngine()
    engine.PBGDIR = tmp_path
    root = engine._configs_dir()
    root.mkdir(parents=True)
    source = {'backtest': {'exchanges': ['bybit']}, 'bot': {}, 'live': {}}
    public = root / 'PB8_named_config'
    public.mkdir()
    (public / engine._CONFIG_FILENAME).write_text(json.dumps(source))
    hidden = []
    for suffix in ('stage', 'backup'):
        directory = root / (f'.PB8_named_config.{suffix}-' + 'a' * 32)
        directory.mkdir()
        (directory / engine._CONFIG_FILENAME).write_text('not a published document')
        hidden.append(directory)
    logs = []
    monkeypatch.setattr(engine, '_log', lambda *args, **kwargs: logs.append(args))
    original = engine._validate_name
    def validate(name):
        """The public validator must never even receive an internal transaction name."""
        assert not name.startswith('.')
        return original(name)
    monkeypatch.setattr(engine, '_validate_name', validate)
    result = engine.list_configs(None, include_result_summary=False, include_backtest_counts=False)
    assert [row['name'] for row in result['configs']] == ['PB8_named_config']
    assert logs == []
    assert all(directory.is_dir() for directory in hidden)


@pytest.mark.parametrize('version', ['v8', 'v9'])
def test_shared_optimizer_errors_name_the_owning_generation(tmp_path, monkeypatch, version):
    """Shared diagnostics identify PB9 while preserving names containing PB8."""
    from optimize_profile import OptimizeProfile
    from pb8_config import PB8ConfigurationError
    engine = OptimizeEngine(OptimizeProfile(version)) if version == 'v8' else optimize_v9.NativeOptimizeEngine()
    engine.PBGDIR = tmp_path
    messages = []
    monkeypatch.setattr(engine, '_log', lambda service, message, **kwargs: messages.append(message))
    error = engine._configuration_error(f'Queueing {engine.label} optimize config PB8_keep_name', PB8ConfigurationError('native failure'))
    assert error.status_code == 422
    assert messages == [f'Queueing {engine.label} optimize config PB8_keep_name failed: native failure']
    engine._configuration_error('Saving optimize config PB8_keep_name', PB8ConfigurationError('native failure'))
    assert messages[-1] == 'Saving optimize config PB8_keep_name failed: native failure'
    def unavailable(**kwargs):
        """Exercise the real shared metadata route's generation-labelled failure path."""
        raise engine.PB8ConfigurationError('native_failure')
    monkeypatch.setattr(engine, 'get_pb8_optimize_metadata', unavailable)
    with pytest.raises(HTTPException):
        engine._load_optimizer_metadata()
    assert messages[-1] == f'Loading {engine.label} optimize metadata failed: {engine.PB8ConfigurationError("native_failure")}'


def test_native_cli_options_survive_snapshot_and_result_resume(native_optimizer):
    """Freeze scenario selection in the queue and restore it from the result sidecar."""
    from pb9_optimize_options import OPTIONS_FILENAME
    engine, metadata, _ = native_optimizer
    metadata['native_cli_options'] = ['scenarios', 'filter_starting_configs', 'compress_starting_configs', 'preparation_manifest']
    source = copy.deepcopy(metadata['template'])
    source['backtest'].update(suite_enabled=True, scenarios=[{'label': 'train'}, {'label': 'holdout'}])
    source['pbgui'] = {'optimize_runtime': {'scenarios': ['train']}}
    engine.save_config('native-cli', {'config': source}, session=None)
    filename = engine.add_to_queue({'name': 'native-cli'}, session=None)['filename']
    record = engine._read_json(engine._queue_file(filename))
    assert record['launch_options']['scenarios'] == ['train']
    stored = engine._read_json(engine._snapshot_file(filename))
    assert stored['pbgui']['optimize_runtime']['scenarios'] == ['train']
    result = engine.PBGDIR/'result'; result.mkdir()
    (result/OPTIONS_FILENAME).write_text(json.dumps(record['launch_options']))
    assert engine._result_launch_options(result, stored)['scenarios'] == ['train']
    assert engine._result_launch_options(result, stored)['mode'] == 'checkpoint_resume'
    (result/OPTIONS_FILENAME).write_text('[]')
    with pytest.raises(HTTPException, match='Invalid native result launch options'):
        engine._result_launch_options(result, stored)
    stored['backtest']['scenarios'] = []
    with pytest.raises(HTTPException):
        engine._validate_options_for_config(record['launch_options'], stored)


def test_native_cli_unknown_capability_is_rejected_before_queue(native_optimizer):
    """Old native parsers cannot silently ignore newly selected launch controls."""
    engine, metadata, _ = native_optimizer
    source = copy.deepcopy(metadata['template'])
    source['backtest']['scenarios'] = [{'label': 'train'}]
    source['pbgui'] = {'optimize_runtime': {'scenarios': ['train']}}
    with pytest.raises(HTTPException, match='does not advertise'):
        engine.save_config('unsupported-cli', {'config': source}, session=None)


def test_native_manifest_import_is_small_in_config_and_frozen_in_queue(native_optimizer):
    """The authenticated import route stores content; queue options capture verified bytes."""
    engine, metadata, _ = native_optimizer
    metadata['native_cli_options'] = ['preparation_manifest']
    text = json.dumps({'schema_version':1, 'kind':'prepared-inputs', 'consumer':'optimize',
        'suite':{}, 'datasets':[{'manifest_present':True, 'manifest_sha256':'a'*64}], 'cases':[{}]})
    reference = optimize_v9.import_preparation_manifest({'name':'prepared.json', 'text':text}, None)['manifest']
    source = copy.deepcopy(metadata['template'])
    source['pbgui'] = {'optimize_runtime':{'preparation_manifest':reference}}
    engine.save_config('manifest', {'config':source}, session=None)
    filename = engine.add_to_queue({'name':'manifest'}, session=None)['filename']
    snapshot = engine._read_json(engine._snapshot_file(filename))
    assert snapshot['pbgui']['optimize_runtime']['preparation_manifest'] == reference
    frozen = engine._read_json(engine._queue_file(filename))['launch_options']['preparation_manifest']
    assert frozen['text'] == text
    (engine._manifest_root()/(reference['artifact'] + '.json')).unlink()
    assert engine._validate_launch_options({'preparation_manifest':frozen})['preparation_manifest']['text'] == text
    with pytest.raises(HTTPException, match='missing or invalid'):
        engine._runtime_options_from_config(snapshot)


def test_native_save_and_queue_use_shared_gateway_and_exact_provenance(native_optimizer):
    """Native source stays sparse and its immutable queue snapshot binds the selected runtime."""
    engine,metadata,runtime=native_optimizer
    source=copy.deepcopy(metadata['template'])
    source['bot']['short']={'enabled':False}
    source['pbgui']={'execution':'local','note':'native preference'}
    engine.save_config('native',{'config':source},session=None)
    saved=engine.native.load(engine._config_file('native'))
    assert saved['bot']['short']=={'enabled':False}
    result=engine.add_to_queue({'name':'native','operation_id':'owned-request'},session=None)
    filename=result['filename'];record=engine._read_json(engine._queue_file(filename))
    assert record['runtime']=='pb9' and record['commit']==runtime['commit']
    assert record['schema']=='v9.0.0'
    assert record['snapshot_sha256']==hashlib.sha256(engine._snapshot_file(filename).read_bytes()).hexdigest()
    assert engine._read_json(engine._snapshot_file(filename))['bot']['short']=={'enabled':False}
    assert 'pbgui' not in engine._execution_config(saved)
    assert source['pbgui']['note']=='native preference'


@pytest.mark.parametrize('changed',['snapshot','commit','schema','runtime','source_sha256'])
def test_native_launch_identity_rejects_changed_or_legacy_input(native_optimizer,changed):
    """No native launch can accept a mismatched generation, schema, source or snapshot."""
    engine,metadata,runtime=native_optimizer
    engine.save_config('native',{'config':metadata['template']},session=None)
    filename=engine.add_to_queue({'name':'native'},session=None)['filename']
    record=engine._read_json(engine._queue_file(filename));snapshot=engine._snapshot_file(filename)
    if changed=='snapshot':snapshot.write_bytes(snapshot.read_bytes()+b' ')
    else:record[changed]='changed'
    with pytest.raises(HTTPException) as exc:
        engine._verify_queue_identity(record,runtime,snapshot)
    assert exc.value.status_code==409


def test_native_owned_dispatch_propagates_cancel_and_validates_arguments(native_optimizer,monkeypatch):
    """The shared PB9 worker selects an allowlisted engine operation with a request stop owner."""
    engine,_,_=native_optimizer;cancel=PB9CancelSignal()
    def metadata(session=None):
        """Observe the actual operation context rather than a module-global override."""
        assert engine.native.stop_flag() is cancel
        return {'native':True}
    monkeypatch.setattr(engine,'get_metadata',metadata)
    assert optimize_v9._operation('opt9_get_metadata',cancel,json.dumps({'args':{},'body':None}).encode())=={'native':True}
    assert engine.native.stop_flag() is engine.native.cancel
    with pytest.raises(HTTPException):
        optimize_v9._operation('opt9_get_metadata',cancel,json.dumps({'args':{'unsafe':True},'body':None}).encode())


def test_native_process_environment_excludes_credentials_and_python_injection(native_optimizer,monkeypatch):
    """Both direct and systemd launches use the existing native safe environment policy."""
    engine,_,_=native_optimizer
    monkeypatch.setenv('PYTHONPATH','/untrusted')
    monkeypatch.setenv('EXCHANGE_API_SECRET','fixture-only-secret')
    environment=engine._optimizer_environment('/private/env/bin/python')
    assert 'PYTHONPATH' not in environment and 'EXCHANGE_API_SECRET' not in environment
    command=engine._optimizer_command(['python','-I','native.py'],environment)
    assert command[:2]==['/usr/bin/env','-i']
    assert not any('fixture-only-secret' in value for value in command)
    assert engine._runner_python_flags()==['-I','-B']


def test_unknown_optimizer_generation_never_defaults_to_legacy():
    """An unsupported profile fails before constructing any queue or worker state."""
    with pytest.raises(ValueError):OptimizeProfile('v10')


def test_native_settings_use_exchange_capabilities_not_template_selection(native_optimizer):
    """Unselected supported venues remain available without altering the template defaults."""
    engine,metadata,_=native_optimizer
    choices=engine.get_settings(session=None)['exchange_options']
    assert choices==metadata['exchange_options']['optimize']
    assert 'hyperliquid' in choices and 'okx' in choices
    assert metadata['template']['backtest']['exchanges']==['binance','bybit']


@pytest.mark.parametrize('preference', [None, 'True', 'False'])
def test_new_native_draft_shows_selected_candle_source(native_optimizer, monkeypatch, preference):
    """PBGui data is the visible default; an explicit native-source choice survives."""
    engine, metadata, _ = native_optimizer
    original = copy.deepcopy(metadata)
    monkeypatch.setattr(engine, 'load_ini_section', lambda section: (
        {} if preference is None else {'use_pbgui_market_data': preference}))
    monkeypatch.setattr(engine, 'get_pbgui_data_path', lambda session: {'path': str(engine.PBGDIR/'candles')})
    result = engine.new_config(session=None)
    expected = (metadata['template']['backtest'].get('ohlcv_source_dir') if preference == 'False'
                else str(engine.PBGDIR/'candles'))
    assert result['config']['backtest'].get('ohlcv_source_dir') == expected
    assert metadata == original


@pytest.mark.parametrize('namespace,status',[('backtests/pbgui/..',400),('backtests/pbgui/a/../../other',422),
    ('/backtests/pbgui/native',422),('backtests/pbgui/a\\b',400),('backtests/pbgui/unsafe\x00',400)])
def test_native_execution_namespace_rejects_unsafe_names(native_optimizer,namespace,status):
    """Launch artifacts cannot redirect the shared result namespace through a saved path."""
    engine,metadata,_=native_optimizer
    config=copy.deepcopy(metadata['template']);config['backtest']['base_dir']=namespace
    with pytest.raises(HTTPException) as exc:engine._execution_config(config)
    assert exc.value.status_code==status


def test_sparse_native_execution_uses_installed_defaults_without_saving_them(native_optimizer):
    """Backend/write-all requirements follow native defaults even in sparse authored configs."""
    engine,metadata,_=native_optimizer
    config={'optimize':{}}
    for key in ('backend','write_all_results'):
        assert engine._effective_optimize_value(config,key)==metadata['optimize_defaults'][key]
    assert config=={'optimize':{}}
    assert engine._effective_optimize_value({'optimize':{'write_all_results':False}},'write_all_results') is False


def test_resume_and_convergence_use_the_native_effective_policy(native_optimizer,monkeypatch):
    """Inline coin overrides participate in both identities; no PB8 projection is consulted."""
    engine,metadata,_=native_optimizer
    def effective(config):
        """Represent the separately tested native helper boundary without running its interpreter."""
        return {'bot':copy.deepcopy(config['bot']),'coin_overrides':copy.deepcopy(config.get('coin_overrides',{}))}
    monkeypatch.setattr(engine.native,'effective',effective)
    config=copy.deepcopy(metadata['template']);config['coin_overrides']={'BTC':{'bot':{'long':{'enabled':False}}}}
    first=engine._convergence_identity(config)
    assert engine._resume_compatibility_fields(config)['coin_overrides']==config['coin_overrides']
    config['coin_overrides']['BTC']['bot']['long']['enabled']=True
    assert engine._convergence_identity(config)!=first


def test_pareto_handoff_uses_native_result_extraction_and_preserves_original(native_optimizer, monkeypatch):
    """A managed result is converted before config consumers see runtime dataset membership."""
    import pb9_optimize_adapter as adapter
    engine, metadata, runtime = native_optimizer
    record = copy.deepcopy(metadata['template'])
    record['backtest']['coins'] = {'bybit': ['HYPE']}
    record['metrics'] = {'objectives': {'fixture': 0.2}}
    record['pbgui'] = {'note': 'retained'}
    path = engine._results_root() / 'fixture-result' / 'pareto' / 'candidate.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(record))
    before = path.read_bytes()
    calls = []

    def extract(document, *args, **kwargs):
        """Represent the separately exercised isolated native result reader."""
        calls.append((copy.deepcopy(document), kwargs))
        assert kwargs['validation_target'] == 'result'
        config = copy.deepcopy(document)
        gui = config.pop('pbgui')
        config.pop('metrics')
        config['backtest'].pop('coins')
        return {'config': config, 'pbgui': gui, 'source': runtime}

    monkeypatch.setattr(adapter, 'validate_pb9_document', extract)
    response = engine.get_pareto_file(str(path), session=None)
    assert len(calls) == 1 and calls[0][0] == record
    assert response['config']['config_version'] == 'v9.0.0'
    assert response['config']['bot'] == record['bot']
    assert response['config']['backtest'] == metadata['template']['backtest']
    assert response['config']['pbgui'] == record['pbgui']
    assert response['override_configs'] == {}
    assert path.read_bytes() == before


def test_native_queue_workload_shares_counting_without_loading_pb8(native_optimizer, monkeypatch):
    """Native training plans count once and cache independently of legacy source loaders."""
    import pb8_config
    import pb9_optimize_adapter as adapter
    engine, metadata, runtime = native_optimizer
    root = engine._queue_dir()
    root.mkdir(parents=True, exist_ok=True)
    path = root / 'workload.json'
    path.write_text('{}')
    source = copy.deepcopy(metadata['template'])
    source['pbgui'] = {'note': 'GUI only'}
    loaded, planned = [], []
    monkeypatch.setattr(pb8_config, 'load_pb8_config', lambda *_: pytest.fail('PB9 called PB8 helper'))
    monkeypatch.setattr(engine.native, 'load', lambda path: loaded.append(path) or copy.deepcopy(source))

    def plan(config, *args, **kwargs):
        """Supply native-resolved per-scenario universes and exchange choices."""
        planned.append(config)
        assert 'pbgui' not in config and config['config_version'] == 'v9.0.0'
        assert args[0] == runtime['pb9dir']
        def window(start, end, exchanges, coins):
            """Match the existing native export's data-plan representation."""
            return {'backtest': {'start_date': start, 'end_date': end,
                    'exchanges': exchanges, 'suite_enabled': True},
                    'live': {'approved_coins': {'long': coins, 'short': coins}}}
        return {'data_plans': [
            window('2026-01-01', '2026-12-31', ['bybit', 'hyperliquid'], ['HYPE']),
            window('2026-01-01', '2026-01-01', ['bybit', 'hyperliquid'], ['HYPE']),
            window('2026-01-03', '2026-01-04', ['bybit'], ['BTC', 'ETH'])]}

    monkeypatch.setattr(adapter, 'get_pb9_optimize_cloud_plan', plan)
    assert engine.estimate_snapshot.__self__ is engine.native
    assert engine.estimate_snapshot(path, root) == 8640
    assert engine.estimate_snapshot(path, root) == 8640
    assert len(loaded) == len(planned) == 1
    runtime['commit'] = 'b' * 40
    assert engine.estimate_snapshot(path, root) == 8640
    assert len(loaded) == len(planned) == 2
    assert engine.estimate_snapshot(path, root / 'other') is None
    assert len(loaded) == 2


def test_dynamic_native_workload_is_unknown_without_requesting_a_concrete_plan(native_optimizer, monkeypatch):
    """Unresolved all-coins policy stays unknown instead of causing repeated helper failures."""
    import pb9_optimize_adapter as adapter
    engine, metadata, _runtime = native_optimizer
    source = copy.deepcopy(metadata['template'])
    source['bot']['global']['universe']['approved_coins'] = 'all'
    root = engine._queue_dir()
    root.mkdir(parents=True, exist_ok=True)
    path = root / 'dynamic.json'
    path.write_text('{}')
    monkeypatch.setattr(engine.native, 'load', lambda _: copy.deepcopy(source))
    monkeypatch.setattr(adapter, 'get_pb9_optimize_cloud_plan', lambda *_args, **_kwargs: pytest.fail('dynamic plan requested'))
    assert engine.estimate_snapshot(path, root) is None
