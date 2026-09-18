"""Offline scenario-aware cloud export, validation and frozen job regressions."""
import copy
import json
from pathlib import Path

import pytest

from setup.vast_gpu_benchmark.prepare import select_shards
from vast_config_validation import validate_cloud_config
from vast_exchanges import CCXT_EXCHANGES, quote_currency
from vast_jobs import JobStore, native_job_config
from vast_scenarios import scenario_plan


@pytest.fixture(autouse=True)
def native_warmup(monkeypatch):
    """The scenario fixtures require 31 days of native optimizer warmup."""
    monkeypatch.setattr('pb8_config._call_helper', lambda operation, **payload:
                        {'minutes': [31 * 1440] * len(payload['configs'])})


def test_requeue_rebuilds_history_instead_of_reusing_old_bundle(config, tmp_path, monkeypatch):
    """Requeue invokes the real exporter; repeated clicks reuse only its new job."""
    import api.vast as api
    from vast_queue import CloudQueue
    from vast_jobs import write_json

    store = JobStore(tmp_path / 'vast')
    queue = CloudQueue(store)
    old = store.create_preparation('suite', 512, 4, False)
    store.update(old['id'], status='cancelled')
    legacy = {'files': [{'path': 'ohlcv/binance/1m/BTC_USDT:USDT/2019-01-01.npy'}]}
    write_json(store.directory(old['id']) / 'input/manifest.json', legacy)
    make_market(tmp_path, 'binance', ['BTC'])
    monkeypatch.setattr('pb8_config.save_prepared_pb8_config',
                        lambda value, path: path.write_text(json.dumps(value)))
    monkeypatch.setattr(api, 'CloudQueue', lambda: queue)

    def prepare(body, *, requeue_from):
        """Build a real replacement using an isolated saved-config equivalent."""
        return store.prepare(body.config_name, config, 'source', tmp_path / 'raw',
                             tmp_path / 'mapping', tmp_path / 'results',
                             body.iterations, body.workers, body.use_adg,
                             requeue_from=requeue_from)

    monkeypatch.setattr(api, '_prepare_job', prepare)
    row = api.requeue_job(old['id'], object())
    assert row['id'] != old['id'] and row['status'] == 'ready'
    manifest = store.read(row['id'], 'input/manifest.json')
    assert {Path(item['path']).stem for item in manifest['files']} == {'2023-12-01', '2024-03-01'}
    assert store.read(old['id'], 'input/manifest.json') == legacy
    assert api.requeue_job(old['id'], object())['id'] == row['id']


@pytest.fixture
def config():
    """Use explicit common-side coin lists and canonical PB8 override paths."""
    return {'live': {'strategy_kind': 'ema_anchor', 'approved_coins': {'long': ['BTC'], 'short': ['BTC']},
                     'ignored_coins': {'long': [], 'short': []}, 'hedge_mode': True},
            'bot': {side: {'risk': {'n_positions': 3, 'total_wallet_exposure_limit': 1.0},
                           'hsl': {'enabled': False}} for side in ('long', 'short')},
            'backtest': {'suite_enabled': True, 'exchanges': ['binance'], 'start_date': '2024-01-01',
                         'end_date': '2024-12-31', 'starting_balance': 1000,
                         'scenarios': [{'label': 'base'}]},
            'optimize': {'iters': 512, 'n_cpus': 4, 'scoring': [{'metric': 'adg_strategy_eq', 'goal': 'max'}], 'limits': []}}


def make_market(tmp_path, exchange, coins, *, missing=()):
    """Provide separate local mappings and daily shards without accessing real data."""
    quote = quote_currency(exchange)
    mapping = tmp_path / 'mapping' / exchange
    mapping.mkdir(parents=True, exist_ok=True)
    rows = []
    for coin in coins:
        symbol = f'{coin}/{quote}:{quote}'
        rows.append(dict(coin=coin, quote=quote, swap=True, linear=True, ccxt_symbol=symbol))
        if coin not in missing:
            folder = tmp_path / 'raw' / CCXT_EXCHANGES[exchange] / '1m' / symbol.replace('/', '_')
            folder.mkdir(parents=True, exist_ok=True)
            for date in ('2023-12-01', '2024-03-01', '2025-01-01'):
                (folder / (date + '.npy')).write_bytes(b'isolated candle fixture')
    (mapping / 'mapping.json').write_text(json.dumps(rows))


def test_suite_exports_added_coins_and_exchanges_once(config, tmp_path):
    """Pack a scenario-only venue and coin, preserve full warmup and deduplicate shared data."""
    config['backtest']['scenarios'] = [
        {'label': 'base', 'exchanges': [], 'overrides': {}, 'coin_sources': {}, 'ignored_coins': []},
        {'label': 'bybit-sol', 'exchanges': ['bybit'], 'coins': ['SOL'], 'start_date': '2025-01-01', 'end_date': '2025-01-01'},
        {'label': 'repeat', 'exchanges': ['bybit'], 'coins': ['SOL'],
         'overrides': {'bot.long.risk.n_positions': 5}}]
    before = copy.deepcopy(config)
    make_market(tmp_path, 'binance', ['BTC'])
    make_market(tmp_path, 'bybit', ['BTC', 'SOL'])
    assert validate_cloud_config(config) == []
    shards = select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')
    assert len(shards) == 9
    assert len({relative for _, relative in shards}) == len(shards)
    assert any(relative == Path('bybit/1m/SOL_USDT:USDT/2023-12-01.npy') for _, relative in shards)
    assert config == before
    prepared = native_job_config(config, 512, 4, False)
    assert prepared['backtest']['scenarios'] == before['backtest']['scenarios']
    assert prepared['backtest']['exchanges'] == ['binance']


def test_missing_scenario_history_identifies_consumer(config, tmp_path):
    """Report the exact extra dataset rather than a generic unsupported-scenario error."""
    config['backtest']['scenarios'] = [{'label': 'bybit-sol', 'exchanges': ['bybit'], 'coins': ['SOL']}]
    make_market(tmp_path, 'binance', ['BTC'])
    make_market(tmp_path, 'bybit', ['BTC', 'SOL'], missing=['SOL'])
    with pytest.raises(ValueError, match="Scenario 1 'bybit-sol'.*bybit/SOL.*Download 1m"):
        select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')


def test_missing_scenario_mapping_identifies_coin_and_venue(config, tmp_path):
    """A missing coin mapping is attributed to its scenario and permitted venues."""
    config['backtest']['scenarios'] = [{'label': 'extra', 'exchanges': ['bybit'], 'coins': ['SOL']}]
    make_market(tmp_path, 'binance', ['BTC'])
    make_market(tmp_path, 'bybit', ['BTC'])
    with pytest.raises(ValueError, match="Scenario 1 'extra'.*SOL on bybit"):
        select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')


def test_forced_source_does_not_require_another_venues_history(config, tmp_path):
    """Suite-wide coin_sources restrict data collection to the assigned source."""
    config['backtest']['exchanges'] = ['binance', 'bybit']
    config['backtest']['scenarios'] = [{'label': 'forced', 'coins': ['SOL'], 'coin_sources': {'SOL': 'bybit'}}]
    make_market(tmp_path, 'binance', ['BTC', 'SOL'], missing=['SOL'])
    make_market(tmp_path, 'bybit', ['BTC', 'SOL'])
    assert validate_cloud_config(config) == []
    shards = select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')
    assert {p.parts[0] for _, p in shards if 'SOL_' in str(p)} == {'bybit'}


def test_conflicting_sources_are_rejected_with_scenario_identity(config):
    """Match PB8's single shared assignment instead of silently picking a source."""
    config['backtest']['scenarios'] = [
        {'label': 'first', 'coin_sources': {'BTC': 'binance'}},
        {'label': 'second', 'coin_sources': {'BTC': 'bybit'}}]
    errors = validate_cloud_config(config)
    assert any("Scenario 2 'second'" in error['message'] and 'consistent source' in error['message'] for error in errors)


def test_overrides_are_applied_to_validation_without_mutating_config(config):
    """Validate nested and dotted parameter overrides on independent scenario copies."""
    config['backtest']['scenarios'] = [
        {'label': 'nested', 'overrides': {'bot': {'long': {'risk': {'n_positions': 5}}}}},
        {'label': 'balance', 'overrides': {'backtest.starting_balance': 2000}},
        {'label': 'hsl', 'overrides': {'bot.short.hsl.enabled': True}}]
    before = copy.deepcopy(config)
    contexts, _, _ = scenario_plan(config)
    assert contexts[0]['config']['bot']['long']['risk']['n_positions'] == 5
    assert contexts[1]['config']['backtest']['starting_balance'] == 2000
    errors = validate_cloud_config(config)
    assert errors == []
    assert contexts[2]['config']['bot']['short']['hsl']['enabled'] is True
    assert config == before


@pytest.mark.parametrize('overrides,expected', [
    ({'backtest.ohlcv_source_dir': '/other'}, 'outside the cloud GPU'),
    ({'live.approved_coins.long': ['SOL']}, 'scenario coins/exchanges/date'),
    ({'bot.long.risk.unknown': 1}, 'does not exist'),
    ({'bot.long.risk.n_positions': 'bad'}, 'compatible finite scalar'),
    ([], 'overrides must be an object'),
    ({'bot': {'long': {'risk': {'n_positions': 5}}}, 'bot.long.risk.n_positions': 6}, 'duplicate override'),
])
def test_actual_override_restrictions_are_specific(config, overrides, expected):
    """Keep unsupported filesystem/data redirection and invalid values explicit."""
    config['backtest']['scenarios'] = [{'label': 'bad', 'overrides': overrides}]
    errors = validate_cloud_config(config)
    assert any(expected in item['message'] for item in errors)


def test_coin_limit_is_per_scenario_not_combined_universe(config):
    """Two valid 40-coin scenarios must not trip a 64-coin limit on their union."""
    config['live']['approved_coins'] = {side: ['C' + str(i) for i in range(80)] for side in ('long', 'short')}
    config['backtest']['scenarios'] = [
        {'label': 'a', 'coins': ['C' + str(i) for i in range(40)]},
        {'label': 'b', 'coins': ['C' + str(i) for i in range(40, 80)]}]
    assert validate_cloud_config(config) == []
    config['backtest']['scenarios'][1]['coins'] = None
    assert any("Scenario 2 'b'" in item['message'] and '80' in item['message'] for item in validate_cloud_config(config))


def test_null_coins_inherit_but_empty_coins_do_not(config):
    """Follow PB8's explicit empty-selection semantics instead of silently widening it."""
    config['backtest']['scenarios'] = [{'label': 'inherited', 'coins': None, 'exchanges': [], 'coin_sources': {}, 'overrides': {}}]
    assert validate_cloud_config(config) == []
    config['backtest']['scenarios'][0]['coins'] = []
    assert any('[] selects no coins' in item['message'] for item in validate_cloud_config(config))


def test_disabled_scenarios_do_not_expand_export(config, tmp_path):
    """Inactive draft scenarios must not request their extra data."""
    config['backtest'].update(suite_enabled=False, scenarios=[{'label': 'inactive', 'coins': ['SOL'], 'exchanges': ['bybit']}])
    make_market(tmp_path, 'binance', ['BTC'])
    assert {p.parts[0] for _, p in select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')} == {'binance'}


def test_frozen_job_stages_every_exported_exchange(config, tmp_path, monkeypatch):
    """Persist added scenario exchanges for market metadata and inception staging."""
    import pb8_config
    monkeypatch.setattr(pb8_config, 'save_prepared_pb8_config', lambda value, path: path.write_text(json.dumps(value)))
    config['backtest']['scenarios'] = [{'label': 'extra', 'coins': ['SOL'], 'exchanges': ['bybit']}]
    make_market(tmp_path, 'binance', ['BTC'])
    make_market(tmp_path, 'bybit', ['BTC', 'SOL'])
    store = JobStore(tmp_path / 'vast')
    row = store.prepare('suite', config, 'source', tmp_path / 'raw', tmp_path / 'mapping', tmp_path / 'results', 512, 4, False)
    assert row['status'] == 'ready' and row['exchanges'] == ['binance', 'bybit']
    manifest = store.read(row['id'], 'input/manifest.json')
    assert any(item['path'].startswith('ohlcv/bybit/1m/SOL_') for item in manifest['files'])
    saved = json.loads((store.directory(row['id']) / 'input/optimize.json').read_text())
    assert saved['backtest']['exchanges'] == ['binance']
    assert saved['backtest']['scenarios'] == config['backtest']['scenarios']


def test_nested_overrides_are_flattened_only_in_frozen_copy(config):
    """The worker GPU preflight receives dotted paths; the user's draft is untouched."""
    config['backtest']['scenarios'] = [{'label': 'nested', 'overrides': {'bot': {'long': {'risk': {'n_positions': 5}}}}}]
    before = copy.deepcopy(config)
    prepared = native_job_config(config, 512, 4, False)
    assert prepared['backtest']['scenarios'][0]['overrides'] == {'bot.long.risk.n_positions': 5}
    assert config == before
