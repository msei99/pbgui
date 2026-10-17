"""Offline prompt-builder choices preserve canonical cached market identifiers."""
import json

from ai_loop_templates import local_markets


def test_local_markets_filters_and_preserves_coin_names(tmp_path):
    """Only active linear USDT swaps are offered; no network or runtime mutation."""
    directory = tmp_path / 'bybit'; directory.mkdir()
    rows = [{'coin': coin, 'quote': quote, 'swap': swap, 'linear': linear, 'active': active}
            for coin, quote, swap, linear, active in [
                ('BTC', 'USDT', True, True, True), ('BAD\nPROMPT', 'USDT', True, True, True), ('1000PEPE', 'USDT', True, True, True),
                ('ETH', 'USDC', True, True, True), ('SPOT', 'USDT', False, True, True),
                ('INVERSE', 'USDT', True, False, True), ('OLD', 'USDT', True, True, False)]]
    (directory / 'mapping.json').write_text(json.dumps(rows))
    assert local_markets(tmp_path) == {'bybit': ['1000PEPE', 'BTC']}


def test_local_markets_ignores_damaged_and_symlinked_data(tmp_path):
    """Damaged caches do not hide healthy exchanges or follow symlinked files."""
    bad = tmp_path / 'bad'; bad.mkdir(); (bad / 'mapping.json').write_text('{')
    link = tmp_path / 'link'; link.symlink_to(bad, target_is_directory=True)
    assert local_markets(tmp_path) == {}
    assert local_markets(tmp_path / 'missing') == {}


def test_options_route_uses_authenticated_read_only_catalog(monkeypatch, tmp_path):
    """Only config names are scanned; full listing and bot config reads are avoided."""
    from api import ai, optimize_v8
    monkeypatch.setattr(optimize_v8, '_configs_dir', lambda: tmp_path)
    def listing(*args, **kwargs):
        raise AssertionError('Expensive config listing must not run')
    monkeypatch.setattr(optimize_v8, 'list_configs', listing)
    directory = tmp_path / 'Start'
    directory.mkdir()
    (directory / 'optimize.json').write_text('{}')
    (tmp_path / 'link').symlink_to(directory, target_is_directory=True)
    session = object()
    result = json.loads(ai.loop_template_options(session).body)
    assert result == {'configs': [{'name': 'Start'}]}
    route = next(r for r in ai.router.routes if r.path == '/loop-template-options')
    assert route.dependant.dependencies[0].call is ai.require_auth
