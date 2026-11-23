"""Current PB9 risk boundaries reject development layouts without rewriting data."""

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from pb9_config import PB9ConfigError
from pb9_config_helper import (
    INCOMPATIBLE_PB9_MESSAGE, _HelperError, check_inline_config, incompatible_pb9_layout,
)


@pytest.mark.parametrize('patch', [
    {'bot': {'global': {'unstuck': {'enabled': True}}}},
    {'bot': {'short': {'hsl': {'enabled': False}}}},
    {'bot': {'portfolio': {'loss_gate': {'max_drawdown_fraction': 1}}}},
    {'bot': {'portfolio': {'unstuck': {'memory_ema_span_minutes': 10080}}}},
    {'bot': {'long': {'risk': {'unstuck': {'close_fraction': .1}}}}},
    {'optimize': {'bounds': {'long': {'unstuck': {'close_fraction': [.1, .2]}}}}},
    {'optimize': {'fixed_params': ['portfolio.loss_gate.max_drawdown_fraction']}},
    {'optimize': {'fixed_runtime_overrides': {'bot.short.hsl.enabled': True}}},
    {'backtest': {'scenarios': [{'overrides': {'bot.global.unstuck.enabled': True}}]}},
    {'coin_overrides': {'BTC': {'bot': {'long': {'hsl': {'enabled': True}}}}}},
])
def test_retired_layout_is_rejected_without_changing_authored_input(patch):
    """Presence, including disabled policy and optimizer selectors, requires regeneration."""
    source = {'config_version': 'v9.0.0', **patch}
    before = deepcopy(source)
    with pytest.raises(_HelperError, match='incompatible_pb9_config'):
        check_inline_config(source, allow_inline_overrides=True)
    assert source == before
    assert 'Delete it and create a new config' in str(PB9ConfigError('incompatible_pb9_config'))


def test_current_sparse_policy_and_display_text_are_not_legacy():
    """Current inherited risk fields and ordinary strings do not become migration aliases."""
    source = {'config_version': 'v9.0.0', 'bot': {
        'portfolio': {'risk': {'hsl': {'scope': 'coin'}, 'loss_realization': {
            'memory_ema_span_minutes': 10080, 'loss_gate': {'enabled': False}}}},
        'global': {'risk': {'unstuck': {'enabled': True}, 'hsl': {'enabled': False}}},
        'long': {'enabled': True}, 'short': {'enabled': False}},
        'logging': {'label': 'bot.long.hsl.enabled is retired'}}
    before = deepcopy(source)
    check_inline_config(source, allow_inline_overrides=True)
    assert not incompatible_pb9_layout(source)
    assert source == before


def test_current_native_metadata_has_complete_risk_controls():
    """The checked-in optimizer fixture describes native current fields and no retired bounds."""
    from pathlib import Path
    data = json.loads((Path(__file__).parent / 'fixtures/pb9_optimize_metadata.json').read_text())
    paths = {'.'.join(field['path']) for field in data['fields']}
    assert 'bot.portfolio.risk.loss_realization.loss_gate.enabled' in paths
    assert 'bot.portfolio.risk.loss_realization.loss_gate.max_realized_drawdown_fraction' in paths
    assert 'bot.portfolio.risk.loss_realization.memory_ema_span_minutes' in paths
    assert 'bot.global.risk.unstuck.enabled' in paths
    assert 'bot.global.unstuck.close_fraction' not in paths
    assert not incompatible_pb9_layout(data['template'])
    assert not incompatible_pb9_layout({'optimize': {'bounds': data['all_bounds']}})


def test_optimizer_list_marks_retired_source_without_native_or_file_mutation():
    """List projection leaves incompatible files available for explicit deletion."""
    from api.optimize_v9 import NativeOptimizeEngine
    old = {'config_version': 'v9.0.0', 'bot': {'portfolio': {'loss_gate': {}}}}
    before = deepcopy(old)
    assert NativeOptimizeEngine._config_compatibility_error(None, old) == INCOMPATIBLE_PB9_MESSAGE
    assert old == before
    assert NativeOptimizeEngine._config_compatibility_error(None, {'bot': {}}) == ''


def test_live_editor_rejects_retired_bundle_before_native_loading(monkeypatch):
    """A stale file cannot silently open with the new defaults; deletion has a separate path."""
    from api import v9_instances as api
    from contextlib import nullcontext
    old = {'config_version': 'v9.0.0', 'bot': {'long': {'unstuck': {}}}}
    monkeypatch.setattr(api, '_read_lock', nullcontext)
    monkeypatch.setattr(api, '_load_instance', lambda name: (None, old, {}))
    with pytest.raises(PB9ConfigError) as error:
        api._op_load({'name': 'test'}, {}, SimpleNamespace())
    assert error.value.error_codes == ('incompatible_pb9_config',)


def test_api_errors_explain_development_config_rejection():
    """The public fixed-code boundary gives a recovery action without source content."""
    from api import pb9
    error = pb9._error(422, 'incompatible_pb9_config', 'generic')
    assert error.status_code == 422
    assert 'Delete it and create a new config' in error.detail
    assert error.headers['X-PBGUI-Error-Codes'] == 'incompatible_pb9_config'


@pytest.mark.parametrize('retired', [False, True])
def test_converter_output_must_use_current_risk_layout(tmp_path, monkeypatch, retired):
    """Reject an outdated native converter without modifying the PB8 source bundle."""
    import pb9_config_helper as helper
    result_config = {'config_version': 'v9.0.0', 'bot': {
        'portfolio': {'risk': {'loss_realization': {'loss_gate': {'enabled': False}}}},
        'long': {'hsl': {}} if retired else {'risk': {'hsl': {'enabled': False}}}}}
    source = {'config': {'config_version': 'v8.6.0'}, 'files': {}}
    before = deepcopy(source)
    converter = SimpleNamespace(migrate=lambda *a, **k: SimpleNamespace(
        config=result_config, changes=[], warnings=[]))
    monkeypatch.setattr(helper.importlib, 'import_module', lambda name: converter)
    monkeypatch.setattr(helper, '_module_path', lambda *a: str(tmp_path / 'converter.py'))
    result, _ = helper._migrate_v8(source, tmp_path, 'v9.0.0')
    assert source == before
    assert result['status'] == ('blocked' if retired else 'ready')
    if retired:
        assert result['message'] == INCOMPATIBLE_PB9_MESSAGE
        assert 'config' not in result
    else:
        assert result['config'] == result_config
