"""Reviewed window/scenario invariants are native output constraints, not only prose."""
import copy

import pytest
from jsonschema import Draft202012Validator

from pb8_loop_claude import _variant_config_schema
from pb8_loop_store import scenario_state
from tests.test_ai_loop_windows import run_record
from tests.test_pb8_loop_optimizer import record


@pytest.mark.parametrize('field', ['start_date', 'end_date', 'scenarios', 'suite_enabled',
                                 'starting_balance', 'scenario_aggregation', 'market_settings_sources', 'reducer'])
def test_native_schema_rejects_changed_reviewed_window_or_disabled_scenario(tmp_path, field):
    """Both observed live rejection classes are rejected by the SDK's schema validator."""
    _, row = run_record(tmp_path)
    row['settings']['scenario_enabled'] = False
    before = copy.deepcopy(row)
    validator = Draft202012Validator(_variant_config_schema(row))
    assert validator.is_valid(row['initial_config'])
    changed = copy.deepcopy(row['initial_config'])
    changed['backtest'][field] = 'unreviewed replacement'
    assert not validator.is_valid(changed)
    if field in row['initial_config']['backtest']:
        del changed['backtest'][field]
        assert not validator.is_valid(changed)
    assert row == before


@pytest.mark.parametrize('change', ['omit', 'replace', 'edit_windows'])
def test_native_schema_preserves_original_window_provenance(tmp_path, change):
    """Correct dates alone cannot conceal missing or fabricated reviewed metadata."""
    _, row = run_record(tmp_path)
    row['settings']['scenario_enabled'] = True
    validator = Draft202012Validator(_variant_config_schema(row))
    changed = copy.deepcopy(row['initial_config'])
    if change == 'omit':
        del changed['pbgui']
    elif change == 'replace':
        changed['pbgui'] = {}
    else:
        changed['pbgui']['scenario_template']['parameters']['windows'][0]['start_date'] = '1999-01-01'
    assert not validator.is_valid(changed)


def test_disabled_editor_schema_keeps_effective_missing_values_and_optimizer_freedom(record):
    """Unconfigured fixed fields stay absent/null, while optimizer edits remain available."""
    _, row = record
    row['settings']['scenario_enabled'] = False
    schema = _variant_config_schema(row)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    changed = copy.deepcopy(row['initial_config'])
    changed['optimize']['scoring'] = [{'metric': 'gain', 'goal': 'max'}]
    changed['optimize']['bounds']['long'] = {'risk': {'total_wallet_exposure_limit': [0.1, 0.3]}}
    changed['backtest']['n_cpus'] = 2
    for key, value in scenario_state(row['initial_config'])['backtest'].items():
        if key not in changed['backtest']:
            assert value is None
            changed['backtest'][key] = None
    assert validator.is_valid(changed)


def test_enabled_unbound_scenarios_remain_editable(record):
    """The schema never freezes an editor that the user actually enabled."""
    _, row = record
    row['settings']['scenario_enabled'] = True
    changed = copy.deepcopy(row['initial_config'])
    changed['backtest']['start_date'] = '2024-02-01'
    changed['backtest']['scenarios'] = []
    assert Draft202012Validator(_variant_config_schema(row)).is_valid(changed)
