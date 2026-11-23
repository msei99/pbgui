"""Native PB9 Run metadata and inline live validation with isolated fake modules."""

import copy
import json
from pathlib import Path
import sys

import pytest

import pb9_config
from tests.test_pb9_config import runtime, validation_runtime


@pytest.fixture
def metadata_runtime(runtime):
    """Provide native-looking field and strategy modules inside the chosen checkout."""
    package = runtime.root / "src" / "config"
    with runtime.schema.open("a") as stream:
        stream.write('\ndef get_expanded_config_prototype():\n    return get_template_config()\n')
    (package / "field_spec.py").write_text('from enum import Enum\nclass Missing(Enum):\n    VALUE = "no default"\nNO_DEFAULT = Missing.VALUE\n')
    site = runtime.python.parent.parent / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    (site / "passivbot_rust.py").write_text('"""Inert Rust-module fixture with selected-environment provenance."""\n')
    live = runtime.root / 'src/live';live.mkdir()
    (live / '__init__.py').write_text('')
    (live / 'order_churn_gate.py').write_text("ORDER_CHURN_GATE_SUPPORTED_EXCHANGES = {'binance','bybit','gateio','hyperliquid','fake','unavailable'}\n")
    (runtime.root / 'src/utils.py').write_text("def to_ccxt_client_id(value):\n    return {'binance':'binanceusdm','gateio':'gate'}.get(value,value)\n")
    ccxt = site / 'ccxt';ccxt.mkdir()
    (ccxt / '__init__.py').write_text('')
    (ccxt / 'async_support.py').write_text("exchanges = ['binanceusdm','bybit','gate','hyperliquid','fake']\n")
    (package / "field_registry.py").write_text('''from enum import Enum
from types import SimpleNamespace as NS
class Kind(str, Enum):
    INTEGER = "integer"
from .field_spec import NO_DEFAULT
def get_field_registry():
    common = dict(types=(Kind.INTEGER,), nullable=False, description="Maximum positions", unit="positions", scope="global", order=0, coin_override=None)
    domain = NS(minimum=0, maximum=100, exclusive_minimum=False, exclusive_maximum=False, finite=True, string_pattern=None, choices=())
    return [NS(path=("bot","global","risk","max_positions"), default=7, domain=domain, **common),
            NS(path=("bot","long","risk","max_positions"), default=NO_DEFAULT, domain=None, **common)]
''')
    (package / "strategy_spec.py").write_text('''def get_supported_strategy_kinds():
    return ("trailing_martingale",)
def get_strategy_spec(kind):
    return {"parameters": [{"config_path": ("strategy", "long", "entry", "qty_fraction"), "bounds": (0.0,1.0)}]}
def get_all_strategy_defaults():
    return {side: {"trailing_martingale": {"entry": {"qty_fraction": 0.01}}} for side in ("long","short")}
''')
    return runtime


def test_metadata_uses_native_fields_and_keeps_no_default_distinct(metadata_runtime):
    """Descriptions, choices and native strategy contracts reach the shared editor."""
    env = metadata_runtime
    result = pb9_config.get_pb9_editor_metadata(env.root, env.python)
    assert result["contract_version"] == 1
    assert result["template"]["config_version"] == "v9.0.0"
    assert result["prototype"]["config_version"] == "v9.0.0"
    assert result["strategies"] == ["trailing_martingale"]
    assert result["fields"][0]["path"] == ["bot", "global", "risk", "max_positions"]
    assert result["fields"][0]["types"] == ["integer"]
    assert result["fields"][0]["default"] == 7
    assert result["fields"][0]["domain"]["maximum"] == 100
    assert "default" not in result["fields"][1]
    assert result["source"]["repo_dir"] == str(env.root)
    assert result["strategy_specs"]["trailing_martingale"]["parameters"][0]["bounds"] == [0.0,1.0]
    assert Path(result["source"]["rust_module_file"]).is_relative_to(env.python.parent.parent)
    assert result["source"]["rust_source_agreement"] == "not_checked"
    assert result['exchange_options']['optimize'] == ['binance','bybit','gateio','hyperliquid']
    assert result['exchange_options']['backtest'] == result['exchange_options']['suite']
    assert 'unavailable' in result['exchange_options']['live']
    assert 'fake' not in result['exchange_options']['live']


def test_live_validation_uses_live_only_and_preserves_inline_source(validation_runtime):
    """Live Run may use inline overrides while the stricter library boundary stays intact."""
    env = validation_runtime
    config = {"config_version": "v9.0.0", "live": {"account": {"user": "alice"}},
              "bot": {"long": {"enabled": True}},
              "coin_overrides": {"BTC": {"bot": {"long": {"risk": {"hsl": {"enabled": False}}}}}}}
    before = copy.deepcopy(config)
    result = pb9_config.validate_pb9_live_config(config, env.root, env.python)
    assert result["valid"] is True
    call = json.loads(env.record.read_text())
    assert call["config"] == before
    assert call["kwargs"] == {"base_config_path": "", "verbose": False, "target": "canonical", "runtime": None, "live_only": True}
    assert config == before
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config(config, env.root, env.python)
    assert caught.value.error_codes == ("external_reference_rejected",)


@pytest.mark.parametrize("patch", [
    {"bot": {"long": {"universe": {"approved_coins": ["../credentials.json"]}}}},
    {"base_config_path": "/unapproved.json"},
    {"override_config_path": "/unapproved.json"},
    {"coin_sources": {"BTC": "unapproved"}},
])
def test_inline_live_overrides_cannot_request_external_files(validation_runtime, patch):
    """Allowing inline overrides does not relax file/reference gates anywhere in a patch."""
    env = validation_runtime
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_live_config({"config_version": "v9.0.0", "coin_overrides": {"BTC": patch}}, env.root, env.python)
    assert caught.value.error_codes == ("external_reference_rejected",)
    assert not env.import_marker.exists()


def test_metadata_module_must_belong_to_selected_source(metadata_runtime, tmp_path):
    """A module symlink cannot borrow the other runtime's field registry."""
    source = metadata_runtime.root / "src" / "config" / "field_registry.py"
    other = tmp_path / "unapproved.py"
    other.write_bytes(source.read_bytes())
    source.unlink()
    source.symlink_to(other)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.get_pb9_editor_metadata(metadata_runtime.root, metadata_runtime.python)
    assert caught.value.error_codes == ("runtime_identity_mismatch",)


@pytest.mark.parametrize("operation", [None, [], {}, 9])
def test_non_string_operation_has_fixed_protocol_rejection(operation):
    """Malformed operation values fail before imports or filesystem reads."""
    from pb9_config_helper import _HelperError, handle

    with pytest.raises(_HelperError, match="^invalid_request$"):
        handle({"operation": operation})


@pytest.mark.parametrize("config", [
    {"coin_overrides": {"BTC": []}},
    {"coin_overrides.BTC": {}},
    {"backtest": {"scenarios": [{"coin_overrides": {"BTC": {"bot": {}}}}]}},
    {"coin_overrides": {"../keys.json": {"bot": {}}}},
])
def test_live_override_relaxation_does_not_accept_nested_or_unsafe_shapes(validation_runtime, config):
    """Inline permission is root-only and never accepts path-like selectors."""
    env = validation_runtime
    config = {"config_version": "v9.0.0", **config}
    with pytest.raises(pb9_config.PB9ConfigError):
        pb9_config.validate_pb9_live_config(config, env.root, env.python)
    assert not env.import_marker.exists()
