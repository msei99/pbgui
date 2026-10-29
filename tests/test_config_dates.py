"""Offline regressions for calendar dates across PB8 preparation and persistence."""

import copy
import json

import pytest

import pb8_config
import pb8_config_helper
from config_dates import normalize_backtest_dates, parse_config_date


@pytest.mark.parametrize("suffix", ["", "T00:00:00", " 12:34:56", "T23:59:59.123Z", "T00:00:00+02:00"])
def test_parse_config_date_preserves_calendar_day(suffix):
    """Datetime compatibility must not shift an offset timestamp to another day."""
    assert parse_config_date("2019-10-04" + suffix).isoformat() == "2019-10-04"


@pytest.mark.parametrize("value", ["2019-02-30", "2019-10-04Tbroken", "2019-10-04T25:00:00", "2019-10-04 garbage", None, 123])
def test_parse_config_date_rejects_invalid_values(value):
    """Do not accept an invalid value merely because its first ten characters look valid."""
    with pytest.raises((ValueError, TypeError)):
        parse_config_date(value)


def test_normalization_preserves_relative_inherited_and_unrelated_values():
    """Only valid backtest datetime fields change; sparse scenarios stay sparse."""
    config = {"backtest": {"start_date": "2019-10-04", "end_date": "now", "scenarios": [
        {"label": "inherit"}, {"start_date": None, "end_date": "today"},
        {"start_date": "", "end_date": "2019-10-04Tbroken"}]},
        "pbgui": {"created_at": "2019-10-04T12:34:56"}}
    before = copy.deepcopy(config)
    assert normalize_backtest_dates(config) == before
    assert normalize_backtest_dates({}) == {}


@pytest.mark.parametrize("kind", ["optimize", "backtest"])
@pytest.mark.parametrize("suffix", ["", "T00:00:00", " 00:00:00"])
def test_api_save_persists_date_only_scenario_bundle(tmp_path, monkeypatch, kind, suffix):
    """Both API save paths must write normalized dates through the real shared helper."""
    from api import backtest_v8, optimize_v8

    api = optimize_v8 if kind == "optimize" else backtest_v8
    root = tmp_path / kind
    monkeypatch.setattr(api, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(api, "_configs_dir", lambda: root)
    monkeypatch.setattr(api, "cache_prepared_pb8_config", lambda *_args: None)
    monkeypatch.setattr(api, "validate_pb8_override_bundle", lambda *_args: None)
    if kind == "optimize":
        monkeypatch.setattr(api, "validate_pb8_optimizer_overrides", lambda *_args, **_kwargs: None)
    modules = {"prepare_config": lambda config, **_kwargs: copy.deepcopy(config), "sanitize": copy.deepcopy}
    monkeypatch.setattr(pb8_config_helper, "_load_pb8_modules", lambda _root: modules)
    monkeypatch.setattr(pb8_config, "_call_helper", lambda operation, **payload: pb8_config_helper.handle(
        {"operation": operation, "pb8_dir": str(tmp_path), **payload}))
    config = {"backtest": {"start_date": "2019-10-04" + suffix, "end_date": "2020-01-01" + suffix,
        "suite_enabled": True, "scenarios": [
            {"label": f"train_{index}", "start_date": "2019-10-04" + suffix,
             "end_date": "2019-12-31" + suffix} for index in range(30)]}}
    before = copy.deepcopy(config)

    response = api.save_config("dates", config, session=None)
    path = root / "dates" / ("optimize.json" if kind == "optimize" else "backtest.json")
    saved = json.loads(path.read_text())

    assert response["config"] == saved
    assert config == before
    assert saved["backtest"]["start_date"] == "2019-10-04"
    assert saved["backtest"]["end_date"] == "2020-01-01"
    assert len(saved["backtest"]["scenarios"]) == 30
    assert all(item["start_date"] == "2019-10-04" and item["end_date"] == "2019-12-31"
               for item in saved["backtest"]["scenarios"])


def test_preparation_normalizes_dates_returned_by_native_runtime(monkeypatch):
    """Normalize after native preparation too, not just the client's submitted values."""
    modules = {"prepare_config": lambda config, **_kwargs: {"backtest": {"scenarios": [
        {"start_date": "2019-10-04T00:00:00", "end_date": "2019-12-31 00:00:00"}]}},
        "sanitize": copy.deepcopy}
    result = pb8_config_helper._prepare(modules, {"backtest": {}})
    assert result["backtest"]["scenarios"] == [{"start_date": "2019-10-04", "end_date": "2019-12-31"}]


def test_legacy_load_normalizes_in_memory_without_rewriting_file(tmp_path, monkeypatch):
    """Opening a legacy config shares canonical dates but never silently edits the source."""
    config = {"backtest": {"start_date": "2019-10-04T00:00:00", "end_date": "2020-01-01 00:00:00",
        "scenarios": [{"start_date": "2019-10-04T00:00:00", "end_date": "2019-12-31T00:00:00"}]}}
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(config))
    before = path.read_bytes()
    modules = {"load_prepared_config": lambda *_args, **_kwargs: copy.deepcopy(config),
        "sanitize": copy.deepcopy}
    monkeypatch.setattr(pb8_config_helper, "_load_pb8_modules", lambda _root: modules)

    loaded = pb8_config_helper.handle({"operation": "load", "pb8_dir": str(tmp_path),
        "config_path": str(path)})["config"]

    assert path.read_bytes() == before
    assert loaded["backtest"]["start_date"] == "2019-10-04"
    assert loaded["backtest"]["end_date"] == "2020-01-01"
    assert loaded["backtest"]["scenarios"] == [{"start_date": "2019-10-04", "end_date": "2019-12-31"}]
