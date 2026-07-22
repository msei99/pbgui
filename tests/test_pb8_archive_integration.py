"""Focused PB7/PB8 archive ownership and routing integration tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from fastapi import HTTPException

import pb8_config
from api import archive_helpers, backtest_v7, backtest_v8, optimize_v8


def _write_json(path: Path, payload: dict) -> None:
    """Write one isolated JSON fixture."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _v8_config(name: str = "demo") -> dict:
    """Return a compact PB8 backtest/Optimize config with nested risk values."""
    return {
        "config_version": "v8.0.0",
        "backtest": {
            "base_dir": f"backtests/pbgui/{name}",
            "exchanges": ["bybit"],
            "start_date": "2025-01-01",
            "end_date": "2025-12-31",
            "starting_balance": 1000,
        },
        "bot": {
            "long": {"risk": {"total_wallet_exposure_limit": 1.5, "n_positions": 5}},
            "short": {"risk": {"total_wallet_exposure_limit": 0.75, "n_positions": 3}},
        },
        "live": {"strategy_kind": "trailing_grid_v7"},
        "optimize": {"n_cpus": 2, "bounds": {}},
    }


def _make_result(root: Path, config: dict, name: str = "run") -> Path:
    """Create one minimal managed backtest result."""
    result = root / name
    _write_json(result / "config.json", config)
    _write_json(
        result / "analysis.json",
        {"adg": 0.01, "gain": 1.2, "drawdown_worst": 0.2, "sharpe_ratio": 1.1},
    )
    return result


def _patch_pb8_config_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace PB8 helper subprocess calls with isolated JSON round trips."""
    monkeypatch.setattr(pb8_config, "load_pb8_config", lambda path: json.loads(Path(path).read_text(encoding="utf-8")))
    monkeypatch.setattr(optimize_v8, "load_pb8_config", pb8_config.load_pb8_config)
    monkeypatch.setattr(optimize_v8, "prepare_pb8_config", lambda config, **_kwargs: copy.deepcopy(config))
    monkeypatch.setattr(optimize_v8, "cache_prepared_pb8_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backtest_v8, "load_pb8_config", pb8_config.load_pb8_config)
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: copy.deepcopy(config))

    def save_prepared(config: dict, path: Path) -> dict:
        """Persist a prepared PB8 snapshot under a temporary managed root."""
        _write_json(Path(path), config)
        return config

    monkeypatch.setattr(backtest_v8, "save_prepared_pb8_config", save_prepared)


def test_generic_archive_metadata_preserves_pb7_aliases_and_nested_v8_risk(tmp_path: Path) -> None:
    """Archive summaries expose generic ownership while retaining legacy PB7 keys."""
    v7 = archive_helpers.config_version_info({"config_version": "v7.12.0"})
    v8 = archive_helpers.config_version_info({"config_version": "v8.0.0"})

    assert (v7["config_family"], v7["backtest_version"], v7["optimize_version"]) == ("pb7", "v7", "v7")
    assert (v8["config_family"], v8["backtest_version"], v8["optimize_version"]) == ("pb8", "v8", "v8")
    assert v8["config_version"] == v8["pb7_config_version"] == "v8.0.0"

    archive = tmp_path / "archive"
    result = _make_result(
        archive / "pbgui/configs/v8.0.0/backtests/demo/bybit",
        _v8_config(),
    )
    summary = archive_helpers.summarize_backtest_result(result, archive)

    assert summary["config_version"] == "v8.0.0"
    assert summary["backtest_version"] == "v8"
    assert summary["twe_long"] == 1.5
    assert summary["twe_short"] == 0.75
    assert summary["pos_long"] == 5
    assert summary["pos_short"] == 3


def test_v8_add_result_accepts_only_its_managed_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """V8 exports resolve their declared local root and reject a false PB7 declaration."""
    archives = tmp_path / "data/archives"
    archive = archives / "mine"
    archive.mkdir(parents=True)
    results_root = tmp_path / "pb8/backtests/pbgui"
    result = _make_result(results_root / "demo/bybit", _v8_config())
    copied = []

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: results_root)
    monkeypatch.setattr(backtest_v7, "maybe_migrate_own_archive", lambda *_args, **_kwargs: {"status": {"status": "current"}})
    monkeypatch.setattr(backtest_v7, "copy_backtest_result_to_archive", lambda source, _root: copied.append(source) or {"ok": True, "relative_path": "v8"})
    monkeypatch.setattr(backtest_v7, "rebuild_archive_manifest", lambda _root: {"schema_version": 1, "items": []})
    monkeypatch.setattr(backtest_v7, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v7._add_config_to_archive_sync("mine", str(result), "v8")
    assert response["backtest_version"] == "v8"
    assert copied == [result.resolve()]

    with pytest.raises(HTTPException) as exc_info:
        backtest_v7._add_config_to_archive_sync("mine", str(result), "v7")
    assert exc_info.value.status_code == 400


def test_archive_result_read_routes_enforce_the_owning_backend(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """PB8 archive reads succeed only through PB8 and cannot use its local delete route."""
    archives = tmp_path / "data/archives"
    result = _make_result(
        archives / "mine/pbgui/configs/v8.0.0/backtests/demo/bybit",
        _v8_config(),
    )
    monkeypatch.setattr(backtest_v8, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: tmp_path / "pb8/backtests/pbgui")
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives)

    assert backtest_v8._resolve_result_dir(str(result)) == result.resolve()
    with pytest.raises(HTTPException, match="PB8"):
        backtest_v7._resolve_result_dir(str(result))
    with pytest.raises(HTTPException) as exc_info:
        backtest_v8.delete_result(str(result), session=None)
    assert exc_info.value.status_code == 400
    assert result.is_dir()


def test_v8_optimize_archive_add_list_view_import_and_delete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The shared archive service round-trips PB8 Optimize bundles with version filtering."""
    archives = tmp_path / "data/archives"
    archive = archives / "mine"
    archive.mkdir(parents=True)
    configs = tmp_path / "data/opt_v8"
    _patch_pb8_config_io(monkeypatch)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(optimize_v8, "_configs_dir", lambda: configs)
    monkeypatch.setattr(backtest_v7, "maybe_migrate_own_archive", lambda *_args, **_kwargs: {"status": {"status": "current"}})
    monkeypatch.setattr(backtest_v7, "_log", lambda *_args, **_kwargs: None)

    optimize_v8.save_config("source", _v8_config("source"), session=None)
    exported = backtest_v7._add_optimize_config_to_archive_sync("mine", "source", "v8")
    archive_path = Path(exported["path"])

    assert archive_path.relative_to(archive).parts[:4] == ("pbgui", "configs", "v8.0.0", "optimize")
    listed = backtest_v7.list_archive_optimize("mine", version="v8", session=None)["configs"]
    assert [(item["name"], item["optimize_version"]) for item in listed] == [("source", "v8")]
    assert backtest_v7.list_archive_optimize("mine", version="v7", session=None)["configs"] == []
    viewed = backtest_v7.get_archive_optimize_config("mine", str(archive_path), version="v8", session=None)
    assert viewed["config_version"] == "v8.0.0"

    imported = backtest_v7.import_archive_optimize_config(
        "mine",
        {"path": str(archive_path), "name": "restored", "collision": "error", "optimize_version": "v8"},
        session=None,
    )
    assert imported == {"ok": True, "name": "restored", "collision": "created", "optimize_version": "v8"}
    assert json.loads((configs / "restored/optimize.json").read_text(encoding="utf-8"))["config_version"] == "v8.0.0"

    deleted = backtest_v7.delete_archive_optimize_config(
        "mine", str(archive_path), version="v8", session=None
    )
    assert deleted["ok"] is True
    assert not archive_path.exists()


def test_v8_archive_rebacktest_uses_v8_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Rebacktest routes each PB8 archived config to the PB8 snapshot queue."""
    archives = tmp_path / "archives"
    archive = archives / "mine"
    result = _make_result(
        archive / "pbgui/configs/v8.0.0/backtests/demo/bybit",
        _v8_config(),
    )
    queued = []
    _patch_pb8_config_io(monkeypatch)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives)
    monkeypatch.setattr(backtest_v8, "add_to_queue", lambda body, session=None: queued.append(copy.deepcopy(body)) or {"ok": True, "filename": "v8-job"})
    monkeypatch.setattr(backtest_v7, "add_to_queue", lambda *_args, **_kwargs: pytest.fail("PB8 config entered PB7 queue"))

    response = backtest_v7.rebacktest_archive_results(
        "mine",
        {"paths": [str(result)], "overrides": {"exchanges": ["bybit", "hyperliquid"]}},
        session=None,
    )

    assert response["queued"] == 2
    assert [item["config"]["backtest"]["exchanges"] for item in queued] == [["bybit"], ["hyperliquid"]]
    assert all(item["config"]["bot"]["long"]["risk"]["n_positions"] == 5 for item in queued)
    assert all(item["backtest_version"] == "v8" for item in response["queue_items"])


def test_v8_archive_retest_queues_immutable_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Retest-and-replace stores PB8 configs in the immutable V8 queue snapshot root."""
    archive = tmp_path / "archives/mine"
    result = _make_result(
        archive / "pbgui/configs/v8.0.0/backtests/demo/bybit",
        _v8_config(),
    )
    configs = tmp_path / "data/bt_v8"
    queue = tmp_path / "data/bt_v8_queue"
    logs = tmp_path / "data/logs/backtests_v8"
    _patch_pb8_config_io(monkeypatch)
    monkeypatch.setattr(backtest_v8, "_configs_dir", lambda: configs)
    monkeypatch.setattr(backtest_v8, "_queue_dir", lambda: queue)
    monkeypatch.setattr(backtest_v8, "_log_dir", lambda: logs)
    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: tmp_path / "data/bt_v7_queue")

    run = backtest_v7._queue_archive_retest_run("mine", archive, result, {})
    snapshot = Path(run["queue_config"])
    snapshot_before = json.loads(snapshot.read_text(encoding="utf-8"))
    changed = _v8_config()
    changed["bot"]["long"]["risk"]["n_positions"] = 99
    _write_json(result / "config.json", changed)

    assert run["backtest_version"] == "v8"
    assert snapshot.is_relative_to(queue / "configs")
    assert json.loads(snapshot.read_text(encoding="utf-8")) == snapshot_before
    assert snapshot_before["bot"]["long"]["risk"]["n_positions"] == 5
    assert not (tmp_path / "data/bt_v7_queue").exists()
