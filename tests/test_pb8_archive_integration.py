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
    """Isolate the helper boundary; archive tests check snapshots and routing.

    Schema/override validation has separate coverage in test_pb8_config.py.
    """
    monkeypatch.setattr(pb8_config, "load_pb8_config", lambda path: json.loads(Path(path).read_text(encoding="utf-8")))
    monkeypatch.setattr(optimize_v8, "load_pb8_config", pb8_config.load_pb8_config)
    monkeypatch.setattr(optimize_v8, "prepare_pb8_config", lambda config, **_kwargs: copy.deepcopy(config))
    monkeypatch.setattr(optimize_v8, "cache_prepared_pb8_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(optimize_v8, "validate_pb8_override_bundle", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(optimize_v8, "validate_pb8_optimizer_overrides", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(backtest_v8, "load_pb8_config", pb8_config.load_pb8_config)
    monkeypatch.setattr(backtest_v8, "prepare_pb8_config", lambda config, **_kwargs: copy.deepcopy(config))
    monkeypatch.setattr(backtest_v8, "validate_pb8_override_bundle", lambda *_args, **_kwargs: None)

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
    _write_json(
        result / "analysis.json",
        {
            "adg_strategy_eq": 0.01,
            "adg_strategy_eq_w": 0.02,
            "gain": 1.2,
            "drawdown_worst_strategy_eq": 0.2,
            "sharpe_ratio_strategy_eq": 1.1,
            "sharpe_ratio_strategy_eq_w": 1.4,
        },
    )
    summary = archive_helpers.summarize_backtest_result(result, archive)

    assert summary["config_version"] == "v8.0.0"
    assert summary["backtest_version"] == "v8"
    assert summary["strategy"] == "trailing_grid_v7"
    assert summary["twe_long"] == 1.5
    assert summary["twe_short"] == 0.75
    assert summary["pos_long"] == 5
    assert summary["pos_short"] == 3
    assert summary["adg"] == summary["adg_usd"] == 0.01
    assert summary["adg_w_usd"] == 0.02
    assert summary["sharpe_ratio_w_usd"] == 1.4


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
    monkeypatch.setattr(backtest_v7, "update_archive_manifest_results", lambda _root, _copied: {"schema_version": 1, "items": []})
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

    config = _v8_config("source")
    config["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    override = {"bot": {"long": {"risk": {"n_positions": 1}}}}
    optimize_v8.save_config(
        "source",
        {"config": config, "override_configs": {"BTC.json": override}},
        session=None,
    )
    exported = backtest_v7._add_optimize_config_to_archive_sync("mine", "source", "v8")
    archive_path = Path(exported["path"])

    assert archive_path.relative_to(archive).parts[:4] == ("pbgui", "configs", "v8.0.0", "optimize")
    assert json.loads(
        (archive_path.parent / f"{archive_path.stem}.overrides/BTC.json").read_text(encoding="utf-8")
    ) == override
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
    assert json.loads((configs / "restored/BTC.json").read_text(encoding="utf-8")) == override

    deleted = backtest_v7.delete_archive_optimize_config(
        "mine", str(archive_path), version="v8", session=None
    )
    assert deleted["ok"] is True
    assert not archive_path.exists()
    assert not (archive_path.parent / f"{archive_path.stem}.overrides").exists()


def test_v8_optimize_archive_repairs_orphan_override_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Retrying an interrupted archive publish replaces its orphan sidecars."""
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
    config = _v8_config("source")
    config["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    override = {"bot": {"long": {"risk": {"n_positions": 1}}}}
    optimize_v8.save_config("source", {"config": config, "override_configs": {"BTC.json": override}}, None)

    destination, _meta, _skipped = archive_helpers.resolve_optimize_archive_destination(
        archive, "source", optimize_v8.load_pb8_config(optimize_v8._config_file("source"))
    )
    orphan = destination.parent / f"{destination.stem}.overrides"
    _write_json(orphan / "stale.json", {"stale": True})

    exported = backtest_v7._add_optimize_config_to_archive_sync("mine", "source", "v8")

    assert Path(exported["path"]).is_file()
    assert not (orphan / "stale.json").exists()
    assert json.loads((orphan / "BTC.json").read_text(encoding="utf-8")) == override


def test_v8_archive_rebacktest_uses_v8_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Rebacktest routes each PB8 archived config to the PB8 snapshot queue."""
    archives = tmp_path / "archives"
    archive = archives / "mine"
    config = _v8_config()
    config["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    result = _make_result(
        archive / "pbgui/configs/v8.0.0/backtests/demo/bybit",
        config,
    )
    override = {"bot": {"long": {"risk": {"n_positions": 1}}}}
    _write_json(result / "BTC.json", override)
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
    assert all(item["override_configs"] == {"BTC.json": override} for item in queued)
    assert all(item["backtest_version"] == "v8" for item in response["queue_items"])


def test_v8_archive_retest_queues_immutable_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Retest-and-replace stores PB8 configs in the immutable V8 queue snapshot root."""
    archive = tmp_path / "archives/mine"
    config = _v8_config()
    config["coin_overrides"] = {"BTC": {"override_config_path": "BTC.json"}}
    result = _make_result(
        archive / "pbgui/configs/v8.0.0/backtests/demo/bybit",
        config,
    )
    override = {"bot": {"long": {"risk": {"n_positions": 1}}}}
    _write_json(result / "BTC.json", override)
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
    assert json.loads((snapshot.parent / "BTC.json").read_text(encoding="utf-8")) == override
    assert not (tmp_path / "data/bt_v7_queue").exists()


def test_pb9_schema_uses_native_archive_family_without_affecting_v7_v8():
    """The archive generation follows config schema rather than package-version guesses."""
    for schema in ('v9.0.0','v9.1.0'):
        info = archive_helpers.config_version_info({'config_version': schema})
        assert info['config_family'] == 'pb9'
        assert info['backtest_version'] == info['optimize_version'] == 'v9'
    assert archive_helpers.config_version_info({'config_version':'v8.6.0'})['backtest_version'] == 'v8'
    assert archive_helpers.config_version_info({'config_version':'v7.12.0'})['backtest_version'] == 'v7'


def test_archive_settings_never_return_stored_token_and_blank_save_keeps_it(monkeypatch):
    """Stored secrets remain server-owned while users can explicitly replace or clear them."""
    import configparser
    parser = configparser.ConfigParser()
    parser.read_dict({'config_archive': {'my_archive_access_token': 'fixture-private-token'}})
    monkeypatch.setattr(backtest_v7, 'load_ini_snapshot', lambda: parser)
    response = backtest_v7.get_archive_settings()
    assert response['has_access_token'] is True
    assert 'access_token' not in response
    assert 'fixture-private-token' not in json.dumps(response)
    saves = []
    monkeypatch.setattr(backtest_v7, 'save_ini_section', lambda section, values: saves.append((section, values)))
    backtest_v7.save_archive_settings({'username':'tester','access_token':''})
    assert saves[-1][1] == {'my_archive_username':'tester'}
    backtest_v7.save_archive_settings({'access_token':'', 'clear_access_token':True})
    assert saves[-1][1] == {'my_archive_access_token':''}
    backtest_v7.save_archive_settings({'access_token':'replacement-fixture-token'})
    assert saves[-1][1] == {'my_archive_access_token':'replacement-fixture-token'}


def test_archive_git_auth_stays_out_of_argv_and_remote_config(tmp_path, monkeypatch):
    """A synthetic Git command receives transient credentials only through its environment."""
    import base64
    import subprocess
    calls = []
    def run(command, **kwargs):
        """Return a fixed remote without contacting a server or invoking Git."""
        calls.append((command, kwargs))
        if command[1:3] == ['remote','get-url']:
            return subprocess.CompletedProcess(command, 0, 'https://example.test/own/archive.git\n', '')
        return subprocess.CompletedProcess(command, 0, 'ok', '')
    monkeypatch.setattr(backtest_v7.subprocess, 'run', run)
    monkeypatch.setattr(backtest_v7, '_log_archive', lambda *_a, **_k: None)
    token = 'fixture-test-token'
    backtest_v7._run_archive_git_step('own', tmp_path, 'push', ['git','push','--dry-run'], timeout=1, secret=token)
    command, kwargs = calls[-1]
    assert command == ['git','push','--dry-run']
    assert all(token not in str(item[0]) for item in calls)
    env = kwargs['env']
    assert env['GIT_CONFIG_KEY_0'] == 'http.https://example.test/own/archive.git.extraHeader'
    encoded = env['GIT_CONFIG_VALUE_0'].split()[-1]
    assert base64.b64decode(encoded).decode() == token + ':'
    assert token not in backtest_v7._mask_archive_secret(encoded, token)
    assert backtest_v7._archive_push_url(tmp_path, token) is None


def test_archive_clear_only_request_removes_the_stored_token(monkeypatch):
    """Explicit clearing is sufficient without serializing any existing secret value."""
    saves = []
    monkeypatch.setattr(backtest_v7, 'save_ini_section', lambda section, values: saves.append(values))
    backtest_v7.save_archive_settings({'clear_access_token':True})
    assert saves[-1] == {'my_archive_access_token':''}
    backtest_v7.save_archive_settings({'clear_access_token':True,'access_token':None})
    assert saves[-1] == {'my_archive_access_token':''}


def test_archive_token_requires_https_and_preserves_git_config_hardening(tmp_path, monkeypatch):
    """Credential transport rejects cleartext HTTP and retains explicit config isolation."""
    import subprocess
    remote = ['http://example.test/archive.git']
    monkeypatch.setattr(backtest_v7.subprocess, 'run', lambda cmd, **kwargs:
        subprocess.CompletedProcess(cmd, 0, remote[0], ''))
    with pytest.raises(HTTPException, match='HTTPS'):
        backtest_v7._archive_authenticated_git_env(tmp_path,['git','pull'],'fixture-token')
    remote[0] = 'https://example.test/archive.git'
    monkeypatch.setenv('GIT_CONFIG_GLOBAL','/fixture/global-disabled')
    monkeypatch.setenv('GIT_CONFIG_NOSYSTEM','1')
    env = backtest_v7._archive_authenticated_git_env(tmp_path,['git','pull'],'fixture-token')
    assert env['GIT_CONFIG_GLOBAL'] == '/fixture/global-disabled'
    assert env['GIT_CONFIG_NOSYSTEM'] == '1'
    assert env['GIT_CONFIG_VALUE_1'] == 'false'


@pytest.mark.parametrize('raw, public', [
    ('https://fixture-token@example.test/archive.git?token=hidden#secret','https://example.test/archive.git'),
    ('ssh://user:fixture-token@example.test/archive.git','ssh://user@example.test/archive.git'),
    ('git@example.test:own/archive.git','git@example.test:own/archive.git'),
])
def test_archive_public_origin_never_reflects_credentials(raw, public):
    """The shared archive list displays repository coordinates without stored auth."""
    assert backtest_v7._archive_public_remote_url(raw) == public
