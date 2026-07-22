"""Focused tests for PB8 optimize config, queue, results, and arbitration contracts."""

from __future__ import annotations

import asyncio
import copy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import httpx
import msgpack
import psutil
import pytest
from fastapi import HTTPException
from starlette.requests import Request

import optimize_autostart
from api import optimize_v8
from pb8_config import PB8RuntimeBusyError


@pytest.fixture
def optimize_v8_roots(tmp_path, monkeypatch):
    """Redirect every PB8 optimize managed root to isolated test storage."""
    data = tmp_path / "data"
    configs = data / "opt_v8"
    queue = data / "opt_v8_queue"
    logs = data / "logs" / "optimizes_v8"
    results = tmp_path / "pb8" / "optimize_results"
    backtests = tmp_path / "pb8" / "backtests" / "pbgui"
    v7_configs = data / "opt_v7"
    for path in (configs, queue, logs, results, backtests, v7_configs):
        path.mkdir(parents=True)
    monkeypatch.setattr(optimize_v8, "_configs_dir", lambda: configs)
    monkeypatch.setattr(optimize_v8, "_data_dir", lambda: data)
    monkeypatch.setattr(optimize_v8, "_queue_dir", lambda: queue)
    monkeypatch.setattr(optimize_v8, "_log_dir", lambda: logs)
    monkeypatch.setattr(optimize_v8, "_results_root", lambda: results)
    monkeypatch.setattr(optimize_v8, "_backtests_root", lambda: backtests)
    monkeypatch.setattr(optimize_v8, "_v7_configs_dir", lambda: v7_configs)
    monkeypatch.setattr(optimize_v8, "prepare_pb8_config", lambda config, **kwargs: copy.deepcopy(config))
    monkeypatch.setattr(optimize_v8, "load_pb8_config", lambda path: json.loads(Path(path).read_text(encoding="utf-8")))
    with optimize_v8._result_progress_cache_lock:
        optimize_v8._result_progress_cache.clear()
    with optimize_v8._backtest_count_cache_lock:
        optimize_v8._backtest_count_cache.clear()
    with optimize_v8._pareto_list_cache_lock:
        optimize_v8._pareto_list_cache.clear()
        optimize_v8._pareto_warning_cache.clear()
    return configs, queue, logs, results


@pytest.fixture
def dash_runtime_roots(optimize_v8_roots):
    """Open Dash admission with clean in-memory state below the isolated data root."""
    with optimize_v8._dash_lock:
        optimize_v8._dash_sessions.clear()
        optimize_v8._dash_pending_sessions.clear()
        optimize_v8._dash_admission_open = True
    yield optimize_v8_roots
    with optimize_v8._dash_lock:
        optimize_v8._dash_sessions.clear()
        optimize_v8._dash_pending_sessions.clear()
        optimize_v8._dash_admission_open = False


def _full_pb8_config() -> dict:
    """Return a compact config containing all strategies and uncommon optimizer controls."""
    strategy_defaults = {
        "trailing_martingale": {"entry": {"grid_spacing_pct": 0.01}},
        "ema_anchor": {"entry": {"ema_dist": -0.02}},
        "trailing_grid_v7": {"entry": {"grid_spacing_pct": 0.03}},
    }
    strategy_bounds = {
        "trailing_martingale": {"entry": {"grid_spacing_pct": [0.001, 0.1]}},
        "ema_anchor": {"entry": {"ema_dist": [-0.2, 0.0]}},
        "trailing_grid_v7": {"entry": {"grid_spacing_pct": [0.001, 0.2]}},
    }
    return {
        "config_version": "v8.0.0",
        "backtest": {"exchanges": ["bybit", "binance"], "start_date": "2022-01-01", "end_date": "2026-01-01"},
        "live": {"strategy_kind": "ema_anchor"},
        "bot": {
            "long": {"strategy": copy.deepcopy(strategy_defaults), "risk": {"n_positions": 5}},
            "short": {"strategy": copy.deepcopy(strategy_defaults), "risk": {"n_positions": 3}},
        },
        "optimize": {
            "backend": "pymoo",
            "seed": 12345,
            "n_cpus": 7,
            "bounds": {
                "long": {"strategy": copy.deepcopy(strategy_bounds)},
                "short": {"strategy": copy.deepcopy(strategy_bounds)},
            },
            "pymoo": {
                "algorithm": "nsga3",
                "algorithms": {"nsga2": {}, "nsga3": {"ref_dirs": {"method": "das_dennis", "n_partitions": 8}}},
                "shared": {"eliminate_duplicates": False, "mutation_prob_var": 0.2},
            },
            "scoring": [{"metric": "adg_strategy_eq", "goal": "max"}],
            "limits": [{"metric": "drawdown_worst_strategy_eq", "penalize_if": "greater_than", "value": 0.5}],
            "fixed_params": ["bot.long.risk.n_positions"],
            "fixed_runtime_overrides": {"bot.short.risk.n_positions": 2},
            "enable_overrides": ["mirror_short_from_long"],
            "write_all_results": True,
            "compress_results_file": False,
        },
        "pbgui": {
            "optimize_runtime": {
                "mode": "fresh",
                "fine_tune_params": ["bot.long.risk.n_positions"],
                "polish_percentage": 10,
                "polish_bounds_mode": "override-all",
            },
            "additional_parameters": {"future_runtime_option": {"enabled": True}},
        },
    }


def _forager_search_config() -> dict:
    """Return a compact active-side config with tunable forager signals."""
    return {
        "backtest": {"exchanges": ["bybit"]},
        "bot": {
            "long": {
                "risk": {"n_positions": 1, "total_wallet_exposure_limit": 2.0},
                "forager": {
                    "score_weights": {"ema_readiness": 0.3, "volume": 0.4, "volatility": 0.3},
                    "volume_drop_pct": 0.0,
                    "volume_ema_span_1m": 1400.0,
                    "volatility_ema_span_1m": 360.0,
                },
            },
            "short": {"risk": {"n_positions": 0, "total_wallet_exposure_limit": 0.0}},
        },
        "optimize": {
            "bounds": {
                "long": {
                    "forager": {
                        "score_weights_volume": [0.0, 1.0, 0.01],
                        "score_weights_volatility": [0.0, 1.0, 0.01],
                        "volume_drop_pct": [0.0, 0.0, 0.01],
                        "volume_ema_span_1m": [0.0, 0.0, 1.0],
                        "volatility_ema_span_1m": [0.0, 0.0, 1.0],
                    }
                }
            },
            "fixed_params": [],
        },
    }


def _write_all_results(path: Path, *entries: dict) -> None:
    """Write complete MessagePack records to one isolated result artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(msgpack.packb(entry, use_bin_type=True) for entry in entries))


def _make_resumable_result(result: Path, config: dict | None = None) -> Path:
    """Create the minimum native artifacts required for exact PB8 resume."""
    result.mkdir(parents=True, exist_ok=True)
    (result / "checkpoint.pkl").write_bytes(b"managed-test-checkpoint")
    _write_all_results(result / "all_results.bin", copy.deepcopy(config or _full_pb8_config()))
    return result


def test_config_bundle_round_trips_all_strategies_and_optimizer_options(optimize_v8_roots) -> None:
    """PB8 nested strategies, bounds, backends, and expert fields must survive save/load unchanged."""
    config = _full_pb8_config()
    optimize_v8.save_config("all-options", config, session=None)
    loaded = optimize_v8.get_config("all-options", None)["config"]

    assert loaded["live"]["strategy_kind"] == "ema_anchor"
    assert set(loaded["bot"]["long"]["strategy"]) == {"trailing_martingale", "ema_anchor", "trailing_grid_v7"}
    assert set(loaded["optimize"]["bounds"]["long"]["strategy"]) == {
        "trailing_martingale",
        "ema_anchor",
        "trailing_grid_v7",
    }
    assert loaded["optimize"]["pymoo"]["algorithms"]["nsga3"]["ref_dirs"]["n_partitions"] == 8
    assert loaded["optimize"]["fixed_runtime_overrides"] == {"bot.short.risk.n_positions": 2}
    assert loaded["pbgui"]["additional_parameters"]["future_runtime_option"]["enabled"] is True


def test_config_save_moves_misplaced_hsl_values_to_pb8_bot_schema(optimize_v8_roots) -> None:
    """Old editor payloads must persist HSL values where PB8's loader accepts them."""
    config = _full_pb8_config()
    config["optimize"]["fixed_runtime_overrides"].update(
        {
            "bot.long.hsl.enabled": True,
            "bot.long.hsl.no_restart_drawdown_threshold": 0.75,
            "bot.long.hsl.restart_after_red_policy": "threshold",
        }
    )

    optimize_v8.save_config("hsl-schema", config, session=None)
    loaded = optimize_v8.get_config("hsl-schema", None)["config"]

    assert loaded["bot"]["long"]["hsl"] == {
        "enabled": True,
        "no_restart_drawdown_threshold": 0.75,
    }
    assert loaded["optimize"]["fixed_runtime_overrides"] == {
        "bot.short.risk.n_positions": 2,
        "bot.long.hsl.restart_after_red_policy": "threshold",
    }


def test_save_rejects_all_invalid_forager_span_ranges_together(optimize_v8_roots) -> None:
    """A search space must not emit Pareto configs that PB8 Backtest cannot reload."""
    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.save_config("invalid-forager", _forager_search_config(), session=None)

    assert exc_info.value.status_code == 422
    assert "volume_ema_span_1m must stay > 0" in exc_info.value.detail
    assert "volatility_ema_span_1m must stay > 0" in exc_info.value.detail
    assert not (optimize_v8_roots[0] / "invalid-forager").exists()


def test_forager_zero_spans_remain_valid_when_their_signals_are_fixed_off() -> None:
    """Zero spans remain available when no candidate can enable their signals."""
    config = _forager_search_config()
    forager_bounds = config["optimize"]["bounds"]["long"]["forager"]
    forager_bounds["score_weights_volume"] = [0.0, 0.0, 0.01]
    forager_bounds["score_weights_volatility"] = [0.0, 0.0, 0.01]
    config["bot"]["long"]["forager"]["score_weights"]["volume"] = 0.0
    config["bot"]["long"]["forager"]["score_weights"]["volatility"] = 0.0

    optimize_v8._validate_forager_optimize_search_space(config)


def test_fixed_forager_spans_use_current_positive_values() -> None:
    """Fixed span selectors must ignore dormant zero-valued optimize bounds."""
    config = _forager_search_config()
    config["optimize"]["fixed_params"] = [
        "bot.long.forager.volume_ema_span_1m",
        "bot.long.forager.volatility_ema_span_1m",
    ]

    optimize_v8._validate_forager_optimize_search_space(config)


def test_queue_snapshot_is_immutable_after_config_save(optimize_v8_roots) -> None:
    """Editing a managed config after queueing must not mutate the queued PB8 snapshot."""
    config = _full_pb8_config()
    optimize_v8.save_config("immutable", config, session=None)
    filename = optimize_v8.add_to_queue({"name": "immutable"}, None)["filename"]
    snapshot_before = optimize_v8._read_json(optimize_v8._snapshot_file(filename))

    edited = copy.deepcopy(config)
    edited["optimize"]["seed"] = 999
    optimize_v8.save_config("immutable", edited, session=None)

    snapshot_after = optimize_v8._read_json(optimize_v8._snapshot_file(filename))
    assert snapshot_before == snapshot_after
    assert snapshot_after["optimize"]["seed"] == 12345


def test_config_lock_recovers_interrupted_bundle_swap(optimize_v8_roots) -> None:
    """A bundle backup left between atomic renames must become the authoritative config again."""
    configs, _queue, _logs, _results = optimize_v8_roots
    backup = configs / f".recover-me.backup-{'a' * 32}"
    backup.mkdir()
    (backup / "optimize.json").write_text(json.dumps(_full_pb8_config()), encoding="utf-8")

    with optimize_v8._config_lock():
        pass

    assert (configs / "recover-me" / "optimize.json").is_file()
    assert not backup.exists()


def test_fresh_pareto_and_checkpoint_actions_are_distinct(optimize_v8_roots) -> None:
    """Fresh requeue, Pareto seeding, and checkpoint resume must persist separate next-launch modes."""
    _configs, _queue, _logs, results = optimize_v8_roots
    optimize_v8.save_config("modes", _full_pb8_config(), session=None)
    filename = optimize_v8.add_to_queue({"name": "modes"}, None)["filename"]
    result = results / "managed-result"
    pareto = result / "pareto" / "seed.json"
    pareto.parent.mkdir(parents=True)
    pareto.write_text(json.dumps(_full_pb8_config()), encoding="utf-8")
    _make_resumable_result(result)

    assert optimize_v8.requeue_fresh(filename, None)["mode"] == "fresh"
    assert optimize_v8.continue_from_pareto(filename, {"source": str(pareto)}, None)["mode"] == "pareto_seed"
    assert optimize_v8.resume_checkpoint(filename, {"source": str(result)}, None)["mode"] == "checkpoint_resume"
    assert optimize_v8._read_json(optimize_v8._queue_file(filename))["launch_options"]["source"] == str(result)


def test_pareto_directory_and_editor_seed_metadata_are_valid_launch_sources(optimize_v8_roots) -> None:
    """The shared editor's result/pareto path and self seed mode map to PB8 --start."""
    _configs, _queue, _logs, results = optimize_v8_roots
    pareto_dir = results / "managed-result" / "pareto"
    pareto_dir.mkdir(parents=True)
    (pareto_dir / "seed.json").write_text("{}", encoding="utf-8")

    directory_options = optimize_v8._validate_launch_options(
        {"mode": "pareto_seed", "source": str(pareto_dir)}
    )
    path_options = optimize_v8._runtime_options_from_config(
        {"pbgui": {"optimize_seed_mode": "path", "optimize_seed_path": str(pareto_dir)}}
    )
    self_options = optimize_v8._runtime_options_from_config(
        {"pbgui": {"optimize_seed_mode": "self"}}
    )

    assert directory_options["source"] == str(pareto_dir)
    assert path_options["mode"] == "pareto_seed"
    assert path_options["source"] == str(pareto_dir)
    assert self_options == {
        "mode": "pareto_seed",
        "source": "__self__",
        "fine_tune_params": [],
        "polish_percentage": None,
        "polish_bounds_mode": "clamp",
    }


def test_polish_percentage_is_a_fraction_with_intentional_expansion_gating() -> None:
    """The runner value stays fractional and values above 100% require an override bounds mode."""
    fractional = optimize_v8._validate_launch_options(
        {"polish_percentage": 0.2, "polish_bounds_mode": "clamp"}
    )
    expanded = optimize_v8._validate_launch_options(
        {"polish_percentage": 1.5, "polish_bounds_mode": "override-all"}
    )

    assert fractional["polish_percentage"] == 0.2
    assert expanded["polish_percentage"] == 1.5
    with pytest.raises(HTTPException, match="100%"):
        optimize_v8._validate_launch_options(
            {"polish_percentage": 1.01, "polish_bounds_mode": "clamp"}
        )


def test_checkpoint_resume_rejects_unmanaged_pickle(optimize_v8_roots, tmp_path) -> None:
    """Checkpoint resume must never accept a pickle outside PBGui's PB8 result root."""
    external = tmp_path / "external-result"
    external.mkdir()
    (external / "checkpoint.pkl").write_bytes(b"untrusted")

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8._validate_launch_options({"mode": "checkpoint_resume", "source": str(external)})
    assert exc_info.value.status_code == 400


def test_process_ownership_requires_create_time_and_exact_runner_markers(optimize_v8_roots, monkeypatch) -> None:
    """PID reuse or a command mismatch must not grant process-control ownership."""
    filename = "ownership-job"
    expected_command = [
        "/venv/bin/python",
        str(Path(optimize_v8.PBGDIR) / "pb8_optimize_runner.py"),
        "optimize",
        str(optimize_v8._state_file(filename)),
        str(optimize_v8._pid_file(filename)),
        str(optimize_v8._ready_file(filename)),
        "/venv/bin/passivbot",
        "/pb8",
        str(optimize_v8._launch_config_file(filename).resolve()),
        str(optimize_v8._launch_options_file(filename).resolve()),
    ]

    class FakeProcess:
        """Minimal process identity returned by psutil."""

        def __init__(self, pid: int) -> None:
            self.pid = pid

        def create_time(self) -> float:
            return 50.0

        def cmdline(self) -> list[str]:
            return expected_command

    monkeypatch.setattr(optimize_v8.psutil, "Process", FakeProcess)

    assert optimize_v8._process_matches(filename, {"pid": 42, "create_time": 50.0}) is True
    assert optimize_v8._process_matches(filename, {"pid": 42, "create_time": 49.0}) is False
    expected_command[2] = "backtest"
    assert optimize_v8._process_matches(filename, {"pid": 42, "create_time": 50.0}) is False


def test_linux_optimizer_uses_separate_transient_systemd_unit(optimize_v8_roots, monkeypatch) -> None:
    """A PB8 optimizer must leave the API service cgroup so API restarts cannot stop it."""
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(optimize_v8, "_systemd_user_manager_available", lambda: True)
    monkeypatch.setattr(optimize_v8, "which", lambda _name: "/usr/bin/systemd-run")
    monkeypatch.setattr(optimize_v8.subprocess, "run", fake_run)
    log_path = optimize_v8._log_dir() / "persistent-job.log"
    command = ["/venv/bin/python", "/pbgui/pb8_optimize_runner.py", "optimize"]

    process = optimize_v8._launch_optimizer_runner("persistent-job", command, Path("/pb8"), log_path)

    assert process is None
    launched, kwargs = calls[0]
    assert launched[:4] == ["/usr/bin/systemd-run", "--user", "--quiet", "--collect"]
    assert any(part.startswith("--unit=pbgui-pb8-optimize-persistent-job-") for part in launched)
    assert "--property=Type=exec" in launched
    assert f"--property=StandardOutput=append:{log_path}" in launched
    assert launched[-3:] == command
    assert kwargs["timeout"] == 15
    assert log_path.read_text(encoding="utf-8") == ""


def test_all_results_parser_reconstructs_incremental_msgpack(tmp_path) -> None:
    """PB8 compressed all_results entries must be reconstructed rather than treated as standalone rows."""
    path = tmp_path / "all_results.bin"
    first = {"metrics": {"objectives": {"adg": 0.1, "drawdown": 0.4}}, "candidate": 1}
    diff = {
        "metrics": {"objectives": {"adg": 0.2, "drawdown": {"__passivbot_diff_delete__": True}}},
        "candidate": 2,
    }
    path.write_bytes(msgpack.packb(first, use_bin_type=True) + msgpack.packb(diff, use_bin_type=True))

    progress = optimize_v8._all_results_progress(path)

    assert progress["evaluations"] == 2
    assert progress["latest"] == {"adg": 0.2}
    assert progress["trailing_partial_entry"] is False

    path.write_bytes(path.read_bytes() + msgpack.packb({"candidate": "incomplete"}, use_bin_type=True)[:-2])
    partial_progress = optimize_v8._all_results_progress(path)
    assert partial_progress["evaluations"] == 2
    assert partial_progress["trailing_partial_entry"] is True


def test_suite_metrics_keep_names_goals_scenarios_and_median() -> None:
    """Suite summaries must prefer named metrics over engine-space objectives for every projection."""
    data = {
        "optimize": {
            "scoring": [
                {"metric": "adg_w_usd", "goal": "max"},
                {"metric": "drawdown_worst_usd", "goal": "min"},
            ]
        },
        "metrics": {"objectives": {"w_0": -1.0, "w_1": 2.0}},
        "suite_metrics": {
            "scenario_labels": ["bull", "bear"],
            "metrics": {
                "adg_w_usd": {
                    "aggregated": 0.12,
                    "stats": {"mean": 0.12, "min": 0.03, "max": 0.2, "std": 0.04, "median": 0.11},
                    "scenarios": {"bull": 0.2, "bear": 0.03},
                },
                "drawdown_worst_usd": {
                    "aggregated": 0.3,
                    "stats": {"mean": 0.3, "min": 0.2, "max": 0.4, "std": 0.1, "median": 0.28},
                    "scenarios": {"bull": 0.2, "bear": 0.4},
                },
            },
        },
    }

    assert optimize_v8._pareto_summary(data, "median") == {"adg_w_usd": 0.11, "drawdown_worst_usd": 0.28}
    assert optimize_v8._pareto_summary(data, "std") == {"adg_w_usd": 0.04, "drawdown_worst_usd": 0.1}
    assert optimize_v8._pareto_summary(data, "mean", "bear") == {"adg_w_usd": 0.03, "drawdown_worst_usd": 0.4}
    contract = optimize_v8._pareto_contract(data)
    assert contract == {
        "mode": "suite",
        "scenario_count": 2,
        "scenario_labels": ["bull", "bear"],
        "objectives": [
            {"metric": "adg_w_usd", "goal": "max"},
            {"metric": "drawdown_worst_usd", "goal": "min"},
        ],
    }
    single = {"metrics": {"objectives": {"w_0": 0.5, "w_1": 0.7}}}
    assert optimize_v8._pareto_contract(single)["mode"] == "stats"
    assert optimize_v8._pareto_summary(single, "mean") == {"w_0": 0.5, "w_1": 0.7}
    assert optimize_v8._pareto_contract({"analyses_combined": {}})["mode"] == "legacy"
    assert optimize_v8._pareto_contract({})["mode"] == "unknown"


def test_incremental_result_cache_appends_truncates_and_bounds_entries(tmp_path, monkeypatch) -> None:
    """Progress polling decodes append deltas, resets replacements, survives bad tails, and stays bounded."""
    first_path = tmp_path / "first.bin"
    _write_all_results(first_path, {"metrics": {"objectives": {"score": 1.0}}})
    assert optimize_v8._all_results_progress(first_path)["evaluations"] == 1

    with first_path.open("ab") as handle:
        handle.write(msgpack.packb({"metrics": {"objectives": {"score": 2.0}}}, use_bin_type=True))
    appended = optimize_v8._all_results_progress(first_path)
    assert appended["evaluations"] == 2
    assert appended["latest"] == {"score": 2.0}

    _write_all_results(first_path, {"metrics": {"objectives": {"replacement": 9.0}}})
    replaced = optimize_v8._all_results_progress(first_path)
    assert replaced["evaluations"] == 1
    assert replaced["latest"] == {"replacement": 9.0}

    with first_path.open("ab") as handle:
        handle.write(b"\xc1")
    malformed = optimize_v8._all_results_progress(first_path)
    assert malformed["evaluations"] == 1
    assert "error" in malformed

    monkeypatch.setattr(optimize_v8, "_RESULT_PROGRESS_CACHE_MAX_ENTRIES", 2)
    for index in range(3):
        path = tmp_path / f"cache-{index}.bin"
        _write_all_results(path, {"candidate": index})
        optimize_v8._all_results_progress(path)
    assert len(optimize_v8._result_progress_cache) == 2


def test_checkpoint_readiness_and_checkpoint_only_config_recovery(optimize_v8_roots) -> None:
    """Exact resume is advertised only for strict artifacts and can recover config without Pareto JSON."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = _make_resumable_result(results / "checkpoint-only")

    readiness = optimize_v8._checkpoint_resume_readiness(result)
    listed = optimize_v8.list_results(None)["results"][0]
    loaded = optimize_v8.get_result_config(str(result), None)["config"]

    assert readiness["ready"] is True
    assert listed["checkpoint"] is True
    assert listed["resumable"] is True
    assert listed["has_config"] is True
    assert listed["has_pareto"] is False
    assert listed["supports_3d"] is False
    assert listed["supports_dash"] is False
    assert loaded["optimize"]["write_all_results"] is True
    assert loaded["live"]["strategy_kind"] == "ema_anchor"

    (result / "all_results.bin").write_bytes(b"\xc1")
    rejected = optimize_v8._checkpoint_resume_readiness(result)
    assert rejected["ready"] is False
    assert any("malformed" in reason for reason in rejected["reasons"])


def test_large_result_listing_defers_full_stream_scan(optimize_v8_roots, monkeypatch) -> None:
    """Configs and results remain immediately visible before a large stream cache is warm."""
    _configs, _queue, _logs, results = optimize_v8_roots
    config = _full_pb8_config()
    optimize_v8._save_config_bundle("visible-config", config)
    result = results / "large-result"
    result.mkdir()
    (result / "checkpoint.pkl").write_bytes(b"checkpoint")
    _write_all_results(result / "all_results.bin", config)
    pareto = result / "pareto"
    pareto.mkdir()
    (pareto / "candidate.json").write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.setattr(optimize_v8, "_RESULT_LIST_SCAN_LIMIT_BYTES", 0)
    monkeypatch.setattr(
        optimize_v8,
        "_all_results_progress",
        lambda _path: pytest.fail("large result stream must not be decoded for list views"),
    )

    listed_results = optimize_v8.list_results(None)["results"]
    listed_configs = optimize_v8.list_configs(None)["configs"]

    assert [item["result"] for item in listed_results] == ["large-result"]
    assert listed_results[0]["progress"]["scan_deferred"] is True
    assert listed_results[0]["resumable"] is True
    assert [item["name"] for item in listed_configs] == ["visible-config"]


def test_resume_compatibility_rejects_before_queue_mutation(optimize_v8_roots, monkeypatch) -> None:
    """A native-prepared incompatible checkpoint must not stop or alter the existing queue item."""
    _configs, _queue, _logs, results = optimize_v8_roots
    optimize_v8.save_config("resume-source", _full_pb8_config(), None)
    filename = optimize_v8.add_to_queue({"name": "resume-source"}, None)["filename"]
    incompatible = _full_pb8_config()
    incompatible["optimize"]["backend"] = "other-backend"
    result = _make_resumable_result(results / "incompatible", incompatible)
    terminated = []
    monkeypatch.setattr(optimize_v8, "_terminate_verified", lambda value: terminated.append(value))

    before = optimize_v8._read_json(optimize_v8._queue_file(filename))
    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.resume_checkpoint(filename, {"source": str(result)}, None)
    after = optimize_v8._read_json(optimize_v8._queue_file(filename))

    assert exc_info.value.status_code == 422
    assert "optimize.backend" in str(exc_info.value.detail)
    assert terminated == []
    assert after == before


def test_transactional_result_resume_supports_checkpoint_only_artifacts(optimize_v8_roots) -> None:
    """One result action creates a config and resumable queue item from native checkpoint artifacts only."""
    configs, queue, _logs, results = optimize_v8_roots
    result = _make_resumable_result(results / "checkpoint-only-transaction")

    response = optimize_v8.queue_result_resume(
        {"name": "checkpoint-only-resume", "path": str(result)},
        None,
    )

    assert response["ok"] is True
    assert (configs / "checkpoint-only-resume" / "optimize.json").is_file()
    record = optimize_v8._read_json(queue / f"{response['filename']}.json")
    assert record["name"] == "checkpoint-only-resume"
    assert record["launch_options"]["mode"] == "checkpoint_resume"
    assert record["launch_options"]["source"] == str(result)


def test_transactional_result_resume_rolls_back_new_config_on_queue_failure(optimize_v8_roots, monkeypatch) -> None:
    """A queue persistence failure must not leave the newly managed resume config behind."""
    configs, queue, _logs, results = optimize_v8_roots
    result = _make_resumable_result(results / "rollback-result")
    monkeypatch.setattr(
        optimize_v8,
        "_create_queue_record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("queue unavailable")),
    )

    with pytest.raises(OSError, match="queue unavailable"):
        optimize_v8.queue_result_resume({"name": "rollback-resume", "path": str(result)}, None)

    assert not (configs / "rollback-resume").exists()
    assert list(queue.glob("*.json")) == []


def test_queue_record_creation_removes_partial_snapshot_on_write_failure(optimize_v8_roots, monkeypatch) -> None:
    """A failed queue record write must clean its generated immutable snapshot directory."""
    _configs, queue, _logs, _results = optimize_v8_roots
    original_write = optimize_v8._write_json

    def fail_queue_record(path: Path, payload: dict) -> None:
        if path.parent == queue and path.suffix == ".json":
            raise OSError("queue record write failed")
        original_write(path, payload)

    monkeypatch.setattr(optimize_v8, "_write_json", fail_queue_record)

    with pytest.raises(OSError, match="queue record write failed"):
        optimize_v8._create_queue_record("partial", _full_pb8_config(), {"mode": "fresh", "source": ""})

    assert list(queue.glob("*.json")) == []
    snapshots = queue / "snapshots"
    assert not snapshots.exists() or list(snapshots.iterdir()) == []


def test_transactional_result_resume_validates_before_creating_managed_state(optimize_v8_roots) -> None:
    """Malformed checkpoint artifacts are rejected before a config or queue artifact is created."""
    configs, queue, _logs, results = optimize_v8_roots
    result = results / "invalid-resume"
    result.mkdir()
    (result / "checkpoint.pkl").write_bytes(b"checkpoint")
    (result / "all_results.bin").write_bytes(b"\xc1")

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.queue_result_resume({"name": "must-not-exist", "path": str(result)}, None)

    assert exc_info.value.status_code == 422
    assert not (configs / "must-not-exist").exists()
    assert list(queue.glob("*.json")) == []


def test_nested_suite_3d_plot_uses_temporary_normalized_candidates(optimize_v8_roots, monkeypatch) -> None:
    """PB8 suite values are projected into a disposable stage without changing native Pareto files."""
    from api import optimize_v7

    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "suite-plot"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    original = {}
    for index in range(2):
        payload = {
            "backtest": {"base_dir": "backtests/pbgui/suite-plot"},
            "optimize": {
                "scoring": [
                    {"metric": "quality", "goal": "max"},
                    {"metric": "risk", "goal": "min"},
                    {"metric": "stability", "goal": "max"},
                ]
            },
            "suite_metrics": {
                "scenario_labels": ["bear"],
                "metrics": {
                    "quality": {"stats": {"median": 1.0 + index}, "scenarios": {"bear": 0.5 + index}},
                    "risk": {"stats": {"median": 0.2 + index}, "scenarios": {"bear": 0.4 + index}},
                    "stability": {"stats": {"median": 3.0 + index}, "scenarios": {"bear": 2.0 + index}},
                },
            },
        }
        path = pareto / f"candidate-{index}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        original[path] = path.read_bytes()
    captured = {}

    def fake_plot(stage):
        captured["stage"] = Path(stage)
        staged = json.loads(next((Path(stage) / "pareto").glob("*.json")).read_text(encoding="utf-8"))
        captured["staged"] = staged
        return {"ok": True, "message": "PB7 staged", "html": "<div></div>", "output": "ok"}

    monkeypatch.setattr(optimize_v7, "_build_pareto_3d_plot_payload", fake_plot)
    response = optimize_v8.launch_result_3d_plot(
        {"path": str(result), "scenario": "bear", "statistic": "median"},
        None,
    )

    assert response["ok"] is True
    assert captured["staged"]["result"]["objectives"] == {"quality": 0.5, "risk": 0.4, "stability": 2.0}
    assert captured["staged"]["optimize"]["scoring"][1] == {"metric": "risk", "goal": "min"}
    assert not captured["stage"].exists()
    assert all(path.read_bytes() == content for path, content in original.items())


@pytest.mark.parametrize("blocker", ["optimizer", "dash", "queue_source"])
def test_result_deletion_blocks_every_active_owner(optimize_v8_roots, monkeypatch, blocker) -> None:
    """Optimizer ownership, exact Dash references, and queue sources each preserve result data."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / f"blocked-{blocker}"
    (result / "pareto").mkdir(parents=True)
    (result / "pareto" / "seed.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})
    if blocker == "optimizer":
        optimize_v8._write_json(optimize_v8._pid_file("active-job"), {"pid": 77, "create_time": 7.0})
        monkeypatch.setattr(optimize_v8, "_process_matches", lambda filename, _record: filename == "active-job")
    elif blocker == "dash":
        monkeypatch.setattr(
            optimize_v8,
            "_read_dash_registry",
            lambda: {"a" * 12: {"result_dir": str(result)}},
        )
    else:
        _write_queue_job("source-job", 0)
        data = optimize_v8._read_json(optimize_v8._queue_file("source-job"))
        data["launch_options"] = {"mode": "pareto_seed", "source": str(result / "pareto")}
        optimize_v8._write_json(optimize_v8._queue_file("source-job"), data)

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.delete_result(str(result), None)

    assert exc_info.value.status_code == 409
    assert result.is_dir()


def test_result_deletion_blocks_exact_active_owner_but_allows_unrelated_result(optimize_v8_roots, monkeypatch) -> None:
    """An optimizer's open result file blocks only that immediate result directory."""
    _configs, _queue, _logs, results = optimize_v8_roots
    active = results / "active-result"
    old = results / "old-result"
    for result in (active, old):
        result.mkdir()
        (result / "all_results.bin").write_bytes(b"result")
    optimize_v8._write_json(optimize_v8._pid_file("active-queue"), {"pid": 77, "create_time": 7.0})

    class Process:
        """Expose the active artifact through a recursive optimizer child."""

        def __init__(self, open_files=(), children=()):
            self._open_files = open_files
            self._children = children

        def children(self, recursive=False):
            assert recursive is True
            return self._children

        def open_files(self):
            return [type("Opened", (), {"path": str(path)})() for path in self._open_files]

    child = Process([active / "all_results.bin"])
    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(optimize_v8.psutil, "Process", lambda _pid: Process(children=[child]))
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})

    assert optimize_v8.delete_result(str(old), None)["removed"] == 1
    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.delete_result(str(active), None)

    assert exc_info.value.status_code == 409
    assert "active-queue" in str(exc_info.value.detail)
    assert "active-result" in str(exc_info.value.detail)
    assert active.is_dir()


@pytest.mark.parametrize("failure", [psutil.AccessDenied(pid=77), psutil.NoSuchProcess(pid=77)])
def test_result_deletion_blocks_when_active_ownership_inspection_fails(
    optimize_v8_roots, monkeypatch, failure
) -> None:
    """Access denial and process races must not make a potentially owned result appear safe."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "possibly-active"
    result.mkdir()
    optimize_v8._write_json(optimize_v8._pid_file("uncertain-queue"), {"pid": 77, "create_time": 7.0})

    class Process:
        """Fail exact open-file inspection after process verification."""

        def children(self, recursive=False):
            return []

        def open_files(self):
            raise failure

    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(optimize_v8.psutil, "Process", lambda _pid: Process())
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.delete_result(str(result), None)

    assert exc_info.value.status_code == 409
    assert "uncertain-queue" in str(exc_info.value.detail)
    assert result.is_dir()


def test_result_deletion_blocks_active_optimizer_before_result_is_observed(optimize_v8_roots, monkeypatch) -> None:
    """An active optimizer with no currently open result file must keep deletion conservative."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "unknown-active"
    result.mkdir()
    optimize_v8._write_json(optimize_v8._pid_file("unknown-queue"), {"pid": 77, "create_time": 7.0})

    class Process:
        """Expose a fully inspectable optimizer between result writes."""

        def children(self, recursive=False):
            assert recursive is True
            return []

        def open_files(self):
            return []

    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(optimize_v8.psutil, "Process", lambda _pid: Process())
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.delete_result(str(result), None)

    assert exc_info.value.status_code == 409
    assert "unknown-queue" in str(exc_info.value.detail)
    assert result.is_dir()


def test_observed_optimizer_result_ownership_survives_open_file_gaps(optimize_v8_roots, monkeypatch) -> None:
    """Persisted optimizer ownership must bridge intervals with no open result file."""
    _configs, _queue, _logs, results = optimize_v8_roots
    active = results / "durable-active"
    old_first = results / "durable-old-first"
    old_second = results / "durable-old-second"
    for result in (active, old_first, old_second):
        result.mkdir()
    artifact = active / "all_results.bin"
    artifact.write_bytes(b"active")
    optimize_v8._write_json(optimize_v8._pid_file("durable-queue"), {"pid": 77, "create_time": 7.0})
    open_paths = [artifact]

    class Process:
        """Switch from an open result artifact to an idle optimizer interval."""

        def children(self, recursive=False):
            assert recursive is True
            return []

        def open_files(self):
            return [type("Opened", (), {"path": str(path)})() for path in open_paths]

    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(optimize_v8.psutil, "Process", lambda _pid: Process())
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})

    assert optimize_v8.delete_result(str(old_first), None)["removed"] == 1
    record = optimize_v8._read_json(optimize_v8._pid_file("durable-queue"))
    assert record["owned_results"] == [str(active)]

    open_paths.clear()
    assert optimize_v8.delete_result(str(old_second), None)["removed"] == 1
    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.delete_result(str(active), None)

    assert exc_info.value.status_code == 409
    assert active.is_dir()


def test_known_active_result_allows_unrelated_delete_despite_child_access_denied(optimize_v8_roots, monkeypatch) -> None:
    """A positive owner mapping remains useful when inspection of another child is denied."""
    _configs, _queue, _logs, results = optimize_v8_roots
    active = results / "known-active"
    old = results / "known-old"
    active.mkdir()
    old.mkdir()
    artifact = active / "all_results.bin"
    artifact.write_bytes(b"active")
    optimize_v8._write_json(optimize_v8._pid_file("known-queue"), {"pid": 77, "create_time": 7.0})

    class Process:
        """Expose one known result while recursive child enumeration is denied."""

        def children(self, recursive=False):
            raise psutil.AccessDenied(pid=77)

        def open_files(self):
            return [type("Opened", (), {"path": str(artifact)})()]

    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(optimize_v8.psutil, "Process", lambda _pid: Process())
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})

    response = optimize_v8.delete_result(str(old), None)

    assert response["removed"] == 1
    assert active.is_dir()


def test_batch_delete_preserves_conflict_detail_without_staged_remnants(optimize_v8_roots, monkeypatch) -> None:
    """A meaningful batch conflict is re-raised before any selected result is staged."""
    _configs, _queue, _logs, results = optimize_v8_roots
    first = results / "batch-first"
    blocked = results / "batch-blocked"
    first.mkdir()
    blocked.mkdir()

    def assert_deletable(result_dir: Path) -> None:
        if result_dir == blocked:
            raise HTTPException(status_code=409, detail="batch-blocked is an active continuation source")

    monkeypatch.setattr(optimize_v8, "_assert_result_deletable", assert_deletable)

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.delete_results({"paths": [str(first), str(blocked)]}, None)

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "batch-blocked is an active continuation source"
    assert first.is_dir() and blocked.is_dir()
    assert list(results.glob(".*.delete-*")) == []


def test_batch_delete_handles_result_disappearance_deterministically(optimize_v8_roots, monkeypatch) -> None:
    """A result disappearing during staging is reported missing without rolling back other deletions."""
    _configs, _queue, _logs, results = optimize_v8_roots
    vanished = results / "vanished-during-delete"
    removed = results / "removed-normally"
    vanished.mkdir()
    removed.mkdir()
    original_stage = optimize_v8._stage_delete_result

    def stage_with_race(result_dir: Path) -> Path:
        if result_dir == vanished:
            optimize_v8.rmtree(result_dir)
            raise FileNotFoundError(str(result_dir))
        return original_stage(result_dir)

    monkeypatch.setattr(optimize_v8, "_assert_result_deletable", lambda _path: None)
    monkeypatch.setattr(optimize_v8, "_stage_delete_result", stage_with_race)

    response = optimize_v8.delete_results({"paths": [str(vanished), str(removed)]}, None)

    assert response == {"ok": True, "removed": 1, "missing": [str(vanished)]}
    assert not vanished.exists() and not removed.exists()
    assert list(results.glob(".*.delete-*")) == []


@pytest.mark.parametrize(
    ("statistic", "quality"),
    [("mean", 1.0), ("min", 2.0), ("max", 3.0), ("std", 4.0), ("median", 5.0)],
)
def test_pareto_stats_projection_prefers_stats_and_adds_canonical_gain(
    optimize_v8_roots, statistic, quality
) -> None:
    """Every supported statistic uses metric stats before scalar objective fallbacks."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "stats-result"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    values = {"aggregated": 101.0, "mean": 1.0, "min": 2.0, "max": 3.0, "std": 4.0, "median": 5.0}
    gain_values = {name: value + 10.0 for name, value in values.items()}
    (pareto / "candidate.json").write_text(
        json.dumps(
            {
                "optimize": {"scoring": [{"metric": "quality", "goal": "max"}, {"metric": "risk", "goal": "min"}]},
                "metrics": {
                    "stats": {"quality": values, "gain_usd": gain_values},
                    "objectives": {"quality": 99.0, "risk": 0.25, "gain_usd": 999.0, "gain_strategy_eq": 888.0},
                },
            }
        ),
        encoding="utf-8",
    )

    payload = optimize_v8.list_paretos(str(result), "Aggregated", statistic, None)

    assert payload["paretos"][0]["summary"] == {"quality": quality, "risk": 0.25, "gain": quality + 10.0}
    assert payload["meta"]["selected_statistic"] == statistic
    assert [spec["metric"] for spec in payload["meta"]["objectives"]] == ["quality", "risk"]


def test_suite_pareto_projection_excludes_unrelated_metrics(optimize_v8_roots) -> None:
    """Suite list rows contain configured objectives and canonical gain, not the complete metric suite."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "large-suite"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    metrics = {
        "quality": {"stats": {"median": 1.5}, "scenarios": {"bear": 1.1}},
        "gain_strategy_eq": {"stats": {"median": 2.5}, "scenarios": {"bear": 2.1}},
    }
    metrics.update({f"unrelated_{index}": {"stats": {"median": float(index)}} for index in range(150)})
    (pareto / "suite.json").write_text(
        json.dumps(
            {
                "optimize": {"scoring": [{"metric": "quality", "goal": "max"}]},
                "suite_metrics": {"scenario_labels": ["bear"], "metrics": metrics},
            }
        ),
        encoding="utf-8",
    )

    aggregated = optimize_v8.list_paretos(str(result), "Aggregated", "median", None)
    scenario = optimize_v8.list_paretos(str(result), "bear", "median", None)

    assert aggregated["paretos"][0]["summary"] == {"quality": 1.5, "gain": 2.5}
    assert scenario["paretos"][0]["summary"] == {"quality": 1.1, "gain": 2.1}
    assert aggregated["meta"]["mode"] == "suite"
    assert aggregated["meta"]["scenario_labels"] == ["bear"]
    assert "unrelated_149" not in json.dumps(aggregated)


def test_compact_pareto_cache_reuses_touches_prunes_and_clears(optimize_v8_roots, monkeypatch) -> None:
    """Only changed candidates are decoded again, deleted members vanish, and result deletion clears cache entries."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "cached-result"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    payload = {
        "optimize": {"scoring": [{"metric": "quality", "goal": "max"}]},
        "metrics": {"stats": {"quality": {"mean": 1.0}}, "objectives": {"quality": 0.0}},
    }
    paths = []
    for index in range(3):
        path = pareto / f"candidate-{index}.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        paths.append(path)
    original_read = optimize_v8._read_json
    reads = []

    def counted_read(path: Path) -> dict:
        if path.parent == pareto:
            reads.append(path.name)
        return original_read(path)

    monkeypatch.setattr(optimize_v8, "_read_json", counted_read)

    assert len(optimize_v8.list_paretos(str(result), "Aggregated", "mean", None)["paretos"]) == 3
    assert len(reads) == 3
    assert len(optimize_v8.list_paretos(str(result), "Aggregated", "median", None)["paretos"]) == 3
    assert len(reads) == 3

    touched_payload = copy.deepcopy(payload)
    touched_payload["unrelated_size_change"] = "force a new cache signature"
    paths[1].write_text(json.dumps(touched_payload), encoding="utf-8")
    optimize_v8.list_paretos(str(result), "Aggregated", "mean", None)
    assert reads.count(paths[1].name) == 2
    assert len(reads) == 4

    paths[2].unlink()
    assert len(optimize_v8.list_paretos(str(result), "Aggregated", "mean", None)["paretos"]) == 2
    assert len(reads) == 4

    monkeypatch.setattr(optimize_v8, "_assert_result_deletable", lambda _path: None)
    optimize_v8.delete_result(str(result), None)
    assert all(not key[0].startswith(str(result.resolve()) + os.sep) for key in optimize_v8._pareto_list_cache)


def test_compact_pareto_cache_has_bounded_lru_eviction(optimize_v8_roots, monkeypatch) -> None:
    """The compact per-file cache evicts least-recently-used entries at its configured bound."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "bounded-cache"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    payload = {"metrics": {"objectives": {"quality": 1.0}}}
    for index in range(3):
        (pareto / f"candidate-{index}.json").write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(optimize_v8, "_PARETO_LIST_CACHE_MAX_ENTRIES", 2)

    optimize_v8.list_paretos(str(result), "Aggregated", "mean", None)

    assert len(optimize_v8._pareto_list_cache) == 2


def test_pareto_list_skips_active_file_churn_and_malformed_json(optimize_v8_roots, monkeypatch) -> None:
    """One disappearing or malformed PB8 candidate must not fail or suppress valid rows."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "churning-result"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    valid_payload = {"metrics": {"objectives": {"quality": 1.0}}}
    valid = pareto / "valid.json"
    churn = pareto / "churn.json"
    malformed = pareto / "malformed.json"
    valid.write_text(json.dumps(valid_payload), encoding="utf-8")
    churn.write_text(json.dumps(valid_payload), encoding="utf-8")
    malformed.write_text("{not-json", encoding="utf-8")
    original_read = optimize_v8._read_json
    warnings = []

    def churning_read(path: Path) -> dict:
        if path == churn:
            path.unlink()
        return original_read(path)

    monkeypatch.setattr(optimize_v8, "_read_json", churning_read)
    monkeypatch.setattr(optimize_v8, "_log", lambda _service, message, **kwargs: warnings.append((message, kwargs)))

    payload = optimize_v8.list_paretos(str(result), "Aggregated", "mean", None)

    assert [row["name"] for row in payload["paretos"]] == ["valid"]
    assert len(warnings) == 1
    assert "Skipped 2" in warnings[0][0]


def test_result_mtime_skips_files_replaced_during_active_optimize(tmp_path: Path) -> None:
    """Result listing must retain its directory when a scanned Pareto file disappears."""
    result = tmp_path / "active-result"
    result.mkdir()
    candidate = result / "candidate.json"
    candidate.write_text("{}", encoding="utf-8")
    candidate.unlink()

    modified = optimize_v8._latest_existing_mtime([candidate, result])

    assert modified == result.stat().st_mtime


def test_thousand_pareto_candidates_keep_structural_payload_bounded(optimize_v8_roots) -> None:
    """Large Pareto fronts return fixed compact summaries without timing-dependent assertions."""
    _configs, _queue, _logs, results = optimize_v8_roots
    result = results / "thousand-candidates"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    for index in range(1000):
        payload = {
            "optimize": {"scoring": [{"metric": "quality", "goal": "max"}]},
            "metrics": {
                "stats": {"quality": {"mean": float(index)}, "gain_usd": {"mean": float(index + 1)}},
                "objectives": {"unrelated": 999.0},
            },
        }
        (pareto / f"candidate-{index:04d}.json").write_text(json.dumps(payload), encoding="utf-8")

    response = optimize_v8.list_paretos(str(result), "Aggregated", "mean", None)

    assert len(response["paretos"]) == 1000
    assert all(set(row["summary"]) == {"quality", "gain"} for row in response["paretos"])
    assert len(json.dumps(response)) < 500_000


def test_queue_status_returns_complete_shared_dashboard_shape(optimize_v8_roots) -> None:
    """PB8 queue status includes progress, runtime, process, host, log, totals, and runner sections."""
    _write_queue_job("status-job", 0)
    launch = optimize_v8._launch_dir("status-job")
    launch.mkdir(parents=True)
    optimize_v8._write_json(
        optimize_v8._launch_config_file("status-job"),
        {
            "backtest": {"exchanges": ["bybit"]},
            "optimize": {
                "backend": "pymoo",
                "iters": 200,
                "n_cpus": 4,
                "pymoo": {"algorithm": "nsga3"},
                "scoring": [
                    {"metric": "quality", "goal": "max"},
                    {"metric": "risk", "goal": "min"},
                ],
            },
        },
    )
    (optimize_v8._log_dir() / "status-job.log").write_text(
        "2026-07-21T12:00:00Z INFO Selected optimizer backend: pymoo\n"
        "2026-07-21T12:00:01Z INFO Using pymoo nsga3 | n_obj=2\n"
        "2026-07-21T12:00:02Z INFO Pareto update | eval=50 | front=4 | objectives=[quality=1.2,risk=0.3] | constraint=0\n",
        encoding="utf-8",
    )

    status = optimize_v8.get_queue_status("status-job", None)

    assert {"progress", "runtime", "metrics", "process", "system", "queue", "log", "runner"} <= status.keys()
    assert status["progress"]["evaluations"] == 50
    assert status["progress"]["target_evaluations"] == 200
    assert status["progress"]["percent"] == 25.0
    assert status["runtime"]["backend"] == "pymoo"
    assert status["runtime"]["algorithm"] == "nsga3"
    assert status["runtime"]["objective_count"] == 2
    assert status["metrics"]["ranges"] == {}
    assert status["log"]["exists"] is True
    assert status["queue"]["queued"] == 1
    assert {"cpu_percent", "memory_percent", "swap_percent"} <= status["system"].keys()


def test_config_metadata_seed_backtests_and_result_mode(optimize_v8_roots) -> None:
    """Config rows use shared seed metadata, owned backtest counts, null uncertainty, and result mode."""
    configs, _queue, _logs, results = optimize_v8_roots
    config = _full_pb8_config()
    config["pbgui"] = {"optimize_runtime": {"mode": "fresh"}, "optimize_seed_mode": "self"}
    optimize_v8.save_config("owned", config, None)
    analysis = optimize_v8._backtests_root() / "owned" / "run-1" / "analysis.json"
    analysis.parent.mkdir(parents=True)
    analysis.write_text("{}", encoding="utf-8")

    optimize_v8.save_config("uncertain", _full_pb8_config(), None)
    uncertain_path = configs / "uncertain" / "optimize.json"
    uncertain = json.loads(uncertain_path.read_text(encoding="utf-8"))
    uncertain["backtest"]["base_dir"] = "custom/location"
    uncertain_path.write_text(json.dumps(uncertain), encoding="utf-8")

    pareto = results / "suite-owned" / "pareto" / "candidate.json"
    pareto.parent.mkdir(parents=True)
    suite_result = _full_pb8_config()
    suite_result["backtest"]["base_dir"] = "backtests/pbgui/owned"
    suite_result["suite_metrics"] = {
        "scenario_labels": ["bull", "bear"],
        "metrics": {"quality": {"aggregated": 1.0}},
    }
    pareto.write_text(json.dumps(suite_result), encoding="utf-8")

    rows = {row["name"]: row for row in optimize_v8.list_configs(None)["configs"]}

    assert rows["owned"]["seed_mode"] == "self"
    assert rows["owned"]["seed_source"] == "__self__"
    assert rows["owned"]["backtest_count"] == 1
    assert rows["owned"]["result_mode"] == "suite"
    assert rows["owned"]["scenario_count"] == 2
    assert rows["uncertain"]["backtest_count"] is None


def test_pareto_seed_validation_rejects_empty_and_unrelated_sources(optimize_v8_roots) -> None:
    """Only confined native PB8 seed files and non-empty managed seed directories are accepted."""
    _configs, _queue, _logs, results = optimize_v8_roots
    pareto = results / "seed-validation" / "pareto"
    pareto.mkdir(parents=True)

    with pytest.raises(HTTPException) as empty_error:
        optimize_v8._validate_launch_options({"mode": "pareto_seed", "source": str(pareto)})
    assert empty_error.value.status_code == 422

    unrelated = pareto / "notes.txt"
    unrelated.write_text("not a seed", encoding="utf-8")
    with pytest.raises(HTTPException) as unrelated_error:
        optimize_v8._validate_launch_options({"mode": "pareto_seed", "source": str(unrelated)})
    assert unrelated_error.value.status_code == 422

    native = pareto / "population_pareto.txt"
    native.write_text("native seed", encoding="utf-8")
    assert optimize_v8._validate_launch_options({"mode": "pareto_seed", "source": str(native)})["source"] == str(native)
    assert optimize_v8._validate_launch_options({"mode": "pareto_seed", "source": str(pareto)})["source"] == str(pareto)


def test_global_autostart_claim_blocks_only_other_automatic_claims(tmp_path, monkeypatch) -> None:
    """One version's automatic claim blocks the other while manual launch code remains independent."""
    lock_root = tmp_path / "locks"
    monkeypatch.setattr(optimize_autostart, "_state_root", lambda: lock_root)
    monkeypatch.setattr(optimize_autostart, "_optimizer_process_running", lambda: False)

    assert optimize_autostart.claim_autostart("v7", "v7-job") is True
    assert optimize_autostart.claim_autostart("v8", "v8-job") is False
    optimize_autostart.release_autostart("v7", "v7-job")
    assert optimize_autostart.claim_autostart("v8", "v8-job") is True


@pytest.mark.parametrize(
    "command",
    [
        ["/venv/bin/python", "-u", "/pb7/src/optimize.py", "/tmp/config.json"],
        ["/venv/bin/python", "/pbgui/pb8_optimize_runner.py", "optimize", "/tmp/state"],
        ["/venv/bin/passivbot", "optimize", "/tmp/config.json"],
    ],
)
def test_optimizer_command_recognizes_manual_and_managed_jobs(command) -> None:
    """Autostart arbitration recognizes every supported PB7/PB8 launch shape."""
    assert optimize_autostart._is_optimizer_command(command) is True


def test_autostart_waits_while_a_manual_optimizer_is_running(tmp_path, monkeypatch) -> None:
    """Automatic PB7/PB8 starts must not overlap an already manual optimizer job."""
    monkeypatch.setattr(optimize_autostart, "_state_root", lambda: tmp_path / "locks")
    monkeypatch.setattr(optimize_autostart, "_optimizer_process_running", lambda: True)

    assert optimize_autostart.claim_autostart("v8", "queued-v8-job") is False


def test_live_pending_autostart_claim_survives_ttl(tmp_path, monkeypatch) -> None:
    """A slow PB8 preparation handshake cannot lose its global slot while its API owner is alive."""
    monkeypatch.setattr(optimize_autostart, "_state_root", lambda: tmp_path / "locks")
    monkeypatch.setattr(optimize_autostart, "_optimizer_process_running", lambda: False)
    monkeypatch.setattr(optimize_autostart, "_PENDING_TTL_SECONDS", 1.0)

    assert optimize_autostart.claim_autostart("v8", "slow-job") is True
    state = optimize_autostart._read_state()
    monkeypatch.setattr(optimize_autostart.time, "time", lambda: float(state["claimed_at"]) + 1000.0)

    assert optimize_autostart._claim_is_active(state) is True
    assert optimize_autostart.claim_autostart("v7", "other-job") is False


def test_manual_start_does_not_acquire_autostart_slot(monkeypatch) -> None:
    """The explicit PB8 start endpoint must call the worker without global autostart arbitration."""
    captured = {}
    monkeypatch.setattr(optimize_v8, "claim_autostart", lambda *args: pytest.fail("manual start claimed autostart"))
    monkeypatch.setattr(
        optimize_v8._worker,
        "launch",
        lambda filename, options, automatic: captured.update(filename=filename, options=options, automatic=automatic)
        or {"pid": 42},
    )

    assert optimize_v8.start_queue_item("manual-job", {"launch_options": {"mode": "fresh"}}, None) == {
        "ok": True,
        "pid": 42,
    }
    assert captured == {"filename": "manual-job", "options": {"mode": "fresh"}, "automatic": False}


def test_route_surface_covers_configs_queue_results_and_paretos() -> None:
    """The PB8 router must expose the backend panels and distinct runtime actions."""
    routes = {(method, route.path) for route in optimize_v8.router.routes for method in getattr(route, "methods", set())}
    expected = {
        ("GET", "/configs"),
        ("PUT", "/configs/{name}"),
        ("POST", "/queue"),
        ("POST", "/queue/{filename}/requeue-fresh"),
        ("POST", "/queue/{filename}/continue-pareto"),
        ("POST", "/queue/{filename}/resume-checkpoint"),
        ("GET", "/results"),
        ("GET", "/paretos"),
        ("POST", "/paretos/seed-bundle"),
        ("POST", "/results/3d-plot"),
        ("POST", "/results/pareto-dash"),
        ("POST", "/results/resume"),
        ("GET", "/main_page"),
        ("POST", "/ohlcv-preflight"),
    }
    assert expected <= routes


def test_ohlcv_preflight_logs_and_transforms_runtime_unavailable(monkeypatch) -> None:
    """PB8 update/runtime failures must be logged and exposed as retryable HTTP 503."""
    messages = []

    async def fail_preflight(_config):
        raise optimize_v8.PB8OhlcvUnavailableError("PB8 update active")

    monkeypatch.setattr(optimize_v8, "build_pb8_ohlcv_preflight", fail_preflight)
    monkeypatch.setattr(
        optimize_v8,
        "_log",
        lambda service, message, **kwargs: messages.append((service, message, kwargs)),
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(optimize_v8.get_ohlcv_preflight({"config": {}}, None))

    assert error.value.status_code == 503
    assert error.value.detail == "PB8 update active"
    assert any("OHLCV readiness failed" in message for _service, message, _kwargs in messages)


def test_configuration_update_busy_preserves_retryable_http_status(monkeypatch) -> None:
    """Config helper update contention must remain an actionable HTTP 503."""
    monkeypatch.setattr(optimize_v8, "_log", lambda *_args, **_kwargs: None)

    error = optimize_v8._configuration_error(
        "Preparing PB8 config",
        PB8RuntimeBusyError("PB8 update active"),
    )

    assert error.status_code == 503
    assert error.detail == "PB8 update active"


def test_v7_pareto_migration_source_is_confined_to_managed_results(tmp_path, monkeypatch) -> None:
    """Official V7 migration may read Pareto JSON only from PB7's managed result root."""
    pb7_root = tmp_path / "pb7"
    pareto = pb7_root / "optimize_results" / "run" / "pareto" / "candidate.json"
    pareto.parent.mkdir(parents=True)
    pareto.write_text("{}", encoding="utf-8")
    outside = tmp_path / "candidate.json"
    outside.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(optimize_v8, "pb7dir", lambda: str(pb7_root))

    assert optimize_v8._resolve_v7_pareto_config(str(pareto)) == pareto.resolve()
    with pytest.raises(HTTPException) as exc_info:
        optimize_v8._resolve_v7_pareto_config(str(outside))
    assert exc_info.value.status_code == 400


def test_queue_settings_match_pb7_and_apply_only_to_launch_copy(monkeypatch) -> None:
    """PB8 queue settings expose and apply the same CPU and market-data semantics as PB7."""
    saved = {}
    monkeypatch.setattr(optimize_v8.multiprocessing, "cpu_count", lambda: 8)
    monkeypatch.setattr(
        optimize_v8,
        "load_ini_section",
        lambda _section: {
            "autostart": "True",
            "cpu": "6",
            "cpu_override": "True",
            "use_pbgui_market_data": "True",
        },
    )
    monkeypatch.setattr(optimize_v8, "get_metadata", lambda _session: {"backends": [], "optimize_defaults": {}})
    monkeypatch.setattr(optimize_v8, "save_ini_section", lambda section, values: saved.update(section=section, values=values))

    settings = optimize_v8.get_settings(None)
    assert settings["autostart"] is True
    assert settings["cpu"] == 6
    assert settings["cpu_override"] is True
    assert settings["use_pbgui_market_data"] is True
    assert settings["cpu_max"] == 8
    assert settings["hsl_signal_modes"] == ["coin", "pside", "unified"]

    source = {"backtest": {"ohlcv_source_dir": "original"}, "optimize": {"n_cpus": 2}}
    automatic = optimize_v8._apply_queue_launch_settings(
        source,
        {"cpu": "6", "cpu_override": "True", "use_pbgui_market_data": "True"},
        automatic=True,
        pbgui_data_path="/managed/market-data",
    )
    manual = optimize_v8._apply_queue_launch_settings(
        source,
        {"cpu": "6", "cpu_override": "True", "use_pbgui_market_data": "True"},
        automatic=False,
        pbgui_data_path="/managed/market-data",
    )
    assert automatic["optimize"]["n_cpus"] == 6
    assert manual["optimize"]["n_cpus"] == 2
    assert automatic["backtest"]["ohlcv_source_dir"] == "/managed/market-data"
    assert source == {"backtest": {"ohlcv_source_dir": "original"}, "optimize": {"n_cpus": 2}}

    optimize_v8.update_settings(
        {"autostart": False, "cpu": 99, "cpu_override": False, "use_pbgui_market_data": False},
        None,
    )
    assert saved == {
        "section": "optimize_v7",
        "values": {
            "autostart": "False",
            "cpu": "8",
            "cpu_override": "False",
            "use_pbgui_market_data": "False",
        },
    }
    assert optimize_v8._QUEUE_SETTINGS_SECTION == "optimize_v7"
    source = Path(optimize_v8.__file__).read_text(encoding="utf-8")
    assert 'load_ini_section("optimize_v8")' not in source
    assert 'save_ini_section("optimize_v8"' not in source


class _RunningDashProcess:
    """Minimal live Popen-like object for session lifecycle tests."""

    def poll(self):
        """Report that the fake Dash process remains active."""
        return None


def _dash_record(session_id: str, pid: int, create_time: float, port: int = 8050) -> dict:
    """Build one complete durable Dash record rooted in the active test cache."""
    stage_root = optimize_v8._dash_cache_root() / session_id
    script = Path("/test/pb8/src/tools/pareto_dash.py")
    command = [
        "/test/pb8venv/bin/python",
        "-u",
        str(script),
        "--data-root",
        str(stage_root / "runs"),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]
    return {
        "session_id": session_id,
        "pid": pid,
        "create_time": create_time,
        "owner_pid": os.getpid(),
        "owner_create_time": 1.0,
        "command": command,
        "command_markers": command[1:],
        "result_dir": "/test/pb8/optimize_results/run",
        "proxy_root": f"/api/optimize-v8/results/pareto-dash/{session_id}/",
        "port": port,
        "process": _RunningDashProcess(),
        "stage_root": str(stage_root),
        "created_at": 100.0,
        "last_access": 100.0,
    }


def _request(headers: list[tuple[bytes, bytes]], body: bytes = b"") -> Request:
    """Build one Starlette request without a live ASGI server."""
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/dash",
            "raw_path": b"/dash",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
        },
        receive,
    )


def test_dash_proxy_forwards_only_allowlisted_request_headers(dash_runtime_roots, monkeypatch) -> None:
    """Cookies, bearer credentials, API keys, and origin metadata must never reach PB8 Dash."""
    session_id = "a" * 12
    record = _dash_record(session_id, 101, 10.0)
    with optimize_v8._dash_lock:
        optimize_v8._dash_sessions[session_id] = record
    captured = {}

    class FakeAsyncClient:
        """Capture the request that the proxy would send to PB8 Dash."""

        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, method, url, **kwargs):
            captured.update(method=method, url=url, **kwargs)
            return httpx.Response(
                200,
                headers={"content-type": "application/json", "set-cookie": "dash_secret=value"},
                content=b'{"requests_pathname_prefix":"/"}',
            )

    monkeypatch.setattr(optimize_v8.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(optimize_v8, "_persist_dash_session", lambda _session: None)
    monkeypatch.setattr(optimize_v8.time, "time", lambda: 105.0)
    request = _request(
        [
            (b"accept", b"application/json"),
            (b"content-type", b"application/json"),
            (b"user-agent", b"pytest"),
            (b"cookie", b"pbgui_session=secret"),
            (b"authorization", b"Bearer secret"),
            (b"proxy-authorization", b"Basic secret"),
            (b"x-api-key", b"secret"),
            (b"x-auth-token", b"secret"),
            (b"origin", b"https://credentialed.example"),
            (b"referer", b"https://credentialed.example/private"),
        ],
        b"{}",
    )

    response = asyncio.run(optimize_v8.proxy_result_pareto_dash(session_id, request, "", None))

    assert response.status_code == 200
    assert "set-cookie" not in response.headers
    assert response.body == (
        f'{{"requests_pathname_prefix":"{record["proxy_root"]}"}}'.encode("utf-8")
    )
    assert captured["headers"] == {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": "pytest",
    }
    assert captured["content"] == b"{}"


@pytest.mark.parametrize("encoded_root", ["/", "\\/", "\\u002f", "\\u002F"])
def test_dash_proxy_rewrites_serialized_root_prefixes(encoded_root: str) -> None:
    """Dash config prefixes must stay below the authenticated PB8 proxy for every encoding."""
    proxy_root = "/api/optimize-v8/results/pareto-dash/abc123def456/"
    content = (
        '{"requests_pathname_prefix":"' + encoded_root
        + '","routes_pathname_prefix":null,"url_base_pathname":"' + encoded_root + '"}'
    ).encode("utf-8")

    rewritten = optimize_v8._rewrite_dash_content(content, "text/html", proxy_root).decode("utf-8")

    assert rewritten.count(proxy_root) == 3
    assert '"requests_pathname_prefix":"/"' not in rewritten


def test_dash_proxy_preserves_native_html_without_injected_theme() -> None:
    """The authenticated proxy must not override the native PB8 dashboard presentation."""
    html = optimize_v8._rewrite_dash_content(
        b"<html><head></head><body></body></html>",
        "text/html; charset=utf-8",
        "/api/optimize-v8/results/pareto-dash/abc123def456/",
    )
    payload = optimize_v8._rewrite_dash_content(
        b'{"background":"white"}',
        "application/json",
        "/api/optimize-v8/results/pareto-dash/abc123def456/",
    )

    assert html == b"<html><head></head><body></body></html>"
    assert b"pbgui-pareto-dash-dark" not in html
    assert payload == b'{"background":"white"}'


def test_dash_response_strips_set_cookie_and_declared_hop_headers() -> None:
    """Dash cannot set browser cookies or return static and Connection-declared hop headers."""
    headers = httpx.Headers(
        {
            "content-type": "text/html",
            "set-cookie": "dash_secret=value; HttpOnly",
            "connection": "keep-alive, x-private-hop",
            "keep-alive": "timeout=5",
            "x-private-hop": "remove-me",
            "transfer-encoding": "chunked",
            "x-safe": "keep-me",
        }
    )

    filtered = optimize_v8._dash_response_headers(headers)

    assert filtered == {"content-type": "text/html", "x-safe": "keep-me"}


def test_dash_session_cap_reaps_expired_sessions_before_admission(dash_runtime_roots, monkeypatch) -> None:
    """Idle sessions are removed before a new launch consumes the bounded session capacity."""
    monkeypatch.setattr(optimize_v8, "_DASH_MAX_ACTIVE_SESSIONS", 2)
    monkeypatch.setattr(optimize_v8, "_DASH_IDLE_TTL_SECONDS", 10)
    monkeypatch.setattr(optimize_v8.time, "time", lambda: 200.0)
    monkeypatch.setattr(optimize_v8, "_cleanup_dash_record", lambda *_args, **_kwargs: None)
    expired_id = "b" * 12
    active_id = "c" * 12
    with optimize_v8._dash_lock:
        optimize_v8._dash_sessions[expired_id] = {
            "process": _RunningDashProcess(),
            "created_at": 100.0,
            "last_access": 100.0,
        }
        optimize_v8._dash_sessions[active_id] = {
            "process": _RunningDashProcess(),
            "created_at": 195.0,
            "last_access": 195.0,
        }

    optimize_v8._reserve_dash_launch("d" * 12)

    assert expired_id not in optimize_v8._dash_sessions
    assert active_id in optimize_v8._dash_sessions
    assert "d" * 12 in optimize_v8._dash_pending_sessions
    with pytest.raises(HTTPException) as exc_info:
        optimize_v8._reserve_dash_launch("e" * 12)
    assert exc_info.value.status_code == 429


def test_dash_startup_recovery_signals_only_exact_process_owner(dash_runtime_roots, monkeypatch) -> None:
    """Recovery cleans every stale record but signals only a non-reused PID with an exact command."""
    verified_id = "1" * 12
    reused_id = "2" * 12
    verified = _dash_record(verified_id, 111, 11.0)
    reused = _dash_record(reused_id, 222, 22.0)
    for record in (verified, reused):
        stage = Path(record["stage_root"])
        stage.mkdir(parents=True)
        optimize_v8._dash_log_path(record["session_id"]).parent.mkdir(parents=True, exist_ok=True)
        optimize_v8._dash_log_path(record["session_id"]).write_text("test log", encoding="utf-8")
    with optimize_v8._dash_registry_lock():
        optimize_v8._write_dash_registry_unlocked({verified_id: verified, reused_id: reused})
    terminated = []

    class FakePsProcess:
        """Expose an exact process and one PID-reuse create-time mismatch."""

        def __init__(self, pid: int) -> None:
            self.pid = pid

        def create_time(self) -> float:
            return 11.0 if self.pid == 111 else 99.0

        def cmdline(self) -> list[str]:
            return verified["command"] if self.pid == 111 else reused["command"]

        def terminate(self) -> None:
            terminated.append(self.pid)

        def wait(self, timeout: float) -> None:
            return None

        def kill(self) -> None:
            terminated.append(self.pid)

    monkeypatch.setattr(optimize_v8.psutil, "Process", FakePsProcess)
    monkeypatch.setattr(optimize_v8.psutil, "pid_exists", lambda pid: pid in {111, 222})
    monkeypatch.setattr(optimize_v8.platform, "system", lambda: "Windows")

    optimize_v8._recover_dash_registry()

    assert terminated == [111]
    assert not Path(verified["stage_root"]).exists()
    assert not Path(reused["stage_root"]).exists()
    assert not optimize_v8._dash_log_path(verified_id).exists()
    assert optimize_v8._read_dash_registry() == {}


def test_dash_launch_racing_shutdown_cleans_ready_process(dash_runtime_roots, monkeypatch, tmp_path) -> None:
    """A Dash process becoming ready after shutdown admission closes must never be registered."""
    session_id = "3" * 12
    result_dir = tmp_path / "pb8" / "optimize_results" / "run"
    result_dir.mkdir(parents=True)
    pb8_dir = tmp_path / "pb8-runtime"
    script = pb8_dir / "src" / "tools" / "pareto_dash.py"
    script.parent.mkdir(parents=True)
    script.write_text("# test", encoding="utf-8")
    monkeypatch.setattr(
        optimize_v8,
        "pb8_runtime_status",
        lambda: {"ready": True, "pb8dir": str(pb8_dir), "pb8venv": str(tmp_path / "venv" / "python")},
    )
    monkeypatch.setattr(optimize_v8, "_find_free_local_port", lambda: 8123)
    monkeypatch.setattr(optimize_v8, "rotate_managed_log_before_open", lambda *_args, **_kwargs: None)

    class FakeLease:
        """Track no state; release is intentionally idempotent for this race."""

        def release(self) -> None:
            return None

    class FakePopen:
        """Represent the newly spawned Dash process."""

        def __init__(self, command, **_kwargs) -> None:
            self.pid = 333
            self.command = command

        def poll(self):
            return None

    class FakePsProcess:
        """Provide stable start times for Dash and its API owner."""

        def __init__(self, pid: int) -> None:
            self.pid = pid

        def create_time(self) -> float:
            return 33.0 if self.pid == 333 else 1.0

    terminated = []

    def close_admission(_process, _port) -> None:
        with optimize_v8._dash_lock:
            optimize_v8._dash_admission_open = False

    monkeypatch.setattr(optimize_v8, "acquire_master_runtime_lock", lambda _root: FakeLease())
    monkeypatch.setattr(optimize_v8.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(optimize_v8.psutil, "Process", FakePsProcess)
    monkeypatch.setattr(optimize_v8, "_wait_for_dash", close_admission)
    monkeypatch.setattr(optimize_v8, "_dash_process_matches", lambda record: record.get("pid") == 333)
    monkeypatch.setattr(optimize_v8, "_terminate_dash_record", lambda record: terminated.append(record["pid"]))

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8._launch_dash_session(session_id, result_dir, f"/dash/{session_id}/")

    assert exc_info.value.status_code == 503
    assert terminated == [333]
    assert session_id not in optimize_v8._dash_sessions
    assert session_id not in optimize_v8._dash_pending_sessions
    assert not (optimize_v8._dash_cache_root() / session_id).exists()
    assert optimize_v8._read_dash_registry() == {}


def test_stop_all_dash_sessions_isolates_cleanup_failures(dash_runtime_roots, monkeypatch) -> None:
    """One broken session cleanup must not skip later sessions, and repeated cleanup is harmless."""
    first = "4" * 12
    second = "5" * 12
    with optimize_v8._dash_lock:
        optimize_v8._dash_sessions[first] = {"process": _RunningDashProcess()}
        optimize_v8._dash_sessions[second] = {"process": _RunningDashProcess()}
    calls = []

    def isolated_stop(session_id: str) -> None:
        calls.append(session_id)
        with optimize_v8._dash_lock:
            optimize_v8._dash_sessions.pop(session_id, None)
        if session_id == first:
            raise RuntimeError("test cleanup failure")

    monkeypatch.setattr(optimize_v8, "_stop_dash_session", isolated_stop)
    monkeypatch.setattr(optimize_v8, "_log", lambda *_args, **_kwargs: None)

    optimize_v8._stop_all_dash_sessions()
    optimize_v8._stop_all_dash_sessions()

    assert calls == [first, second]
    assert optimize_v8._dash_sessions == {}
    assert optimize_v8._dash_admission_open is False


def _write_queue_job(filename: str, order: int, *, snapshot: bool = True) -> None:
    """Create one minimal isolated PB8 queue record for recovery tests."""
    if snapshot:
        optimize_v8._snapshot_dir(filename).mkdir(parents=True, exist_ok=True)
        optimize_v8._write_json(optimize_v8._snapshot_file(filename), {"backtest": {}, "optimize": {}})
    optimize_v8._write_json(
        optimize_v8._queue_file(filename),
        {
            "filename": filename,
            "name": filename,
            "snapshot_path": str(optimize_v8._snapshot_file(filename)),
            "launch_options": {"mode": "fresh"},
            "order": order,
        },
    )


def test_permanent_head_failure_allows_next_autostart_job(optimize_v8_roots, monkeypatch) -> None:
    """A missing head snapshot becomes actionable error and no longer blocks the next queue item."""
    _write_queue_job("broken-head", 0, snapshot=False)
    _write_queue_job("next-job", 1)
    worker = optimize_v8.OptimizeV8Worker()
    worker._running = True
    launched = []

    def fake_launch(filename, options, automatic):
        launched.append(filename)
        if filename == "broken-head":
            return optimize_v8.OptimizeV8Worker().launch(filename, options, automatic)
        worker._running = False
        return {"pid": 99, "create_time": 9.0}

    async def immediate_to_thread(func, *args):
        return func(*args)

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(optimize_v8, "load_ini_section", lambda _section: {"autostart": "True"})
    monkeypatch.setattr(optimize_v8, "claim_autostart", lambda *_args: True)
    monkeypatch.setattr(optimize_v8, "release_autostart", lambda *_args: None)
    monkeypatch.setattr(optimize_v8, "publish_autostart_process", lambda *_args: None)
    monkeypatch.setattr(optimize_v8.asyncio, "to_thread", immediate_to_thread)
    monkeypatch.setattr(optimize_v8.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(worker, "launch", fake_launch)

    asyncio.run(worker._loop())

    assert launched == ["broken-head", "next-job"]
    failed = optimize_v8._read_json(optimize_v8._queue_file("broken-head"))
    assert failed["status_override"] == "error"
    assert failed["error_code"] == "prelaunch_failed"
    assert "snapshot" in failed["error_reason"].lower()
    assert "snapshot" in (optimize_v8._log_dir() / "broken-head.log").read_text(encoding="utf-8").lower()


def test_transient_runtime_failure_stays_queued_for_retry(optimize_v8_roots, monkeypatch) -> None:
    """Temporary PB8 runtime unavailability retries the same item instead of skipping it."""
    _write_queue_job("retry-job", 0)
    worker = optimize_v8.OptimizeV8Worker()
    worker._running = True
    attempts = []

    def fake_launch(filename, _options, _automatic):
        attempts.append(filename)
        if len(attempts) == 1:
            raise HTTPException(status_code=503, detail="PB8 runtime is not ready")
        worker._running = False
        return {"pid": 100, "create_time": 10.0}

    async def immediate_to_thread(func, *args):
        return func(*args)

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(optimize_v8, "load_ini_section", lambda _section: {"autostart": "True"})
    monkeypatch.setattr(optimize_v8, "claim_autostart", lambda *_args: True)
    monkeypatch.setattr(optimize_v8, "release_autostart", lambda *_args: None)
    monkeypatch.setattr(optimize_v8, "publish_autostart_process", lambda *_args: None)
    monkeypatch.setattr(optimize_v8.asyncio, "to_thread", immediate_to_thread)
    monkeypatch.setattr(optimize_v8.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(worker, "launch", fake_launch)

    asyncio.run(worker._loop())

    assert attempts == ["retry-job", "retry-job"]
    data = optimize_v8._read_json(optimize_v8._queue_file("retry-job"))
    assert "status_override" not in data
    assert "will retry" in data["launch_message"]


def test_stopped_item_is_actionable_and_requeue_preserves_log(optimize_v8_roots, monkeypatch) -> None:
    """Stop records a useful error and fresh requeue clears state without deleting diagnostics."""
    _write_queue_job("stop-job", 0)
    monkeypatch.setattr(optimize_v8, "_terminate_verified", lambda _filename: None)

    optimize_v8.stop_queue_item("stop-job", None)
    stopped = optimize_v8._read_json(optimize_v8._queue_file("stop-job"))
    assert stopped["status_override"] == "error"
    assert stopped["error_code"] == "stopped"
    log_path = optimize_v8._log_dir() / "stop-job.log"
    assert log_path.is_file()

    optimize_v8.requeue_fresh("stop-job", None)
    reset = optimize_v8._read_json(optimize_v8._queue_file("stop-job"))
    assert "status_override" not in reset
    assert "error_reason" not in reset
    assert log_path.is_file()


def test_stale_reused_pid_is_never_signalled_and_can_be_reset(optimize_v8_roots, monkeypatch) -> None:
    """A reused live PID loses ownership immediately and cannot block stop or repair actions."""
    _write_queue_job("reused-pid", 0)
    data = optimize_v8._read_json(optimize_v8._queue_file("reused-pid"))
    data["started_at"] = 1.0
    optimize_v8._write_json(optimize_v8._queue_file("reused-pid"), data)
    optimize_v8._write_json(optimize_v8._pid_file("reused-pid"), {"pid": 777, "create_time": 1.0})
    (optimize_v8._log_dir() / "reused-pid.log").write_text("Optimization complete\n", encoding="utf-8")
    signalled = []
    monkeypatch.setattr(optimize_v8.psutil, "pid_exists", lambda pid: pid == 777)
    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: False)
    monkeypatch.setattr(optimize_v8, "_terminate_process", lambda pid: signalled.append(pid))

    assert optimize_v8._queue_item(optimize_v8._queue_file("reused-pid"))["status"] == "complete"
    assert signalled == []
    assert not optimize_v8._pid_file("reused-pid").exists()

    optimize_v8.stop_queue_item("reused-pid", None)
    optimize_v8.requeue_fresh("reused-pid", None)
    assert optimize_v8._queue_item(optimize_v8._queue_file("reused-pid"))["status"] == "queued"


def test_repair_config_rewrites_immutable_snapshot_without_second_prepare(optimize_v8_roots, monkeypatch) -> None:
    """Repair binds the selected managed config and replaces snapshot content without losing queue options."""
    optimize_v8.save_config(
        "replacement",
        {"backtest": {"exchanges": ["bybit"]}, "optimize": {"seed": 44, "n_cpus": 8}},
        None,
    )
    _write_queue_job("repair-job", 3)
    before = optimize_v8._read_json(optimize_v8._queue_file("repair-job"))
    before["launch_options"] = {"mode": "fresh", "fine_tune_params": ["bot.long.x"]}
    optimize_v8._write_json(optimize_v8._queue_file("repair-job"), before)
    monkeypatch.setattr(
        optimize_v8,
        "prepare_pb8_config",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("config prepared twice")),
    )

    optimize_v8.repair_queue_config("repair-job", {"name": "replacement"}, None)

    repaired = optimize_v8._read_json(optimize_v8._queue_file("repair-job"))
    snapshot = optimize_v8._read_json(optimize_v8._snapshot_file("repair-job"))
    assert repaired["filename"] == "repair-job"
    assert repaired["order"] == 3
    assert repaired["launch_options"] == before["launch_options"]
    assert repaired["exchange"] == ["bybit"]
    assert snapshot["optimize"]["seed"] == 44
    assert snapshot["optimize"]["n_cpus"] == 8


def test_startup_reconciliation_repairs_partial_and_cleans_only_stale_orphans(optimize_v8_roots, monkeypatch) -> None:
    """Startup repairs partial queue metadata, preserves live orphans, and retains every orphan log."""
    optimize_v8._snapshot_dir("partial").mkdir(parents=True)
    optimize_v8._write_json(optimize_v8._snapshot_file("partial"), {"backtest": {}, "optimize": {}})
    optimize_v8._write_json(optimize_v8._queue_file("partial"), {"name": ""})
    for filename, pid in (("live-orphan", 10), ("stale-orphan", 20)):
        optimize_v8._snapshot_dir(filename).mkdir(parents=True)
        optimize_v8._launch_dir(filename).mkdir(parents=True)
        optimize_v8._write_json(optimize_v8._pid_file(filename), {"pid": pid, "create_time": float(pid)})
        optimize_v8._state_file(filename).parent.mkdir(parents=True, exist_ok=True)
        optimize_v8._state_file(filename).write_text("{}", encoding="utf-8")
        (optimize_v8._log_dir() / f"{filename}.log").write_text("keep\n", encoding="utf-8")
    monkeypatch.setattr(
        optimize_v8,
        "_process_matches",
        lambda filename, _record: filename == "live-orphan",
    )

    optimize_v8._reconcile_queue_artifacts()

    partial = optimize_v8._read_json(optimize_v8._queue_file("partial"))
    assert partial["filename"] == "partial"
    assert partial["name"] == "partial"
    assert isinstance(partial["order"], int)
    assert optimize_v8._snapshot_dir("live-orphan").exists()
    assert optimize_v8._launch_dir("live-orphan").exists()
    assert not optimize_v8._snapshot_dir("stale-orphan").exists()
    assert not optimize_v8._launch_dir("stale-orphan").exists()
    assert (optimize_v8._log_dir() / "live-orphan.log").exists()
    assert (optimize_v8._log_dir() / "stale-orphan.log").exists()


def test_partial_reorder_is_deterministic_and_recoverable(optimize_v8_roots, monkeypatch) -> None:
    """The reorder journal remains authoritative until every atomic queue-file update completes."""
    for index, filename in enumerate(("one", "two", "three")):
        _write_queue_job(filename, index)
    original_write = optimize_v8._write_json
    failed = False

    def interrupted_write(path, payload):
        nonlocal failed
        if path == optimize_v8._queue_file("one") and payload.get("order") == 1 and not failed:
            failed = True
            raise OSError("simulated interruption")
        return original_write(path, payload)

    monkeypatch.setattr(optimize_v8, "_write_json", interrupted_write)
    with pytest.raises(OSError):
        optimize_v8.reorder_queue({"filenames": ["three", "one", "two"]}, None)
    assert [item["filename"] for item in optimize_v8._load_queue()] == ["three", "one", "two"]

    monkeypatch.setattr(optimize_v8, "_write_json", original_write)
    with optimize_v8._queue_lock():
        optimize_v8._recover_pending_reorder_unlocked()
    assert [optimize_v8._read_json(optimize_v8._queue_file(name))["order"] for name in ("three", "one", "two")] == [0, 1, 2]
    assert not optimize_v8._reorder_file().exists()


def test_automatic_launch_revalidates_final_queue_order(optimize_v8_roots) -> None:
    """A worker selection made before a reorder cannot launch the old head."""
    _write_queue_job("old-head", 1)
    _write_queue_job("new-head", 0)

    with pytest.raises(HTTPException) as exc_info:
        optimize_v8.OptimizeV8Worker().launch("old-head", None, True)

    assert exc_info.value.status_code == 409
    assert "order changed" in str(exc_info.value.detail).lower()


def test_worker_loop_retries_unexpected_iteration_error(monkeypatch) -> None:
    """An unexpected settings/read failure is logged and followed by another worker iteration."""
    worker = optimize_v8.OptimizeV8Worker()
    worker._running = True
    calls = 0

    def flaky_settings(_section):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary read failure")
        worker._running = False
        return {"autostart": "False"}

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(optimize_v8, "load_ini_section", flaky_settings)
    monkeypatch.setattr(optimize_v8.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(optimize_v8, "_log", lambda *_args, **_kwargs: None)

    asyncio.run(worker._loop())

    assert calls == 2
