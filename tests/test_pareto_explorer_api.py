"""Tests for Pareto Explorer FastAPI loader helpers."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pandas as pd

from api import pareto_explorer


ROOT = Path(__file__).resolve().parents[1]


def test_load_loader_falls_back_to_all_results_when_pareto_jsons_missing(tmp_path, monkeypatch) -> None:
    """Fast mode should try all_results.bin when pareto JSON loading finds no data."""
    result_dir = tmp_path / "optimize_results" / "result_001"
    result_dir.mkdir(parents=True)
    (result_dir / "all_results.bin").write_bytes(b"placeholder")
    calls: list[tuple[str, object]] = []

    class FakeLoader:
        """Minimal ParetoDataLoader replacement for fallback behavior."""

        def __init__(self, result_path: str) -> None:
            self.result_path = result_path
            self.last_error = None

        def load_pareto_jsons_only(self) -> bool:
            calls.append(("pareto", self.result_path))
            return False

        def load(self, *, load_strategy: list[str], max_configs: int, progress_callback=None) -> bool:
            calls.append(("all_results", (tuple(load_strategy), max_configs, progress_callback)))
            return True

    monkeypatch.setattr(pareto_explorer, "ParetoDataLoader", FakeLoader)
    monkeypatch.setattr(pareto_explorer, "_get_cached_loader", lambda cache_key: None)
    monkeypatch.setattr(pareto_explorer, "_cache_loader", lambda cache_key, loader: None)
    monkeypatch.setattr(pareto_explorer, "_clone_loader", lambda loader, **kwargs: loader)
    monkeypatch.setattr(pareto_explorer, "_optimize_result_roots", lambda: [(tmp_path / "optimize_results").resolve()])

    loader = pareto_explorer._load_loader(
        str(result_dir),
        all_results_loaded=False,
        load_strategy=["performance"],
        max_configs=500,
    )

    assert isinstance(loader, FakeLoader)
    assert calls == [
        ("pareto", str(result_dir)),
        ("all_results", (("performance",), 500, None)),
    ]


def test_resolve_result_dir_rejects_plain_directories(tmp_path) -> None:
    """Arbitrary directories should not be accepted as optimize result paths."""
    assert pareto_explorer._resolve_result_dir(str(tmp_path)) is None


def test_resolve_result_dir_rejects_result_shaped_directories_outside_roots(tmp_path, monkeypatch) -> None:
    """Optimize-shaped directories outside PB7 optimize roots should be rejected."""
    allowed_root = tmp_path / "optimize_results"
    result_dir = tmp_path / "other" / "result_001"
    result_dir.mkdir(parents=True)
    (result_dir / "all_results.bin").write_bytes(b"placeholder")

    monkeypatch.setattr(pareto_explorer, "_optimize_result_roots", lambda: [allowed_root.resolve()])

    assert pareto_explorer._resolve_result_dir(str(result_dir)) is None


def test_resolve_result_dir_accepts_results_inside_optimize_roots(tmp_path, monkeypatch) -> None:
    """Optimize result directories under PB7 optimize roots should be accepted."""
    allowed_root = tmp_path / "optimize_results"
    result_dir = allowed_root / "result_001"
    result_dir.mkdir(parents=True)
    (result_dir / "all_results.bin").write_bytes(b"placeholder")

    monkeypatch.setattr(pareto_explorer, "_optimize_result_roots", lambda: [allowed_root.resolve()])

    assert pareto_explorer._resolve_result_dir(str(result_dir)) == result_dir.resolve()


def test_result_meta_reports_managed_optimize_owner(tmp_path, monkeypatch) -> None:
    """Result metadata must identify whether PB7 or PB8 owns the selected path."""
    v7_root = tmp_path / "pb7" / "optimize_results"
    v8_root = tmp_path / "pb8" / "optimize_results"
    result_dir = v8_root / "result_001"
    result_dir.mkdir(parents=True)
    (result_dir / "all_results.bin").write_bytes(b"placeholder")
    monkeypatch.setattr(
        pareto_explorer,
        "_optimize_result_roots_by_version",
        lambda: [("v7", v7_root.resolve()), ("v8", v8_root.resolve())],
    )

    assert pareto_explorer._result_meta(result_dir)["optimize_version"] == "v8"


def test_pareto_frontend_routes_by_result_version_without_bearer_tokens() -> None:
    """The shared Explorer must use owning APIs and same-origin cookie authentication."""
    source = (ROOT / "frontend" / "v7_pareto_explorer.html").read_text(encoding="utf-8")

    assert 'window.OPTIMIZE_VERSION = "%%OPTIMIZE_VERSION%%"' in source
    assert "'/api/optimize-' + optimizeVersion()" in source
    assert "'/api/backtest-' + optimizeVersion()" in source
    assert "optimizeApiBase() + '/results/pareto-dash/'" in source
    assert "encodeURIComponent(optimizeVersion())" in source
    assert "current: optimizeVersion() === 'v8'" in source
    assert "activeOptimizeVersion" not in source
    assert "Authorization" not in source
    assert "%%TOKEN%%" not in source


def test_select_correlation_configs_skips_unavailable_top_performer_metrics() -> None:
    """Top Performer selection should not invent winners for missing metrics."""

    class FakeConfig:
        """Minimal config object with index and suite metrics."""

        def __init__(self, config_index: int, suite_metrics: dict) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics

    class FakeLoader:
        """Minimal loader exposing pareto configs."""

        def get_pareto_configs(self) -> list[FakeConfig]:
            return [
                FakeConfig(1, {}),
                FakeConfig(2, {"adg_w_usd": 0.123456}),
            ]

    selected, labels = pareto_explorer._select_correlation_configs(
        FakeLoader(),
        strategy="Top Performers",
        num_configs=3,
        use_weighted=True,
        use_btc=False,
    )

    assert selected == [2]
    assert labels == ["#2 Top ADG (0.123456)"]


def test_full_loader_without_cache_returns_background_job(tmp_path, monkeypatch) -> None:
    """Direct full-mode calls should start a background job instead of blocking."""
    result_dir = tmp_path / "optimize_results" / "result_001"
    result_dir.mkdir(parents=True)
    (result_dir / "all_results.bin").write_bytes(b"placeholder")
    started: list[dict] = []

    monkeypatch.setattr(pareto_explorer, "_get_cached_loader", lambda cache_key: None)
    monkeypatch.setattr(pareto_explorer, "_optimize_result_roots", lambda: [(tmp_path / "optimize_results").resolve()])

    def fake_start(result_path: str, *, load_strategy: list[str], max_configs: int, refresh_options: dict | None = None) -> dict:
        started.append({
            "result_path": result_path,
            "load_strategy": load_strategy,
            "max_configs": max_configs,
            "refresh_options": refresh_options,
        })
        return {"job_id": "job-1", "status": "loading"}

    monkeypatch.setattr(pareto_explorer, "_start_full_load_job", fake_start)

    loader, response = pareto_explorer._load_loader_or_background_response(
        str(result_dir),
        all_results_loaded=True,
        load_strategy=["performance"],
        max_configs=500,
        body={"view_range": {"start": 0, "end": 25}},
    )

    assert loader is None
    assert response == {"ok": True, "status": "loading", "job": {"job_id": "job-1", "status": "loading"}}
    assert started[0]["refresh_options"]["view_range"] == {"start": 0, "end": 25}


def test_build_evolution_chart_uses_best_so_far_for_higher_is_better() -> None:
    """Evolution timelines should show cumulative best values, not rolling averages."""

    class FakeLoader:
        """Minimal loader returning evolution dataframe data."""

        def to_dataframe(self, pareto_only: bool = False) -> pd.DataFrame:
            return pd.DataFrame([
                {"config_index": 3, "is_pareto": True, "adg_w_usd": 0.2},
                {"config_index": 1, "is_pareto": True, "adg_w_usd": 0.1},
                {"config_index": 2, "is_pareto": True, "adg_w_usd": 0.05},
                {"config_index": 4, "is_pareto": True, "adg_w_usd": 0.15},
            ])

    chart = pareto_explorer._build_evolution_chart(
        FakeLoader(),
        metric="adg_w_usd",
        window=2,
        show_all=False,
        hide_outliers=False,
        improvement_threshold_pct=25,
    )

    best_trace = next(trace for trace in chart["traces"] if trace["name"].startswith("Best So Far"))
    assert best_trace["x"] == [1, 2, 3, 4]
    assert best_trace["y"] == [0.1, 0.1, 0.2, 0.2]
    assert chart["best_so_far"]["direction"] == "max"
    assert chart["best_so_far"]["best_config_index"] == 3
    assert chart["best_so_far"]["best_value"] == 0.2
    assert chart["best_so_far"]["improvement_count"] == 2
    assert chart["best_so_far"]["meaningful_improvement_count"] == 2
    assert chart["best_so_far"]["last_meaningful_improvement_config_index"] == 3
    assert not any(str(trace["name"]) == "Pareto Density" for trace in chart["traces"])
    assert not any(str(trace["name"]) == "Meaningful Improvements" for trace in chart["traces"])
    assert not any(str(trace["name"]).startswith("Rolling Avg") for trace in chart["traces"])


def test_build_evolution_chart_uses_best_so_far_for_lower_is_better() -> None:
    """Risk-style metrics should use cumulative minimum values."""

    class FakeLoader:
        """Minimal loader returning drawdown evolution dataframe data."""

        def to_dataframe(self, pareto_only: bool = False) -> pd.DataFrame:
            return pd.DataFrame([
                {"config_index": 1, "is_pareto": True, "drawdown_worst_usd": 0.30},
                {"config_index": 2, "is_pareto": True, "drawdown_worst_usd": 0.35},
                {"config_index": 3, "is_pareto": True, "drawdown_worst_usd": 0.20},
            ])

    chart = pareto_explorer._build_evolution_chart(
        FakeLoader(),
        metric="drawdown_worst_usd",
        window=2,
        show_all=False,
        hide_outliers=False,
        improvement_threshold_pct=10,
    )

    best_trace = next(trace for trace in chart["traces"] if trace["name"].startswith("Best So Far"))
    assert best_trace["y"] == [0.3, 0.3, 0.2]
    assert chart["best_so_far"]["direction"] == "min"
    assert chart["best_so_far"]["best_config_index"] == 3
    assert chart["best_so_far"]["last_meaningful_improvement_config_index"] == 3


def test_deep_evolution_payload_requires_full_mode_for_timeline() -> None:
    """Fast mode should not render a fake evolution timeline from pareto JSON ordering."""

    class FakeConfig:
        """Minimal config with suite metrics and pareto flag."""

        config_index = 0
        is_pareto = True
        suite_metrics = {"adg_w_usd": 0.1}

    class FakeLoader:
        """Minimal loader exposing fast-mode pareto configs."""

        configs = [FakeConfig()]
        scoring_metrics = ["adg_w_usd"]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

    payload = pareto_explorer._build_deep_evolution_payload(
        FakeLoader(),
        all_results_loaded=False,
        metric="adg_w_usd",
        use_weighted=True,
        use_btc=False,
        window_percent=5,
        improvement_threshold_pct=1,
        show_all=False,
        hide_outliers=True,
    )

    assert payload["requires_full_mode"] is True
    assert payload["chart"]["traces"] == []
    assert "Load all_results.bin" in payload["chart"]["layout"]["title"]


def test_full_load_jobs_are_deduplicated_and_joined(monkeypatch) -> None:
    """Equivalent requests share one tracked full-load worker that shutdown joins."""
    started = threading.Event()
    release = threading.Event()

    def fake_run(_job_id: str) -> None:
        started.set()
        release.wait(timeout=5)

    monkeypatch.setattr(pareto_explorer, "_run_full_load_job", fake_run)
    pareto_explorer._LOAD_JOBS.clear()
    pareto_explorer._LOAD_WORKERS.clear()
    pareto_explorer._LOAD_CANCEL_EVENTS.clear()
    pareto_explorer._LOAD_KEYS.clear()

    options = {"view_range": {"start": 0, "end": 10}}
    first = pareto_explorer._start_full_load_job(
        "/tmp/result", load_strategy=["performance"], max_configs=100, refresh_options=options,
    )
    assert started.wait(timeout=2)
    second = pareto_explorer._start_full_load_job(
        "/tmp/result", load_strategy=["performance"], max_configs=100, refresh_options=options,
    )

    assert second["job_id"] == first["job_id"]
    assert len(pareto_explorer._LOAD_WORKERS) == 1
    release.set()
    asyncio.run(pareto_explorer.shutdown())
    assert pareto_explorer._LOAD_WORKERS == {}
