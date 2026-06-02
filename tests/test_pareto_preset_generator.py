"""Tests for streamlit-free Pareto optimize preset generation."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import msgpack

from api import pareto_explorer
from ParetoDataLoader import ParetoDataLoader
import pareto_preset_generator as generator


def _metric_sets() -> dict[str, list[str]]:
    """Return a compact PB7 metric set for preset generator unit tests."""
    return {
        "currency_metrics": [
            "adg_w",
            "adg_w_per_exposure_long",
            "adg_w_per_exposure_short",
            "drawdown_worst",
            "drawdown_worst_mean_1pct",
            "expected_shortfall_1pct",
            "gain",
            "mdg_w",
            "sharpe_ratio",
        ],
        "shared_metrics": [
            "adg_strategy_eq",
            "equity_choppiness_w",
            "equity_jerkiness_w",
            "exponential_fit_error_w",
            "loss_profit_ratio",
            "mdg_strategy_eq_w",
            "adg_strategy_eq_w",
            "drawdown_worst_strategy_eq",
            "drawdown_worst_mean_1pct_strategy_eq",
            "peak_recovery_days_strategy_eq",
            "position_held_days_max",
            "position_held_days_mean",
            "position_held_hours_max",
            "position_held_hours_mean",
            "positions_held_per_day",
            "sortino_ratio_strategy_eq",
            "total_wallet_exposure_mean",
        ],
    }


def _default_goals(metric_names: list[str]) -> dict[str, str]:
    """Return PB7-style default goals for the compact test metrics."""
    minimum = {
        "drawdown_worst_usd",
        "drawdown_worst_mean_1pct_usd",
        "drawdown_worst_strategy_eq",
        "drawdown_worst_mean_1pct_strategy_eq",
        "expected_shortfall_1pct_usd",
        "loss_profit_ratio",
        "peak_recovery_days_strategy_eq",
        "position_held_days_max",
        "position_held_days_mean",
        "position_held_hours_max",
        "position_held_hours_mean",
        "positions_held_per_day",
        "total_wallet_exposure_mean",
    }
    return {metric: ("min" if metric in minimum else "max") for metric in metric_names}


def _base_context() -> tuple[dict, dict]:
    """Build a minimal selected-config context and source config."""
    full_config = {
        "backtest": {"base_dir": "backtests/pbgui/source"},
        "bot": {},
        "optimize": {
            "bounds": {
                "long_entry_grid_spacing_pct": [0.001, 0.1, 0.001],
                "long_n_positions": [1, 5, 1],
                "long_total_wallet_exposure_limit": [0.0, 10.0, 0.1],
            },
            "scoring": ["adg_w_usd", "loss_profit_ratio", "sharpe_ratio_usd"],
            "limits": [],
        },
    }
    context = {
        "config_index": 42,
        "bounds": copy.deepcopy(full_config["optimize"]["bounds"]),
        "bot_params": {
            "long_entry_grid_spacing_pct": 0.02,
            "long_n_positions": 3,
            "long_total_wallet_exposure_limit": 4.0,
        },
        "suite_metrics": {
            "drawdown_worst_usd": 0.20,
            "drawdown_worst_mean_1pct_usd": 0.15,
            "drawdown_worst_strategy_eq": 0.22,
            "drawdown_worst_mean_1pct_strategy_eq": 0.16,
            "peak_recovery_days_strategy_eq": 35.0,
            "adg_strategy_eq_w": 0.0018,
            "mdg_strategy_eq_w": 0.0011,
            "adg_w_per_exposure_long_usd": 0.0014,
            "adg_w_per_exposure_short_usd": 0.0010,
            "total_wallet_exposure_mean": 0.82,
            "expected_shortfall_1pct_usd": 0.18,
            "sortino_ratio_strategy_eq": 1.35,
            "sharpe_ratio_usd": 1.2,
        },
        "optimize_settings": {},
    }
    return context, full_config


def _loader_config(score: float, objectives: dict[str, float], scoring: list | None = None) -> dict:
    """Build a minimal PB7 all_results entry for loader regression tests."""

    stats = {"score": {"mean": score}}
    stats.update({metric: {"mean": value} for metric, value in objectives.items()})

    return {
        "bot": {
            "long": {"entry_initial_qty_pct": score / 1000.0},
            "short": {"entry_initial_qty_pct": score / 2000.0},
        },
        "metrics": {
            "constraint_violation": 0.0,
            "objectives": dict(objectives),
            "stats": stats,
        },
        "optimize": {"scoring": scoring or ["score"], "bounds": {}},
    }


def _write_loader_result(tmp_path: Path, configs: list[dict], saved_pareto: dict | None = None) -> Path:
    """Write a minimal optimize result with all_results.bin and optional pareto JSON."""

    result_dir = tmp_path / "result_001"
    result_dir.mkdir()
    with (result_dir / "all_results.bin").open("wb") as f:
        for config in configs:
            f.write(msgpack.packb(config, use_bin_type=True))

    if saved_pareto is not None:
        pareto_dir = result_dir / "pareto"
        pareto_dir.mkdir()
        digest = hashlib.sha256(json.dumps(saved_pareto, sort_keys=True).encode("utf-8")).hexdigest()
        (pareto_dir / f"{digest}.json").write_text(json.dumps(saved_pareto), encoding="utf-8")

    return result_dir


def test_build_optimize_preset_uses_direction_and_existing_bounds(monkeypatch) -> None:
    """Safer presets should adjust scoring, limits, and bounds without Streamlit."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 5,
            "direction": "Safer (lower drawdowns)",
            "risk_adjust": -25,
            "show_near_bounds": False,
        },
    )

    preset = payload["preset_config"]
    assert payload["window_pct"] == 5
    assert preset["optimize"]["scoring"][:2] == [
        {"metric": "drawdown_worst_strategy_eq", "goal": "min"},
        {"metric": "drawdown_worst_mean_1pct_strategy_eq", "goal": "min"},
    ]
    assert any(item.get("metric") == "drawdown_worst_usd" for item in preset["optimize"]["limits"])
    assert preset["optimize"]["bounds"]["long_entry_grid_spacing_pct"] != full_config["optimize"]["bounds"]["long_entry_grid_spacing_pct"]
    assert payload["bounds_preview_rows"]


def test_full_load_does_not_pin_saved_pareto_configs(tmp_path: Path) -> None:
    """Full mode should keep the load-strategy winners, not force saved paretos in."""

    saved_pareto = _loader_config(1.0, {"x": 10.0, "y": 10.0})
    best = _loader_config(100.0, {"x": 1.0, "y": 5.0})
    second_best = _loader_config(90.0, {"x": 5.0, "y": 1.0})
    result_dir = _write_loader_result(tmp_path, [saved_pareto, best, second_best], saved_pareto=saved_pareto)

    loader = ParetoDataLoader(str(result_dir))

    assert loader.load(load_strategy=["performance"], max_configs=2)
    assert [config.config_index for config in loader.configs] == [1, 2]
    assert [config.is_pareto for config in loader.configs] == [True, True]
    assert loader.load_stats["selected_configs"] == 2
    assert loader.load_stats["pareto_configs"] == 2


def test_full_load_recomputes_front_when_saved_pareto_hash_matches(tmp_path: Path) -> None:
    """A saved pareto hash must not short-circuit full-mode Pareto recomputation."""

    saved_pareto = _loader_config(80.0, {"x": 10.0, "y": 10.0})
    best = _loader_config(100.0, {"x": 1.0, "y": 5.0})
    second_best = _loader_config(90.0, {"x": 5.0, "y": 1.0})
    result_dir = _write_loader_result(tmp_path, [saved_pareto, best, second_best], saved_pareto=saved_pareto)

    loader = ParetoDataLoader(str(result_dir))

    assert loader.load(load_strategy=["performance"], max_configs=3)
    flags_by_index = {config.config_index: config.is_pareto for config in loader.configs}
    assert [config.config_index for config in loader.configs] == [1, 2, 0]
    assert flags_by_index == {0: False, 1: True, 2: True}
    assert loader.load_stats["pareto_configs"] == 2


def test_full_load_honors_max_scoring_goals_for_pareto_front(tmp_path: Path) -> None:
    """PB7 `goal: max` objectives should produce the upper Pareto frontier."""

    scoring = [{"metric": "x", "goal": "max"}, {"metric": "y", "goal": "max"}]
    weak = _loader_config(1.0, {"x": 1.0, "y": 1.0}, scoring=scoring)
    balanced = _loader_config(5.0, {"x": 5.0, "y": 5.0}, scoring=scoring)
    tradeoff = _loader_config(4.0, {"x": 4.0, "y": 6.0}, scoring=scoring)
    result_dir = _write_loader_result(tmp_path, [weak, balanced, tradeoff])

    loader = ParetoDataLoader(str(result_dir))

    assert loader.load(load_strategy=["performance"], max_configs=3)
    flags_by_index = {config.config_index: config.is_pareto for config in loader.configs}
    assert flags_by_index == {0: False, 1: True, 2: True}
    assert loader.load_stats["pareto_configs"] == 2


def test_drawdown_load_strategy_prefers_lowest_positive_drawdown(tmp_path: Path) -> None:
    """Drawdown strategy should rank smaller positive drawdown values first."""

    scoring = [{"metric": "adg_w_usd", "goal": "max"}]
    high_profit_high_dd = _loader_config(100.0, {"adg_w_usd": 0.003, "drawdown_worst_usd": 0.30}, scoring=scoring)
    low_dd = _loader_config(90.0, {"adg_w_usd": 0.001, "drawdown_worst_usd": 0.10}, scoring=scoring)
    mid_dd = _loader_config(80.0, {"adg_w_usd": 0.002, "drawdown_worst_usd": 0.20}, scoring=scoring)
    result_dir = _write_loader_result(tmp_path, [high_profit_high_dd, low_dd, mid_dd])

    for _ in range(2):
        loader = ParetoDataLoader(str(result_dir))

        assert loader.load(load_strategy=["drawdown"], max_configs=2)
        assert [config.config_index for config in loader.configs] == [1, 2]


def test_performance_load_strategy_honors_min_primary_goal(tmp_path: Path) -> None:
    """Performance strategy should prefer lower values when PB7 primary scoring goal is min."""

    scoring = [{"metric": "risk_score", "goal": "min"}]
    high_risk = _loader_config(100.0, {"risk_score": 0.30}, scoring=scoring)
    low_risk = _loader_config(90.0, {"risk_score": 0.10}, scoring=scoring)
    mid_risk = _loader_config(80.0, {"risk_score": 0.20}, scoring=scoring)
    result_dir = _write_loader_result(tmp_path, [high_risk, low_risk, mid_risk])

    for _ in range(2):
        loader = ParetoDataLoader(str(result_dir))

        assert loader.load(load_strategy=["performance"], max_configs=2)
        assert [config.config_index for config in loader.configs] == [1, 2]


def test_full_config_uses_selected_all_results_metrics(tmp_path: Path) -> None:
    """All-results full config should not keep metrics/objectives from the pareto template."""

    saved_pareto = _loader_config(10.0, {"score": 10.0, "x": 9.0})
    selected = _loader_config(100.0, {"score": 100.0, "x": 1.0})
    selected["bot"]["long"]["entry_initial_qty_pct"] = 0.123
    result_dir = _write_loader_result(tmp_path, [saved_pareto, selected], saved_pareto=saved_pareto)
    loader = ParetoDataLoader(str(result_dir))

    assert loader.load(load_strategy=["performance"], max_configs=2)
    full_config = loader.get_full_config(1)

    assert full_config["metrics"]["objectives"]["x"] == 1.0
    assert full_config["metrics"]["stats"]["x"]["mean"] == 1.0
    assert full_config["bot"]["long"]["entry_initial_qty_pct"] == 0.123


def test_loader_summary_best_metric_honors_min_goal() -> None:
    """Command-center summaries should show the best min-goal value, not max."""

    class FakeConfig:
        """Minimal pareto config with one metric."""

        is_pareto = True

        def __init__(self, value: float) -> None:
            self.suite_metrics = {"risk_score": value}

    class FakeLoader:
        """Minimal loader with min-goal scoring."""

        scoring_metrics = ["risk_score"]
        scoring_goals = {"risk_score": "min"}

        def __init__(self) -> None:
            self.configs = [FakeConfig(0.30), FakeConfig(0.10), FakeConfig(0.20)]
            self.scenario_labels = []

        def compute_overall_robustness(self, _config: FakeConfig) -> float:
            return 1.0

    summary = pareto_explorer._loader_summary(FakeLoader())

    assert summary["best_metric_value"] == 0.1


def test_champions_rank_min_goal_primary_metric() -> None:
    """Champion ranking should normalize min-goal primary metrics in the right direction."""

    class FakeConfig:
        """Minimal pareto config with risk score."""

        is_pareto = True

        def __init__(self, config_index: int, value: float) -> None:
            self.config_index = config_index
            self.suite_metrics = {"risk_score": value}

    class FakeLoader:
        """Minimal loader exposing champion dependencies."""

        scoring_metrics = ["risk_score"]
        scoring_goals = {"risk_score": "min"}

        def __init__(self) -> None:
            self.configs = [FakeConfig(1, 0.30), FakeConfig(2, 0.10)]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

        def compute_overall_robustness(self, _config: FakeConfig) -> float:
            return 1.0

        def compute_risk_profile_score(self, _config: FakeConfig) -> dict:
            return {"overall": 10.0}

        def compute_trading_style(self, _config: FakeConfig) -> str:
            return "Balanced"

    champions = pareto_explorer._build_champions(FakeLoader(), limit=2)

    assert [item["config_index"] for item in champions] == [2, 1]


def test_best_match_uses_chart_metrics_not_primary_scoring() -> None:
    """Profit vs Risk best match should rank ADG/drawdown, not the first PB7 scoring metric."""

    class FakeConfig:
        """Minimal config object with metrics and pareto flag."""

        is_pareto = True

        def __init__(self, config_index: int, suite_metrics: dict) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics

    class FakeLoader:
        """Minimal loader exposing pareto configs and robustness."""

        scoring_metrics = ["sharpe_ratio_usd", "adg_pnl"]

        def __init__(self) -> None:
            self.configs = [
                FakeConfig(1, {"sharpe_ratio_usd": 1.0, "adg_w_usd": 0.001, "drawdown_worst_usd": 0.10}),
                FakeConfig(2, {"sharpe_ratio_usd": 0.5, "adg_w_usd": 0.003, "drawdown_worst_usd": 0.15}),
            ]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

        def compute_overall_robustness(self, config: FakeConfig) -> float:
            return 1.0

    best_match, score, primary_metric = pareto_explorer._compute_best_match(
        FakeLoader(),
        perf_weight=80,
        risk_weight=60,
        robust_weight=0,
        performance_metric="adg_w_usd",
        risk_metric="drawdown_worst_usd",
    )

    assert best_match.config_index == 2
    assert score == 0.571429
    assert primary_metric == "adg_w_usd"


def test_best_match_can_prioritize_low_drawdown() -> None:
    """Risk aversion should be able to choose the lower-drawdown tradeoff."""

    class FakeConfig:
        """Minimal config object with metrics and pareto flag."""

        is_pareto = True

        def __init__(self, config_index: int, suite_metrics: dict) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics

    class FakeLoader:
        """Minimal loader exposing pareto configs and robustness."""

        scoring_metrics = ["sharpe_ratio_usd", "adg_pnl"]

        def __init__(self) -> None:
            self.configs = [
                FakeConfig(1, {"sharpe_ratio_usd": 1.0, "adg_w_usd": 0.001, "drawdown_worst_usd": 0.10}),
                FakeConfig(2, {"sharpe_ratio_usd": 0.5, "adg_w_usd": 0.003, "drawdown_worst_usd": 0.15}),
            ]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

        def compute_overall_robustness(self, config: FakeConfig) -> float:
            return 1.0

    best_match, score, _ = pareto_explorer._compute_best_match(
        FakeLoader(),
        perf_weight=20,
        risk_weight=100,
        robust_weight=0,
        performance_metric="adg_w_usd",
        risk_metric="drawdown_worst_usd",
    )

    assert best_match.config_index == 1
    assert score == 0.833333


def test_config_detail_score_uses_visible_score_context() -> None:
    """Selected config detail score should be normalized against visible configs when provided."""

    class FakeConfig:
        """Minimal config object for detail serialization."""

        is_pareto = True
        config_hash = "hash"
        objectives = {}
        constraint_violation = 0.0
        scenario_metrics = {}
        optimize_settings = {}
        scenario_details = []
        metric_stats = {}
        details_loaded = True
        bot_params_loaded = True
        bot_params = {}

        def __init__(self, config_index: int, adg: float) -> None:
            self.config_index = config_index
            self.suite_metrics = {"adg_w_usd": adg, "drawdown_worst_usd": 0.10}

    class FakeLoader:
        """Minimal loader exposing detail dependencies."""

        scoring_metrics = ["adg_w_usd"]
        scoring_goals = {"adg_w_usd": "max"}
        scenario_labels = []

        def __init__(self) -> None:
            self.configs = [FakeConfig(1, 0.5), FakeConfig(2, 1000.0)]

        def get_config_by_index(self, config_index: int) -> FakeConfig | None:
            return next((config for config in self.configs if config.config_index == config_index), None)

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

        def ensure_bot_params(self, _config: FakeConfig) -> None:
            return None

        def ensure_details(self, _config: FakeConfig) -> None:
            return None

        def compute_risk_profile_score(self, _config: FakeConfig) -> dict:
            return {"overall": 10.0}

        def get_full_config(self, config_index: int) -> dict:
            return {"config_index": config_index}

        def compute_trading_style(self, _config: FakeConfig) -> str:
            return "Balanced"

        def compute_overall_robustness(self, _config: FakeConfig) -> float:
            return 0.0

    loader = FakeLoader()
    visible_peer = FakeConfig(3, 0.0)

    detail = pareto_explorer._serialize_config_detail(
        loader,
        1,
        perf_weight=100,
        risk_weight=0,
        robust_weight=0,
        score_configs_override=[loader.configs[0], visible_peer],
    )

    assert detail["explorer_score"] == 1.0


def test_correlation_top_performers_deduplicates_configs() -> None:
    """Top Performer radar selection should not return the same config repeatedly."""

    class FakeConfig:
        """Minimal config with several top metrics."""

        def __init__(self, config_index: int, suite_metrics: dict) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics

    class FakeLoader:
        """Minimal loader exposing pareto configs and scoring goals."""

        scoring_metrics = ["adg_w_usd"]
        scoring_goals = {}

        def get_pareto_configs(self) -> list[FakeConfig]:
            return [
                FakeConfig(1, {"adg_w_usd": 0.30, "sharpe_ratio_w_usd": 3.0, "drawdown_worst_w_usd": 0.05}),
                FakeConfig(2, {"adg_w_usd": 0.10, "sharpe_ratio_w_usd": 1.0, "drawdown_worst_w_usd": 0.20}),
            ]

    selected, labels = pareto_explorer._select_correlation_configs(
        FakeLoader(),
        strategy="Top Performers",
        num_configs=3,
        use_weighted=True,
        use_btc=False,
    )

    assert selected == [1]
    assert len(labels) == 1


def test_preview_display_metrics_honor_weighted_toggle_for_scoring_metrics() -> None:
    """Preview axes should prefer available `_w` variants when weighted mode is enabled."""

    class FakeConfig:
        """Minimal config object with weighted and unweighted metrics."""

        suite_metrics = {
            "sharpe_ratio_usd": 0.10,
            "sharpe_ratio_w_usd": 0.11,
            "adg_pnl": 0.003,
            "adg_pnl_w": 0.004,
        }

    class FakeLoader:
        """Minimal loader exposing PB7 scoring metric names."""

        scoring_metrics = ["sharpe_ratio_usd", "adg_pnl"]

    assert pareto_explorer._preview_display_metrics(FakeLoader(), use_weighted=True, configs=[FakeConfig()]) == [
        "sharpe_ratio_w_usd",
        "adg_pnl_w",
    ]
    assert pareto_explorer._preview_display_metrics(FakeLoader(), use_weighted=False, configs=[FakeConfig()]) == [
        "sharpe_ratio_usd",
        "adg_pnl",
    ]


def test_best_match_scores_all_preview_metrics() -> None:
    """Preview Best Match should consider both axes and the color risk metric."""

    class FakeConfig:
        """Minimal config object with preview metrics."""

        is_pareto = True

        def __init__(self, config_index: int, suite_metrics: dict) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics

    class FakeLoader:
        """Minimal loader exposing pareto configs and robustness."""

        scoring_metrics = ["sharpe_ratio_w_usd", "adg_pnl_w"]

        def __init__(self) -> None:
            self.configs = [
                FakeConfig(1, {"sharpe_ratio_w_usd": 1.0, "adg_pnl_w": 0.0, "drawdown_worst_w_usd": 0.10}),
                FakeConfig(2, {"sharpe_ratio_w_usd": 0.8, "adg_pnl_w": 1.0, "drawdown_worst_w_usd": 0.10}),
                FakeConfig(3, {"sharpe_ratio_w_usd": 0.0, "adg_pnl_w": 0.0, "drawdown_worst_w_usd": 0.10}),
            ]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

        def compute_overall_robustness(self, config: FakeConfig) -> float:
            return 0.0

    best_match, score, _ = pareto_explorer._compute_best_match(
        FakeLoader(),
        perf_weight=80,
        risk_weight=60,
        robust_weight=0,
        performance_metric="sharpe_ratio_w_usd",
        score_metrics=["sharpe_ratio_w_usd", "adg_pnl_w", "drawdown_worst_w_usd"],
    )

    assert best_match.config_index == 2
    assert score == 0.942857


def test_best_match_scores_3d_z_metric() -> None:
    """3D Best Match should include the third axis, not just x/y."""

    class FakeConfig:
        """Minimal config object with 3D metrics."""

        is_pareto = True

        def __init__(self, config_index: int, suite_metrics: dict) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics

    class FakeLoader:
        """Minimal loader exposing pareto configs and robustness."""

        scoring_metrics = ["adg_w_usd"]

        def __init__(self) -> None:
            self.configs = [
                FakeConfig(1, {"adg_w_usd": 0.1, "drawdown_worst_w_usd": 0.1, "equity_jerkiness_w_usd": 0.5}),
                FakeConfig(2, {"adg_w_usd": 0.1, "drawdown_worst_w_usd": 0.1, "equity_jerkiness_w_usd": 0.1}),
            ]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return list(self.configs)

        def compute_overall_robustness(self, config: FakeConfig) -> float:
            return 0.0

    best_match, score, _ = pareto_explorer._compute_best_match(
        FakeLoader(),
        perf_weight=80,
        risk_weight=60,
        robust_weight=0,
        performance_metric="adg_w_usd",
        score_metrics=["adg_w_usd", "drawdown_worst_w_usd", "equity_jerkiness_w_usd"],
    )

    assert best_match.config_index == 2
    assert score == 1.0


def test_playground_3d_uses_compact_marker_sizes() -> None:
    """3D scatter markers should stay small enough to avoid hiding dense fronts."""

    class FakeConfig:
        """Minimal config object with 3D chart metrics."""

        def __init__(self, config_index: int, suite_metrics: dict, *, is_pareto: bool) -> None:
            self.config_index = config_index
            self.suite_metrics = suite_metrics
            self.is_pareto = is_pareto

    class FakeLoader:
        """Minimal loader exposing all and Pareto configs."""

        def __init__(self) -> None:
            self.configs = [
                FakeConfig(1, {"adg_w_usd": 0.1, "drawdown_worst_usd": 0.2, "equity_jerkiness_w_usd": 0.3}, is_pareto=False),
                FakeConfig(2, {"adg_w_usd": 0.2, "drawdown_worst_usd": 0.1, "equity_jerkiness_w_usd": 0.2}, is_pareto=True),
            ]

        def get_pareto_configs(self) -> list[FakeConfig]:
            return [config for config in self.configs if config.is_pareto]

    loader = FakeLoader()
    chart = pareto_explorer._build_playground_3d(
        loader,
        x_metric="adg_w_usd",
        y_metric="drawdown_worst_usd",
        z_metric="equity_jerkiness_w_usd",
        color_metric=None,
        show_all=True,
        best_match=loader.configs[1],
        title_prefix="3D Scatter",
    )

    sizes = {trace["name"]: trace["marker"]["size"] for trace in chart["traces"]}
    assert sizes == {"All Configs": 3, "Pareto Front": 4, "Best Match": 8}


def test_metric_lower_is_better_includes_volatility_variants() -> None:
    """Volatility metrics should be treated as stability risks where lower is better."""

    assert pareto_explorer._metric_lower_is_better("equity_volatility_usd")
    assert pareto_explorer._metric_lower_is_better("equity_volatility_w_usd")


def test_build_optimize_preset_reports_near_bounds_preview(monkeypatch) -> None:
    """Near-bound rows and expansion notes should be returned for the frontend preview."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": True,
            "expand_near_bounds": True,
            "near_bounds_expand_pct": 25,
        },
        near_bounds={
            "at_lower": {
                "long_total_wallet_exposure_limit": {"value": 0.0, "bound": 0.0},
            },
            "at_upper": {},
        },
    )

    assert payload["near_rows"] == [
        {"param": "long_total_wallet_exposure_limit", "edge": "lower", "value": 0.0, "bound": 0.0}
    ]
    assert payload["bounds_preview_rows"]


def test_balanced_preserves_modern_scoring_objects(monkeypatch) -> None:
    """Balanced presets should keep modern PB7 scoring objects and goals."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()
    full_config["optimize"]["scoring"] = [
        {"metric": "adg_w_usd", "goal": "max"},
        {"metric": "loss_profit_ratio", "goal": "min"},
    ]

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    assert payload["preset_config"]["optimize"]["scoring"] == [
        {"metric": "adg_w_usd", "goal": "max"},
        {"metric": "loss_profit_ratio", "goal": "min"},
    ]


def test_balanced_preserves_up_to_eight_scoring_objectives(monkeypatch) -> None:
    """Balanced presets should keep current PB7's eight-objective scoring shape."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()
    full_config["optimize"]["scoring"] = [
        {"metric": metric, "goal": _default_goals([metric])[metric]}
        for metric in generator.DEFAULT_PRESET_SCORING
    ]

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert len(scoring) == 8
    assert [entry["metric"] for entry in scoring] == generator.DEFAULT_PRESET_SCORING


def test_missing_run_scoring_uses_modern_pb7_defaults(monkeypatch) -> None:
    """Missing run scoring should fall back to current PB7 strategy-equity defaults."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()
    full_config["optimize"].pop("scoring", None)
    context["optimize_settings"] = {}

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert [entry["metric"] for entry in scoring] == generator.DEFAULT_PRESET_SCORING
    assert all("goal" in entry for entry in scoring)


def test_missing_pb7_default_goals_get_explicit_preset_fallbacks(monkeypatch) -> None:
    """Generated presets should stay valid when PB7 has no default goal for a metric."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", lambda _metrics: {})
    context, full_config = _base_context()
    full_config["optimize"].pop("scoring", None)
    context["optimize_settings"] = {}

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert all(isinstance(entry, dict) and entry.get("goal") in {"min", "max"} for entry in scoring)
    goals = {entry["metric"]: entry["goal"] for entry in scoring}
    assert goals["peak_recovery_days_strategy_eq"] == "min"
    assert goals["position_held_days_max"] == "min"


def test_lower_exposure_prefers_exposure_efficiency_metrics(monkeypatch) -> None:
    """Lower-exposure presets should use PB7 exposure-aware metrics when available."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Lower exposure (safer sizing)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert scoring[:3] == [
        {"metric": "adg_w_per_exposure_long_usd", "goal": "max"},
        {"metric": "adg_w_per_exposure_short_usd", "goal": "max"},
        {"metric": "total_wallet_exposure_mean", "goal": "min"},
    ]


def test_fewer_shorter_holds_prefers_duration_exposure_and_quality(monkeypatch) -> None:
    """Less-time-in-market presets should prefer PB7 duration and exposure metrics."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "bounds_window_pct": 0,
            "direction": "Fewer/shorter holds (less time in market)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
    )

    scoring = payload["preset_config"]["optimize"]["scoring"]
    assert scoring[:5] == [
        {"metric": "position_held_days_max", "goal": "min"},
        {"metric": "position_held_days_mean", "goal": "min"},
        {"metric": "total_wallet_exposure_mean", "goal": "min"},
        {"metric": "mdg_strategy_eq_w", "goal": "max"},
        {"metric": "sortino_ratio_strategy_eq", "goal": "max"},
    ]


def test_only_adjust_near_bounds_limits_window_changes(monkeypatch) -> None:
    """Near-bound scope should keep non-near parameters unchanged."""
    monkeypatch.setattr(generator, "get_optimize_metric_sets", _metric_sets)
    monkeypatch.setattr(generator, "get_optimize_scoring_default_goals", _default_goals)
    context, full_config = _base_context()

    payload = generator.build_optimize_preset(
        config_context=context,
        full_config_data=full_config,
        params={
            "only_adjust_near_bounds": True,
            "bounds_window_pct": 10,
            "direction": "Balanced (keep run scoring)",
            "risk_adjust": 0,
            "show_near_bounds": False,
        },
        near_bounds={
            "at_lower": {},
            "at_upper": {
                "long_total_wallet_exposure_limit": {"value": 10.0, "bound": 10.0},
            },
        },
    )

    bounds = payload["preset_config"]["optimize"]["bounds"]
    assert payload["only_adjust_near_bounds"] is True
    assert payload["near_bounds_count"] == 1
    assert bounds["long_entry_grid_spacing_pct"] == full_config["optimize"]["bounds"]["long_entry_grid_spacing_pct"]
    assert bounds["long_total_wallet_exposure_limit"] != full_config["optimize"]["bounds"]["long_total_wallet_exposure_limit"]
