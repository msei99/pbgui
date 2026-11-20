"""PB9 Explorer uses available native metrics and preserves unavailable values."""

import pytest

from api import pareto_explorer as explorer
from ParetoDataLoader import ConfigMetrics, ParetoDataLoader


@pytest.fixture
def native_loader(tmp_path):
    """Match the three-metric native result shown in the reported empty chart."""
    loader = ParetoDataLoader(str(tmp_path))
    loader.optimize_version = "v9"
    loader.scoring_metrics = [
        "cagr_fraction_strategy_equity_ew",
        "ulcer_index_fraction_strategy_equity",
        "peak_recovery_max_days_strategy_equity",
    ]
    loader.scoring_goals = dict(zip(loader.scoring_metrics, ("max", "min", "min")))
    for index in range(3):
        metrics = dict(zip(loader.scoring_metrics, (0.3 + index / 10, 0.1 + index / 20, 10 + index)))
        loader.configs.append(ConfigMetrics(
            config_index=index, config_hash=str(index), objectives=metrics,
            constraint_violation=0, suite_metrics=metrics, scenario_metrics={},
            robustness_scores={}, bot_params={}, is_pareto=True,
        ))
    return loader


def playground(loader, **overrides):
    """Exercise the production chart payload builder without runtime or data reads."""
    options = dict(
        all_results_loaded=False, perf_weight=80, risk_weight=60, robust_weight=70,
        show_all=False, use_weighted=True, preview_use_weighted=True,
        preview_show_all=False, selected_config_index=None, use_btc=False,
        viz_type="2D Scatter", quick_view="Efficiency", color_metric_input="None",
        custom_x_metric="", custom_y_metric="", custom_z_metric="",
    )
    options.update(overrides)
    return explorer._build_playground_payload(loader, **options)


def test_native_default_chart_has_points_and_no_retired_metrics(native_loader):
    """A stored legacy Quick View recovers to a usable native metric pair."""
    payload = playground(native_loader)
    assert payload["quick_view"] == "Growth vs Ulcer"
    assert payload["metrics"]["x_metric"] == "cagr_fraction_strategy_equity_ew"
    assert payload["metrics"]["y_metric"] == "ulcer_index_fraction_strategy_equity"
    assert payload["visualizations"]["scatter_2d"]["traces"][0]["x"] == [0.3, 0.4, 0.5]
    assert "Efficiency" not in payload["quick_views"]
    assert payload["robustness_available"] is False
    assert payload["weighting_available"] is False


@pytest.mark.parametrize("visualization", ["2D Scatter", "3D Scatter", "3D Projections", "Radar Chart"])
def test_every_offered_native_view_uses_present_metrics(native_loader, visualization):
    """Every native Quick View is grounded in the result's actual metric set."""
    payload = playground(native_loader, viz_type=visualization)
    for name in payload["quick_views"]:
        if name == "Custom...":
            continue
        result = playground(native_loader, viz_type=visualization, quick_view=name)
        assert result["quick_view"] == name
        for axis in ("x_metric", "y_metric", "z_metric"):
            assert result["metrics"][axis] in native_loader.configs[0].suite_metrics


def test_native_weighting_only_switches_cagr(native_loader):
    """EW selection keeps unweighted risk metrics available."""
    for config in native_loader.configs:
        config.suite_metrics["cagr_fraction_strategy_equity"] = 0.2
    weighted = playground(native_loader)
    plain = playground(native_loader, use_weighted=False)
    assert weighted["weighting_available"] is True
    assert weighted["metrics"]["x_metric"] == "cagr_fraction_strategy_equity_ew"
    assert plain["metrics"]["x_metric"] == "cagr_fraction_strategy_equity"
    assert plain["metrics"]["y_metric"] == weighted["metrics"]["y_metric"]


def test_native_missing_activity_and_risk_do_not_become_idle_or_perfect(native_loader):
    """Unavailable legacy dimensions never yield fabricated classifications or scores."""
    config = native_loader.configs[0]
    assert native_loader.compute_trading_style(config) == "Activity classification unavailable"
    assert native_loader.compute_risk_profile_score(config) == {
        "ulcer_index_fraction_strategy_equity": 0.1,
        "peak_recovery_max_days_strategy_equity": 10.0,
    }
    assert native_loader.compute_overall_robustness(config) == 0.0
    assert explorer._build_robustness_quadrant(
        native_loader, performance_metric=native_loader.scoring_metrics[0], show_all=False
    )["traces"] == []


def test_native_ulcer_is_minimized_without_an_explicit_objective(native_loader):
    """Native risks omitted from scoring still use the correct direction."""
    native_loader.scoring_goals = {}
    metric = "ulcer_index_fraction_strategy_equity"
    best, _, _ = explorer._compute_best_match(
        native_loader, perf_weight=0, risk_weight=100, robust_weight=70,
        performance_metric=metric, score_metrics=[metric],
    )
    assert best.config_index == 0


def test_native_null_aggregate_stays_unavailable(native_loader):
    """A native unavailable metric must not become a numeric zero in the reader."""
    parsed = native_loader._parse_config(10, {
        "config_version": "v9.0.0",
        "suite_metrics": {"metrics": {
            "drawdown_max_fraction_strategy_equity": {"aggregated": None, "stats": {"mean": None}},
            "cagr_fraction_strategy_equity_ew": {"aggregated": 0.0, "stats": {"mean": 0.0, "std": 0.0}},
        }},
    })
    assert "drawdown_max_fraction_strategy_equity" not in parsed.suite_metrics
    assert parsed.suite_metrics["cagr_fraction_strategy_equity_ew"] == 0.0


@pytest.mark.parametrize("parser", ["_parse_config", "_parse_config_light", "_parse_config_light_raw"])
def test_native_non_suite_saved_reductions_populate_all_views(native_loader, parser):
    """Real PB9 output saves different reductions per objective, not only mean."""
    import msgpack

    record = {
        "config_version": "v9.0.0",
        "metrics": {"stats": {
            "backtest_completion_fraction": {"min": 1.0},
            "cagr_fraction_strategy_equity_ew": {"mean": 0.41462667304604134},
            "peak_recovery_max_days_strategy_equity": {"max": 17.833333333333332},
            "ulcer_index_fraction_strategy_equity": {"max": 0.009217391903547145},
        }},
    }
    if parser.endswith("_raw"):
        record = msgpack.unpackb(msgpack.packb(record), raw=True)
    parsed = getattr(native_loader, parser)(10, record)
    assert parsed.suite_metrics == {
        "backtest_completion_fraction": 1.0,
        "cagr_fraction_strategy_equity_ew": 0.41462667304604134,
        "peak_recovery_max_days_strategy_equity": 17.833333333333332,
        "ulcer_index_fraction_strategy_equity": 0.009217391903547145,
    }
    native_loader.configs = [parsed]
    parsed.is_pareto = True
    chart = playground(native_loader)
    assert chart["metrics"]["y_metric"] == "ulcer_index_fraction_strategy_equity"
    assert chart["visualizations"]["scatter_2d"]["traces"][0]["y"] == pytest.approx([0.009217391903547145])


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("saved,expected", [
    ({"max": 0.125}, 0.125), ({"min": 0.0}, 0.0),
    ({"stats": {"max": 8}}, 8), ({"aggregated": None, "mean": 5}, None),
    ({"mean": None}, None), ({"mean": float("nan")}, None),
    ({"max": float("inf")}, None), ({"std": 4}, None),
    ({"min": 1, "max": 4}, None),
])
def test_native_reduction_reader_keeps_missing_and_ambiguous_values_unavailable(raw, saved, expected):
    """JSON and raw msgpack share finite-value and explicit-null semantics."""
    import msgpack
    from ParetoDataLoader import _native_metric_value

    if raw:
        saved = msgpack.unpackb(msgpack.packb(saved), raw=True)
    assert _native_metric_value(saved) == expected


@pytest.mark.parametrize("rows", [2, 105])
def test_native_compressed_full_results_and_lazy_details_share_pb8_reader(tmp_path, rows):
    """PB9 uses the PB8 incremental reader, retaining max/min metrics in lazy views."""
    import msgpack

    growth = 'cagr_fraction_strategy_equity_ew'
    ulcer = 'ulcer_index_fraction_strategy_equity'
    record = {
        'config_version': 'v9.0.0',
        'optimize': {'compress_results_file': True,
                     'scoring': [{'metric': growth, 'goal': 'max'}, {'metric': ulcer, 'goal': 'min'}]},
        'bot': {'global': {'entry_cooldown': {'base_duration_minutes': 1}}},
        'metrics': {'objectives': {growth: 0.1, ulcer: 0.04},
                    'stats': {growth: {'mean': 0.1}, ulcer: {'max': 0.04},
                              'backtest_completion_fraction': {'min': 1.0}}},
    }
    diff = {'metrics': {'objectives': {growth: 0.3, ulcer: 0.01},
                        'stats': {growth: {'mean': 0.3}, ulcer: {'max': 0.01}}},
            'bot': {'global': {'entry_cooldown': {'base_duration_minutes': 5}}}}
    records = [record, diff] + [
        {'metrics': {'stats': {growth: {'mean': 0.3 + index / 100}},
                     'objectives': {growth: 0.3 + index / 100}}}
        for index in range(2, rows)
    ]
    (tmp_path / 'all_results.bin').write_bytes(b''.join(msgpack.packb(item) for item in records))
    loader = ParetoDataLoader(str(tmp_path))
    assert loader.load(load_strategy=['performance'], max_configs=2), loader.last_error
    assert loader.optimize_version == 'v9'
    assert loader._diff_compressed_results is True
    config = loader.get_config_by_index(rows - 1)
    assert config.suite_metrics[ulcer] == 0.01
    assert config.suite_metrics['backtest_completion_fraction'] == 1.0
    loader.ensure_details(config)
    loader.ensure_bot_params(config)
    assert config.suite_metrics[ulcer] == 0.01
    assert config.bot_params['global.entry_cooldown.base_duration_minutes'] == 5
    assert loader.get_full_config(rows - 1)['metrics']['stats'][ulcer] == {'max': 0.01}
    assert loader.get_full_config(rows - 1)['config_version'] == 'v9.0.0'
    cached = ParetoDataLoader(str(tmp_path))
    assert cached.load(load_strategy=['performance'], max_configs=2), cached.last_error
    assert cached.get_config_by_index(rows - 1).suite_metrics['backtest_completion_fraction'] == 1.0


def test_native_champions_report_unknown_robustness_and_use_native_risk(native_loader):
    """Overview shortlists never fabricate a robustness value or ignore native risks."""
    champions = explorer._build_champions(native_loader)
    assert all(item['robustness'] is None for item in champions)
    assert all(item['risk_overall'] is None for item in champions)
    native_loader.configs[2].suite_metrics['ulcer_index_fraction_strategy_equity'] = 0.001
    best, _, _ = explorer._compute_best_match(
        native_loader, perf_weight=0, risk_weight=100, robust_weight=70,
    )
    assert best.config_index == 2


def test_native_correlation_radar_keeps_requested_candidates_and_labels(native_loader):
    """Correlation selections are not replaced by the Playground's default shortlist."""
    chart = explorer._build_risk_profile_radar(native_loader, config_indices=[2, 0], labels=['Chosen B', 'Chosen A'])
    candidates = [trace for trace in chart['traces'] if 'meta' in trace]
    assert {trace['meta']['config_index'] for trace in candidates} == {2, 0}
    assert [trace['name'] for trace in candidates if trace.get('showlegend')] == ['Chosen B', 'Chosen A']
    del native_loader.configs[0].suite_metrics['ulcer_index_fraction_strategy_equity']
    chart = explorer._build_risk_profile_radar(native_loader, config_indices=[2, 0], labels=['B', 'A'])
    assert not any('Ulcer' in item.get('text', '') for item in chart['layout'].get('annotations', []))
