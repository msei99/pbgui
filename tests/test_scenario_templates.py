"""Tests for deterministic PB8 scenario template generation."""

import pytest

from scenario_templates import ScenarioTemplateError, generate_scenario_template, list_scenario_templates


def test_list_scenario_templates_returns_supported_templates():
    """Template discovery exposes all supported deterministic generators."""
    assert [item["id"] for item in list_scenario_templates()] == [
        "rolling_windows",
        "walk_forward",
        "sweep_cycles",
    ]


def test_rolling_windows_are_chronological_and_deterministic():
    """Identical rolling-window input produces stable chronological scenarios."""
    payload = {
        "template": "rolling_windows",
        "start_date": "2023-01-01",
        "end_date": "2024-01-31",
        "window_days": 90,
        "stride_days": 30,
        "training_windows": 3,
    }

    first = generate_scenario_template(payload)
    second = generate_scenario_template(payload)

    assert first == second
    assert [(item["start_date"], item["end_date"]) for item in first["training_scenarios"]] == [
        ("2023-09-04", "2023-12-02"),
        ("2023-10-04", "2024-01-01"),
        ("2023-11-03", "2024-01-31"),
    ]


def test_walk_forward_keeps_holdout_out_of_training_scenarios():
    """Walk-forward holdout windows remain preview-only and persisted as provenance."""
    result = generate_scenario_template(
        {
            "template": "walk_forward",
            "start_date": "2023-01-01",
            "end_date": "2024-01-31",
            "window_days": 60,
            "stride_days": 60,
            "training_windows": 3,
            "holdout_windows": 1,
        }
    )

    assert len(result["training_scenarios"]) == 3
    assert len(result["holdout_scenarios"]) == 1
    assert result["holdout_scenarios"] == result["provenance"]["holdout_scenarios"]
    assert result["reducer"]["backtest_completion_ratio"] == "min"
    assert all(item["label"].startswith("train_") for item in result["training_scenarios"])
    assert result["holdout_scenarios"][0]["label"].startswith("holdout_")


def test_per_exchange_expansion_is_bounded_and_normalized():
    """Per-exchange mode expands each window with normalized exchange identifiers."""
    result = generate_scenario_template(
        {
            "template": "rolling_windows",
            "start_date": "2023-01-01",
            "end_date": "2024-01-31",
            "window_days": 30,
            "stride_days": 30,
            "training_windows": 2,
            "exchange_mode": "per_exchange",
            "exchanges": ["Binance", "bybit", "BINANCE"],
        }
    )

    assert len(result["training_scenarios"]) == 4
    assert [item["exchanges"] for item in result["training_scenarios"]] == [
        ["binance"],
        ["bybit"],
        ["binance"],
        ["bybit"],
    ]


def test_sweep_cycle_policy_is_provenance_only():
    """Sweep policy is persisted without adding unsupported PB8 scenario fields."""
    result = generate_scenario_template(
        {
            "template": "sweep_cycles",
            "start_date": "2022-01-01",
            "end_date": "2024-01-31",
            "window_days": 90,
            "stride_days": 97,
            "training_windows": 2,
            "holdout_windows": 1,
            "balance_multiplier": 3,
            "starting_balance": 2500,
            "refill_cost": 10,
            "cooldown_days": 7,
        }
    )

    assert result["parameters"]["sweep_policy"]["balance_multiplier"] == 3.0
    assert all("sweep_policy" not in item for item in result["training_scenarios"])
    assert result["reducer"]["backtest_completion_ratio"] == "min"


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"start_date": "2024/01/01"}, "start_date must be an ISO date"),
        ({"window_days": 0}, "window_days must be between"),
        ({"holdout_windows": 1}, "rolling_windows does not support"),
        ({"exchange_mode": "per_exchange"}, "requires at least one exchange"),
        ({"exchange_mode": "per_exchange", "exchanges": ["../bad"]}, "each exchange must contain"),
    ],
)
def test_invalid_template_inputs_are_rejected(override, message):
    """Invalid dates, bounds, modes, and identifiers fail closed."""
    payload = {
        "template": "rolling_windows",
        "start_date": "2023-01-01",
        "end_date": "2024-01-31",
        "window_days": 30,
        "stride_days": 30,
        "training_windows": 2,
    }
    payload.update(override)

    with pytest.raises(ScenarioTemplateError, match=message):
        generate_scenario_template(payload)


def test_template_rejects_insufficient_date_range():
    """Requested windows must fit completely within the configured date range."""
    with pytest.raises(ScenarioTemplateError, match="template requires"):
        generate_scenario_template(
            {
                "template": "walk_forward",
                "start_date": "2024-01-01",
                "end_date": "2024-02-01",
                "window_days": 30,
                "stride_days": 30,
                "training_windows": 2,
                "holdout_windows": 1,
            }
        )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"stride_days": 96}, r"window_days \+ cooldown_days"),
        ({"exchange_mode": "per_exchange", "exchanges": ["binance"]}, "requires inherited combined exchanges"),
    ],
)
def test_sweep_cycles_reject_ambiguous_or_overlapping_tracks(override, message):
    """Real sequential sweep evaluation requires one non-overlapping combined track."""
    payload = {
        "template": "sweep_cycles",
        "start_date": "2020-01-01",
        "end_date": "2024-01-01",
        "window_days": 90,
        "stride_days": 97,
        "training_windows": 3,
        "holdout_windows": 1,
        "cooldown_days": 7,
    }
    payload.update(override)

    with pytest.raises(ScenarioTemplateError, match=message):
        generate_scenario_template(payload)


def test_sweep_cycles_auto_plan_uses_maximum_complete_coverage_windows():
    """Automatic Sweep planning derives stride and training count from the full date range."""
    result = generate_scenario_template(
        {
            "template": "sweep_cycles",
            "auto_windows": True,
            "start_date": "2024-12-31",
            "end_date": "2026-08-29",
            "window_days": 90,
            "stride_days": 1,
            "training_windows": 1,
            "holdout_windows": 1,
            "cooldown_days": 0,
        }
    )

    assert result["parameters"]["auto_windows"] is True
    assert result["parameters"]["stride_days"] == 90
    assert result["parameters"]["training_windows"] == 5
    assert len(result["training_scenarios"]) == 5
    assert len(result["holdout_scenarios"]) == 1
    assert result["coverage"]["unused_leading_days"] == 67
