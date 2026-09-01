"""Tests for deterministic PB8 Suite sweep-cycle evaluation."""

from sweep_cycles import build_sweep_plan, evaluate_sweep_cycles, sweep_holdout_scenarios, validate_sweep_plan


def _config() -> dict:
    """Return one valid four-window sweep config."""
    scenarios = [
        {"label": f"train_{index:02d}", "start_date": f"202{index}-01-01", "end_date": f"202{index}-06-29"}
        for index in range(1, 5)
    ]
    return {
        "backtest": {"suite_enabled": True, "scenarios": scenarios},
        "pbgui": {
            "scenario_template": {
                "template": "sweep_cycles",
                "template_version": 1,
                "parameters": {
                    "window_days": 180,
                    "stride_days": 187,
                    "sweep_policy": {
                        "starting_balance": 1000,
                        "balance_multiplier": 2,
                        "refill_cost": 25,
                        "cooldown_days": 7,
                    },
                },
                "holdout_scenarios": [{"label": "holdout_01"}],
            }
        },
    }


def test_build_and_validate_sweep_plan_preserve_immutable_cycle_order() -> None:
    """Config extraction retains chronological training labels and policy."""
    plan = build_sweep_plan(_config())

    assert [item["label"] for item in plan["training_scenarios"]] == [
        "train_01",
        "train_02",
        "train_03",
        "train_04",
    ]
    assert plan["policy"]["cooldown_days"] == 7
    assert plan["holdout_count"] == 1
    assert validate_sweep_plan(plan) == plan


def test_sweep_evaluation_carries_sweeps_and_refills_window_balances() -> None:
    """Window gains follow carry, sweep-reset, and refill-reset rules exactly."""
    plan = build_sweep_plan(_config())
    gains = {
        "gain_strategy_eq": {
            "scenarios": {
                "train_01": 1.5,
                "train_02": 1.5,
                "train_03": 0.8,
                "train_04": 1.1,
            }
        }
    }

    result = evaluate_sweep_cycles(plan, gains)

    assert result["available"] is True
    assert [item["action"] for item in result["windows"]] == [
        "carry",
        "sweep_reset",
        "refill_reset",
        "carry",
    ]
    assert result["windows"][1]["swept"] == 1250.0
    assert result["windows"][2]["refill"] == 200.0
    assert result["summary"] == {
        "sweep_net_cashflow": 1025.0,
        "sweep_total_swept": 1250.0,
        "sweep_external_capital": 225.0,
        "sweep_cycles_completed": 1.0,
        "sweep_refill_count": 1.0,
        "sweep_final_balance": 1100.0,
        "sweep_target_hit_rate": 0.25,
    }
    assert result["holdout_status"] == "pending_separate_validation"


def test_sweep_evaluation_fails_closed_when_one_window_gain_is_missing() -> None:
    """An incomplete Suite metric payload never produces partial cash-flow metrics."""
    plan = build_sweep_plan(_config())

    result = evaluate_sweep_cycles(plan, {"gain_usd": {"scenarios": {"train_01": 1.1}}})

    assert result == {"available": False, "reason": "No per-scenario gain metric for train_02"}


def test_sweep_plan_rejects_overlapping_cooldown_windows() -> None:
    """A cooldown must be represented by a real no-trading gap between windows."""
    config = _config()
    config["pbgui"]["scenario_template"]["parameters"]["stride_days"] = 180

    assert build_sweep_plan(config) is None


def test_old_sweep_plan_derives_holdout_after_last_training_window() -> None:
    """Existing sidecars without explicit holdout dates remain automatable."""
    plan = build_sweep_plan(_config())
    plan.pop("holdout_scenarios")

    holdouts = sweep_holdout_scenarios(plan)

    assert holdouts == [
        {
            "label": "holdout_01",
            "start_date": "2024-07-07",
            "end_date": "2025-01-02",
        }
    ]
