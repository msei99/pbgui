"""Tests for deterministic PBGui post-processing of official PB8 migrations."""

from __future__ import annotations

import pytest

from api.v8_migration_context import postprocess_v7_migration


def _config(strategy: str = "trailing_grid_v7") -> dict:
    """Return a minimal migrated V8 config with both shared bot sides."""
    return {
        "live": {"strategy_kind": strategy},
        "bot": {
            "long": {
                "risk": {
                    "n_positions": 4,
                    "total_wallet_exposure_limit": 1.6,
                    "position_exposure_enforcer_enabled": False,
                    "position_exposure_enforcer_threshold": 0.938,
                    "total_exposure_enforcer_enabled": False,
                    "total_exposure_enforcer_threshold": 1.01,
                },
                "hsl": {"no_restart_drawdown_threshold": 0.5},
            },
            "short": {
                "risk": {
                    "n_positions": 2,
                    "total_wallet_exposure_limit": 0.8,
                    "position_exposure_enforcer_enabled": False,
                    "position_exposure_enforcer_threshold": 0.0,
                    "total_exposure_enforcer_enabled": False,
                    "total_exposure_enforcer_threshold": 0.0,
                },
                "hsl": {"no_restart_drawdown_threshold": 0.5},
            },
        },
        "optimize": {
            "enable_overrides": [],
            "fixed_params": [],
            "fixed_runtime_overrides": {},
            "bounds": {},
            "scoring": [],
            "limits": [],
        },
    }


def test_trailing_grid_filters_only_incompatible_optimizer_override() -> None:
    """Trailing-grid conversion must retain TP ordering while dropping martingale-only logic."""
    config = _config()
    config["optimize"]["enable_overrides"] = ["lossless_close_trailing", "forward_tp_grid"]

    migrated, report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert migrated["optimize"]["enable_overrides"] == ["forward_tp_grid"]
    assert report["status"] == "ok_with_adjustments"
    assert report["manual_review_required"] is False
    assert any("lossless_close_trailing" in item["detail"] for item in report["pbgui_post_migration_adjustments"])


def test_trailing_martingale_keeps_lossless_override() -> None:
    """The lossless helper remains valid for its native martingale strategy."""
    config = _config("trailing_martingale")
    config["optimize"]["enable_overrides"] = ["lossless_close_trailing"]

    migrated, report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert migrated["optimize"]["enable_overrides"] == ["lossless_close_trailing"]
    assert report["status"] == "ok"


@pytest.mark.parametrize("side", ["long", "short"])
def test_fixed_runtime_hsl_aliases_are_rewritten_to_nested_paths(side: str) -> None:
    """Legacy flat HSL override selectors must be emitted as canonical V8 paths."""
    config = _config()
    config["optimize"]["fixed_runtime_overrides"] = {
        f"bot.{side}.hsl_no_restart_drawdown_threshold": 1,
    }

    migrated, report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert migrated["optimize"]["fixed_runtime_overrides"] == {
        f"bot.{side}.hsl.no_restart_drawdown_threshold": 1,
    }
    assert report["status"] == "ok_with_adjustments"


def test_conflicting_runtime_alias_requires_manual_review() -> None:
    """Two values resolving to the same V8 field must never overwrite each other silently."""
    config = _config()
    config["optimize"]["fixed_runtime_overrides"] = {
        "bot.long.hsl.no_restart_drawdown_threshold": 0.5,
        "bot.long.hsl_no_restart_drawdown_threshold": 1.0,
    }

    _migrated, report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert report["manual_review_required"] is True
    assert report["status"] == "manual_review_required"
    assert report["manual_review_fields"] == [
        "optimize.fixed_runtime_overrides.bot.long.hsl_no_restart_drawdown_threshold"
    ]
    assert report["pbgui_post_migration_review_fields"] == report["manual_review_fields"]


def test_relative_runtime_path_is_emitted_as_canonical_bot_path() -> None:
    """PB8's side-relative selector syntax should remain valid but be stored canonically."""
    config = _config()
    config["optimize"]["fixed_runtime_overrides"] = {"long.risk.n_positions": 2}

    migrated, report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert migrated["optimize"]["fixed_runtime_overrides"] == {"bot.long.risk.n_positions": 2}
    assert report["status"] == "ok_with_adjustments"


def test_disabled_short_is_frozen_without_removing_bounds() -> None:
    """An already disabled side should stay complete but leave the active search space."""
    config = _config()
    config["bot"]["short"]["risk"]["n_positions"] = 0
    config["bot"]["short"]["risk"]["total_wallet_exposure_limit"] = 0.0
    config["optimize"]["bounds"] = {"short": {"risk": {"n_positions": [0, 10]}}}

    migrated, report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert migrated["optimize"]["fixed_params"] == ["bot.short"]
    assert migrated["optimize"]["bounds"] == config["optimize"]["bounds"]
    assert report["status"] == "ok_with_adjustments"


def test_partly_disabled_side_is_not_frozen() -> None:
    """PBGui must not infer a disabled side from only one zero-valued risk control."""
    config = _config()
    config["bot"]["short"]["risk"]["n_positions"] = 0
    config["bot"]["short"]["risk"]["total_wallet_exposure_limit"] = 1.0

    migrated, _report = postprocess_v7_migration({}, config, {"status": "ok"})

    assert migrated["optimize"]["fixed_params"] == []


def test_positive_v7_thresholds_enable_v8_enforcers() -> None:
    """Positive V7 reducer thresholds must preserve their implicit-on behavior in V8."""
    source = {
        "bot": {
            "long": {
                "risk_wel_enforcer_threshold": 0.938,
                "risk_twel_enforcer_threshold": 1.01,
            }
        }
    }

    migrated, report = postprocess_v7_migration(source, _config(), {"status": "ok"})

    risk = migrated["bot"]["long"]["risk"]
    assert risk["position_exposure_enforcer_enabled"] is True
    assert risk["total_exposure_enforcer_enabled"] is True
    assert report["status"] == "ok_with_adjustments"
    assert report.get("review_recommended") is not True
    assert len(report["behavior_change_warnings"]) == 2


def test_explicit_v7_enforcer_switch_is_respected() -> None:
    """An explicit V7 switch takes precedence over threshold-derived compatibility behavior."""
    source = {
        "bot": {
            "long": {
                "risk_wel_enforcer_enabled": False,
                "risk_wel_enforcer_threshold": 0.938,
            }
        }
    }

    migrated, _report = postprocess_v7_migration(source, _config(), {"status": "ok"})

    assert migrated["bot"]["long"]["risk"]["position_exposure_enforcer_enabled"] is False


def test_backtest_conversion_requires_explicit_excess_allowance_choice() -> None:
    """A real bounded-versus-V7 sizing difference must become a marked pre-save decision."""
    source = {
        "bot": {
            "long": {
                "n_positions": 1,
                "total_wallet_exposure_limit": 2.0,
                "risk_we_excess_allowance_pct": 0.4811,
            },
            "short": {
                "n_positions": 1,
                "total_wallet_exposure_limit": 1.0,
                "risk_we_excess_allowance_pct": 0.0,
            },
        }
    }
    config = _config()
    config["bot"]["long"]["risk"].update(
        {
            "n_positions": 1,
            "total_wallet_exposure_limit": 2.0,
            "we_excess_allowance_mode": "bounded",
            "we_excess_allowance_pct": 0.4811,
        }
    )

    migrated, report = postprocess_v7_migration(
        source,
        config,
        {"status": "ok"},
        require_v7_excess_review=True,
    )

    path = "bot.long.risk.we_excess_allowance_mode"
    assert migrated["bot"]["long"]["risk"]["we_excess_allowance_mode"] == "bounded"
    assert report["manual_review_required"] is True
    assert report["status"] == "manual_review_required"
    assert report["manual_review_fields"] == [path]
    assert report["pbgui_post_migration_review_fields"] == [path]
    assert report["pbgui_review_decisions"] == [
        {
            "code": "we_excess_allowance_mode",
            "path": path,
            "side": "long",
            "current_value": "bounded",
            "v7_parity_value": "legacy_raw",
            "excess_allowance_pct": 0.4811,
            "base_wel": 2.0,
            "raw_allowed_wel": 2.9622,
            "side_twel": 2.0,
            "affected_contexts": ["bot.long.risk"],
        }
    ]


def test_backtest_excess_choice_includes_inherited_coin_override_clamp() -> None:
    """A sparse coin override must trigger the side-level mode choice when it inherits bounded."""
    source = {
        "bot": {
            "long": {
                "n_positions": 2,
                "total_wallet_exposure_limit": 2.0,
                "risk_we_excess_allowance_pct": 0.0,
            },
            "short": {},
        }
    }
    config = _config()
    config["bot"]["long"]["risk"].update(
        {
            "n_positions": 2,
            "total_wallet_exposure_limit": 2.0,
            "we_excess_allowance_mode": "bounded",
            "we_excess_allowance_pct": 0.0,
        }
    )
    config["coin_overrides"] = {
        "HYPE": {
            "bot": {
                "long": {
                    "wallet_exposure_limit": 1.8,
                    "risk": {"we_excess_allowance_pct": 0.5},
                }
            }
        }
    }

    _migrated, report = postprocess_v7_migration(
        source,
        config,
        {"status": "ok"},
        require_v7_excess_review=True,
    )

    assert report["manual_review_required"] is True
    decision = report["pbgui_review_decisions"][0]
    assert decision["path"] == "bot.long.risk.we_excess_allowance_mode"
    assert decision["raw_allowed_wel"] == pytest.approx(2.7)
    assert decision["affected_contexts"] == ["coin_overrides.HYPE.bot.long.risk"]


def test_backtest_conversion_requires_minimum_coin_age_semantics_choice() -> None:
    """A positive V7 coin-age value must become an explicit coverage-versus-gate decision."""
    source = {"live": {"minimum_coin_age_days": 30}}
    config = _config()
    config["live"]["minimum_coin_age_days"] = 30

    migrated, report = postprocess_v7_migration(
        source,
        config,
        {"status": "ok"},
        require_minimum_coin_age_review=True,
    )

    path = "live.minimum_coin_age_days"
    assert migrated["live"]["minimum_coin_age_days"] == 30
    assert report["manual_review_required"] is True
    assert report["manual_review_fields"] == [path]
    assert report["pbgui_review_decisions"] == [
        {
            "code": "minimum_coin_age_days",
            "path": path,
            "current_value": 30.0,
            "v7_parity_value": 0.0,
            "pb8_age_gate_value": 30.0,
        }
    ]


def test_search_policy_warnings_do_not_rewrite_optimizer_recipe() -> None:
    """Weighted scoring, floors, and new fixed genes should warn without changing policy."""
    config = _config()
    config["optimize"]["scoring"] = [{"metric": "adg_strategy_eq_w", "goal": "max"}]
    config["optimize"]["limits"] = [
        {"metric": "adg_strategy_eq", "penalize_if": "less_than", "value": 0.001}
    ]
    config["optimize"]["bounds"] = {
        "long": {"risk": {"entry_cooldown_minutes": [0.0, 0.0, 0.1]}}
    }
    report = {"status": "ok", "inserted_v8_defaults": ["bot.long.hsl.restart_after_red_policy"]}

    migrated, updated_report = postprocess_v7_migration({}, config, report)

    assert migrated["optimize"]["scoring"] == config["optimize"]["scoring"]
    assert migrated["optimize"]["limits"] == config["optimize"]["limits"]
    assert migrated["optimize"]["bounds"] == config["optimize"]["bounds"]
    assert updated_report.get("review_recommended") is not True
    assert len(updated_report["warnings"]) == 4
