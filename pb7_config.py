"""
pb7_config.py — Thin wrapper around passivbot's config pipeline.

Loads and saves passivbot configs by delegating to pb7's own
normalize / migrate / hydrate pipeline, while preserving pbgui-specific
fields that passivbot would strip.

Usage:
    from pb7_config import load_pb7_config, save_pb7_config

    cfg = load_pb7_config("/path/to/backtest.json")               # normalized dict
    cfg = load_pb7_config("/path/to/config.json", neutralize_added=True)  # with neutralization

    cfg["backtest"]["starting_balance"] = 50000
    save_pb7_config(cfg, "/path/to/backtest.json")

When ``neutralize_added=True``, any bot parameter that was silently
injected by passivbot's pipeline (because it was missing from the file)
is set to a safe neutral value instead of the potentially behaviour-
altering schema default.  Parameters not in ``NEUTRAL_BOT_PARAMS`` are
kept as passivbot's default but marked as ``"pb_default"`` so the UI
can highlight them for the user.

The result dict contains  ``_pbgui_param_status``  when at least one
parameter was touched:
    {
        "long":  {"risk_we_excess_allowance_pct": "neutralized", ...},
        "short": {"new_unknown_param": "pb_default", ...}
    }
Strip this key with  ``strip_pbgui_param_status(cfg)``  before handing
the config to passivbot for execution.
"""

import json
import os
import sys
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from pbgui_purefunc import pb7dir

# ── Ensure pb7/src is importable ──────────────────────────────

_pb7_src = str(Path(pb7dir()) / "src")
if _pb7_src not in sys.path:
    sys.path.insert(0, _pb7_src)

from config.load import load_prepared_config  # noqa: E402
from config_utils import strip_config_metadata  # noqa: E402

# Keys that PBGui stores inside the config but passivbot strips during
# normalization.  We extract them before the pipeline and re-attach after.
_PBGUI_EXTRA_KEYS = ("pbgui",)

# PB7 still migrates legacy bot fields, but these optimize.bounds keys no longer
# map to canonical bot.long/short scalar parameters and must be dropped.
_LEGACY_UNSUPPORTED_OPTIMIZE_BOUNDS = frozenset(
    {
        "long_close_grid_markup_range",
        "short_close_grid_markup_range",
        "long_filter_relative_volume_clip_pct",
        "short_filter_relative_volume_clip_pct",
        "long_filter_rolling_window",
        "short_filter_rolling_window",
    }
)

# ── Neutral values for known passivbot bot-params ─────────────────────────────
#
# These are values that *disable* the feature in the Rust engine.  Derived from
# the Rust source (passivbot-rust/src/backtest.rs, entries.rs, closes.rs):
#
#   • Weights activate the feature when != 0.0  →  neutral = 0
#   • entry_volatility_ema_span_hours: span > 0.0 activates EMA  →  neutral = 0
#   • risk_we_excess_allowance_pct: allowance_multiplier = 1.0 + pct  →  neutral = 0
#   • risk_wel_enforcer_threshold / risk_twel_enforcer_threshold:
#       value < 1.0 triggers closes earlier  →  neutral = 1.0 (never triggers)
#   • forager_score_weights: weights applied to coin scoring  →  all 0 = off
#   • forager_volume/volatility spans: only used when corresponding weight > 0
#       → neutral = 0 (consistent with weight being 0)
#   • hsl_enabled: feature flag  →  neutral = False
#
# When a parameter is added by the pipeline and it IS in this dict → "neutralized"
# When a parameter is added by the pipeline and it is NOT in this dict → "pb_default"
#   (kept as passivbot chose, but flagged for the user to review)
#
NEUTRAL_BOT_PARAMS: dict[str, Any] = {
    # ── Entry grid spacing ────────────────────────────────────────────────────
    "entry_grid_spacing_volatility_weight":         0,
    "entry_grid_spacing_we_weight":                 0,
    # ── Entry trailing ────────────────────────────────────────────────────────
    "entry_trailing_retracement_volatility_weight": 0,
    "entry_trailing_threshold_volatility_weight":   0,
    "entry_trailing_retracement_we_weight":         0,
    "entry_trailing_threshold_we_weight":           0,
    # ── Volatility EMA ────────────────────────────────────────────────────────
    "entry_volatility_ema_span_hours":              0,
    # ── Risk ─────────────────────────────────────────────────────────────────
    "risk_we_excess_allowance_pct":                 0,
    "risk_wel_enforcer_threshold":                  1.0,
    "risk_twel_enforcer_threshold":                 1.0,
    # ── Forager scoring ───────────────────────────────────────────────────────
    #
    # forager_score_weights must be a *normalized* dict (total == 1.0) so that
    # pb7's own forager_score_weights_are_normalized() check passes on reload.
    # All-zeros ({vol:0, vol2:0, ema:0}) is NOT valid: normalize_forager_score_weights
    # falls back to {volume:1.0, ...}, which then requires forager_volume_ema_span > 0.
    # Neutral choice: ema_readiness=1.0 keeps scoring active but does not require
    # either forager_volume_ema_span or forager_volatility_ema_span to be > 0.
    "forager_score_weights": {"ema_readiness": 1.0, "volume": 0.0, "volatility": 0.0},
    "forager_volatility_ema_span":                  0,
    "forager_volume_drop_pct":                      0,
    "forager_volume_ema_span":                      0,
    # ── HSL ──────────────────────────────────────────────────────────────────
    # hsl_enabled=False is the master switch (Rust: if bp.hsl_enabled).
    # All other hsl_* params are irrelevant when hsl_enabled=False,
    # so we use schema defaults for them — no behaviour change possible.
    "hsl_enabled":                                  False,
    "hsl_cooldown_minutes_after_red":               0,
    "hsl_ema_span_minutes":                         60,
    "hsl_no_restart_drawdown_threshold":            1,
    "hsl_orange_tier_mode":                         "tp_only_with_active_entry_cancellation",
    "hsl_panic_close_order_type":                   "limit",
    "hsl_red_threshold":                            0.2,
    "hsl_tier_ratios":                              {"orange": 0.75, "yellow": 0.5},
}

_BOT_SIDES = ("long", "short")


def _collect_added_bot_params(transform_log: list[dict]) -> dict[str, set[str]]:
    """Parse passivbot's _transform_log and return per-side sets of param names
    that were *added* (not renamed/updated) by the pipeline."""
    added: dict[str, set[str]] = {"long": set(), "short": set()}
    for event in transform_log:
        if event.get("action") != "add":
            continue
        path = event.get("path", "")
        # Expected format: "bot.long.<param>" or "bot.long.forager_score_weights"
        parts = path.split(".")
        if len(parts) >= 3 and parts[0] == "bot" and parts[1] in _BOT_SIDES:
            added[parts[1]].add(parts[2])
    return added


def _apply_neutralization(
    cfg: dict,
    added: dict[str, set[str]],
) -> dict[str, dict[str, str]]:
    """Neutralize known added params and collect status for UI display.

    Returns ``_pbgui_param_status`` dict (may be empty if nothing was added).
    """
    status: dict[str, dict[str, str]] = {}
    for side in _BOT_SIDES:
        side_added = added.get(side, set())
        if not side_added:
            continue
        bot_side = cfg.get("bot", {}).get(side, {})
        side_status: dict[str, str] = {}
        for param in side_added:
            if param in NEUTRAL_BOT_PARAMS:
                neutral = NEUTRAL_BOT_PARAMS[param]
                if isinstance(neutral, dict) and isinstance(bot_side.get(param), dict):
                    # Nested dict (e.g. forager_score_weights): merge neutral values
                    for sub_key, sub_val in neutral.items():
                        bot_side[param][sub_key] = sub_val
                else:
                    bot_side[param] = neutral
                side_status[param] = "neutralized"
            else:
                # Unknown new param: keep passivbot default, flag for user review
                side_status[param] = "pb_default"
        if side_status:
            status[side] = side_status
    return status


def strip_pbgui_param_status(cfg: dict) -> dict:
    """Remove ``_pbgui_param_status`` from a config dict in-place.

    Call this before passing any config to passivbot for execution
    (backtest queue, live bot start).  Returns the same dict.
    """
    cfg.pop("_pbgui_param_status", None)
    return cfg


def _finalize_prepared_pb7_config(
    prepared: dict,
    *,
    extras: dict[str, Any],
    neutralize_added: bool,
) -> dict:
    """Finalize a prepared passivbot config for PBGui use.

    Applies optional neutralization/status collection, strips passivbot
    metadata, and re-attaches PBGui-only sections such as ``pbgui``.
    """
    if neutralize_added:
        transform_log = []
        for step in prepared.get("_transform_log", []):
            transform_log.extend(step.get("details", {}).get("changes", []))
        added = _collect_added_bot_params(transform_log)
        cfg = strip_config_metadata(prepared)
        status = _apply_neutralization(cfg, added)
        if status:
            cfg["_pbgui_param_status"] = status
    else:
        cfg = strip_config_metadata(prepared)

    for key, value in extras.items():
        cfg[key] = value

    return cfg


def _strip_legacy_unsupported_optimize_bounds(config: dict) -> list[str]:
    """Remove obsolete optimize.bounds keys that PB7 no longer accepts."""
    optimize = config.get("optimize") if isinstance(config, dict) else None
    if not isinstance(optimize, dict):
        return []

    bounds = optimize.get("bounds")
    if not isinstance(bounds, dict):
        return []

    removed: list[str] = []
    for key in tuple(bounds):
        if key in _LEGACY_UNSUPPORTED_OPTIMIZE_BOUNDS:
            bounds.pop(key, None)
            removed.append(key)
    return removed


def _restore_raw_backtest_end_date(config: dict, raw_config: dict | None) -> dict:
    """Preserve semantic backtest end_date tokens PB7 materializes during load."""
    if not isinstance(config, dict) or not isinstance(raw_config, dict):
        return config

    raw_backtest = raw_config.get("backtest")
    if not isinstance(raw_backtest, dict):
        return config

    raw_end_date = raw_backtest.get("end_date")
    if isinstance(raw_end_date, str) and raw_end_date.strip().lower() == "now":
        config.setdefault("backtest", {})["end_date"] = "now"
    return config


def _load_prepared_config_from_dict(
    config: dict,
    *,
    verbose: bool = False,
    base_config_path: str = "",
) -> dict:
    """Run PB7's file-based config pipeline against an in-memory config dict."""
    tmp_dir = None
    if base_config_path:
        try:
            tmp_dir = str(Path(base_config_path).resolve().parent)
        except Exception:
            tmp_dir = None

    with tempfile.NamedTemporaryFile(
        suffix=".json",
        delete=False,
        mode="w",
        encoding="utf-8",
        dir=tmp_dir,
    ) as tmp:
        json.dump(config, tmp, indent=4)
        tmp.write("\n")
        tmp_path = tmp.name

    try:
        prepared = load_prepared_config(tmp_path, verbose=verbose)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    if base_config_path and isinstance(prepared.get("live"), dict):
        prepared["live"]["base_config_path"] = str(base_config_path)
    return prepared


def prepare_pb7_config_dict(
    config: dict,
    *,
    verbose: bool = False,
    neutralize_added: bool = False,
    base_config_path: str = "",
) -> dict:
    """Normalize an already-loaded config dict through passivbot's pipeline.

    This is the in-memory counterpart to ``load_pb7_config`` and is intended
    for draft/import flows where PBGui already has a config dict but still
    needs passivbot migration/normalization and optional neutralization of
    pipeline-added bot parameters.

    Implementation detail: we intentionally round-trip through a temporary
    JSON file and reuse ``load_pb7_config()`` instead of calling pb7's lower
    level dict preparation directly. This preserves the exact numeric typing
    and migration behaviour of the normal file-based editor load path.
    """
    if not isinstance(config, dict):
        raise TypeError("config must be a dict")

    source = strip_config_metadata(deepcopy(config))
    source.pop("_pbgui_param_status", None)

    extras = {}
    for key in _PBGUI_EXTRA_KEYS:
        if key in source:
            extras[key] = deepcopy(source[key])

    _strip_legacy_unsupported_optimize_bounds(source)

    prepared = _load_prepared_config_from_dict(
        source,
        verbose=verbose,
        base_config_path=base_config_path,
    )
    finalized = _finalize_prepared_pb7_config(
        prepared,
        extras=extras,
        neutralize_added=neutralize_added,
    )
    return _restore_raw_backtest_end_date(finalized, source)


def load_pb7_config(
    path: str | Path,
    *,
    verbose: bool = False,
    neutralize_added: bool = False,
) -> dict:
    """Load a config through passivbot's full pipeline.

    Returns a clean dict with all passivbot sections normalized/migrated
    and the ``pbgui`` section preserved from the raw file.

    When ``neutralize_added=True``, bot parameters that were silently
    injected by the pipeline are set to their neutral (feature-off) values
    and a ``_pbgui_param_status`` key is attached for UI display.
    """
    path = str(path)

    # 1) Quick raw read to grab pbgui (and any future PBGui-only keys)
    extras = {}
    raw = None
    sanitized_raw = None
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        for key in _PBGUI_EXTRA_KEYS:
            if key in raw:
                extras[key] = deepcopy(raw[key])
        if isinstance(raw, dict):
            sanitized_raw = deepcopy(raw)
            removed_bounds = _strip_legacy_unsupported_optimize_bounds(sanitized_raw)
            if not removed_bounds:
                sanitized_raw = None
    except (OSError, json.JSONDecodeError):
        pass

    # 2) Full passivbot pipeline: migrate, hydrate, validate
    #    Keep _transform_log alive until we have parsed it (if needed).
    if sanitized_raw is not None:
        prepared = _load_prepared_config_from_dict(
            sanitized_raw,
            verbose=verbose,
            base_config_path=path,
        )
    else:
        prepared = load_prepared_config(path, verbose=verbose)
    finalized = _finalize_prepared_pb7_config(
        prepared,
        extras=extras,
        neutralize_added=neutralize_added,
    )
    return _restore_raw_backtest_end_date(finalized, raw)


def save_pb7_config(cfg: dict, path: str | Path) -> None:
    """Save a config dict to disk (atomic write).

    Writes the full dict including ``pbgui`` section.  No ``clean_config``
    is applied — passivbot will normalize on the next load.

    ``_pbgui_param_status`` is always stripped before writing so it never
    leaks into saved config files.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    to_write = deepcopy({k: v for k, v in cfg.items() if k != "_pbgui_param_status"})
    _strip_legacy_unsupported_optimize_bounds(to_write)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(to_write, f, indent=4)
            f.write("\n")
        os.replace(str(tmp), str(path))
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise
