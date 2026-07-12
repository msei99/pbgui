"""
Tests for pb7_config load/save roundtrip on real run_v7 instance configs.

Verifies that switching api/v7_instances.py from ConfigV7 to pb7_config
is safe and produces stable, consistent results.

Test scenarios:
  1. Roundtrip stability   — load → save to temp → load again → identical output
  2. Neutralization        — neutralize_added=True sets new params to safe defaults
  3. pbgui section         — pbgui metadata is always preserved across load/save
  4. coin_overrides        — coin_overrides are preserved as-is (no mangling)
  5. Save-logic compat     — version++, exchange, base_dir logic works on pb7_config dict
  6. No ConfigV7 needed    — the full load+save cycle never imports ConfigV7
"""

import json
import os
import sys
import tempfile
from copy import deepcopy
from pathlib import Path

import pytest
from api.pb7_bridge import get_template_config

# ── Project root on path ──────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── pb7_config import ─────────────────────────────────────────────────────────
from pb7_config import (
    load_pb7_config,
    prepare_pb7_config_dict,
    save_pb7_config,
    strip_pbgui_param_status,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_run_v7_configs(max_count: int = 30) -> list[Path]:
    """Return up to max_count real run_v7 config.json paths."""
    pattern = ROOT / "data" / "run_v7" / "*" / "config.json"
    import glob
    found = sorted(glob.glob(str(pattern)))
    return [Path(p) for p in found[:max_count]]


def _configs_with_coin_overrides() -> list[Path]:
    """Return configs that have coin_overrides set."""
    result = []
    for p in _find_run_v7_configs(max_count=87):
        with open(p) as f:
            raw = json.load(f)
        if raw.get("coin_overrides"):
            result.append(p)
    return result


def _strip_dynamic_fields(cfg: dict) -> dict:
    """Remove fields that legitimately differ between temp-file loads.

    ``live.base_config_path`` is injected by the pb7 pipeline with the
    absolute path of the file being loaded.  It changes whenever we load
    from a different temp file, so we strip it for comparison purposes.
    This field is irrelevant for config correctness (passivbot ignores it
    on startup; PBGui does not use it for editing logic).
    """
    result = deepcopy(cfg)
    result.get("live", {}).pop("base_config_path", None)
    return result


LEGACY_UNSUPPORTED_OPTIMIZE_BOUNDS = {
    "long_close_grid_markup_range",
    "short_close_grid_markup_range",
    "long_filter_relative_volume_clip_pct",
    "short_filter_relative_volume_clip_pct",
    "long_filter_rolling_window",
    "short_filter_rolling_window",
}


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def run_v7_configs() -> list[Path]:
    paths = _find_run_v7_configs()
    if not paths:
        pytest.skip("No run_v7 configs found — skipping (needs live data)")
    return paths


@pytest.fixture(scope="module")
def override_configs() -> list[Path]:
    paths = _configs_with_coin_overrides()
    if not paths:
        pytest.skip("No run_v7 configs with coin_overrides found")
    return paths


# ── Test class ────────────────────────────────────────────────────────────────

@pytest.mark.local_runtime
class TestPb7ConfigRunV7Roundtrip:
    """Roundtrip and compatibility tests for pb7_config on run_v7 instances."""

    # ── 1. Roundtrip stability ────────────────────────────────────────────────

    def test_roundtrip_stability(self, run_v7_configs):
        """load → save → load → save → load: the last two loads must be identical.

        We use a two-cycle approach to avoid two known first-pass-only differences:
          - base_config_path: pb7 injects the actual file path being loaded; on
            the first temp-file load it differs from the real path.  From the
            second load onwards it is stable (same tmp path).
          - Float/int normalization: pb7 may coerce 1.0→1 on migration.  Once
            already saved in the new format, subsequent loads are stable.

        This exactly mirrors the production scenario: user loads existing config
        (first pass may migrate/normalize), edits it, saves it.  All subsequent
        load/save cycles must be idempotent.
        """
        unstable = []
        for path in run_v7_configs:
            # First cycle: original → tmp1
            cfg_original = load_pb7_config(path, neutralize_added=True)
            strip_pbgui_param_status(cfg_original)

            with tempfile.NamedTemporaryFile(
                suffix=".json", delete=False, mode="w", encoding="utf-8"
            ) as t1:
                tmp1 = t1.name
            with tempfile.NamedTemporaryFile(
                suffix=".json", delete=False, mode="w", encoding="utf-8"
            ) as t2:
                tmp2 = t2.name

            try:
                save_pb7_config(cfg_original, tmp1)

                # Second cycle: tmp1 → cfg_a; save → tmp2; load → cfg_b
                cfg_a = load_pb7_config(tmp1, neutralize_added=True)
                strip_pbgui_param_status(cfg_a)
                save_pb7_config(cfg_a, tmp2)

                cfg_b = load_pb7_config(tmp2, neutralize_added=True)
                strip_pbgui_param_status(cfg_b)

                j_a = json.dumps(_strip_dynamic_fields(cfg_a), sort_keys=True, indent=2)
                j_b = json.dumps(_strip_dynamic_fields(cfg_b), sort_keys=True, indent=2)
                if j_a != j_b:
                    unstable.append(path.name)
            finally:
                for p in (tmp1, tmp2):
                    if os.path.exists(p):
                        os.unlink(p)

        assert not unstable, (
            f"Roundtrip unstable (cycles 2→3) for {len(unstable)} config(s): {unstable}\n"
            "load → save → load → save → load produced different output in cycles 2 vs 3."
        )

    # ── 2. Neutralization ─────────────────────────────────────────────────────

    def test_neutralize_added_produces_safe_defaults(self, run_v7_configs):
        """With neutralize_added=True, injected params must be neutral (not random pb7 defaults).

        For known NEUTRAL_BOT_PARAMS: value must equal the neutral value.
        All neutralized params must be flagged in _pbgui_param_status['long'|'short'].
        """
        from pb7_config import NEUTRAL_BOT_PARAMS

        violations = []
        for path in run_v7_configs:
            cfg = load_pb7_config(path, neutralize_added=True)
            status = cfg.get("_pbgui_param_status", {})

            for side in ("long", "short"):
                side_status = status.get(side, {})
                bot_side = cfg.get("bot", {}).get(side, {})

                for param, flag in side_status.items():
                    if flag == "neutralized":
                        expected = NEUTRAL_BOT_PARAMS.get(param)
                        actual = bot_side.get(param)
                        if expected is None:
                            continue
                        # For nested dicts (e.g. forager_score_weights) check sub-keys
                        if isinstance(expected, dict) and isinstance(actual, dict):
                            for sub_k, sub_v in expected.items():
                                if actual.get(sub_k) != sub_v:
                                    violations.append(
                                        f"{path.name} bot.{side}.{param}.{sub_k}: "
                                        f"expected {sub_v}, got {actual.get(sub_k)}"
                                    )
                        elif actual != expected:
                            violations.append(
                                f"{path.name} bot.{side}.{param}: "
                                f"expected {expected}, got {actual}"
                            )

        assert not violations, (
            f"Neutralization produced wrong values:\n" + "\n".join(violations[:20])
        )

    def test_neutralize_status_keys_present_when_params_injected(self, run_v7_configs):
        """_pbgui_param_status must be present whenever params were injected."""
        missing_status = []
        for path in run_v7_configs:
            cfg_neutral = load_pb7_config(path, neutralize_added=True)
            cfg_plain   = load_pb7_config(path, neutralize_added=False)
            strip_pbgui_param_status(cfg_plain)

            # Find params in cfg_plain not in original file
            with open(path) as f:
                raw = json.load(f)

            for side in ("long", "short"):
                plain_side = cfg_plain.get("bot", {}).get(side, {})
                raw_side   = raw.get("bot", {}).get(side, {})
                injected = set(plain_side) - set(raw_side)

                if injected and "_pbgui_param_status" not in cfg_neutral:
                    missing_status.append(
                        f"{path.name}/bot.{side}: injected {injected} but no status"
                    )

        assert not missing_status, (
            "Status missing when params were injected:\n" + "\n".join(missing_status[:10])
        )

    # ── 3. pbgui section preservation ────────────────────────────────────────

    def test_pbgui_section_preserved_after_roundtrip(self, run_v7_configs):
        """pbgui metadata must survive load → save → load unchanged."""
        failures = []
        for path in run_v7_configs:
            with open(path) as f:
                raw_pbgui = json.load(f).get("pbgui", {})

            cfg = load_pb7_config(path, neutralize_added=True)
            strip_pbgui_param_status(cfg)

            with tempfile.NamedTemporaryFile(
                suffix=".json", delete=False, mode="w"
            ) as tmp:
                tmp_path = tmp.name
            try:
                save_pb7_config(cfg, tmp_path)
                cfg2 = load_pb7_config(tmp_path)
                rt_pbgui = cfg2.get("pbgui", {})

                if rt_pbgui != raw_pbgui:
                    failures.append(path.name)
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        assert not failures, (
            f"pbgui section changed after roundtrip in: {failures}"
        )

    def test_end_date_now_preserved_on_load_and_prepare(self):
        """The semantic token ``backtest.end_date = 'now'`` must survive editor loads."""
        cfg = deepcopy(get_template_config())
        cfg.setdefault("backtest", {})["end_date"] = "now"

        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, mode="w", encoding="utf-8"
        ) as tmp:
            tmp_path = tmp.name
        try:
            save_pb7_config(cfg, tmp_path)

            loaded = load_pb7_config(tmp_path)
            prepared = prepare_pb7_config_dict(cfg)

            assert loaded.get("backtest", {}).get("end_date") == "now"
            assert prepared.get("backtest", {}).get("end_date") == "now"
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ── 4. coin_overrides preservation ───────────────────────────────────────

    def test_coin_overrides_preserved(self, override_configs):
        """coin_overrides must survive load → save → load unchanged."""
        failures = []
        for path in override_configs:
            with open(path) as f:
                raw_overrides = json.load(f).get("coin_overrides", {})

            cfg = load_pb7_config(path, neutralize_added=True)
            strip_pbgui_param_status(cfg)

            with tempfile.NamedTemporaryFile(
                suffix=".json", delete=False, mode="w"
            ) as tmp:
                tmp_path = tmp.name
            try:
                save_pb7_config(cfg, tmp_path)
                cfg2 = load_pb7_config(tmp_path)
                roundtrip_overrides = cfg2.get("coin_overrides", {})

                if roundtrip_overrides != raw_overrides:
                    failures.append(
                        f"{path.name}: raw={raw_overrides!r} "
                        f"roundtrip={roundtrip_overrides!r}"
                    )
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        assert not failures, (
            f"coin_overrides changed after roundtrip:\n" + "\n".join(failures)
        )

    # ── 5. Save-logic compatibility ───────────────────────────────────────────

    def test_save_logic_version_increment(self, run_v7_configs):
        """version++ logic from v7_instances.py must work on pb7_config dict."""
        path = run_v7_configs[0]
        cfg = load_pb7_config(path, neutralize_added=True)
        strip_pbgui_param_status(cfg)

        original_version = cfg.get("pbgui", {}).get("version", 0)
        cfg.setdefault("pbgui", {})["version"] = original_version + 1

        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, mode="w"
        ) as tmp:
            tmp_path = tmp.name
        try:
            save_pb7_config(cfg, tmp_path)
            cfg2 = load_pb7_config(tmp_path)
            saved_version = cfg2.get("pbgui", {}).get("version", -1)
            assert saved_version == original_version + 1, (
                f"version not saved correctly: expected {original_version + 1}, "
                f"got {saved_version}"
            )
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_save_logic_backtest_exchange_and_basedir(self, run_v7_configs):
        """backtest.exchange / backtest.base_dir mutation must survive save/load."""
        path = run_v7_configs[0]
        cfg = load_pb7_config(path, neutralize_added=True)
        strip_pbgui_param_status(cfg)

        cfg.setdefault("backtest", {})["exchange"] = "binance"
        cfg["backtest"]["base_dir"] = "backtests/pbgui/test_user"

        with tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, mode="w"
        ) as tmp:
            tmp_path = tmp.name
        try:
            save_pb7_config(cfg, tmp_path)
            cfg2 = load_pb7_config(tmp_path)

            assert cfg2.get("backtest", {}).get("base_dir") == "backtests/pbgui/test_user", \
                "base_dir not preserved after save/load"
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    # ── 6. No ConfigV7 import needed ──────────────────────────────────────────

    def test_no_configv7_import_required(self, run_v7_configs):
        """load_pb7_config must work without importing ConfigV7 (no Streamlit dep)."""
        # If we get here, pb7_config already loaded successfully without ConfigV7.
        # Verify ConfigV7 is NOT in the module graph of pb7_config.
        import pb7_config
        import inspect
        source = inspect.getsource(pb7_config)
        assert "ConfigV7" not in source, \
            "pb7_config.py must not reference ConfigV7"
        assert "import streamlit" not in source, \
            "pb7_config.py must not import streamlit"

    # ── 7. All configs loadable without exception ─────────────────────────────

    def test_all_configs_load_without_exception(self, run_v7_configs):
        """Every run_v7 config must load through pb7_config without raising."""
        failures = []
        for path in run_v7_configs:
            try:
                cfg = load_pb7_config(path, neutralize_added=True)
                assert isinstance(cfg, dict), f"{path.name}: expected dict"
                assert "bot" in cfg, f"{path.name}: missing 'bot' section"
                assert "live" in cfg, f"{path.name}: missing 'live' section"
            except Exception as e:
                failures.append(f"{path.name}: {e}")

        assert not failures, (
            f"Load failed for {len(failures)} config(s):\n" + "\n".join(failures[:10])
        )

    def test_prepare_pb7_config_dict_matches_file_load(self, run_v7_configs):
        """Dict-based preparation must match file-based load for older run_v7 configs.

        This covers draft/import-style flows where PBGui already has a config dict
        in memory but still needs the same neutralization metadata as the normal
        file-based Run editor load path.
        """
        mismatches = []
        for path in run_v7_configs[:20]:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)

            cfg_from_file = load_pb7_config(path, neutralize_added=True)
            cfg_from_dict = prepare_pb7_config_dict(
                raw,
                neutralize_added=True,
                base_config_path=str(path),
            )

            j_file = json.dumps(_strip_dynamic_fields(cfg_from_file), sort_keys=True, indent=2)
            j_dict = json.dumps(_strip_dynamic_fields(cfg_from_dict), sort_keys=True, indent=2)
            if j_file != j_dict:
                mismatches.append(path.name)

        assert not mismatches, (
            "Dict-based pb7 prep diverged from file-based load for: "
            + ", ".join(mismatches)
        )


def test_legacy_unsupported_optimize_bounds_are_stripped_on_load_and_save(tmp_path):
    """Unsupported legacy optimize.bounds keys should be removed transparently."""
    cfg = get_template_config()
    cfg.setdefault("backtest", {})["base_dir"] = "backtests/pbgui/legacy_opt_bounds"
    cfg.setdefault("pbgui", {})["version"] = 0
    cfg.setdefault("optimize", {}).setdefault("bounds", {}).update(
        {
            "long_close_grid_markup_range": [0.0, 0.03],
            "short_close_grid_markup_range": [0.0, 0.03],
            "long_filter_relative_volume_clip_pct": [0.0, 1.0],
            "short_filter_relative_volume_clip_pct": [0.0, 1.0],
            "long_filter_rolling_window": [10.0, 360.0],
            "short_filter_rolling_window": [10.0, 360.0],
        }
    )

    src = tmp_path / "legacy_optimize.json"
    src.write_text(json.dumps(cfg, indent=4) + "\n", encoding="utf-8")

    loaded = load_pb7_config(src, neutralize_added=True)
    loaded_bounds = loaded.get("optimize", {}).get("bounds", {})

    assert loaded.get("live", {}).get("base_config_path") == str(src)
    assert loaded_bounds, "optimize.bounds should still contain canonical bounds"
    for key in LEGACY_UNSUPPORTED_OPTIMIZE_BOUNDS:
        assert key not in loaded_bounds, f"legacy bound {key} should be stripped on load"

    saved = tmp_path / "legacy_optimize_saved.json"
    save_pb7_config(loaded, saved)
    saved_raw = json.loads(saved.read_text(encoding="utf-8"))
    saved_bounds = saved_raw.get("optimize", {}).get("bounds", {})

    for key in LEGACY_UNSUPPORTED_OPTIMIZE_BOUNDS:
        assert key not in saved_bounds, f"legacy bound {key} should not be written back"


def test_prepare_pb7_config_dict_preserves_supported_fixed_runtime_override_keys():
    """Schema-supported optimize.fixed_runtime_overrides keys survive the PB7 load pipeline."""
    cfg = get_template_config()
    cfg.setdefault("optimize", {})["fixed_runtime_overrides"] = {
        "bot.long.hsl_no_restart_drawdown_threshold": 0.77,
        "bot.short.hsl_no_restart_drawdown_threshold": 0.88,
    }

    prepared = prepare_pb7_config_dict(cfg, neutralize_added=False)

    assert prepared["optimize"]["fixed_runtime_overrides"] == {
        "bot.long.hsl_no_restart_drawdown_threshold": 0.77,
        "bot.short.hsl_no_restart_drawdown_threshold": 0.88,
    }


@pytest.mark.parametrize(
    ("override_key", "override_value"),
    [
        ("bot.long.hsl_no_restart_drawdown_threshold", 0.0),
        ("bot.long.hsl_no_restart_drawdown_threshold", 0.1),
        ("bot.long.hsl_no_restart_drawdown_threshold", 0.5),
        ("bot.short.hsl_no_restart_drawdown_threshold", 0.1),
    ],
)
def test_prepare_pb7_config_dict_fills_missing_supported_fixed_runtime_override_key_from_defaults(
    override_key, override_value
):
    """A single supported fixed_runtime_overrides key survives and the other side is filled from schema defaults."""
    cfg = get_template_config()
    expected_overrides = deepcopy(cfg["optimize"]["fixed_runtime_overrides"])
    expected_overrides[override_key] = override_value
    cfg.setdefault("optimize", {})["fixed_runtime_overrides"] = {
        override_key: override_value,
    }

    prepared = prepare_pb7_config_dict(cfg, neutralize_added=False)

    assert prepared["optimize"]["fixed_runtime_overrides"] == expected_overrides


def test_prepare_pb7_config_dict_prunes_unknown_fixed_runtime_override_keys_without_rewriting():
    """Unknown fixed_runtime_overrides keys are dropped, not rewritten to supported paths."""
    cfg = get_template_config()
    expected_defaults = deepcopy(cfg["optimize"]["fixed_runtime_overrides"])
    cfg.setdefault("optimize", {})["fixed_runtime_overrides"] = {
        "bot.long.entry_trailing_threshold_volatility_weight": 123,
        "long.hsl_no_restart_drawdown_threshold": 456,
    }

    prepared = prepare_pb7_config_dict(cfg, neutralize_added=False)
    prepared_overrides = prepared["optimize"]["fixed_runtime_overrides"]

    assert prepared_overrides == expected_defaults
    assert "bot.long.entry_trailing_threshold_volatility_weight" not in prepared_overrides
    assert "long.hsl_no_restart_drawdown_threshold" not in prepared_overrides
