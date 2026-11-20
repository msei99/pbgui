"""Adapt native PB9 universe policy to the shared read-only OHLCV planner."""
from __future__ import annotations

import asyncio
import copy
import importlib.util
import os
import sys
from contextlib import nullcontext
from pathlib import Path

SERVICE = "BacktestV9"
PROBE_ERROR_MESSAGES = {
    "ohlcv_metadata_missing": "PB9 cached market/listing metadata is incomplete. Prepare the native PB9 cache online before using offline mode.",
    "ohlcv_coins_required": "Select explicit approved coins on an enabled side with positive exposure and positions before using 1st/All.",
    "ohlcv_pair_limit": "Reduce the selection to at most 200 exchange/coin pairs for 1st/All.",
    "ohlcv_dataset_override": "1st/All is unavailable with hlcvs_data_dir. Use the prepared dataset's dates.",
}


class ProbeError(ValueError):
    """Expose only a fixed public diagnostic, never native exception contents."""

    def __init__(self, code):
        """Keep user-visible diagnostics independent of native source messages."""
        self.error_code = code
        super().__init__(PROBE_ERROR_MESSAGES[code])


async def check_source_window(manager, coin, start_ts, end_ts, *, offline_error,
                              repo=None, offline_policy=None):
    """Ask the native loader to admit the first source day, without copying/importing it."""
    previous = (manager.start_ts, manager.end_ts)
    previous_cwd = Path.cwd()
    first_day_end = min(end_ts, (start_ts // 86_400_000 + 1) * 86_400_000 - 60_000)
    try:
        # Native date-based setters truncate the end to midnight. Preserve the
        # exact acquisition window and consult the actual execution catalog.
        if repo is not None:
            os.chdir(repo)
        manager.update_timestamp_range(start_ts, first_day_end)
        with offline_policy({'backtest': {'offline': True}}) if offline_policy else nullcontext():
            frame = await manager.get_ohlcvs(coin, source_dir_only=True)
        if not frame.empty:
            return {}
    except offline_error:
        pass
    finally:
        try:
            manager.update_timestamp_range(*previous)
        finally:
            os.chdir(previous_cwd)
    return {'status':'missing_local', 'status_label':'Native source rejected',
            'note':'PB9 rejects the beginning of this source range. File presence alone does not establish native source eligibility; see the PB9 Guide.'}


def _native_modules(repo: Path) -> dict:
    """Load the v9 API, whose date helpers differ from the legacy adapter."""
    sys.path.insert(0, str(repo / "src"))
    from config.access import require_config_value, require_live_value
    from config.age_policy import minimum_coin_age_days
    from config.universe import get_side_universe_sources
    from backtest_universe import backtest_side_enabled
    from config.load import prepare_config
    from hlcv_preparation import HLCVManager
    from ohlcv_catalog import OhlcvCatalog as NativeOhlcvCatalog
    from ohlcv_legacy_import import inspect_legacy_range
    from ohlcv_planner import plan_local_symbol_range
    from utils import (date_to_ts, ts_to_date, format_approved_ignored_coins,
                       format_end_date, to_ccxt_exchange_id, to_standard_exchange_name)
    from warmup_utils import compute_backtest_warmup_minutes
    from simulation_data import OfflineDataError, simulation_data_policy
    from functools import partial
    source_window_check = partial(check_source_window, offline_error=OfflineDataError,
                                  repo=repo, offline_policy=simulation_data_policy)
    age_gate_limits_acquisition = False
    def OhlcvCatalog(path):
        """Inspect the actual execution catalog without creating or updating it."""
        return NativeOhlcvCatalog(path, read_only=True)
    modules = locals()
    modules['source_window_check'] = source_window_check
    return modules


def _shared_planner():
    """Load only PBGui's standard-library planner/report shell by its exact file."""
    path = Path(__file__).parent / "api" / "pb8_ohlcv_runtime_helper.py"
    spec = importlib.util.spec_from_file_location("_pbgui_shared_ohlcv_probe", path)
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    return shared


def build(config: dict, repo: Path, *, work: Path, metadata_roots=(), start_dates=False) -> dict:
    """Use only this PB9 checkout's planner, universe, warmup and data policy."""
    if type(config.get("backtest", {}).get("offline", False)) is not bool:
        raise ValueError("backtest.offline must be a boolean")
    if config.get("backtest", {}).get("hlcvs_data_dir"):
        if start_dates:
            raise ProbeError("ohlcv_dataset_override")
        return {"runtime":"pb9", "summary":{"overall_status":"unknown", "headline":"Prepared dataset selected",
                "detail":"Raw OHLCV readiness does not apply. Native PB9 validates the selected dataset at backtest start.",
                "counts":{}, "preload_supported":False, "preload_label":"Prepared dataset"},
                "request":{"source_dir":config['backtest']['hlcvs_data_dir']}, "universe":{}, "notes":[], "exchanges":[]}
    shared = _shared_planner()
    modules = _native_modules(repo)
    # The source path remains first; only trusted PBGui metadata helpers follow it.
    original_path = list(sys.path)
    try:
        sys.path.insert(1, str(Path(__file__).parent))
        from pb9_backtest_runtime import prepare_market_metadata, require_cached_market_metadata
        import secure_files  # preload its private-directory helper for the call below
    finally:
        sys.path[:] = original_path
    get_side_universe_sources = modules["get_side_universe_sources"]

    age_days = modules["minimum_coin_age_days"]
    def enabled_sides(prepared):
        """Use native activation, exposure and position-count policy."""
        return [side for side in ("long", "short") if modules["backtest_side_enabled"](prepared, side)]

    def approved_sources(prepared):
        """Retain exact identifiers and the native null/inherited side semantics."""
        values = get_side_universe_sources(prepared, "approved_coins", {})
        return {side: values.get(side, []) for side in enabled_sides(prepared)}

    modules["approved_sources"] = approved_sources
    modules["minimum_coin_age_days"] = lambda cfg: max((age_days(cfg, side) for side in enabled_sides(cfg)), default=0)
    modules["coin_minimum_age_ms"] = lambda cfg, sides: int(min((age_days(cfg, side) for side in sides), default=0) * 86_400_000)
    # Metadata lookup owns a disposable working directory. Candle coverage
    # comes from the same persistent native cache used by Backtest and Optimize.
    payload = {"pb8_dir": str(work), "config": config, "include_start_date_options": bool(start_dates)}
    policy = modules.get("simulation_data_policy")
    if config.get("backtest", {}).get("offline") and policy is None:
        raise ValueError("PB9 offline policy is unavailable")
    metadata_config = copy.deepcopy(config)
    metadata_config.setdefault("backtest", {})["offline"] = True
    if config.get('backtest', {}).get('offline') is True:
        try:
            require_cached_market_metadata(repo, config)
        except ValueError:
            raise ProbeError('ohlcv_metadata_missing') from None
    cached_metadata = True
    try:
        prepare_market_metadata(work, metadata_config, Path(__file__).parent / "data" / "coindata", repo, metadata_roots)
    except ValueError:
        cached_metadata = False
        if config.get("backtest", {}).get("offline") is True:
            raise ProbeError("ohlcv_metadata_missing") from None
    previous = Path.cwd()
    def run(policy_config):
        """Preserve native failures internally; expose only known selection errors."""
        try:
            with policy(policy_config) if policy else nullcontext():
                return asyncio.run(shared._build(payload, modules=modules, cache_root=repo / 'caches'))
        except ValueError as exc:
            message = str(exc)
            if message in {"OHLCV start-date lookup requires explicitly selected approved coins",
                           "Select at least one approved coin before looking up an OHLCV start date"}:
                raise ProbeError("ohlcv_coins_required") from None
            if message.startswith("OHLCV start-date lookup expands to "):
                raise ProbeError("ohlcv_pair_limit") from None
            raise
    try:
        os.chdir(work)
        result = None
        # Published metadata links are read-only even when the user's backtest
        # permits acquisition. An online fallback first removes those links so
        # native cache writes can only affect the private temporary workspace.
        if cached_metadata:
            try:
                result = run(metadata_config)
            except modules["OfflineDataError"]:
                if config.get("backtest", {}).get("offline") is True:
                    raise ProbeError("ohlcv_metadata_missing") from None
        if result is None:
            for path in work.rglob('*'):
                if path.is_symlink():
                    path.unlink()
            result = run(config)
    finally:
        os.chdir(previous)
    # Shared reporting labels refer to the selected runtime, not a PB8 execution.
    def labels(value, key=""):
        """Translate only report prose, never a source path or market identifier."""
        if isinstance(value, dict):
            return {name: labels(item, name) for name, item in value.items()}
        if isinstance(value, list):
            return [labels(item, key) for item in value]
        if key == 'status_label' and value == 'Local source import':
            return 'Configured source available'
        prose = {"note", "notes", "detail", "headline", "status_label", "preload_detail", "preload_label"}
        return value.replace("PB8", "PB9") if isinstance(value, str) and key in prose else value
    result = labels(result)
    result["runtime"] = "pb9"
    result["summary"].update(preload_supported=False, preload_label="Use Market Data",
                              preload_detail="Use PBGui Market Data to acquire missing candles for the selected source.")
    result.setdefault("notes", []).append("Readiness does not copy or import candles. Side-specific minimum ages use the first eligible enabled side per coin.")
    return result
