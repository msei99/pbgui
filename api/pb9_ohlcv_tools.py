"""Native PB9 adapters for the shared OHLCV report and inception job controller."""
from __future__ import annotations

from pathlib import Path
import os
import time
from tempfile import TemporaryDirectory
from fastapi import HTTPException

from api import pb8_ohlcv_tools as shared
from pb9_backtest_runtime import launch_environment
from pb9_config import PB9CancelSignal, PB9ConfigError, _checked_response, _config_operation, _run_helper
from pb9_ohlcv_probe import PROBE_ERROR_MESSAGES

SERVICE = "BacktestV9"


def start_date_plan(raw_config: dict, cancel=None) -> dict:
    """Bind an isolated helper to a verified PB9 runtime and approved candle root."""
    from api import backtest_v9
    if cancel is not None and not isinstance(cancel, PB9CancelSignal):
        lookup_cancel = cancel
        class LookupCancel(PB9CancelSignal):
            """Bridge the shared job's Stop/deadline into native helper cancellation."""
            def is_set(self):
                """Observe the shared lookup owner without acquiring helper locks."""
                return lookup_cancel.is_set()
        cancel = LookupCancel()
        cancel.deadline = getattr(lookup_cancel,"deadline",time.monotonic()+180)
    if cancel is None:
        cancel = PB9CancelSignal()
    config, status = backtest_v9._native(raw_config, cancel)
    paths = backtest_v9._paths()
    # Reuse the pure path policy only; no PB8 runtime or native module is loaded.
    try:
        config, _, _ = shared.resolve_pb8_ohlcv_paths(config, {"pb8dir": paths["pb9dir"]})
    except shared.PB8OhlcvError:
        raise HTTPException(422, 'OHLCV source must be inside the PBGui market-data root or the configured PB9 directory') from None
    workspace = TemporaryDirectory(prefix="pbgui-pb9-ohlcv-")
    request = {"operation": "ohlcv_start_dates", "pb9_dir": paths["pb9dir"],
               "config_schema": status["config_schema"], "package_version": status["package_version"],
               "config": config, "work_dir": workspace.name,
               "metadata_roots": [str(Path(paths[key]).expanduser().resolve()) for key in ("pb7dir", "pb8dir") if paths.get(key)]}
    status = {**status, "pb9dir":paths["pb9dir"], "pb9venv":paths["pb9venv"],
              "commit":status.get("commit"), "commit_available":status.get("commit_available",False)}
    context = {key:request[key] for key in ("work_dir","metadata_roots")}
    def run(progress):
        """Reuse PB9's selector transport, cancellation and unreaped-leader ownership."""
        timeout = min(180, max(.01, getattr(cancel,"deadline",time.monotonic()+180)-time.monotonic()))
        try:
            return _run_helper(status,timeout,operation="ohlcv_start_dates",config=config,cancel_event=cancel,
                               probe_context=context,progress_callback=progress)
        except PB9ConfigError as exc:
            if exc.error_codes == ("helper_timeout",) or (exc.error_codes == ("helper_cancelled",)
                    and time.monotonic() >= getattr(cancel,"deadline",float('inf'))):
                raise TimeoutError("PB9 OHLCV start-date lookup timed out.") from None
            raise
    def validate_response(raw):
        """Verify source module paths/schema/package before publishing a recommendation."""
        return _checked_response(raw,status,operation="ohlcv_start_dates")["report"]
    def error_detail(exc):
        """Publish only the same fixed diagnostic as the preflight API."""
        if isinstance(exc,TimeoutError):
            return "PB9 OHLCV start-date lookup timed out."
        codes=getattr(exc,"error_codes",())
        return PROBE_ERROR_MESSAGES.get(codes[0]) if codes else None
    env = launch_environment(paths["pb9venv"])
    env.update({key:os.environ[key] for key in ("TMPDIR","TMP","TEMP") if key in os.environ})
    return {"request": request, "command": [paths["pb9venv"], "-I", "-B", str(Path(__file__).parents[1] / "pb9_config_helper.py")],
            "cwd": paths["pb9dir"], "env": env, "paths": paths, "cleanup": workspace.cleanup,
            "run":run, "validate_response":validate_response, "error_detail":error_detail}


def preflight(raw_config: dict, cancel) -> dict:
    """Inspect candles under request-owned cancellation; never start a downloader."""
    try:
        lease = shared._acquire_runtime_lease()
    except shared.PB8OhlcvBusyError:
        raise HTTPException(503, 'PBGui runtime update is in progress. Retry the OHLCV check when it finishes.') from None
    plan = None
    try:
        plan = start_date_plan(raw_config, cancel)
        paths = plan["paths"]
        return _config_operation("ohlcv_preflight", paths["pb9dir"], paths["pb9venv"],
                                 {key: value for key, value in paths.items() if not key.startswith("pb9")},
                                 config=plan["request"]["config"], timeout=120, cancel_event=cancel,
                                 probe_context={key: plan["request"][key] for key in ("work_dir", "metadata_roots")})["report"]
    finally:
        if plan:
            plan["cleanup"]()
        lease.release()


def start(raw_config: dict, owner) -> dict:
    """Reuse bounded progress, process-group Stop and shutdown ownership for PB9."""
    return shared.start_pb8_ohlcv_start_date_job(raw_config, runtime="v9", owner=owner, adapter=start_date_plan)


def get(job_id: str, owner) -> dict | None:
    """Never expose another runtime's or login's lookup job."""
    return shared.get_pb8_ohlcv_start_date_job(job_id, runtime="v9", owner=owner)


def stop(job_id: str, owner) -> dict | None:
    """Stop only a lookup owned by this authenticated user and PB9."""
    return shared.stop_pb8_ohlcv_start_date_job(job_id, runtime="v9", owner=owner)


def shutdown() -> None:
    """Join API-owned PB9 lookup helpers while leaving native backtests independent."""
    shared.shutdown_pb8_ohlcv_start_date_jobs(runtime="v9")
