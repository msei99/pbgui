from __future__ import annotations

import os
import signal
import time
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from hyperliquid_aws import (
    download_hyperliquid_l2book_aws,
    get_hyperliquid_archive_day_range_aws,
)
from hyperliquid_best_1m import improve_best_hyperliquid_1m_archive_for_coin, _is_stock_perp_coin
from market_data import (
    append_exchange_download_log,
    load_aws_profile_credentials,
    load_aws_profile_region,
    normalize_market_data_coin_dir,
)
from market_data_sources import get_source_codes_for_day, get_oldest_day_with_source_code, SOURCE_CODE_L2BOOK
from pbgui_purefunc import load_ini
from task_queue import (
    clear_worker_pid,
    ensure_task_dirs,
    get_task_state_dir,
    move_job_file,
    update_job_file,
    write_worker_pid,
    enqueue_job,
)


_STOP = False


def _handle_stop(signum, frame):
    global _STOP
    _STOP = True


def _parse_day(day: str) -> datetime:
    return datetime.strptime(str(day).strip(), "%Y%m%d")


def _iter_day_chunks(start_day: str, end_day: str, chunk_days: int) -> list[tuple[str, str]]:
    d0 = _parse_day(start_day).date()
    d1 = _parse_day(end_day).date()
    if d1 < d0:
        raise ValueError("end_day must be >= start_day")
    cur = d0
    out: list[tuple[str, str]] = []
    while cur <= d1:
        ce = min(d1, cur + timedelta(days=int(chunk_days) - 1))
        out.append((cur.strftime("%Y%m%d"), ce.strftime("%Y%m%d")))
        cur = ce + timedelta(days=1)
    return out


def _iter_days(start_day: str, end_day: str) -> list[str]:
    d0 = _parse_day(start_day).date()
    d1 = _parse_day(end_day).date()
    if d1 < d0:
        raise ValueError("end_day must be >= start_day")
    out: list[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y%m%d"))
        cur = cur + timedelta(days=1)
    return out


def _requeue_stale_running_jobs(max_age_s: int = 3600) -> None:
    running_dir = get_task_state_dir("running")
    pending_dir = get_task_state_dir("pending")
    now = int(time.time())
    if not running_dir.is_dir():
        return
    for p in sorted(running_dir.glob("*.json")):
        try:
            st = p.stat()
            age = now - int(st.st_mtime)
            if age > int(max_age_s):
                os.replace(p, pending_dir / p.name)
        except Exception:
            continue


@dataclass
class JobContext:
    path: Path
    obj: dict[str, Any]


def _load_job(path: Path) -> dict[str, Any] | None:
    try:
        import json

        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _is_cancel_requested(job_path: Path) -> bool:
    obj = _load_job(job_path)
    if not obj:
        return False
    return bool(obj.get("cancel_requested"))


def _job_log(msg: str, level: str = "INFO") -> None:
    append_exchange_download_log("hyperliquid", f"[worker] {msg}", level=level)


def _recent_corrupt_l2book_files(*, coin: str, since_ts: float) -> list[str]:
    coin_u = str(coin or "").strip().upper()
    if not coin_u:
        return []
    base = Path(__file__).resolve().parent / "data" / "ohlcv" / "hyperliquid" / "l2Book" / coin_u
    if not base.exists():
        return []
    out: list[str] = []
    try:
        for p in base.glob("*.corrupt.*"):
            try:
                if p.stat().st_mtime >= float(since_ts):
                    out.append(p.name)
            except Exception:
                continue
    except Exception:
        return []
    return sorted(out)


def _hours_missing_in_1m_src(*, coin: str, day: str) -> list[int]:
    """Return hour numbers (0..23) where 1m_src has no minute coverage."""

    coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
    if not coin_dir:
        return list(range(24))

    codes = get_source_codes_for_day(exchange="hyperliquid", coin=coin_dir, day=day)
    if not isinstance(codes, list) or len(codes) < 1440:
        return list(range(24))

    out: list[int] = []
    for hour in range(24):
        h0 = hour * 60
        h1 = h0 + 60
        hour_codes = codes[h0:h1]
        if not any(int(c or 0) > 0 for c in hour_codes):
            out.append(hour)
    return out


def _run_job(job_path: Path) -> None:
    obj = _load_job(job_path)
    if not obj:
        move_job_file(job_path, "failed")
        return

    if bool(obj.get("cancel_requested")):
        update_job_file(job_path, mutate=lambda o: o.update({"status": "failed", "error": "cancelled"}))
        move_job_file(job_path, "failed")
        return

    jtype = str(obj.get("type") or "").strip()
    payload = obj.get("payload")
    payload = payload if isinstance(payload, dict) else {}

    def mark_error(err: str) -> None:
        update_job_file(job_path, mutate=lambda o: o.update({"status": "failed", "error": str(err)}))

    update_job_file(job_path, mutate=lambda o: o.update({"status": "running", "error": ""}))

    try:
        if jtype == "hl_aws_l2book_auto":
            _run_hl_aws_l2book_auto(job_path, payload)
        elif jtype == "hl_best_1m":
            _run_hl_best_1m(job_path, payload)
        else:
            raise RuntimeError(f"Unknown job type: {jtype}")

        update_job_file(job_path, mutate=lambda o: o.update({"status": "done"}))
        move_job_file(job_path, "done")
    except Exception as e:
        _job_log(f"job error {job_path.name}: {e}")
        mark_error(str(e))
        move_job_file(job_path, "failed")


def _run_hl_aws_l2book_auto(job_path: Path, payload: dict[str, Any]) -> None:
    profile = str(payload.get("profile") or "pbgui-hyperliquid").strip() or "pbgui-hyperliquid"
    region = str(payload.get("region") or "").strip()
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    # Keep original coin names (normalization happens in download function for S3 keys only)
    coins = [str(c).strip() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    only_missing_1m_src_hours = bool(payload.get("only_missing_1m_src_hours", True))

    chunk_days = int(payload.get("chunk_days") or 7)
    if chunk_days < 1:
        chunk_days = 7

    creds = load_aws_profile_credentials(profile)
    ak = str(creds.get("aws_access_key_id") or "").strip()
    sk = str(creds.get("aws_secret_access_key") or "").strip()
    if not ak or not sk:
        raise RuntimeError(f"Missing AWS credentials in profile '{profile}'")

    if not region:
        region = load_aws_profile_region(profile) or ""
    if not region:
        region = "us-east-2"

    def _get_ini_int(section: str, key: str, default: int) -> int:
        raw = load_ini(section, key)
        try:
            val = int(str(raw).strip())
            if val >= 1:
                return val
        except Exception:
            pass
        return int(default)

    def _get_ini_float(section: str, key: str, default: float) -> float:
        raw = load_ini(section, key)
        try:
            val = float(str(raw).strip())
            if val > 0:
                return val
        except Exception:
            pass
        return float(default)

    l2book_timeout_s = _get_ini_float("market_data", "hl_l2book_scan_timeout_s", 5.0)
    l2book_workers = _get_ini_int("market_data", "hl_l2book_scan_workers", 8)
    ini_path = Path("pbgui.ini")
    ini_mtime: float | None = None

    def _reload_l2book_settings() -> bool:
        nonlocal ini_mtime, l2book_timeout_s, l2book_workers
        try:
            if not ini_path.exists():
                return False
            mtime = ini_path.stat().st_mtime
            if ini_mtime is not None and mtime == ini_mtime:
                return False
            ini_mtime = mtime
        except Exception:
            return False

        new_timeout = _get_ini_float("market_data", "hl_l2book_scan_timeout_s", 5.0)
        new_workers = _get_ini_int("market_data", "hl_l2book_scan_workers", 8)
        changed = (new_timeout != l2book_timeout_s) or (new_workers != l2book_workers)
        l2book_timeout_s = new_timeout
        l2book_workers = new_workers
        return changed

    # Determine archive range
    start_day = str(payload.get("start_day") or "").strip()
    end_day = str(payload.get("end_day") or "").strip()
    if not start_day or not end_day:
        oldest, newest = get_hyperliquid_archive_day_range_aws(
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=region,
        )
        start_day, end_day = str(oldest), str(newest)

    _job_log(
        "hl_aws_l2book_auto start profile=%s region=%s range=%s->%s coins=%s timeout_s=%s workers=%s only_missing_1m_src_hours=%s"
        % (profile, region, start_day, end_day, len(coins), l2book_timeout_s, l2book_workers, only_missing_1m_src_hours)
    )

    days = _iter_days(start_day, end_day)
    coin_days: dict[str, list[str]] = {}
    for coin in coins:
        filtered = list(days)
        if only_missing_1m_src_hours:
            coin_dir = normalize_market_data_coin_dir("hyperliquid", coin)
            oldest_l2book_day = (
                get_oldest_day_with_source_code(
                    exchange="hyperliquid",
                    coin=coin_dir,
                    code=SOURCE_CODE_L2BOOK,
                )
                if coin_dir
                else None
            )
            if oldest_l2book_day:
                filtered = [d for d in filtered if d >= str(oldest_l2book_day)]
                if len(filtered) != len(days):
                    append_exchange_download_log(
                        "hyperliquid",
                        f"[INFO] [hl_aws_l2book_auto] {coin} skip_older_than_l2book_oldest={oldest_l2book_day} filtered_days={len(days)-len(filtered)}",
                    )
        coin_days[coin] = filtered

    total_steps = max(1, sum(len(v) for v in coin_days.values()))
    step_i = 0

    downloaded_total = 0
    skipped_total = 0
    failed_total = 0
    downloaded_bytes_total = 0
    skipped_bytes_total = 0
    failed_bytes_total = 0
    
    # Track coins with actual downloads to trigger Build OHLCV jobs
    coins_with_downloads: dict[str, int] = {}

    def update_progress(
        *,
        downloaded_total_override: int | None = None,
        skipped_total_override: int | None = None,
        failed_total_override: int | None = None,
        downloaded_bytes_override: int | None = None,
        skipped_bytes_override: int | None = None,
        failed_bytes_override: int | None = None,
        chunk_done: int | None = None,
        chunk_total: int | None = None,
        **kw,
    ):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["downloaded_total"] = int(downloaded_total if downloaded_total_override is None else downloaded_total_override)
            pr["skipped_existing_total"] = int(skipped_total if skipped_total_override is None else skipped_total_override)
            pr["failed_total"] = int(failed_total if failed_total_override is None else failed_total_override)
            pr["downloaded_bytes_total"] = int(
                downloaded_bytes_total if downloaded_bytes_override is None else downloaded_bytes_override
            )
            pr["skipped_existing_bytes_total"] = int(
                skipped_bytes_total if skipped_bytes_override is None else skipped_bytes_override
            )
            pr["failed_bytes_total"] = int(failed_bytes_total if failed_bytes_override is None else failed_bytes_override)
            if chunk_done is not None:
                pr["chunk_done"] = int(chunk_done)
            if chunk_total is not None:
                pr["chunk_total"] = int(chunk_total)
            o["progress"] = pr

        update_job_file(job_path, mutate=mut)

    update_progress(stage="starting", recent_keys=[])
    update_progress(stage="starting", recent_failed=[])

    for coin in coins:
        for day in coin_days.get(coin, []):
            if _STOP:
                raise RuntimeError("Worker stopping")
            if _is_cancel_requested(job_path):
                raise RuntimeError("cancelled")

            if _reload_l2book_settings():
                _job_log(
                    "hl_aws_l2book_auto updated settings timeout_s=%s workers=%s"
                    % (l2book_timeout_s, l2book_workers)
                )
            step_i += 1
            update_progress(coin=coin, chunk_start=day, chunk_end=day)

            hours_to_download: list[int] | None = None
            if only_missing_1m_src_hours:
                hours_to_download = _hours_missing_in_1m_src(coin=coin, day=day)
                if not hours_to_download:
                    res = {
                        "planned": 0,
                        "downloaded": 0,
                        "skipped_existing": 0,
                        "failed": 0,
                        "filtered_existing_1m_src_hours": 24,
                    }
                    append_exchange_download_log(
                        "hyperliquid",
                        f"[INFO] [hl_aws_l2book_auto] {coin} {day} skipped (1m_src already covers all hours)",
                    )
                    update_progress(last_result=res, stage="running", chunk_done=0, chunk_total=0)
                    continue

            # Download all hours (0-23) - missing files will be handled as 404 during download
            last_cb = 0.0
            cb_lock = threading.Lock()

            def progress_cb(snap: dict[str, Any]) -> None:
                nonlocal last_cb
                with cb_lock:
                    now = time.time()
                    if now - last_cb < 1.0 and int(snap.get("done", 0)) != int(snap.get("planned", 0)):
                        return
                    last_cb = now

                disp_downloaded = downloaded_total + int(snap.get("downloaded", 0))
                disp_skipped = skipped_total + int(snap.get("skipped_existing", 0))
                disp_failed = failed_total + int(snap.get("failed", 0))
                disp_dl_b = downloaded_bytes_total + int(snap.get("downloaded_bytes", 0))
                disp_sk_b = skipped_bytes_total + int(snap.get("skipped_existing_bytes", 0))
                disp_fl_b = failed_bytes_total + int(snap.get("failed_bytes", 0))

                update_progress(
                    stage="running",
                    recent_keys=snap.get("recent_keys"),
                    recent_failed=snap.get("recent_failed"),
                    active_downloads=snap.get("active_downloads"),
                    downloaded_total_override=disp_downloaded,
                    skipped_total_override=disp_skipped,
                    failed_total_override=disp_failed,
                    downloaded_bytes_override=disp_dl_b,
                    skipped_bytes_override=disp_sk_b,
                    failed_bytes_override=disp_fl_b,
                    chunk_done=int(snap.get("done", 0)),
                    chunk_total=int(snap.get("planned", 0)),
                )

            res = download_hyperliquid_l2book_aws(
                coin=coin,
                start_date=day,
                end_date=day,
                aws_access_key_id=ak,
                aws_secret_access_key=sk,
                region_name=region,
                overwrite=False,
                recent_keys_limit=8,
                progress_cb=progress_cb,
                hours=hours_to_download,
            )
            # Log concise summary instead of full result dict
            summary = (
                f"planned:{res.get('planned',0)} "
                f"downloaded:{res.get('downloaded',0)} "
                f"skipped:{res.get('skipped_existing',0)} "
                f"failed:{res.get('failed',0)}"
            )
            if res.get('total_bytes', 0) > 0:
                mb = res['total_bytes'] / (1024 * 1024)
                summary += f" ({mb:.1f} MB)"
            append_exchange_download_log("hyperliquid", f"[INFO] [hl_aws_l2book_auto] {coin} {day} {summary}")

            try:
                dl_count = int(res.get("downloaded", 0))
                downloaded_total += dl_count
                skipped_total += int(res.get("skipped_existing", 0))
                failed_total += int(res.get("failed", 0))
                downloaded_bytes_total += int(res.get("downloaded_bytes", 0))
                skipped_bytes_total += int(res.get("skipped_existing_bytes", 0))
                failed_bytes_total += int(res.get("failed_bytes", 0))
                
                # Track coins with actual downloads
                if dl_count > 0:
                    coins_with_downloads[coin] = coins_with_downloads.get(coin, 0) + dl_count
            except Exception:
                pass
            if isinstance(res.get("recent_keys"), list):
                update_progress(
                    last_result=res,
                    stage="running",
                    recent_keys=res.get("recent_keys"),
                    recent_failed=res.get("recent_failed"),
                    active_downloads=[],
                    chunk_done=int(res.get("planned", 0)),
                    chunk_total=int(res.get("planned", 0)),
                )
            else:
                update_progress(last_result=res, stage="running")
    
    # After all downloads: trigger Build OHLCV jobs for coins with new data
    if coins_with_downloads:
        end_day = datetime.utcnow().strftime("%Y%m%d")
        for coin in sorted(coins_with_downloads.keys()):
            try:
                job = enqueue_job(
                    job_type="hl_best_1m",
                    payload={
                        "coins": [coin],
                        "end_day": end_day,
                    },
                )
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_aws_l2book_auto] Auto-triggered Build OHLCV for {coin}, job_id={job.job_id}",
                )
            except Exception as e:
                append_exchange_download_log(
                    "hyperliquid",
                    f"[hl_aws_l2book_auto] Failed to trigger Build OHLCV for {coin}: {e}",
                )


def _run_hl_best_1m(job_path: Path, payload: dict[str, Any]) -> None:
    started_ts = time.time()
    coins = payload.get("coins")
    coins = coins if isinstance(coins, list) else []
    coins = [str(c).strip().upper() for c in coins if str(c).strip()]

    if not coins:
        raise ValueError("No coins in job")

    end_day = str(payload.get("end_day") or "").strip()
    if not end_day:
        end_day = datetime.utcnow().strftime("%Y%m%d")
    start_day = str(payload.get("start_day") or "").strip()
    refetch = bool(payload.get("refetch") or False)

    total_steps = max(1, len(coins))
    step_i = 0

    def update_progress(**kw):
        def mut(o):
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr.update(kw)
            pr["step"] = int(step_i)
            pr["total"] = int(total_steps)
            pr["mode"] = "improve"
            o["progress"] = pr

        update_job_file(job_path, mutate=mut)

    update_progress(
        stage="starting",
        last_result={
            "days_checked": 0,
            "l2book_minutes_added": 0,
            "binance_minutes_filled": 0,
            "bybit_minutes_filled": 0,
            "duration_s": 0,
        },
        last_binance_fill_day="",
        day="",
    )

    for coin in coins:
        if _STOP:
            raise RuntimeError("Worker stopping")
        if _is_cancel_requested(job_path):
            raise RuntimeError("cancelled")
        step_i += 1
        stock_coin = bool(_is_stock_perp_coin(coin))
        update_progress(
            stage="running",
            coin=coin,
            chunk_done=0,
            chunk_total=0,
            last_result={
                "days_checked": 0,
                "tiingo_minutes_filled": 0,
                "tiingo_month_requests_used": 0,
                "l2book_minutes_added": 0 if not stock_coin else None,
                "binance_minutes_filled": 0 if not stock_coin else None,
                "bybit_minutes_filled": 0 if not stock_coin else None,
                "duration_s": int(max(0, time.time() - started_ts)),
            },
            last_binance_fill_day="",
            day="",
        )
        last_chunk_update = 0.0

        def progress_cb(snap: dict[str, Any]) -> None:
            nonlocal last_chunk_update
            now = time.time()
            if now - last_chunk_update < 0.5:
                return
            last_chunk_update = now
            planned = snap.get("planned")
            done = snap.get("done")
            stage = str(snap.get("stage") or "running")
            kw = {"stage": stage}
            if planned is not None:
                kw["chunk_total"] = int(planned)
            if done is not None:
                kw["chunk_done"] = int(done)
            if snap.get("day"):
                kw["day"] = str(snap.get("day"))
            if snap.get("month_key"):
                kw["month_key"] = str(snap.get("month_key"))
            if snap.get("month_day_index") is not None:
                kw["month_day_index"] = int(snap.get("month_day_index") or 0)
            if snap.get("month_day_total") is not None:
                kw["month_day_total"] = int(snap.get("month_day_total") or 0)
            if snap.get("ticker"):
                kw["ticker"] = str(snap.get("ticker"))
            if snap.get("tiingo_wait_s") is not None:
                kw["tiingo_wait_s"] = int(snap.get("tiingo_wait_s") or 0)
            if snap.get("tiingo_wait_reason") is not None:
                kw["tiingo_wait_reason"] = str(snap.get("tiingo_wait_reason") or "")
            if snap.get("tiingo_wait_kind") is not None:
                kw["tiingo_wait_kind"] = str(snap.get("tiingo_wait_kind") or "")
            if stage == "binance_fill" and snap.get("day"):
                kw["last_binance_fill_day"] = str(snap.get("day"))
            if any(k in snap for k in ("days_checked", "l2book_minutes_added", "binance_minutes_filled", "bybit_minutes_filled", "tiingo_minutes_filled", "tiingo_month_requests_used")):
                if stock_coin:
                    kw["last_result"] = {
                        "days_checked": int(snap.get("days_checked") or 0),
                        "tiingo_minutes_filled": int(snap.get("tiingo_minutes_filled") or 0),
                        "tiingo_month_requests_used": int(snap.get("tiingo_month_requests_used") or 0),
                        "duration_s": int(max(0, time.time() - started_ts)),
                    }
                else:
                    kw["last_result"] = {
                        "days_checked": int(snap.get("days_checked") or 0),
                        "l2book_minutes_added": int(snap.get("l2book_minutes_added") or 0),
                        "binance_minutes_filled": int(snap.get("binance_minutes_filled") or 0),
                        "bybit_minutes_filled": int(snap.get("bybit_minutes_filled") or 0),
                        "duration_s": int(max(0, time.time() - started_ts)),
                    }
            update_progress(**kw)

        res = improve_best_hyperliquid_1m_archive_for_coin(
            coin=coin,
            end_date=end_day,
            start_date_override=start_day or None,
            dry_run=False,
            refetch=refetch,
            progress_cb=progress_cb,
        )
        out = res.to_dict()
        if isinstance(out, dict):
            out["duration_s"] = int(max(0, time.time() - started_ts))
            if stock_coin:
                out.pop("l2book_minutes_added", None)
                out.pop("binance_minutes_filled", None)
                out.pop("bybit_minutes_filled", None)
        append_exchange_download_log("hyperliquid", f"[INFO] [hl_best_1m_job] {coin} {out}")
        update_progress(stage="running", last_result=out)


def main() -> int:
    ensure_task_dirs()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    pid = os.getpid()
    write_worker_pid(pid)
    _job_log(f"worker started pid={pid}")

    try:
        _requeue_stale_running_jobs(max_age_s=3600)

        while not _STOP:
            pending_dir = get_task_state_dir("pending")
            running_dir = get_task_state_dir("running")

            jobs = sorted(pending_dir.glob("*.json"), key=lambda p: p.name)
            if not jobs:
                time.sleep(2.0)
                continue

            job_src = jobs[0]
            # move to running atomically
            job_run = move_job_file(job_src, "running")
            try:
                _run_job(job_run)
            except Exception as e:
                _job_log(f"fatal in job runner: {e}")
                # if still in running, mark failed best-effort
                try:
                    if job_run.exists():
                        update_job_file(job_run, mutate=lambda o: o.update({"status": "failed", "error": str(e)}))
                        move_job_file(job_run, "failed")
                except Exception:
                    pass

        _job_log("worker stopping")
        return 0
    finally:
        clear_worker_pid()


if __name__ == "__main__":
    raise SystemExit(main())
