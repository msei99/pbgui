"""Tests for OKX task-worker pipeline scheduling."""

from __future__ import annotations

import json
import threading
import time

import task_worker


class _FakeOkxResult:
    """Minimal result object returned by the fake OKX downloader."""

    def __init__(self, coin: str) -> None:
        """Store the coin name for to_dict()."""

        self.coin = coin

    def to_dict(self) -> dict:
        """Return the result shape consumed by task_worker."""

        return {
            "coin": self.coin,
            "days_checked": 1,
            "archive_daily_downloaded": 0,
            "rest_minutes_fetched": 0,
            "repair_minutes_fetched": 0,
            "minutes_written": 0,
            "notes": [],
        }


def test_okx_pipeline_starts_next_coin_after_archive_stage(monkeypatch, tmp_path) -> None:
    """OKX jobs pipeline the next coin only after the active coin reaches archives."""

    job_path = tmp_path / "okx-job.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    logs: list[str] = []
    started: list[str] = []
    limiter_ids: list[int] = []
    lock = threading.Lock()
    eth_started = threading.Event()

    def fake_improve_best_okx_1m_for_coin(**kwargs):
        """Simulate BTC entering archives before ETH is allowed to start."""

        coin = str(kwargs["coin"])
        progress_cb = kwargs["progress_cb"]
        rest_limiter = kwargs.get("rest_limiter")
        with lock:
            started.append(coin)
            limiter_ids.append(id(rest_limiter))
        if coin == "BTC":
            progress_cb({"stage": "rest_gap", "done": 1, "planned": 2})
            time.sleep(0.2)
            assert not eth_started.is_set()
            progress_cb({"stage": "archive_index", "day": "2021-09-01"})
            assert eth_started.wait(2.0)
        elif coin == "ETH":
            eth_started.set()
        return _FakeOkxResult(coin)

    monkeypatch.setattr(task_worker, "improve_best_okx_1m_for_coin", fake_improve_best_okx_1m_for_coin)
    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, line: logs.append(line))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_refresh_inventory_coin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)

    task_worker._run_okx_best_1m(
        job_path,
        {
            "coins": ["BTC", "ETH"],
            "end_day": "20260626",
            "start_day": "inception",
            "refetch": False,
            "pipeline_workers": 2,
        },
    )

    assert started == ["BTC", "ETH"]
    assert len(set(limiter_ids)) == 1
    assert any("pipeline  workers=2" in line for line in logs)
    assert any("BTC  stage=archive_index" in line for line in logs)
