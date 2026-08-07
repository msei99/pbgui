"""Tests for idempotent integrity task enqueueing."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

import task_queue


def test_enqueue_unique_job_is_atomic_across_threads(monkeypatch, tmp_path: Path) -> None:
    """Concurrent producers must create only one active integrity job."""
    monkeypatch.setattr(task_queue, "get_market_data_root_dir", lambda: tmp_path)

    def enqueue(_index: int):
        return task_queue.enqueue_unique_job(
            job_type="ohlcv_integrity_scan",
            payload={"exchange": "bybit"},
            exchange="bybit",
            dedupe_key="ohlcv-integrity-scan:bybit:v1",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(enqueue, range(16)))

    assert sum(result.created for result in results) == 1
    assert len({result.job_id for result in results}) == 1
    assert len(list((tmp_path / "_tasks" / "pending").glob("*.json"))) == 1


def test_list_jobs_applies_job_type_filter_before_limit(monkeypatch, tmp_path: Path) -> None:
    """Unrelated newer jobs must not hide requested job types behind the result limit."""
    monkeypatch.setattr(task_queue, "get_market_data_root_dir", lambda: tmp_path)
    done_dir = tmp_path / "_tasks" / "done"
    done_dir.mkdir(parents=True)
    (done_dir / "100-hl.json").write_text(
        json.dumps({"id": "100-hl", "type": "hl_best_1m", "status": "done"}),
        encoding="utf-8",
    )
    for index in range(20):
        (done_dir / f"200-{index:02d}.json").write_text(
            json.dumps({"id": f"200-{index:02d}", "type": "other", "status": "done"}),
            encoding="utf-8",
        )

    jobs = task_queue.list_jobs(states=["done"], limit=1, job_types=["hl_best_1m"])

    assert [job["id"] for job in jobs] == ["100-hl"]
