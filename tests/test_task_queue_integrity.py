"""Tests for idempotent integrity task enqueueing."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
