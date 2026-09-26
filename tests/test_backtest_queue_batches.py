"""Offline tests for durable PB8 queue batch submission."""

from __future__ import annotations

import asyncio
from pathlib import Path

from api.backtest_queue_batches import BacktestQueueBatches


async def _wait_for(manager: BacktestQueueBatches, owner: str, status: str) -> dict:
    """Wait briefly for an isolated batch worker state."""
    for _ in range(100):
        current = manager.list_for_owner(owner)[0]
        if current["status"] == status:
            return current
        await asyncio.sleep(0.02)
    raise AssertionError(f"Batch did not become {status}")


def test_batch_returns_before_jobs_finish_and_resumes_failed_item(tmp_path: Path) -> None:
    """One durable command can resume without repeating confirmed jobs."""
    calls = []
    fail_once = True

    def submit_one(item: dict) -> None:
        """Record an isolated submission with one transient failure."""
        nonlocal fail_once
        calls.append(item["name"])
        if item["name"] == "second" and fail_once:
            fail_once = False
            raise RuntimeError("temporary failure")

    async def exercise() -> None:
        """Run the controller using only temporary files."""
        manager = BacktestQueueBatches(tmp_path / "batches", submit_one)
        manager.start()
        items = [
            {"name": name, "operation_id": f"first-{name}", "config": {"pbgui": {"backtest_result_group": {"id": "attempt-one"}}}}
            for name in ("first", "second", "third")
        ]
        accepted = manager.submit("alice", items)
        assert accepted["status"] == "queued"
        assert accepted["total"] == 3
        errored = await _wait_for(manager, "alice", "error")
        assert errored["confirmed"] == 1
        items[0]["config"]["pbgui"]["backtest_result_group"]["id"] = "attempt-two"
        for item in items:
            item["operation_id"] = "second-" + item["name"]
        resumed = manager.submit("alice", items)
        assert resumed["batch_id"] == accepted["batch_id"]
        complete = await _wait_for(manager, "alice", "complete")
        assert complete["confirmed"] == 3
        assert calls == ["first", "second", "second", "third"]
        assert not (tmp_path / "batches" / accepted["batch_id"] / "items.json").exists()
        await manager.stop()

    asyncio.run(exercise())


def test_pb8_batch_route_keeps_status_owner_scoped(tmp_path: Path, monkeypatch) -> None:
    """Authenticated route acceptance returns only the current owner's progress."""
    from api import backtest_v8
    from api.auth import SessionToken

    manager = BacktestQueueBatches(tmp_path / "batches", lambda item: None)
    monkeypatch.setattr(backtest_v8, "_batch_submitter", manager)
    alice = SessionToken(token="test", user_id="alice", created_at=0, expires_at=1)
    bob = SessionToken(token="test", user_id="bob", created_at=0, expires_at=1)
    accepted = backtest_v8.submit_queue_batch(
        {"items": [{"name": "candidate", "config": {}, "override_configs": {}, "operation_id": "once"}]},
        session=alice,
    )
    assert accepted["total"] == 1
    assert backtest_v8.list_queue_batches(session=alice)["batches"][0]["batch_id"] == accepted["batch_id"]
    assert backtest_v8.list_queue_batches(session=bob)["batches"] == []
