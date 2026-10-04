"""Keep Services status polling responsive during worker inspections."""

import asyncio
import time

from api import services


def test_worker_inspection_does_not_block_event_loop(monkeypatch) -> None:
    """A slow synchronous worker read must not delay unrelated API tasks."""

    def slow_item() -> dict[str, str]:
        time.sleep(0.2)
        return {"id": "market-data-task"}

    def item() -> dict[str, str]:
        return {"id": "other"}

    async def async_item() -> dict[str, str]:
        return item()

    monkeypatch.setattr(services, "_get_task_worker_item", slow_item)
    for name in (
        "_get_backtest_v8_worker_item",
        "_get_optimize_v8_worker_item",
        "_get_archive_sync_worker_item",
        "_get_hlcvs_cleanup_worker_item",
    ):
        monkeypatch.setattr(services, name, item)
    monkeypatch.setattr(services, "_get_backtest_worker_item", async_item)
    monkeypatch.setattr(services, "_get_optimize_worker_item", async_item)

    async def verify() -> None:
        pending = asyncio.create_task(services._collect_worker_groups())
        await asyncio.sleep(0.02)
        assert not pending.done(), "Synchronous worker inspection blocked the event loop"
        groups = await pending
        assert groups[0]["items"][0]["id"] == "market-data-task"

    asyncio.run(verify())


def test_overview_summary_reads_live_workers_without_loading_queues(monkeypatch) -> None:
    """Overview counts use task ownership and never scan or parse queued bot configs."""
    from types import SimpleNamespace
    import api
    import task_worker_ownership

    active = SimpleNamespace(_task=SimpleNamespace(done=lambda: False))
    stopped = SimpleNamespace(_task=None)
    monkeypatch.setattr(api, 'backtest_v7', SimpleNamespace(
        _worker=active, _archive_sync_worker=active, _hlcvs_cleanup_worker=stopped), raising=False)
    monkeypatch.setattr(api, 'backtest_v8', SimpleNamespace(_worker=active), raising=False)
    monkeypatch.setattr(api, 'optimize_v7', SimpleNamespace(_worker=stopped), raising=False)
    monkeypatch.setattr(api, 'optimize_v8', SimpleNamespace(_worker=active), raising=False)
    monkeypatch.setattr(task_worker_ownership, 'get_task_worker_status', lambda: SimpleNamespace(running=True))

    async def forbidden_scan():
        """A summary must not request detailed queue inspection."""
        raise AssertionError('Overview triggered a detailed queue scan')

    monkeypatch.setattr(services, '_collect_worker_groups', forbidden_scan)
    start = time.monotonic()
    result = asyncio.run(services.get_workers_summary(session=object()))
    assert result['counts'] == {'total': 7, 'running': 5}
    assert time.monotonic() - start < 1.0


def test_worker_status_polls_share_snapshot_until_worker_action(monkeypatch) -> None:
    """Concurrent polls share one queue inspection; worker actions invalidate it (#394)."""
    scans = []

    async def counted_scan():
        scans.append(1)
        await asyncio.sleep(0.01)
        return [{"id": "queue", "label": "Queue Workers", "items": [{"id": "backtest-queue", "running": True}]}]

    async def no_op(_worker_id):
        return None

    async def found(worker_id):
        return {"id": worker_id}

    monkeypatch.setattr(services, "_collect_worker_groups", counted_scan)
    monkeypatch.setattr(services, "_start_worker", no_op)
    monkeypatch.setattr(services, "_find_worker", found)

    async def verify() -> None:
        monkeypatch.setattr(services, "_workers_status_lock", asyncio.Lock())
        services._invalidate_workers_status()
        results = await asyncio.gather(*(services.get_workers_status(session=object()) for _ in range(5)))
        assert len(scans) == 1
        assert all(result["counts"] == {"total": 1, "running": 1} for result in results)
        await services.get_workers_status(session=object())
        assert len(scans) == 1
        await services.worker_action("backtest-queue", "start", session=object())
        await services.get_workers_status(session=object())
        assert len(scans) == 2

    asyncio.run(verify())
    services._invalidate_workers_status()


def test_worker_status_scan_started_before_action_is_not_cached(monkeypatch) -> None:
    """A scan that began before a worker action cannot repopulate the cache with stale state."""
    state = {"running": False}
    scans = []
    gate = {}

    async def scan():
        scans.append(state["running"])
        snapshot = state["running"]
        if len(scans) == 1:
            await gate["release"].wait()
        return [{"id": "queue", "label": "Queue Workers",
                 "items": [{"id": "backtest-queue", "running": snapshot}]}]

    async def start(_worker_id):
        state["running"] = True

    async def found(worker_id):
        return {"id": worker_id}

    monkeypatch.setattr(services, "_collect_worker_groups", scan)
    monkeypatch.setattr(services, "_start_worker", start)
    monkeypatch.setattr(services, "_find_worker", found)

    async def verify() -> None:
        gate["release"] = asyncio.Event()
        monkeypatch.setattr(services, "_workers_status_lock", asyncio.Lock())
        services._invalidate_workers_status()
        stale_poll = asyncio.create_task(services.get_workers_status(session=object()))
        await asyncio.sleep(0)
        await services.worker_action("backtest-queue", "start", session=object())
        gate["release"].set()
        await stale_poll
        fresh = await services.get_workers_status(session=object())
        assert fresh["counts"]["running"] == 1

    asyncio.run(verify())
    services._invalidate_workers_status()
