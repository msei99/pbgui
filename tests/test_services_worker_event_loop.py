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
