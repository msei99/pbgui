"""Coalesce passive restart inspections without caching action admission decisions."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from time import monotonic

from logging_helpers import human_log as _log

SERVICE = "PBApiServer"


class RestartStatusUnavailable(RuntimeError):
    """Signal a transient display-read failure without changing restart admission."""


class RestartStatusReader:
    """Own one read worker and one short-lived, version-bound display snapshot."""

    def __init__(self, inspect, key, *, ttl=0.25, timeout=2.0, clock=monotonic):
        """Bind value-free inspection callbacks without starting a task or thread."""
        self.inspect = inspect
        self.key = key
        self.ttl = ttl
        self.timeout = timeout
        self.clock = clock
        self.loop = None
        self.task = None
        self.pool = None
        self.worker = None
        self.closed = False
        self.generation = 0
        self.cached = None
        self.closers = 0

    def start(self):
        """Start a lifespan, discarding the previous instance's display snapshot."""
        if self.closers or ((self.task is not None and not self.task.done()) or
                (self.worker is not None and not self.worker.done())):
            raise RuntimeError("Restart inspection is still running")
        loop = asyncio.get_running_loop()
        if self.loop is not None and self.loop is not loop and self.loop.is_running():
            raise RuntimeError("Restart inspection belongs to another running loop")
        self.loop = loop
        self.task = None
        self.worker = None
        self.cached = None
        self.closed = False
        self.generation += 1

    def _finished(self, task):
        """Consume orphaned errors even when every requesting browser disconnected."""
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                _log(SERVICE, "Restart display inspection failed", level="ERROR",
                     meta={"error_type": type(error).__name__})

    async def _inspect(self, key, generation):
        """Inspect off the event loop; cancellation of a viewer cannot cancel this job."""
        started = self.clock()
        # Keep the concurrent Future: cancelling an asyncio wrapper cannot make
        # the real running thread disappear from lifecycle ownership.
        self.worker = self.pool.submit(self.inspect)
        completion = asyncio.wrap_future(self.worker)
        try:
            result = await asyncio.shield(completion)
        except asyncio.CancelledError:
            while not completion.done():
                try:
                    await asyncio.wait({completion})
                except asyncio.CancelledError:
                    pass
            if not completion.cancelled():
                error = completion.exception()
                if error is not None:
                    _log(SERVICE, "Restart display inspection failed", level="ERROR",
                         meta={"error_type": type(error).__name__})
            raise
        if not self.closed and generation == self.generation and key == self.key():
            self.cached = (key, started + self.ttl, deepcopy(result))
        return key, generation, result

    async def read(self):
        """Share overlapping work; immediately recheck a changed code/settings key."""
        if self.closed:
            raise RestartStatusUnavailable("Restart display inspection is closing")
        loop = asyncio.get_running_loop()
        if self.loop is not loop:
            self.start()
        deadline = loop.time() + self.timeout
        for _ in range(2):
            if self.closed:
                raise RestartStatusUnavailable("Restart display inspection is closing")
            key = self.key()
            if self.cached is not None:
                cached_key, expires, result = self.cached
                if key == cached_key and self.clock() < expires:
                    return deepcopy(result)
            task = self.task
            if task is None or task.done():
                if self.pool is None:
                    self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="restart-status")
                task = asyncio.create_task(self._inspect(key, self.generation),
                                           name="restart-display-inspection")
                task.add_done_callback(self._finished)
                self.task = task
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise RestartStatusUnavailable("Restart display inspection timed out")
            try:
                result_key, generation, result = await asyncio.wait_for(asyncio.shield(task), remaining)
            except asyncio.TimeoutError as exc:
                if task.done() and not task.cancelled() and task.exception() is exc:
                    raise RestartStatusUnavailable("Restart display inspection failed") from exc
                raise RestartStatusUnavailable("Restart display inspection timed out") from exc
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    raise
                raise RestartStatusUnavailable("Restart display inspection was interrupted") from None
            except Exception as exc:
                raise RestartStatusUnavailable("Restart display inspection failed") from exc
            if generation == self.generation and result_key == self.key():
                return deepcopy(result)
        raise RestartStatusUnavailable("Restart display state changed; retry shortly")

    async def close(self):
        """Stop admission and join the real worker, even if shutdown is cancelled."""
        self.closers += 1
        try:
            await self._drain()
        finally:
            self.closers -= 1

    async def _drain(self):
        """Join the old inspection; reopening is forbidden until all closers finish."""
        self.closed = True
        self.cached = None
        task = self.task
        interrupted = False
        while task is not None and not task.done():
            try:
                await asyncio.wait({task})
            except asyncio.CancelledError:
                interrupted = True
        if task is not None and not task.cancelled():
            task.exception()
        # The task drains its actual concurrent Future before completing. Join
        # the now-idle executor thread; no inspection runs in this synchronous step.
        if self.pool is not None:
            self.pool.shutdown(wait=True)
            self.pool = None
        if interrupted:
            raise asyncio.CancelledError
