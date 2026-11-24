"""API-owned, read-only Python processes for CPU-heavy Loop display snapshots."""
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import multiprocessing
import threading

from logging_helpers import human_log as _log

SERVICE = 'PB8Loop'


class LoopReportUnavailable(RuntimeError):
    """An owned worker is temporarily unavailable; the browser can retry safely."""


class LoopSnapshotMissing(LookupError):
    """An optimizer snapshot does not belong to the requested owned run."""


def warm_report_worker():
    """Load the pure report renderer without starting controllers or provider clients."""
    from api import loop_optimizer_v8  # noqa: F401


def build_report(root, owner, loop_id, runtime="v8"):
    """Verify the owned archive in this process and return only the existing UI projection."""
    from loop_runtime import LoopRuntime
    api = __import__("api.loop_optimizer_" + runtime, fromlist=["engine"])
    _DISPLAY_FIELDS, projection = api._DISPLAY_FIELDS, api.projection
    from fastapi.responses import JSONResponse
    from pb8_loop_store import LoopStore
    row = LoopStore(root).read(owner, loop_id, fields=_DISPLAY_FIELDS)
    return row['created_at'], JSONResponse({**projection(row), 'view_revision': row['_view_revision']}).body


def build_diff(root, owner, loop_id, operation, baseline, runtime="v8"):
    """Decode only comparison inputs, without contending with API background Python work."""
    api = __import__("api.loop_optimizer_" + runtime, fromlist=["engine"])
    _snapshot_diff = api._snapshot_diff
    from pb8_loop_store import LoopStore
    row = LoopStore(root).read(owner, loop_id, fields={'jobs', 'initial_config', 'initial_overrides'})
    return _snapshot_diff(row, operation, baseline)


class LoopReportWorker:
    """Own two spawned workers, bounded by the API's coalesced admission registry."""

    def __init__(self, runtime="v8"):
        from loop_runtime import LoopRuntime
        self.runtime = LoopRuntime(runtime).version
        self._lock = threading.RLock()
        self._closed = False
        self._slots = threading.BoundedSemaphore(8)
        self._pool = self._create_pool()

    @staticmethod
    def _create_pool():
        """Use spawn: forking a multithreaded API can inherit locked runtime state."""
        pool = ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context('spawn'))
        try:
            for _ in range(2):
                pool.submit(warm_report_worker).add_done_callback(LoopReportWorker._warmed)
            return pool
        except BaseException:
            pool.shutdown(wait=True, cancel_futures=True)
            raise

    @staticmethod
    def _warmed(future):
        """Observe warm-up failures; a later read can replace a broken executor once."""
        if future.cancelled():
            return
        try:
            future.result()
        except Exception as exc:
            _log(SERVICE, 'Loop report worker warm-up failed', level='WARNING',
                 meta={'error_type': type(exc).__name__})

    def build(self, root, owner, loop_id):
        """Read one owned display snapshot through bounded process admission."""
        return self._execute(build_report, root, owner, loop_id, self.runtime)

    def diff(self, root, owner, loop_id, operation, baseline):
        """Read one immutable snapshot comparison through the same bounded workers."""
        return self._execute(build_diff, root, owner, loop_id, operation, baseline, self.runtime)

    def _execute(self, function, root, *args):
        """Bound queued work across reports and comparisons, including multiple browser tabs."""
        if not self._slots.acquire(blocking=False):
            raise LoopReportUnavailable('Loop report workers are busy')
        try:
            return self._execute_owned(function, str(root), *args)
        finally:
            self._slots.release()

    def _execute_owned(self, function, root, *args):
        """Recover one crashed pool without sharing mutable records or authentication state."""
        for attempt in range(2):
            pool = None
            try:
                with self._lock:
                    if self._closed:
                        raise LoopReportUnavailable('Loop report workers are shutting down')
                    pool = self._pool
                    future = pool.submit(function, root, *args)
                return future.result()
            except BrokenProcessPool as exc:
                retired = None
                with self._lock:
                    if not self._closed and self._pool is pool:
                        self._pool = self._create_pool()
                        retired = pool
                if retired is not None:
                    retired.shutdown(wait=True, cancel_futures=True)
                if attempt:
                    raise LoopReportUnavailable('Loop report worker failed; retrying automatically') from exc

    def close(self):
        """Stop admission, cancel queued reads and join every owned worker; native jobs are separate."""
        with self._lock:
            self._closed = True
            pool, self._pool = self._pool, None
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)
