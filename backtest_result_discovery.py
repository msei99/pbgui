"""Bounded, API-owned read-only result discovery without runtime imports."""

from concurrent.futures import Future
import copy
import importlib.util
import json
import sqlite3
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

SERVICE = 'BacktestResultDiscovery'
_MAX_RESULTS = 200000
_MAX_OUTPUT_BYTES = 16 * 1024 * 1024


class ResultDiscoveryUnavailable(RuntimeError):
    """A bounded discovery read cannot currently finish."""

    def __init__(self, code):
        """Expose a stable reason without child tracebacks or arbitrary input values."""
        self.code = code
        messages = {
            'busy': 'Result discovery busy; retry shortly',
            'closed': 'Result discovery is shutting down',
            'timeout': 'Result discovery timed out',
            'limit': 'Result discovery limit exceeded; select a config filter',
            'invalid': 'Invalid result discovery input or response',
            'io': 'Result discovery failed',
        }
        super().__init__(messages.get(code, messages['io']))


def discover_result_paths(root, search_root):
    """Scan each directory once, returning current contained regular results."""
    root, search_root = Path(root), Path(search_root)
    if not root.is_absolute() or not search_root.is_absolute():
        raise ValueError('Absolute result roots required')
    root = root.resolve()
    search_root = search_root.resolve()
    search_root.relative_to(root)
    pending, indexed = [search_root], []
    deadline = time.monotonic() + 20
    while pending:
        if time.monotonic() >= deadline:
            raise ResultDiscoveryUnavailable('timeout')
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.name == 'analysis.json' and entry.is_file(follow_symlinks=False):
                            resolved = Path(entry.path).resolve()
                            resolved.relative_to(root)
                            if resolved.is_file():
                                indexed.append((resolved.stat().st_mtime, str(resolved)))
                                if len(indexed) > _MAX_RESULTS:
                                    raise ResultDiscoveryUnavailable('limit')
                    except (OSError, ValueError):
                        continue
        except OSError:
            continue
    indexed.sort(reverse=True)
    return [path for _modified, path in indexed]


class ResultDiscoveryWorker:
    """Own at most two disposable isolated Python readers, never bot processes."""

    def __init__(self):
        """Create bounded ownership registries without starting a process."""
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(2)
        self._processes = set()
        self._closed = False
        self._reads = {}

    def discover(self, root, search_root):
        """Read fresh paths without importing API code in the child."""
        return [Path(path) for path in self._request({
            'operation': 'discover', 'root': str(root), 'search_root': str(search_root)})]

    def cached_page(self, root, paths, gui_version, summary_schema):
        """Verify and read only warm summaries; missing rows use the normal index builder."""
        return self._request({'operation': 'cache', 'root': str(root),
            'paths': [str(path) for path in paths], 'gui_version': gui_version,
            'summary_schema': summary_schema})

    def _request(self, request):
        """Coalesce only concurrent identical reads; never retain a completed scan."""
        key = json.dumps(request, sort_keys=True)
        with self._lock:
            if self._closed:
                raise ResultDiscoveryUnavailable('closed')
            future = self._reads.get(key)
            owner = future is None
            if owner:
                if not self._slots.acquire(blocking=False):
                    raise ResultDiscoveryUnavailable('busy')
                future = self._reads[key] = Future()
        if not owner:
            return copy.deepcopy(future.result(timeout=30))
        try:
            paths = self._discover_owned(request)
            future.set_result(copy.deepcopy(paths))
            return paths
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._lock:
                self._reads.pop(key, None)
            self._slots.release()

    def _discover_owned(self, request):
        """Run one bounded child with deterministic timeout and process cleanup."""
        process = None
        try:
            with self._lock:
                if self._closed:
                    raise ResultDiscoveryUnavailable('closed')
                environment = {'LANG': 'C.UTF-8', 'PATH': os.defpath}
                if 'SystemRoot' in os.environ:
                    environment['SystemRoot'] = os.environ['SystemRoot']
                process = subprocess.Popen(
                    [sys.executable, '-I', '-S', str(Path(__file__).resolve())],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    env=environment,
                )
                self._processes.add(process)
            try:
                output, _stderr = process.communicate(
                    json.dumps(request).encode(), timeout=25)
            except subprocess.TimeoutExpired as exc:
                self._terminate(process)
                process.communicate()
                raise ResultDiscoveryUnavailable('timeout') from exc
            if process.returncode:
                raise ResultDiscoveryUnavailable({2: 'timeout', 3: 'limit', 4: 'invalid'}.get(process.returncode, 'io'))
            if len(output) > _MAX_OUTPUT_BYTES:
                raise ResultDiscoveryUnavailable('limit')
            payload = json.loads(output)
            if request['operation'] == 'cache':
                if not isinstance(payload, dict) or not all(isinstance(value, dict) for value in payload.values()):
                    raise ResultDiscoveryUnavailable('invalid')
            elif not isinstance(payload, list) or not all(isinstance(path, str) for path in payload):
                raise ResultDiscoveryUnavailable('invalid')
            return payload
        except (OSError, ValueError) as exc:
            raise ResultDiscoveryUnavailable('io') from exc
        finally:
            if process is not None:
                try:
                    self._terminate(process)
                finally:
                    with self._lock:
                        self._processes.discard(process)
                    for pipe in (process.stdin, process.stdout, process.stderr):
                        if pipe is not None:
                            pipe.close()

    @staticmethod
    def _terminate(process):
        """Handle the ordinary exit-between-poll-and-kill race, then reap the child."""
        if process.poll() is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass  # It exited after poll; wait still reaps it.
        process.wait()

    def close(self):
        """Stop admission, terminate only owned readers and reap them deterministically."""
        with self._lock:
            self._closed = True
            processes = tuple(self._processes)
        failures = []
        for process in processes:
            try:
                self._terminate(process)
            except OSError as exc:
                failures.append(exc)
        if failures:
            raise ExceptionGroup('Result reader shutdown failed', failures)


def _cached_page(request):
    """Reuse the exact fingerprint/decoder implementation with a read-only cache connection."""
    root = Path(request['root'])
    if not root.is_absolute() or not isinstance(request['paths'], list) or len(request['paths']) > 100:
        raise ValueError('Invalid result page')
    root = root.resolve()
    module_path = Path(__file__).resolve().with_name('backtest_result_metadata.py')
    spec = importlib.util.spec_from_file_location('_result_metadata', module_path)
    metadata = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(metadata)
    signatures = {}
    for raw in request['paths']:
        path = Path(raw)
        if not path.is_absolute() or path.is_symlink():
            continue
        path = path.resolve()
        path.relative_to(root)
        if path.name != 'analysis.json' or not path.is_file():
            continue
        signatures[str(path.parent)] = metadata.result_signature(path, version='v8',
            gui_version=request['gui_version'], summary_schema=request['summary_schema'])
    cache = Path(__file__).resolve().parent / 'data' / 'cache' / 'backtest_results.sqlite'
    if not cache.is_file() or cache.is_symlink():
        return {}
    try:
        connection = sqlite3.connect(cache.as_uri() + '?mode=ro', uri=True, timeout=0.5)
        try:
            return metadata.cached_payloads(connection, 'v8', signatures)
        finally:
            connection.close()
    except sqlite3.Error:
        return {}  # A reconstructable cache miss follows the normal index path.


def _main():
    """Run a single stdlib-only read; never import PBGui or Passivbot modules."""
    try:
        request = json.loads(sys.stdin.buffer.read(1024 * 1024 + 1))
        result = (_cached_page(request) if request.get('operation') == 'cache'
            else discover_result_paths(request['root'], request['search_root']))
        output = json.dumps(result, separators=(',', ':')).encode()
        if len(output) > _MAX_OUTPUT_BYTES:
            return 3
        sys.stdout.buffer.write(output)
        return 0
    except ResultDiscoveryUnavailable as exc:
        return {'timeout': 2, 'limit': 3}.get(exc.code, 5)
    except (ValueError, KeyError, TypeError):
        return 4
    except OSError:
        return 5


if __name__ == '__main__':
    raise SystemExit(_main())
