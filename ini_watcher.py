"""
IniWatcher — Reusable file-change watcher for pbgui.ini.

Monitors pbgui.ini via mtime polling (one stat() call every 0.5s).
When the file changes, sets a threading.Event that can wake up any
sleeping main loop instantly.

Usage in any daemon (PBCoinData, PBData, …):

    from ini_watcher import IniWatcher

    watcher = IniWatcher()
    watcher.start()

    # In main loop — replaces sleep():
    while running:
        do_work()
        watcher.changed.wait(timeout=loop_interval)

        if watcher.changed.is_set():
            watcher.changed.clear()
            reload_config()

    watcher.stop()

The ``changed`` attribute is a threading.Event.  Calling
``wait(timeout=N)`` blocks up to *N* seconds **but returns immediately**
when pbgui.ini is modified — giving sub-second reaction time without
any busy-polling in the main loop.
"""

import asyncio
import threading
from pathlib import Path

from pbgui_purefunc import IniSignature, PBGDIR

# Default check interval for mtime polling (seconds).
# One stat() syscall every 0.5s is essentially zero overhead.
_POLL_INTERVAL = 0.5


class IniWatcher:
    """
    Lightweight pbgui.ini change detector.

    Parameters
    ----------
    poll_interval : float
        How often to stat() the file (default 0.5s).
    ini_path : Path | None
        Override the watched file (default: ``{PBGDIR}/pbgui.ini``).
        Useful for testing.
    """

    def __init__(
        self,
        poll_interval: float = _POLL_INTERVAL,
        ini_path: Path | None = None,
    ):
        self._ini_path: Path = ini_path or Path(f'{PBGDIR}/pbgui.ini')
        self._poll_interval = max(0.1, poll_interval)

        # Public event — consumers wait on this
        self.changed = threading.Event()

        # Internal stop signal
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lifecycle_lock = threading.RLock()
        self._signature = IniSignature(False, None, None, None)
        self._async_loop: asyncio.AbstractEventLoop | None = None
        self._async_changed: asyncio.Event | None = None

    # ── Lifecycle ──

    def start(self):
        """Start the watcher thread. Safe to call multiple times."""
        with self._lifecycle_lock:
            if self._thread is not None:
                if self._thread.is_alive():
                    return
                self._thread = None
            self._signature = self._read_signature()
            self._stop.clear()
            self.changed.clear()
            if self._async_changed is not None:
                self._async_changed.clear()
            self._thread = threading.Thread(
                target=self._watch_loop, daemon=True, name="ini-watcher"
            )
            self._thread.start()

    def stop(self, timeout: float = 2.0):
        """Stop the watcher thread (blocks up to 2s)."""
        with self._lifecycle_lock:
            thread = self._thread
            self._stop.set()
            self.changed.set()
            if self._async_loop is not None and self._async_changed is not None:
                try:
                    self._async_loop.call_soon_threadsafe(self._async_changed.set)
                except RuntimeError:
                    pass
            if thread is None:
                return
            thread.join(timeout=max(0.0, timeout))
            if not thread.is_alive() and self._thread is thread:
                self._thread = None

    def bind_asyncio(
        self,
        loop: asyncio.AbstractEventLoop,
        changed: asyncio.Event,
    ) -> None:
        """Route generation notifications to an owner-loop asyncio event."""
        with self._lifecycle_lock:
            self._async_loop = loop
            self._async_changed = changed
            if self.changed.is_set():
                loop.call_soon_threadsafe(changed.set)

    def unbind_asyncio(self) -> None:
        """Detach the asynchronous notification owner."""
        with self._lifecycle_lock:
            self._async_loop = None
            self._async_changed = None

    @property
    def is_running(self) -> bool:
        """True if the watcher thread is alive."""
        with self._lifecycle_lock:
            return self._thread is not None and self._thread.is_alive()

    # ── Internal ──

    def _read_signature(self) -> IniSignature:
        try:
            stat_result = self._ini_path.stat()
        except FileNotFoundError:
            return IniSignature(False, None, None, None)
        except OSError:
            return self._signature
        return IniSignature(
            True,
            stat_result.st_mtime_ns,
            stat_result.st_size,
            getattr(stat_result, "st_ino", None),
        )

    def _notify_changed(self) -> None:
        self.changed.set()
        loop = self._async_loop
        changed = self._async_changed
        if loop is not None and changed is not None:
            try:
                loop.call_soon_threadsafe(changed.set)
            except RuntimeError:
                pass

    def _watch_loop(self):
        """Poll the generation signature and publish level-triggered changes."""
        last_signature = self._signature
        while not self._stop.wait(self._poll_interval):
            current_signature = self._read_signature()
            if current_signature != last_signature:
                last_signature = current_signature
                self._signature = current_signature
                self._notify_changed()
