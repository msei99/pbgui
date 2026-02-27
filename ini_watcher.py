"""
IniWatcher — Reusable file-change watcher for pbgui.ini.

Monitors pbgui.ini via mtime polling (one stat() call every 0.5s).
When the file changes, sets a threading.Event that can wake up any
sleeping main loop instantly.

Usage in any daemon (PBMaster, PBCoinData, PBData, …):

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

import threading
from pathlib import Path

from pbgui_purefunc import PBGDIR

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

    # ── Lifecycle ──

    def start(self):
        """Start the watcher thread. Safe to call multiple times."""
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.changed.clear()
        self._thread = threading.Thread(
            target=self._watch_loop, daemon=True, name="ini-watcher"
        )
        self._thread.start()

    def stop(self):
        """Stop the watcher thread (blocks up to 2s)."""
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    @property
    def is_running(self) -> bool:
        """True if the watcher thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    # ── Internal ──

    def _watch_loop(self):
        """Poll mtime; set ``changed`` event when file is modified."""
        last_mtime = 0.0
        try:
            last_mtime = self._ini_path.stat().st_mtime
        except OSError:
            pass

        while not self._stop.is_set():
            self._stop.wait(self._poll_interval)
            if self._stop.is_set():
                break
            try:
                cur_mtime = self._ini_path.stat().st_mtime
                if cur_mtime != last_mtime:
                    last_mtime = cur_mtime
                    self.changed.set()
            except OSError:
                pass
