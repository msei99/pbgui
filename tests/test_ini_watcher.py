"""Tests for ini_watcher.IniWatcher — reusable pbgui.ini change detector."""

import tempfile
import time
from pathlib import Path

import pytest

from ini_watcher import IniWatcher


class TestIniWatcher:
    """Tests for IniWatcher lifecycle and change detection."""

    def _make_watcher(self, tmp_path: Path) -> tuple[IniWatcher, Path]:
        """Create a watcher with a temp ini file."""
        ini = tmp_path / "test.ini"
        ini.write_text("[test]\nkey = value\n")
        w = IniWatcher(poll_interval=0.1, ini_path=ini)
        return w, ini

    def test_start_stop(self, tmp_path):
        """Watcher thread starts and stops cleanly."""
        w, _ = self._make_watcher(tmp_path)
        assert not w.is_running
        w.start()
        assert w.is_running
        w.stop()
        assert not w.is_running

    def test_start_idempotent(self, tmp_path):
        """Calling start() twice doesn't create duplicate threads."""
        w, _ = self._make_watcher(tmp_path)
        w.start()
        thread1 = w._thread
        w.start()  # second call
        assert w._thread is thread1
        w.stop()

    def test_changed_event_on_modify(self, tmp_path):
        """changed event is set when the file is modified."""
        w, ini = self._make_watcher(tmp_path)
        w.start()
        try:
            assert not w.changed.is_set()
            # Modify the file
            time.sleep(0.05)
            ini.write_text("[test]\nkey = new_value\n")
            # Wait for watcher to detect (poll_interval=0.1s)
            result = w.changed.wait(timeout=1.0)
            assert result, "changed event was not set within 1s"
            assert w.changed.is_set()
        finally:
            w.stop()

    def test_changed_event_not_set_without_modify(self, tmp_path):
        """changed event stays clear when file is not modified."""
        w, _ = self._make_watcher(tmp_path)
        w.start()
        try:
            result = w.changed.wait(timeout=0.5)
            assert not result, "changed event should not be set"
        finally:
            w.stop()

    def test_clear_and_redetect(self, tmp_path):
        """After clearing, a new modification is detected again."""
        w, ini = self._make_watcher(tmp_path)
        w.start()
        try:
            # First change
            time.sleep(0.05)
            ini.write_text("[test]\nkey = v2\n")
            w.changed.wait(timeout=1.0)
            assert w.changed.is_set()

            # Clear and modify again
            w.changed.clear()
            assert not w.changed.is_set()
            time.sleep(0.05)
            ini.write_text("[test]\nkey = v3\n")
            result = w.changed.wait(timeout=1.0)
            assert result
        finally:
            w.stop()

    def test_missing_file_no_crash(self, tmp_path):
        """Watcher handles missing file gracefully."""
        ini = tmp_path / "nonexistent.ini"
        w = IniWatcher(poll_interval=0.1, ini_path=ini)
        w.start()
        try:
            assert w.is_running
            # Create the file — should be detected
            time.sleep(0.15)
            ini.write_text("[test]\nkey = value\n")
            result = w.changed.wait(timeout=1.0)
            assert result
        finally:
            w.stop()

    def test_wait_as_sleep_replacement(self, tmp_path):
        """wait() returns False on timeout (acts like interruptible sleep)."""
        w, _ = self._make_watcher(tmp_path)
        w.start()
        try:
            start = time.monotonic()
            result = w.changed.wait(timeout=0.3)
            elapsed = time.monotonic() - start
            assert not result
            assert elapsed >= 0.25, f"Returned too early: {elapsed:.3f}s"
        finally:
            w.stop()

    def test_wait_returns_early_on_change(self, tmp_path):
        """wait() returns True early when file changes during wait."""
        w, ini = self._make_watcher(tmp_path)
        w.start()
        try:
            import threading

            def modify_later():
                time.sleep(0.2)
                ini.write_text("[test]\nkey = changed\n")

            t = threading.Thread(target=modify_later)
            t.start()

            start = time.monotonic()
            result = w.changed.wait(timeout=5.0)
            elapsed = time.monotonic() - start

            assert result, "Should have been woken up"
            assert elapsed < 2.0, f"Took too long: {elapsed:.3f}s"
            t.join()
        finally:
            w.stop()

    def test_default_path(self):
        """Default ini_path points to PBGDIR/pbgui.ini."""
        from pbgui_purefunc import PBGDIR
        w = IniWatcher()
        assert w._ini_path == Path(f'{PBGDIR}/pbgui.ini')

    def test_poll_interval_minimum(self):
        """Poll interval is clamped to at least 0.1s."""
        w = IniWatcher(poll_interval=0.01)
        assert w._poll_interval == 0.1
