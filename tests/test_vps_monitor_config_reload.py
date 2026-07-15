"""Focused tests for VPSMonitor's transactional INI reload lifecycle."""

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import master.async_monitor as monitor_mod
from master.async_monitor import VPSMonitor
from pbgui_purefunc import load_ini_snapshot


class FakePool:
    """Record local resource actions without opening remote connections."""

    def __init__(self) -> None:
        self.hosts = {"old", "new"}
        self.disconnected: list[str] = []
        self.connected: list[str] = []
        self.removed: list[str] = []

    async def disconnect(self, host: str) -> None:
        self.disconnected.append(host)

    async def connect(self, host: str) -> bool:
        self.connected.append(host)
        return True

    def remove_host(self, host: str) -> None:
        self.removed.append(host)
        self.hosts.discard(host)

    def hostnames(self) -> list[str]:
        return list(self.hosts)

    def load_vps_configs(self) -> None:
        self.hosts.add("new")

    async def disconnect_all(self) -> None:
        """Provide the monitor shutdown contract without remote work."""


def _write_ini(path: Path, *, enabled: str = "new", cpu: str = "91.5") -> None:
    """Write one complete monitor candidate to an isolated INI."""
    path.write_text(
        "[vps_monitor]\n"
        f"enabled_hosts = {enabled}\n"
        "auto_restart = false\n"
        "debug_logging = true\n"
        "[main]\n"
        "telegram_token = secret-token\n"
        "telegram_chat_id = chat-id\n"
        "[vps_monitor_alerts]\n"
        "offline_gui = false\n"
        "system_problem_telegram = false\n"
        "[vps_monitor_ui]\n"
        "compact = true\n"
        "[monitor]\n"
        f"cpu_error_server = {cpu}\n",
        encoding="utf-8",
    )


def _make_monitor(path: Path) -> VPSMonitor:
    """Build a reload owner with mocked stores and no production I/O."""
    monitor = object.__new__(VPSMonitor)
    monitor.pool = FakePool()
    monitor.store = SimpleNamespace(
        _ui_settings={"compact": "false"},
        changed=asyncio.Event(),
        remove_host=lambda host: None,
    )
    monitor._ini_watcher = SimpleNamespace(changed=threading.Event(), _ini_path=path)
    monitor._config_changed = asyncio.Event()
    monitor._config_changed.set()
    monitor._ini_watcher.changed.set()
    monitor._enabled_hosts = {"old"}
    monitor._auto_restart = True
    monitor._debug_logging = False
    monitor._telegram_token = "old-token"
    monitor._telegram_chat_id = "old-chat"
    monitor._alert_gui_routes = dict(monitor_mod.ALERT_ROUTE_GUI_DEFAULTS)
    monitor._alert_telegram_routes = dict(monitor_mod.ALERT_ROUTE_TELEGRAM_DEFAULTS)
    monitor._alert_routes_loaded = True
    monitor._monitor_config = SimpleNamespace(cpu_error_server=95.0)
    monitor._config_signature = None
    monitor._config_retry_count = 0
    monitor._config_retry_task = None
    monitor._running = False
    monitor._tasks = []
    monitor._stream_tasks = {}
    monitor._stop_metrics_stream = lambda host: None
    monitor._start_metrics_stream = lambda host: None

    async def collect_host_meta_now(host: str, *, include_package_status: bool) -> None:
        del host, include_package_status

    monitor.collect_host_meta_now = collect_host_meta_now
    return monitor


def test_invalid_snapshot_preserves_last_good_and_recovery(monkeypatch, tmp_path):
    """A malformed generation changes nothing and the next valid one recovers."""
    async def exercise() -> None:
        path = tmp_path / "pbgui.ini"
        _write_ini(path, cpu="invalid")
        monitor = _make_monitor(path)
        loads = 0

        def load_once(candidate_path):
            nonlocal loads
            loads += 1
            return load_ini_snapshot(candidate_path)

        monkeypatch.setattr(monitor_mod, "load_ini_snapshot", load_once)
        await monitor._apply_config_changes()
        assert loads == 1
        assert monitor._enabled_hosts == {"old"}
        assert monitor._telegram_token == "old-token"
        assert monitor._monitor_config.cpu_error_server == 95.0

        _write_ini(path)
        monitor._config_changed.set()
        await monitor._apply_config_changes()
        assert loads == 2
        assert monitor._enabled_hosts == {"new"}
        assert monitor._auto_restart is False
        assert monitor._debug_logging is True
        assert monitor._telegram_token == "secret-token"
        assert monitor._telegram_chat_id == "chat-id"
        assert monitor._alert_gui_routes[monitor_mod.ALERT_KIND_OFFLINE] is False
        assert monitor._alert_telegram_routes["system_problem"] is False
        assert monitor._monitor_config.cpu_error_server == 91.5
        assert monitor.store._ui_settings == {"compact": "true", "debug_logging": "true"}

    asyncio.run(exercise())


def test_host_reload_preserves_enable_disable_actions(tmp_path):
    """A committed host delta disconnects old and starts resources for new."""
    async def exercise() -> None:
        path = tmp_path / "pbgui.ini"
        _write_ini(path)
        monitor = _make_monitor(path)
        started: list[str] = []
        monitor._start_metrics_stream = started.append

        await monitor._apply_config_changes()
        await asyncio.gather(*monitor._tasks)

        assert monitor.pool.disconnected == ["old"]
        assert "old" in monitor.pool.removed
        assert monitor.pool.connected == ["new"]
        assert started == ["new"]
        assert all(task.done() for task in monitor._tasks)

    asyncio.run(exercise())


def test_candidate_uses_exact_snapshot_generation(tmp_path):
    """Candidate parsing reads UI, alerts, hosts, and thresholds from one snapshot."""
    path = tmp_path / "pbgui.ini"
    _write_ini(path, enabled="one,two")
    snapshot = load_ini_snapshot(path)
    path.write_text("[vps_monitor]\nenabled_hosts = later\n", encoding="utf-8")

    candidate = VPSMonitor._build_config_candidate(snapshot)

    assert candidate.enabled_hosts == frozenset({"one", "two"})
    assert candidate.monitor_values["cpu_error_server"] == 91.5
    assert candidate.ui_settings["compact"] == "true"


def test_main_loop_has_no_executor_wait() -> None:
    """The async config bridge must not occupy a default-executor worker."""
    source = Path(monitor_mod.__file__).read_text(encoding="utf-8")
    main_loop = source[source.index("    async def _main_loop"):source.index("    async def _hl_expiry_loop")]
    assert "run_in_executor" not in main_loop
    assert "self._config_changed.wait()" in main_loop


def test_startup_baselines_watcher_before_initial_snapshot() -> None:
    """Startup ordering closes the watcher/snapshot lost-generation window."""
    source = Path(monitor_mod.__file__).read_text(encoding="utf-8")
    start = source[source.index("    async def start(self):"):source.index("    async def stop(self):")]
    assert start.index("self._ini_watcher.start()") < start.index("load_ini_snapshot(")


def test_shutdown_cancels_owned_reload_tasks_and_stops_watcher(tmp_path):
    """Shutdown awaits reload-owned tasks and leaves no watcher or executor wait."""
    async def exercise() -> None:
        monitor = _make_monitor(tmp_path / "pbgui.ini")
        monitor._running = True
        watcher_calls: list[str] = []
        monitor._ini_watcher.stop = lambda: watcher_calls.append("stop")
        monitor._ini_watcher.unbind_asyncio = lambda: watcher_calls.append("unbind")
        monitor._host_metric_history = {}
        monitor._bot_metric_history = {}
        monitor._bot_count_history = {}
        monitor._bot_cpu_history = SimpleNamespace(maybe_flush=lambda **kwargs: None)
        monitor._bot_pnl_history = SimpleNamespace(maybe_flush=lambda **kwargs: None)
        monitor.loop = asyncio.get_running_loop()
        monitor._tasks = [asyncio.create_task(asyncio.sleep(60), name="vps-config-retry")]

        await monitor.stop()

        assert watcher_calls == ["stop", "unbind"]
        assert monitor._tasks == []
        assert monitor._config_retry_task is None
        assert monitor.loop is None

    asyncio.run(exercise())
