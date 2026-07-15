"""Focused tests for PBData's atomic runtime configuration reload."""

import asyncio
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

import PBData as pbdata_module
from PBData import PBData, PBDataConfigError, PBDataRuntimeConfig, _bounded_shutdown_step
from ini_watcher import IniWatcher
from pbgui_purefunc import load_ini_snapshot


def _owner(path: Path, users=("alice", "bob")) -> PBData:
    """Create a config-only PBData owner without production DB/runtime setup."""
    owner = PBData.__new__(PBData)
    owner.users = SimpleNamespace(list=lambda: list(users))
    owner._runtime_config = PBDataRuntimeConfig()
    owner._config_generation = None
    owner._config_changed = None
    owner._config_schedule_changed = None
    owner._config_reload_task = None
    owner._ini_watcher = IniWatcher(poll_interval=0.1, ini_path=path)
    owner._ws_max_loaded = None
    owner._log_level_loaded = None
    owner._pollers_enabled_after_ts = 0.0
    owner._poll_intervals_changed = False
    owner._fetch_users = []
    owner._trades_users = []
    return owner


def _write(path: Path, text: str) -> None:
    """Publish test INI text."""
    path.write_text(text, encoding="utf-8")


def _valid_ini() -> str:
    """Return an INI containing every reloadable PBData key."""
    return """[pbdata]
ws_max = 33
log_level = DEBUG
pollers_delay_seconds = 4
poll_interval_combined_seconds = 11
poll_interval_history_seconds = 12
poll_interval_executions_seconds = 13
poll_interval_balance_seconds = 14
poll_interval_positions_seconds = 15
poll_interval_orders_seconds = 16
latest_1m_interval_seconds = 101
latest_1m_coin_pause_seconds = 1.1
latest_1m_api_timeout_seconds = 21
latest_1m_min_lookback_days = 3
latest_1m_max_lookback_days = 5
shared_rest_user_pause_seconds = 1.2
shared_rest_pause_by_exchange_json = {"okx": 2.5}
price_watch_timeout = 91
rest_semaphore_acquire_timeout = 7
fetch_users = ['alice']
trades_users = ['bob']

[binance_data]
latest_1m_interval_seconds = 201
latest_1m_coin_pause_seconds = 0.1
latest_1m_api_timeout_seconds = 31
latest_1m_min_lookback_days = 2
latest_1m_max_lookback_days = 6

[bybit_data]
latest_1m_interval_seconds = 202
latest_1m_coin_pause_seconds = 0.2
latest_1m_api_timeout_seconds = 32
latest_1m_min_lookback_days = 3
latest_1m_max_lookback_days = 7

[okx_data]
latest_1m_interval_seconds = 203
latest_1m_coin_pause_seconds = 0.3
latest_1m_api_timeout_seconds = 33
latest_1m_min_lookback_days = 4
latest_1m_max_lookback_days = 8

[bitget_data]
latest_1m_interval_seconds = 204
latest_1m_coin_pause_seconds = 0.4
latest_1m_api_timeout_seconds = 34
latest_1m_min_lookback_days = 5
latest_1m_max_lookback_days = 9
"""


def test_all_runtime_keys_apply_from_one_candidate(tmp_path: Path, monkeypatch) -> None:
    """Every runtime-loaded key is validated and published together."""
    path = tmp_path / "pbgui.ini"
    _write(path, _valid_ini())
    owner = _owner(path)
    ws_calls = []
    level_calls = []
    monkeypatch.setattr(pbdata_module, "set_ws_limits", lambda **kwargs: ws_calls.append(kwargs))
    monkeypatch.setattr(pbdata_module, "set_service_min_level", lambda *args: level_calls.append(args))

    assert owner._apply_config_snapshot(load_ini_snapshot(path)) is True
    assert owner._shared_combined_interval_seconds == 11
    assert owner._shared_history_interval_seconds == 12
    assert owner._shared_executions_interval_seconds == 13
    assert owner._latest_1m_interval_seconds == 101
    assert owner._binance_latest_1m_interval_seconds == 201
    assert owner._bybit_latest_1m_coin_pause_seconds == 0.2
    assert owner._okx_latest_1m_api_timeout_seconds == 33
    assert owner._bitget_latest_1m_max_lookback_days == 9
    assert owner._shared_rest_pause_by_exchange == {
        "bybit": 3.0,
        "hyperliquid": 3.0,
        "okx": 2.5,
    }
    assert owner.fetch_users == ["alice"]
    assert owner.trades_users == ["bob"]
    assert owner._poll_intervals_changed is True
    assert ws_calls == [{"global_max": 33}]
    assert level_calls == [("PBData", "DEBUG")]


def test_missing_keys_restore_defaults_and_removed_overrides(tmp_path: Path, monkeypatch) -> None:
    """Deletion restores model defaults and removes stale JSON overrides."""
    path = tmp_path / "pbgui.ini"
    owner = _owner(path)
    monkeypatch.setattr(pbdata_module, "set_ws_limits", lambda **kwargs: None)
    monkeypatch.setattr(pbdata_module, "set_service_min_level", lambda *args: None)
    _write(path, _valid_ini())
    owner._apply_config_snapshot(load_ini_snapshot(path))
    _write(path, "[pbdata]\n")
    owner._apply_config_snapshot(load_ini_snapshot(path))

    defaults = PBDataRuntimeConfig()
    assert owner._runtime_config == defaults
    assert owner._shared_rest_pause_by_exchange == dict(defaults.shared_rest_pause_by_exchange)
    assert owner.fetch_users == []
    assert owner.trades_users == []


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("shared_rest_pause_by_exchange_json", "{bad-json"),
        ("fetch_users", "not-a-list"),
        ("trades_users", "{'alice': 1}"),
        ("poll_interval_history_seconds", "nan"),
        ("price_watch_timeout", "-1"),
    ],
)
def test_invalid_value_preserves_entire_last_good(tmp_path: Path, monkeypatch, key: str, value: str) -> None:
    """Malformed JSON, users, and numbers reject the whole generation."""
    path = tmp_path / "pbgui.ini"
    owner = _owner(path)
    monkeypatch.setattr(pbdata_module, "set_ws_limits", lambda **kwargs: None)
    monkeypatch.setattr(pbdata_module, "set_service_min_level", lambda *args: None)
    _write(path, _valid_ini())
    owner._apply_config_snapshot(load_ini_snapshot(path))
    prior = owner._runtime_config
    prior_generation = owner._config_generation
    _write(path, f"[pbdata]\n{key} = {value}\nws_max = 2\n")

    with pytest.raises(PBDataConfigError) as exc_info:
        owner._apply_config_snapshot(load_ini_snapshot(path))
    assert exc_info.value.key == key
    assert owner._runtime_config is prior
    assert owner._config_generation == prior_generation


def test_load_settings_reads_exactly_one_snapshot(tmp_path: Path, monkeypatch) -> None:
    """One reload attempt consumes exactly one coherent snapshot generation."""
    path = tmp_path / "pbgui.ini"
    _write(path, "[pbdata]\n")
    owner = _owner(path)
    snapshot = load_ini_snapshot(path)
    calls = []
    monkeypatch.setattr(pbdata_module, "load_ini_snapshot", lambda requested: calls.append(requested) or snapshot)
    monkeypatch.setattr(pbdata_module, "set_ws_limits", lambda **kwargs: None)
    monkeypatch.setattr(pbdata_module, "set_service_min_level", lambda *args: None)

    assert owner._load_settings() is True
    assert calls == [path]


def test_rapid_changes_latest_wins_single_watcher_and_clean_shutdown(tmp_path: Path, monkeypatch) -> None:
    """The sole watcher coalesces rapid writes and leaves no task or thread."""
    async def exercise() -> None:
        path = tmp_path / "pbgui.ini"
        _write(path, "[pbdata]\nws_max = 20\n")
        owner = _owner(path)
        monkeypatch.setattr(pbdata_module, "set_ws_limits", lambda **kwargs: None)
        monkeypatch.setattr(pbdata_module, "set_service_min_level", lambda *args: None)

        await owner.start_config_reload()
        first_task = owner._config_reload_task
        first_thread = owner._ini_watcher._thread
        await owner.start_config_reload()
        assert owner._config_reload_task is first_task
        assert owner._ini_watcher._thread is first_thread
        _write(path, "[pbdata]\nws_max = 21\n")
        _write(path, "[pbdata]\nws_max = 29\n")
        for _ in range(30):
            if owner._runtime_config.ws_max == 29:
                break
            await asyncio.sleep(0.05)
        assert owner._runtime_config.ws_max == 29

        await owner.stop_config_reload()
        assert owner._config_reload_task is None
        assert owner._ini_watcher.is_running is False
        assert first_task.done()

    asyncio.run(exercise())


def test_interval_change_cancels_and_awaits_each_poller_once(tmp_path: Path) -> None:
    """Interval application cannot leave an old poller beside its replacement."""
    async def exercise() -> None:
        owner = _owner(tmp_path / "pbgui.ini")
        started = asyncio.Event()

        async def poller() -> None:
            started.set()
            await asyncio.sleep(60)

        shared = asyncio.create_task(poller())
        combined = asyncio.create_task(poller())
        await started.wait()
        owner._shared_combined_task = shared
        owner._shared_history_task = None
        owner._shared_executions_task = None
        owner._shared_history_tasks_by_exchange = {"okx": shared}
        owner._shared_combined_tasks_by_exchange = {"binance": combined}
        owner._poll_intervals_changed = True

        await owner._restart_pollers_for_interval_change()
        assert shared.cancelled()
        assert combined.cancelled()
        assert owner._shared_history_tasks_by_exchange == {}
        assert owner._shared_combined_tasks_by_exchange == {}
        assert owner._poll_intervals_changed is False
        await owner._restart_pollers_for_interval_change()

    asyncio.run(exercise())


def test_protected_sleeps_are_not_config_interruptible() -> None:
    """Config wakeups remain outside rate/backoff and per-coin pacing sleeps."""
    source = inspect.getsource(pbdata_module)
    assert "await asyncio.sleep(float(self._latest_1m_coin_pause_seconds))" in source
    assert "await asyncio.sleep(pause_val + jitter)" in source
    assert "await asyncio.sleep(min(5 * subscribe_backoff, 30))" in source
    assert "_config_schedule_changed.wait()" in source
    for loop_name in ("_latest_1m_loop", "_binance_latest_1m_loop", "_bybit_latest_1m_loop", "_okx_latest_1m_loop", "_bitget_latest_1m_loop"):
        assert "_load_settings" not in inspect.getsource(getattr(PBData, loop_name))


def test_bounded_shutdown_does_not_wait_for_cancellation_resistant_io(monkeypatch) -> None:
    """A stuck client close cannot consume the systemd shutdown deadline."""
    async def exercise() -> None:
        release = asyncio.Event()
        tasks = []

        async def stuck_close() -> None:
            tasks.append(asyncio.current_task())
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

        monkeypatch.setattr(pbdata_module, "_human_log", lambda *_args, **_kwargs: None)
        started = asyncio.get_running_loop().time()
        assert await _bounded_shutdown_step(stuck_close(), 0.01, "test close") is False
        assert asyncio.get_running_loop().time() - started < 0.5
        assert tasks[0] is not None and not tasks[0].done()
        release.set()
        await tasks[0]

    asyncio.run(exercise())
