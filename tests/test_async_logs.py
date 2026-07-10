"""Tests for async log path resolution helpers."""

import asyncio
from types import SimpleNamespace

import pytest

from master import async_logs
from master.async_monitor import VPSMonitor


class FakePool:
    """Minimal pool stub for PB7 log path resolution tests."""

    def __init__(self, pb7dir: str = ""):
        """Store a fake host pb7dir value."""
        self._pb7dir = pb7dir

    def get_connection(self, hostname: str):
        """Return a fake connection entry with cached host data."""
        return SimpleNamespace(data={"pb7dir": self._pb7dir})


class FakeMonitorPool:
    """Capture fixed remote commands and return configured process output."""

    def __init__(self, process_output: str = "") -> None:
        """Store the fake process listing."""
        self.process_output = process_output
        self.commands: list[str] = []

    async def run(self, hostname: str, command: str, timeout: int = 15):
        """Return process output for ps and success for a numeric kill command."""
        del hostname, timeout
        self.commands.append(command)
        if command == "ps -eo pid=,args=":
            return SimpleNamespace(exit_status=0, stdout=self.process_output)
        return SimpleNamespace(exit_status=0, stdout="")


def test_pb7_logs_path_is_home_relative():
    """Treat sidebar PB7 log paths as host-home/PB7 paths, not PBGui paths."""
    assert async_logs._is_home_relative_log_path("pb7/logs/20260610_215403__bot_config_run.json.log") is True


def test_pbcluster_service_resolves_to_remote_pbgui_log_path():
    """Resolve PBCluster sidebar selection to its PBGui data log file."""

    assert async_logs._resolve_log_path("PBCluster") == "data/logs/PBCluster.log"


def test_remote_pb7_logs_path_uses_cached_pb7dir():
    """Resolve remote sidebar PB7 log paths through cached pb7dir."""
    path = async_logs._remote_log_shell_path(
        FakePool("/srv/passivbot7"),
        "manibot52",
        "pb7/logs/20260610_215403__bot_config_run.json.log",
    )

    assert path == "/srv/passivbot7/logs/20260610_215403__bot_config_run.json.log"


def test_remote_pb7_logs_path_defaults_to_home_software_pb7():
    """Resolve remote sidebar PB7 log paths to ~/software/pb7 when pb7dir is unknown."""
    path = async_logs._remote_log_shell_path(
        FakePool(),
        "manibot52",
        "pb7/logs/20260610_215403__bot_config_run.json.log",
    )

    assert path == '"$HOME"/software/pb7/logs/20260610_215403__bot_config_run.json.log'


@pytest.mark.parametrize(
    "path",
    [
        "../../.ssh/id_ed25519",
        "data/logs/../../../etc/passwd",
        "/etc/passwd",
        "~/secret.log",
        "data/logs/config.json",
        "data/run_v7/bot/config.json",
    ],
)
def test_remote_log_path_rejects_traversal_and_non_log_files(path):
    """Keep remote reads inside supported log roots and file types."""
    with pytest.raises(ValueError):
        async_logs._resolve_log_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "data/logs/PBRun.log",
        "data/logs/PBRun.log.1",
        "data/run_v7/bybit_SOLUSDT/passivbot_err.log",
        "pb7/logs/20260610_215403__bot_config_run.json.log",
        "software/pb7/logs/bybit_SOLUSDT.log",
    ],
)
def test_remote_log_path_accepts_supported_log_locations(path):
    """Preserve all remote log locations used by the monitor UI."""
    assert async_logs._resolve_log_path(path) == path


def test_remote_bot_log_rejects_traversal_name():
    """Prevent bot names from escaping the configured PB7 log directory."""
    with pytest.raises(ValueError):
        async_logs.resolve_bot_log_path("../../.ssh/id_ed25519", "7")


@pytest.mark.parametrize("value", ["1; touch /tmp/pwn", "$(id)", -1, 50_001, True])
def test_remote_log_line_count_rejects_unsafe_values(value):
    """Reject values which could alter a remote shell command or exhaust output."""
    with pytest.raises(ValueError):
        async_logs.normalize_remote_log_lines(value)


@pytest.mark.parametrize(("value", "expected"), [(0, 0), (200, 200), ("5000", 5000)])
def test_remote_log_line_count_accepts_supported_values(value, expected):
    """Accept integer line counts used by the shared log viewer."""
    assert async_logs.normalize_remote_log_lines(value) == expected


def test_get_recent_logs_rejects_injection_before_remote_command():
    """Never pass a malicious line-count value to the SSH pool."""
    streamer = async_logs.AsyncLogStreamer(FakePool())

    with pytest.raises(ValueError):
        asyncio.run(streamer.get_recent_logs("host", "PBRun", "1; touch /tmp/pwn"))


def test_kill_instance_never_interpolates_name_into_remote_shell():
    """Keep shell metacharacters in a bot name out of every remote command."""
    pool = FakeMonitorPool()
    monitor = SimpleNamespace(pool=pool)
    malicious_name = "bot';touch_pwn;#"

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", malicious_name))

    assert result == {"success": False, "pid": ""}
    assert pool.commands == ["ps -eo pid=,args="]
    assert all(malicious_name not in command for command in pool.commands)


def test_kill_instance_uses_only_parsed_numeric_pid():
    """Kill the process whose config path exactly identifies the requested bot."""
    process_output = (
        " 123 /usr/bin/python /srv/pb7/src/main.py "
        "/home/pbgui/software/pbgui/data/run_v7/bybit_SOLUSDT/config_run.json\n"
    )
    pool = FakeMonitorPool(process_output)
    monitor = SimpleNamespace(pool=pool)

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", "bybit_SOLUSDT"))

    assert result == {"success": True, "pid": "123"}
    assert pool.commands == ["ps -eo pid=,args=", "kill -- 123"]
