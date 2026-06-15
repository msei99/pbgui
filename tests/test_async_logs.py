"""Tests for async log path resolution helpers."""

from types import SimpleNamespace

from master import async_logs


class FakePool:
    """Minimal pool stub for PB7 log path resolution tests."""

    def __init__(self, pb7dir: str = ""):
        """Store a fake host pb7dir value."""
        self._pb7dir = pb7dir

    def get_connection(self, hostname: str):
        """Return a fake connection entry with cached host data."""
        return SimpleNamespace(data={"pb7dir": self._pb7dir})


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
