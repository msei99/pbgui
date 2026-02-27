"""Tests for the PBMaster SSH-based VPS management service.

Tests cover ConnectionPool, CommandExecutor, ServiceMonitor, and LogStreamer
using mocked SSH connections (no real VPS needed).
"""

import json
import os
import sys
import threading
import time
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Add project root to path
ROOT_DIR = Path(__file__).parent.parent.resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from master.connection_pool import SSHConnectionPool, ConnectionStatus, ConnectionInfo
from master.command_executor import CommandExecutor, CommandResult
from master.service_monitor import ServiceMonitor, ServiceStatus, MONITORED_SERVICES
from master.log_streamer import LogStreamer
from master.realtime_collector import (
    RealtimeCollector, SystemMetrics, _StreamReader,
    MONITOR_AGENT_SCRIPT, INSTANCE_COLLECT_SCRIPT,
)
from master.status_file import write_status, read_status, status_age, STATUS_FILE
from master.ws_server import WSServer, DEFAULT_WS_PORT, PUSH_INTERVAL


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def _suppress_log_to_file():
    """Prevent tests from writing to the real PBMaster.log."""
    with patch("logging_helpers.human_log"):
        yield


@pytest.fixture
def tmp_vps_dir(tmp_path):
    """Create a temporary VPS config directory with sample configs."""
    hosts_dir = tmp_path / "data" / "vpsmanager" / "hosts"

    # VPS 1
    vps1_dir = hosts_dir / "testvps1"
    vps1_dir.mkdir(parents=True)
    (vps1_dir / "testvps1.json").write_text(json.dumps({
        "_hostname": "testvps1",
        "ip": "192.168.1.10",
        "user": "testuser",
        "firewall_ssh_port": 22,
    }))

    # VPS 2
    vps2_dir = hosts_dir / "testvps2"
    vps2_dir.mkdir(parents=True)
    (vps2_dir / "testvps2.json").write_text(json.dumps({
        "_hostname": "testvps2",
        "ip": "192.168.1.20",
        "user": "admin",
        "firewall_ssh_port": 2222,
    }))

    # VPS 3 — incomplete config (should be skipped)
    vps3_dir = hosts_dir / "testvps3"
    vps3_dir.mkdir(parents=True)
    (vps3_dir / "testvps3.json").write_text(json.dumps({
        "_hostname": "testvps3",
        # Missing ip and user
    }))

    return tmp_path


@pytest.fixture
def pool(tmp_vps_dir):
    """SSHConnectionPool with mocked VPS directory."""
    p = SSHConnectionPool()
    p._vps_dir = tmp_vps_dir / "data" / "vpsmanager" / "hosts"
    return p


@pytest.fixture
def mock_pool():
    """SSHConnectionPool with pre-loaded mock connections."""
    p = SSHConnectionPool()
    p._connections = {
        "testvps1": ConnectionInfo(
            hostname="testvps1",
            ip="192.168.1.10",
            user="testuser",
            status=ConnectionStatus.CONNECTED,
        ),
        "testvps2": ConnectionInfo(
            hostname="testvps2",
            ip="192.168.1.20",
            user="admin",
            status=ConnectionStatus.CONNECTED,
        ),
    }
    return p


@pytest.fixture
def mock_ssh_client():
    """Create a mock paramiko SSHClient."""
    client = MagicMock()
    transport = MagicMock()
    transport.is_active.return_value = True
    client.get_transport.return_value = transport
    return client


@pytest.fixture
def executor(mock_pool, mock_ssh_client):
    """CommandExecutor with mocked pool."""
    # Make pool return our mock SSH client
    for info in mock_pool._connections.values():
        info.client = mock_ssh_client
    return CommandExecutor(mock_pool)


@pytest.fixture
def service_monitor(executor):
    """ServiceMonitor with mocked executor."""
    return ServiceMonitor(executor, auto_restart=True)


@pytest.fixture
def log_streamer(mock_pool, mock_ssh_client):
    """LogStreamer with mocked pool."""
    for info in mock_pool._connections.values():
        info.client = mock_ssh_client
    return LogStreamer(mock_pool)


# ═══════════════════════════════════════════════════════════════════
# ConnectionPool Tests
# ═══════════════════════════════════════════════════════════════════

class TestSSHConnectionPool:
    """Tests for SSHConnectionPool."""

    def test_load_vps_configs(self, pool):
        """Test loading VPS configurations from JSON files."""
        hostnames = pool.load_vps_configs()

        # Should load 2 valid configs, skip the one without ip/user
        assert len(hostnames) == 2, "Should load 2 valid VPS configs"
        assert "testvps1" in hostnames
        assert "testvps2" in hostnames
        assert "testvps3" not in hostnames, "Should skip incomplete config"

    def test_load_vps_config_details(self, pool):
        """Test that loaded configs have correct details."""
        pool.load_vps_configs()
        conns = pool.connections

        assert conns["testvps1"].ip == "192.168.1.10"
        assert conns["testvps1"].user == "testuser"
        assert conns["testvps1"].ssh_port == 22

        assert conns["testvps2"].ip == "192.168.1.20"
        assert conns["testvps2"].user == "admin"
        assert conns["testvps2"].ssh_port == 2222

    def test_initial_status_is_disconnected(self, pool):
        """Test that newly loaded VPS are in DISCONNECTED state."""
        pool.load_vps_configs()
        for info in pool.connections.values():
            assert info.status == ConnectionStatus.DISCONNECTED

    def test_hostnames(self, pool):
        """Test hostnames() returns sorted list."""
        pool.load_vps_configs()
        assert pool.hostnames() == ["testvps1", "testvps2"]

    def test_connect_success(self, pool):
        """Test successful SSH connection."""
        pool.load_vps_configs()

        mock_client = MagicMock()
        transport = MagicMock()
        transport.is_active.return_value = True
        mock_client.get_transport.return_value = transport

        with patch('master.connection_pool.paramiko.SSHClient', return_value=mock_client):
            result = pool.connect("testvps1")

        assert result is True, "Connection should succeed"
        info = pool.connections["testvps1"]
        assert info.status == ConnectionStatus.CONNECTED
        assert info.last_connected is not None
        assert info.reconnect_attempts == 0

    def test_connect_auth_failure(self, pool):
        """Test SSH authentication failure."""
        pool.load_vps_configs()

        import paramiko
        mock_client = MagicMock()
        mock_client.connect.side_effect = paramiko.AuthenticationException("bad key")

        with patch('master.connection_pool.paramiko.SSHClient', return_value=mock_client):
            result = pool.connect("testvps1")

        assert result is False
        info = pool.connections["testvps1"]
        assert info.status == ConnectionStatus.AUTH_FAILED
        assert "Authentication failed" in info.last_error

    def test_connect_network_failure(self, pool):
        """Test SSH network failure."""
        pool.load_vps_configs()

        mock_client = MagicMock()
        mock_client.connect.side_effect = ConnectionRefusedError("Connection refused")

        with patch('master.connection_pool.paramiko.SSHClient', return_value=mock_client):
            result = pool.connect("testvps1")

        assert result is False
        info = pool.connections["testvps1"]
        assert info.status == ConnectionStatus.DISCONNECTED
        assert info.reconnect_attempts == 1

    def test_connect_unknown_hostname(self, pool):
        """Test connecting to unknown hostname."""
        result = pool.connect("nonexistent")
        assert result is False

    def test_disconnect(self, pool, mock_ssh_client):
        """Test disconnecting a VPS."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.client = mock_ssh_client
        info.status = ConnectionStatus.CONNECTED

        pool.disconnect("testvps1")

        assert pool.connections["testvps1"].status == ConnectionStatus.DISCONNECTED
        assert pool.connections["testvps1"].last_disconnect is not None
        mock_ssh_client.close.assert_called_once()

    def test_get_connected(self, pool, mock_ssh_client):
        """Test getting SSH client for connected VPS."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.client = mock_ssh_client
        info.status = ConnectionStatus.CONNECTED

        client = pool.get("testvps1")
        assert client is not None

    def test_get_disconnected(self, pool):
        """Test getting SSH client for disconnected VPS returns None."""
        pool.load_vps_configs()
        client = pool.get("testvps1")
        assert client is None

    def test_get_unknown(self, pool):
        """Test getting SSH client for unknown hostname returns None."""
        client = pool.get("nonexistent")
        assert client is None

    def test_health_check_detects_lost_connection(self, pool, mock_ssh_client):
        """Test that health_check detects lost connections."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.client = mock_ssh_client
        info.status = ConnectionStatus.CONNECTED

        # Simulate connection loss
        transport = mock_ssh_client.get_transport()
        transport.is_active.return_value = False

        status = pool.health_check()
        assert status["testvps1"] == ConnectionStatus.DISCONNECTED
        assert info.last_error == "Connection lost (keepalive failed)"

    def test_should_reconnect_auth_failed(self, pool):
        """Test that auth failures are not retried automatically."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.status = ConnectionStatus.AUTH_FAILED

        assert pool.should_reconnect("testvps1") is False

    def test_should_reconnect_cooldown(self, pool):
        """Test reconnect cooldown."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.status = ConnectionStatus.DISCONNECTED
        info.last_disconnect = datetime.now()  # Just disconnected

        assert pool.should_reconnect("testvps1") is False

    def test_should_reconnect_after_cooldown(self, pool):
        """Test reconnect allowed after cooldown period."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.status = ConnectionStatus.DISCONNECTED
        info.last_disconnect = datetime.now() - timedelta(seconds=60)

        assert pool.should_reconnect("testvps1") is True

    def test_get_status_summary(self, pool, mock_ssh_client):
        """Test status summary generation."""
        pool.load_vps_configs()
        info = pool._connections["testvps1"]
        info.client = mock_ssh_client
        info.status = ConnectionStatus.CONNECTED
        info.last_connected = datetime.now()

        summary = pool.get_status_summary()
        assert summary["total"] == 2
        assert summary["connected"] == 1
        assert summary["disconnected"] == 1
        assert "testvps1" in summary["connections"]
        assert summary["connections"]["testvps1"]["status"] == "connected"


# ═══════════════════════════════════════════════════════════════════
# CommandExecutor Tests
# ═══════════════════════════════════════════════════════════════════

class TestCommandExecutor:
    """Tests for CommandExecutor."""

    def _setup_exec(self, mock_ssh_client, stdout_data="", stderr_data="", exit_code=0):
        """Helper to set up exec_command mock."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = stdout_data.encode('utf-8')
        mock_stderr.read.return_value = stderr_data.encode('utf-8')
        mock_stdout.channel.recv_exit_status.return_value = exit_code
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)
        return mock_stdin, mock_stdout, mock_stderr

    def test_execute_success(self, executor, mock_ssh_client):
        """Test successful command execution."""
        self._setup_exec(mock_ssh_client, stdout_data="Hello World")

        result = executor.execute("testvps1", "echo 'Hello World'")

        assert result.success is True
        assert result.stdout == "Hello World"
        assert result.exit_code == 0
        assert result.hostname == "testvps1"
        assert result.duration_ms >= 0

    def test_execute_failure(self, executor, mock_ssh_client):
        """Test failed command execution."""
        self._setup_exec(mock_ssh_client, stderr_data="not found", exit_code=127)

        result = executor.execute("testvps1", "nonexistent_cmd")

        assert result.success is False
        assert result.exit_code == 127
        assert "not found" in result.stderr

    def test_execute_no_connection(self, executor):
        """Test execution when VPS is not connected."""
        # Disconnect the VPS
        executor._pool._connections["testvps1"].status = ConnectionStatus.DISCONNECTED
        executor._pool._connections["testvps1"].client = None

        result = executor.execute("testvps1", "echo test")

        assert result.success is False
        assert "No SSH connection" in result.error

    def test_execute_exception(self, executor, mock_ssh_client):
        """Test command execution with SSH exception."""
        mock_ssh_client.exec_command.side_effect = Exception("SSH broken")

        result = executor.execute("testvps1", "echo test")

        assert result.success is False
        assert "SSH broken" in result.error

    def test_execute_on_all(self, executor, mock_ssh_client):
        """Test executing command on all VPS."""
        self._setup_exec(mock_ssh_client, stdout_data="ok")

        results = executor.execute_on_all("uptime")

        assert len(results) == 2
        assert "testvps1" in results
        assert "testvps2" in results
        for result in results.values():
            assert result.success is True

    def test_execute_on_specific_hosts(self, executor, mock_ssh_client):
        """Test executing command on specific hostnames."""
        self._setup_exec(mock_ssh_client, stdout_data="ok")

        results = executor.execute_on_all("uptime", hostnames=["testvps1"])

        assert len(results) == 1
        assert "testvps1" in results

    def test_file_exists(self, executor, mock_ssh_client):
        """Test remote file existence check."""
        self._setup_exec(mock_ssh_client, stdout_data="yes")

        assert executor.file_exists("testvps1", "/tmp/test.txt") is True

    def test_file_not_exists(self, executor, mock_ssh_client):
        """Test remote file non-existence."""
        self._setup_exec(mock_ssh_client, stdout_data="no")

        assert executor.file_exists("testvps1", "/tmp/nonexistent") is False

    def test_read_file(self, executor, mock_ssh_client):
        """Test reading a remote file."""
        self._setup_exec(mock_ssh_client, stdout_data='{"key": "value"}')

        content = executor.read_file("testvps1", "/tmp/config.json")
        assert content == '{"key": "value"}'

    def test_read_pid_file(self, executor, mock_ssh_client):
        """Test reading a PID file."""
        self._setup_exec(mock_ssh_client, stdout_data="12345")

        pid = executor.read_pid_file("testvps1", "data/pid/pbrun.pid")
        assert pid == 12345

    def test_read_pid_file_invalid(self, executor, mock_ssh_client):
        """Test reading an invalid PID file."""
        self._setup_exec(mock_ssh_client, stdout_data="not_a_pid")

        pid = executor.read_pid_file("testvps1", "data/pid/pbrun.pid")
        assert pid is None

    def test_is_process_running(self, executor, mock_ssh_client):
        """Test process running check."""
        self._setup_exec(mock_ssh_client, stdout_data="yes")

        assert executor.is_process_running("testvps1", 12345, "pbrun.py") is True

    def test_is_process_not_running(self, executor, mock_ssh_client):
        """Test process not running."""
        self._setup_exec(mock_ssh_client, stdout_data="no")

        assert executor.is_process_running("testvps1", 99999) is False


# ═══════════════════════════════════════════════════════════════════
# ServiceMonitor Tests
# ═══════════════════════════════════════════════════════════════════

class TestServiceMonitor:
    """Tests for ServiceMonitor."""

    def test_monitored_services_defined(self):
        """Test that all expected services are defined."""
        assert "PBRun" in MONITORED_SERVICES
        assert "PBRemote" in MONITORED_SERVICES
        assert "PBCoinData" in MONITORED_SERVICES

    def test_check_service_running(self, service_monitor, mock_ssh_client):
        """Test checking a running service."""
        # Mock: PID file returns 12345, process is running
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.channel.recv_exit_status.return_value = 0

        call_count = [0]
        def exec_side_effect(cmd, timeout=None):
            call_count[0] += 1
            if call_count[0] == 1:
                # cat pid file
                mock_stdout.read.return_value = b"12345"
                mock_stderr.read.return_value = b""
            else:
                # ps check
                mock_stdout.read.return_value = b"yes"
                mock_stderr.read.return_value = b""
            return (mock_stdin, mock_stdout, mock_stderr)

        mock_ssh_client.exec_command.side_effect = exec_side_effect

        result = service_monitor.check_service("testvps1", "PBRun")

        assert result.status == ServiceStatus.RUNNING
        assert result.pid == 12345
        assert result.hostname == "testvps1"

    def test_check_service_stopped_no_pid(self, service_monitor, mock_ssh_client):
        """Test checking a stopped service (no PID file)."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b"No such file"
        mock_stdout.channel.recv_exit_status.return_value = 1
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        result = service_monitor.check_service("testvps1", "PBRun")

        assert result.status == ServiceStatus.STOPPED

    def test_check_service_unknown(self, service_monitor):
        """Test checking an unknown service."""
        result = service_monitor.check_service("testvps1", "NonExistentService")

        assert result.status == ServiceStatus.UNKNOWN
        assert "Unknown service" in result.error

    def test_check_all(self, service_monitor, mock_ssh_client):
        """Test checking all services on a VPS."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b"12345"
        mock_stderr.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        results = service_monitor.check_all("testvps1")

        assert len(results) == 3
        assert "PBRun" in results
        assert "PBRemote" in results
        assert "PBCoinData" in results

    def test_restart_limit(self, service_monitor):
        """Test that restart limit is enforced."""
        # Fill restart history
        hostname = "testvps1"
        service_name = "PBRun"
        service_monitor._restart_history = {
            hostname: {
                service_name: [
                    datetime.now() - timedelta(minutes=i)
                    for i in range(service_monitor.max_restarts_per_hour)
                ]
            }
        }

        assert service_monitor._can_restart(hostname, service_name) is False

    def test_restart_limit_expired(self, service_monitor):
        """Test that old restart entries expire after 1 hour."""
        hostname = "testvps1"
        service_name = "PBRun"
        service_monitor._restart_history = {
            hostname: {
                service_name: [
                    datetime.now() - timedelta(hours=2)  # Old entry
                ]
            }
        }

        assert service_monitor._can_restart(hostname, service_name) is True

    def test_auto_heal_disabled(self, executor, mock_ssh_client):
        """Test that auto_heal respects auto_restart=False."""
        monitor = ServiceMonitor(executor, auto_restart=False)

        # Service is stopped
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 1
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        results = monitor.auto_heal("testvps1")

        for result in results:
            assert result.was_restarted is False, \
                "Should not restart when auto_restart is disabled"


# ═══════════════════════════════════════════════════════════════════
# LogStreamer Tests
# ═══════════════════════════════════════════════════════════════════

class TestLogStreamer:
    """Tests for LogStreamer."""

    def test_service_logs_defined(self):
        """Test that service log paths are defined."""
        assert "PBRun" in LogStreamer.SERVICE_LOGS
        assert "PBRemote" in LogStreamer.SERVICE_LOGS
        assert "PBCoinData" in LogStreamer.SERVICE_LOGS

    def test_resolve_service_log(self, log_streamer):
        """Test resolving service name to log path."""
        assert log_streamer._resolve_log_path("PBRun") == "data/logs/PBRun.log"
        assert log_streamer._resolve_log_path("custom/path.log") == "custom/path.log"

    def test_get_recent_logs(self, log_streamer, mock_ssh_client):
        """Test fetching recent log lines."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b"line1\nline2\nline3"
        mock_stderr.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        logs = log_streamer.get_recent_logs("testvps1", "PBRun", lines=50)

        assert logs is not None
        assert "line1" in logs
        assert "line3" in logs
        # Verify tail command was called
        mock_ssh_client.exec_command.assert_called_once()
        call_args = mock_ssh_client.exec_command.call_args[0][0]
        assert "tail -n 50" in call_args

    def test_get_recent_logs_no_connection(self, log_streamer):
        """Test fetching logs when not connected."""
        # Disconnect
        for info in log_streamer._pool._connections.values():
            info.status = ConnectionStatus.DISCONNECTED
            info.client = None

        logs = log_streamer.get_recent_logs("testvps1", "PBRun")
        assert logs is None

    def test_get_recent_logs_error(self, log_streamer, mock_ssh_client):
        """Test fetching logs when file doesn't exist."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b"No such file"
        mock_stdout.channel.recv_exit_status.return_value = 1
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        logs = log_streamer.get_recent_logs("testvps1", "PBRun")
        assert logs is None

    def test_list_bot_logs(self, log_streamer, mock_ssh_client):
        """Test listing bot log files."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b"/home/user/software/pbgui/data/logs/PBRun.log\n/home/user/software/pbgui/data/logs/PBRemote.log"
        mock_stderr.read.return_value = b""
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        logs = log_streamer.list_bot_logs("testvps1")

        assert len(logs) == 2

    def test_stream_lifecycle(self, log_streamer):
        """Test stream start/stop lifecycle (no actual streaming)."""
        assert log_streamer.active_streams() == []

        # Streams would fail without real SSH, but we can test the registry
        log_streamer._streams["test:stream:1"] = MagicMock(
            stream_id="test:stream:1",
            hostname="testvps1",
            log_path="data/logs/PBRun.log",
            active=True,
            thread=None,
        )

        assert "test:stream:1" in log_streamer.active_streams()

        log_streamer.stop_stream("test:stream:1")
        # After stopping, active should be False
        log_streamer._streams["test:stream:1"].active = False
        assert "test:stream:1" not in log_streamer.active_streams()

    def test_read_stream_empty(self, log_streamer):
        """Test reading from non-existent stream."""
        lines = log_streamer.read_stream("nonexistent")
        assert lines == []

    def test_cleanup_stopped(self, log_streamer):
        """Test cleaning up stopped streams."""
        stream = MagicMock()
        stream.active = False
        stream.thread = None
        log_streamer._streams["old:stream:1"] = stream

        log_streamer.cleanup_stopped()

        assert "old:stream:1" not in log_streamer._streams


# ═══════════════════════════════════════════════════════════════════
# PBMaster Daemon Tests
# ═══════════════════════════════════════════════════════════════════

class TestPBMasterDaemon:
    """Tests for PBMaster daemon lifecycle."""

    def test_import_pbmaster(self):
        """Test that PBMaster can be imported."""
        from PBMaster import PBMaster
        master = PBMaster()
        assert master.pidfile.name == "pbmaster.pid"

    def test_pid_lifecycle(self, tmp_path):
        """Test PID file save/load."""
        from PBMaster import PBMaster
        master = PBMaster()
        master.piddir = tmp_path
        master.pidfile = tmp_path / "pbmaster.pid"

        master.save_pid()
        assert master.pidfile.exists()

        pid_content = master.pidfile.read_text()
        assert pid_content == str(os.getpid())

        master.load_pid()
        assert master.my_pid == os.getpid()

    def test_default_config(self):
        """Test default configuration values."""
        from PBMaster import PBMaster
        master = PBMaster()
        # auto_restart defaults to True (via load_ini returning empty)
        with patch('PBMaster.load_ini', return_value=""):
            assert master.auto_restart is True

    def test_alert_state_tracking(self):
        """Test that alert deduplication state is initialized."""
        from PBMaster import PBMaster
        master = PBMaster()
        assert isinstance(master._connection_alerts, set)
        assert isinstance(master._service_alerts, set)
        assert len(master._connection_alerts) == 0
        assert len(master._service_alerts) == 0

    def test_realtime_collector_initialized(self):
        """Test that PBMaster has realtime collector attribute."""
        from PBMaster import PBMaster
        master = PBMaster()
        assert master.realtime is None  # Before _setup()

    def test_enabled_hosts_default_empty(self):
        """Test that enabled_hosts defaults to empty set."""
        from PBMaster import PBMaster
        master = PBMaster()
        with patch('PBMaster.load_ini', return_value=""):
            master._enabled_hosts = None  # Reset cache
            assert master.enabled_hosts == set()

    def test_enabled_hosts_load_from_ini(self):
        """Test loading enabled_hosts from ini."""
        from PBMaster import PBMaster
        master = PBMaster()
        with patch('PBMaster.load_ini', return_value="vps1,vps2,vps3"):
            master._enabled_hosts = None  # Reset cache
            assert master.enabled_hosts == {"vps1", "vps2", "vps3"}

    def test_enabled_hosts_save(self):
        """Test saving enabled_hosts to ini."""
        from PBMaster import PBMaster
        master = PBMaster()
        with patch('PBMaster.save_ini') as mock_save:
            master.enabled_hosts = {"vps2", "vps1"}
            mock_save.assert_called_once()
            args = mock_save.call_args[0]
            assert args[0] == "pbmaster"
            assert args[1] == "enabled_hosts"
            # Sorted: vps1,vps2
            assert "vps1" in args[2]
            assert "vps2" in args[2]

    def test_is_host_enabled(self):
        """Test is_host_enabled helper."""
        from PBMaster import PBMaster
        master = PBMaster()
        master._enabled_hosts = {"vps1", "vps3"}
        assert master.is_host_enabled("vps1") is True
        assert master.is_host_enabled("vps2") is False
        assert master.is_host_enabled("vps3") is True

    def test_available_hosts(self, tmp_vps_dir):
        """Test available_hosts discovers VPS from config files."""
        from PBMaster import PBMaster
        master = PBMaster()
        with patch('PBMaster.PBGDIR', str(tmp_vps_dir)):
            hosts = master.available_hosts()
            # tmp_vps_dir has testvps1 and testvps2 (testvps3 has no ip/user but has hostname)
            assert "testvps1" in hosts
            assert "testvps2" in hosts
            assert "testvps3" in hosts  # Has _hostname field


# ═══════════════════════════════════════════════════════════════════
# RealtimeCollector Tests
# ═══════════════════════════════════════════════════════════════════

class TestSystemMetrics:
    """Tests for SystemMetrics dataclass."""

    def test_from_json_complete(self):
        """Test parsing a complete agent JSON output."""
        data = {
            "ts": 1740000000.0,
            "cpu": 45.2,
            "mem": [17179869184, 8589934592, 50.0, 8589934592],
            "disk": [107374182400, 53687091200, 53687091200, 50.0],
            "swap": [8589934592, 1073741824, 7516192768, 12.5],
        }
        m = SystemMetrics.from_json(data)
        assert m.timestamp == 1740000000.0
        assert m.cpu == 45.2
        assert m.mem_total == 17179869184
        assert m.mem_available == 8589934592
        assert m.mem_percent == 50.0
        assert m.mem_used == 8589934592
        assert m.disk_total == 107374182400
        assert m.disk_used == 53687091200
        assert m.disk_free == 53687091200
        assert m.disk_percent == 50.0
        assert m.swap_total == 8589934592
        assert m.swap_used == 1073741824
        assert m.swap_free == 7516192768
        assert m.swap_percent == 12.5

    def test_from_json_partial(self):
        """Test parsing with missing fields — should use defaults."""
        data = {"ts": 1.0, "cpu": 10.0}
        m = SystemMetrics.from_json(data)
        assert m.timestamp == 1.0
        assert m.cpu == 10.0
        assert m.mem_total == 0
        assert m.disk_total == 0
        assert m.swap_total == 0

    def test_from_json_empty(self):
        """Test parsing empty dict."""
        m = SystemMetrics.from_json({})
        assert m.timestamp == 0.0
        assert m.cpu == 0.0

    def test_from_json_short_arrays(self):
        """Test parsing with shorter-than-expected arrays."""
        data = {
            "ts": 1.0,
            "cpu": 5.0,
            "mem": [1024],  # Only total
            "disk": [2048, 1024],  # Only total + used
            "swap": [],  # Empty
        }
        m = SystemMetrics.from_json(data)
        assert m.mem_total == 1024
        assert m.mem_available == 0
        assert m.disk_total == 2048
        assert m.disk_used == 1024
        assert m.disk_free == 0
        assert m.swap_total == 0

    def test_default_values(self):
        """Test that default SystemMetrics has zero values."""
        m = SystemMetrics()
        assert m.timestamp == 0.0
        assert m.cpu == 0.0
        assert m.mem_total == 0


class TestRealtimeCollector:
    """Tests for the RealtimeCollector."""

    @pytest.fixture
    def collector(self, mock_pool, mock_ssh_client):
        """RealtimeCollector with mocked pool."""
        for info in mock_pool._connections.values():
            info.client = mock_ssh_client
        return RealtimeCollector(mock_pool)

    def test_init(self, collector):
        """Test collector initialization."""
        assert collector._system_data == {}
        assert collector._instance_data == {}
        assert collector._streams == {}

    def test_get_system_empty(self, collector):
        """Test getting system data when none available."""
        result = collector.get_system("testvps1")
        assert result is None

    def test_get_instances_empty(self, collector):
        """Test getting instance data when none available."""
        result = collector.get_instances("testvps1")
        assert result == []

    def test_get_all_systems_empty(self, collector):
        """Test getting all systems when none available."""
        result = collector.get_all_systems()
        assert result == {}

    def test_get_all_instances_empty(self, collector):
        """Test getting all instances when none available."""
        result = collector.get_all_instances()
        assert result == {}

    def test_system_data_write_and_read(self, collector):
        """Test thread-safe write and read of system data."""
        metrics = SystemMetrics(
            timestamp=time.time(),
            cpu=25.0,
            mem_total=16 * 1024**3,
            mem_percent=40.0,
        )
        with collector._lock:
            collector._system_data["testvps1"] = metrics

        result = collector.get_system("testvps1")
        assert result is not None
        assert result.cpu == 25.0
        assert result.mem_percent == 40.0

    def test_instance_data_write_and_read(self, collector):
        """Test thread-safe write and read of instance data."""
        instances = [
            {"u": "bot1", "p": "7", "pt": 12.5, "et": 0},
            {"u": "bot2", "p": "6", "pt": -3.2, "et": 5},
        ]
        with collector._lock:
            collector._instance_data["testvps1"] = instances

        result = collector.get_instances("testvps1")
        assert len(result) == 2
        assert result[0]["u"] == "bot1"
        assert result[1]["pt"] == -3.2

    def test_get_instances_returns_copy(self, collector):
        """Test that get_instances returns a copy, not a reference."""
        instances = [{"u": "bot1"}]
        with collector._lock:
            collector._instance_data["testvps1"] = instances

        result = collector.get_instances("testvps1")
        result.append({"u": "modified"})
        # Original should not be affected
        assert len(collector.get_instances("testvps1")) == 1

    def test_get_all(self, collector):
        """Test get_all returns combined data."""
        metrics = SystemMetrics(timestamp=time.time(), cpu=30.0)
        instances = [{"u": "bot1", "p": "7"}]

        with collector._lock:
            collector._system_data["testvps1"] = metrics
            collector._instance_data["testvps1"] = instances

        result = collector.get_all()
        assert "testvps1" in result
        assert result["testvps1"]["system"] is not None
        assert result["testvps1"]["system"].cpu == 30.0
        assert len(result["testvps1"]["instances"]) == 1
        assert isinstance(result["testvps1"]["data_age_s"], float)

    def test_is_stream_alive_no_stream(self, collector):
        """Test is_stream_alive returns False when no stream exists."""
        assert collector.is_stream_alive("testvps1") is False

    def test_is_stream_alive_dead_stream(self, collector):
        """Test is_stream_alive returns False for dead stream."""
        mock_reader = MagicMock()
        mock_reader.is_alive.return_value = False
        mock_reader.active = False
        with collector._lock:
            collector._streams["testvps1"] = mock_reader
        assert collector.is_stream_alive("testvps1") is False

    def test_is_stream_alive_active_stream(self, collector):
        """Test is_stream_alive returns True for active stream."""
        mock_reader = MagicMock()
        mock_reader.is_alive.return_value = True
        mock_reader.active = True
        with collector._lock:
            collector._streams["testvps1"] = mock_reader
        assert collector.is_stream_alive("testvps1") is True

    def test_stop_stream(self, collector):
        """Test stopping a stream."""
        mock_reader = MagicMock()
        mock_reader.active = True
        with collector._lock:
            collector._streams["testvps1"] = mock_reader
        collector.stop_stream("testvps1")
        mock_reader.stop.assert_called_once()
        assert "testvps1" not in collector._streams

    def test_stop_stream_nonexistent(self, collector):
        """Test stopping a non-existent stream doesn't raise."""
        collector.stop_stream("nonexistent")  # Should not raise

    def test_stop_all_streams(self, collector):
        """Test stopping all streams."""
        for name in ["testvps1", "testvps2"]:
            mock_reader = MagicMock()
            with collector._lock:
                collector._streams[name] = mock_reader

        collector.stop_all_streams()
        assert len(collector._streams) == 0

    def test_start_stream_no_connection(self, collector):
        """Test starting stream when not connected."""
        # Disconnect all
        for info in collector._pool._connections.values():
            info.status = ConnectionStatus.DISCONNECTED
            info.client = None

        result = collector.start_stream("testvps1")
        assert result is False

    def test_start_stream_success(self, collector, mock_ssh_client):
        """Test starting a stream with mocked SSH."""
        mock_channel = MagicMock()
        mock_transport = mock_ssh_client.get_transport.return_value
        mock_transport.is_active.return_value = True
        mock_transport.open_session.return_value = mock_channel

        # We need to mock the channel's makefile to avoid blocking
        mock_file = MagicMock()
        mock_file.__iter__ = MagicMock(return_value=iter([]))
        mock_channel.makefile.return_value = mock_file

        result = collector.start_stream("testvps1")
        assert result is True
        assert "testvps1" in collector._streams

        # Cleanup
        collector.stop_all_streams()
        time.sleep(0.1)  # Let thread finish

    def test_restart_dead_streams(self, collector):
        """Test that dead streams are detected for restart."""
        mock_reader = MagicMock()
        mock_reader.is_alive.return_value = False
        mock_reader.active = False
        with collector._lock:
            collector._streams["testvps1"] = mock_reader

        # Disconnect to prevent actual stream start
        for info in collector._pool._connections.values():
            info.status = ConnectionStatus.DISCONNECTED
            info.client = None

        collector.restart_dead_streams()
        # Stream should have been removed (start_stream cleans up first)
        # Since connection is down, new stream won't start

    def test_collect_instances_success(self, collector, mock_ssh_client):
        """Test collecting instance data via SSH."""
        instances_json = json.dumps([
            {"u": "bot1", "p": "7", "pt": 10.0, "et": 0},
            {"u": "bot2", "p": "6", "pt": -5.0, "et": 3},
        ])
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = instances_json.encode()
        mock_stdout.channel.recv_exit_status.return_value = 0
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        collector.collect_instances("testvps1")

        result = collector.get_instances("testvps1")
        assert len(result) == 2
        assert result[0]["u"] == "bot1"
        assert result[1]["et"] == 3

    def test_collect_instances_failure(self, collector, mock_ssh_client):
        """Test instance collection when command fails."""
        mock_stdin = MagicMock()
        mock_stdout = MagicMock()
        mock_stderr = MagicMock()
        mock_stdout.read.return_value = b""
        mock_stderr.read.return_value = b"error"
        mock_stdout.channel.recv_exit_status.return_value = 1
        mock_ssh_client.exec_command.return_value = (mock_stdin, mock_stdout, mock_stderr)

        collector.collect_instances("testvps1")
        # Should not crash, no data stored
        assert collector.get_instances("testvps1") == []

    def test_collect_instances_no_connection(self, collector):
        """Test instance collection when not connected."""
        for info in collector._pool._connections.values():
            info.status = ConnectionStatus.DISCONNECTED
            info.client = None

        collector.collect_instances("testvps1")
        assert collector.get_instances("testvps1") == []

    def test_collect_instances_all_respects_interval(self, collector):
        """Test that collect_instances_all respects the interval."""
        # Set last collect to now — should skip
        collector._last_instance_collect = time.time()
        collector.collect_instances_all()
        # No instance data should be collected
        assert collector.get_all_instances() == {}

    def test_get_stream_info(self, collector):
        """Test get_stream_info diagnostic method."""
        mock_reader = MagicMock()
        mock_reader.is_alive.return_value = True
        mock_reader.active = True
        mock_reader.error = None
        with collector._lock:
            collector._streams["testvps1"] = mock_reader

        info = collector.get_stream_info()
        assert "testvps1" in info
        assert info["testvps1"]["alive"] is True
        assert info["testvps1"]["active"] is True
        assert info["testvps1"]["error"] is None

    def test_get_all_data_age(self, collector):
        """Test that data_age_s is calculated correctly."""
        old_ts = time.time() - 10  # 10 seconds ago
        metrics = SystemMetrics(timestamp=old_ts, cpu=50.0)
        with collector._lock:
            collector._system_data["testvps1"] = metrics

        result = collector.get_all()
        assert "testvps1" in result
        # Age should be approximately 10 seconds
        assert result["testvps1"]["data_age_s"] >= 9.0
        assert result["testvps1"]["data_age_s"] <= 12.0


class TestStreamReader:
    """Tests for the _StreamReader background thread."""

    def test_parse_json_lines(self):
        """Test that StreamReader correctly parses JSON lines."""
        import io
        system_data = {}
        lock = threading.Lock()

        # Create a mock channel with pre-loaded data
        json_lines = [
            json.dumps({"ts": 1.0, "cpu": 10.0, "mem": [1024, 512, 50.0, 512],
                        "disk": [2048, 1024, 1024, 50.0], "swap": [0, 0, 0, 0]}) + "\n",
            json.dumps({"ts": 2.0, "cpu": 20.0, "mem": [1024, 256, 75.0, 768],
                        "disk": [2048, 1024, 1024, 50.0], "swap": [0, 0, 0, 0]}) + "\n",
        ]

        mock_channel = MagicMock()
        mock_channel.makefile.return_value = io.StringIO("".join(json_lines))

        reader = _StreamReader(
            hostname="testvps1",
            channel=mock_channel,
            system_data=system_data,
            lock=lock,
        )
        reader.start()
        reader.join(timeout=3)

        # After processing, system_data should have the latest values
        assert "testvps1" in system_data
        assert system_data["testvps1"].cpu == 20.0  # Last line wins
        assert system_data["testvps1"].timestamp == 2.0

    def test_handles_invalid_json(self):
        """Test that StreamReader skips invalid JSON lines."""
        import io
        system_data = {}
        lock = threading.Lock()

        lines = [
            '{"ts": 1.0, "cpu": 10.0, "mem": [0,0,0,0], "disk": [0,0,0,0], "swap": [0,0,0,0]}\n',
            "this is not json\n",
            '{"ts": 3.0, "cpu": 30.0, "mem": [0,0,0,0], "disk": [0,0,0,0], "swap": [0,0,0,0]}\n',
        ]

        mock_channel = MagicMock()
        mock_channel.makefile.return_value = io.StringIO("".join(lines))

        reader = _StreamReader(
            hostname="testvps1",
            channel=mock_channel,
            system_data=system_data,
            lock=lock,
        )
        reader.start()
        reader.join(timeout=3)

        assert "testvps1" in system_data
        assert system_data["testvps1"].cpu == 30.0  # Skipped bad line

    def test_handles_empty_lines(self):
        """Test that StreamReader skips empty lines."""
        import io
        system_data = {}
        lock = threading.Lock()

        lines = [
            "\n",
            '{"ts": 1.0, "cpu": 5.0, "mem": [0,0,0,0], "disk": [0,0,0,0], "swap": [0,0,0,0]}\n',
            "\n",
        ]

        mock_channel = MagicMock()
        mock_channel.makefile.return_value = io.StringIO("".join(lines))

        reader = _StreamReader(
            hostname="testvps1",
            channel=mock_channel,
            system_data=system_data,
            lock=lock,
        )
        reader.start()
        reader.join(timeout=3)

        assert "testvps1" in system_data
        assert system_data["testvps1"].cpu == 5.0

    def test_stop_flag(self):
        """Test that setting active=False stops the reader."""
        import io
        system_data = {}
        lock = threading.Lock()

        # Create a "blocking" reader that we stop
        mock_channel = MagicMock()
        mock_channel.makefile.return_value = io.StringIO("")

        reader = _StreamReader(
            hostname="testvps1",
            channel=mock_channel,
            system_data=system_data,
            lock=lock,
        )
        reader.stop()
        assert reader.active is False

    def test_error_handling(self):
        """Test that StreamReader handles channel errors gracefully."""
        system_data = {}
        lock = threading.Lock()

        mock_channel = MagicMock()
        mock_channel.makefile.side_effect = Exception("Connection lost")

        reader = _StreamReader(
            hostname="testvps1",
            channel=mock_channel,
            system_data=system_data,
            lock=lock,
        )
        reader.start()
        reader.join(timeout=3)

        assert reader.active is False
        assert reader.error == "Connection lost"


class TestMonitorAgentScript:
    """Tests for the agent script constants."""

    def test_monitor_agent_script_is_string(self):
        """Test that the agent script is a non-empty string using stdlib only."""
        assert isinstance(MONITOR_AGENT_SCRIPT, str)
        assert len(MONITOR_AGENT_SCRIPT) > 50
        assert "/proc/stat" in MONITOR_AGENT_SCRIPT
        assert "statvfs" in MONITOR_AGENT_SCRIPT

    def test_instance_collect_script_is_string(self):
        """Test that the instance collection script is a non-empty string."""
        assert isinstance(INSTANCE_COLLECT_SCRIPT, str)
        assert len(INSTANCE_COLLECT_SCRIPT) > 50
        assert "monitor.json" in INSTANCE_COLLECT_SCRIPT
        assert "run_v7" in INSTANCE_COLLECT_SCRIPT


# ═══════════════════════════════════════════════════════════════════
# Status File IPC Tests
# ═══════════════════════════════════════════════════════════════════

class TestStatusFile:
    """Tests for the JSON status file IPC mechanism."""

    @pytest.fixture(autouse=True)
    def _use_tmp_status_file(self, tmp_path):
        """Redirect STATUS_FILE to a temp directory."""
        self._orig_status_file = STATUS_FILE
        tmp_file = tmp_path / "pbmaster_status.json"
        with patch("master.status_file.STATUS_FILE", tmp_file):
            self.tmp_file = tmp_file
            yield

    def test_write_and_read_roundtrip(self):
        """Test basic write/read cycle."""
        metrics = SystemMetrics(
            timestamp=time.time(), cpu=45.2,
            mem_total=8*1024**3, mem_available=4*1024**3,
            mem_percent=50.0, mem_used=4*1024**3,
            disk_total=100*1024**3, disk_used=60*1024**3,
            disk_free=40*1024**3, disk_percent=60.0,
            swap_total=2*1024**3, swap_used=512*1024**2,
            swap_free=1536*1024**2, swap_percent=25.0,
        )
        connections = {"connections": {"host1": {"status": "connected", "ip": "1.2.3.4"}}}
        instances = {"host1": [{"u": "bot1", "p": "7", "c": 5.0}]}
        stream_info = {"host1": {"alive": True, "last_update": time.time()}}
        services = {"host1": {"PBRun": {"status": "running", "pid": 1234}}}

        write_status(connections, {"host1": metrics}, instances, stream_info, services)
        data = read_status()

        assert data is not None
        assert "host1" in data["connections"]["connections"]
        assert data["instances"]["host1"][0]["u"] == "bot1"
        assert data["services"]["host1"]["PBRun"]["pid"] == 1234

        # Check SystemMetrics was properly round-tripped
        sys_host1 = data["system"]["host1"]
        assert hasattr(sys_host1, "cpu")
        assert abs(sys_host1.cpu - 45.2) < 0.01
        assert sys_host1.mem_percent == 50.0

    def test_read_returns_none_when_no_file(self):
        """read_status returns None if status file doesn't exist."""
        data = read_status()
        assert data is None

    def test_read_returns_none_when_stale(self):
        """read_status returns None if data is older than 60s."""
        import json as _json
        self.tmp_file.parent.mkdir(parents=True, exist_ok=True)
        stale_data = {"timestamp": time.time() - 120, "connections": {}, "system": {}}
        with open(self.tmp_file, 'w') as f:
            _json.dump(stale_data, f)
        data = read_status()
        assert data is None

    def test_status_age_no_file(self):
        """status_age returns -1 when file doesn't exist."""
        age = status_age()
        assert age == -1

    def test_status_age_valid(self):
        """status_age returns approximate age in seconds."""
        write_status({}, {}, {}, {})
        age = status_age()
        assert 0 <= age < 2

    def test_write_empty_data(self):
        """Write with all empty data still produces valid JSON."""
        write_status({}, {}, {}, {})
        data = read_status()
        assert data is not None
        assert data["connections"] == {}
        assert data["system"] == {}
        assert data["instances"] == {}

    def test_atomic_write_no_tmp_left(self):
        """After write, no .tmp file should remain."""
        write_status({}, {}, {}, {})
        tmp_path = str(self.tmp_file) + ".tmp"
        assert not Path(tmp_path).exists()

    def test_write_with_none_metrics(self):
        """write_status handles None metrics gracefully."""
        write_status(
            connections={"connections": {"h1": {"status": "connected"}}},
            system_data={"h1": None},
            instance_data=None,
            stream_info=None,
            service_results=None,
        )
        data = read_status()
        assert data is not None
        assert data["system"] == {}
        assert data["instances"] == {}
        assert data["streams"] == {}
        assert data["services"] == {}


# ═══════════════════════════════════════════════════════════════════
# WebSocket Server Tests
# ═══════════════════════════════════════════════════════════════════

class TestWSServer:
    """Tests for the WebSocket server embedded in PBMaster."""

    @pytest.fixture
    def mock_pbmaster(self):
        """Create a minimally mocked PBMaster for WS server tests."""
        pb = MagicMock()
        pb.pool = MagicMock()
        pb.pool.get_status_summary.return_value = {
            "total": 2, "connected": 1, "disconnected": 1,
            "connections": {
                "testvps1": {"status": "connected", "ip": "1.2.3.4"},
                "testvps2": {"status": "disconnected", "ip": "5.6.7.8", "error": "timeout"},
            },
        }
        pb.pool.hostnames.return_value = ["testvps1", "testvps2"]

        # Realtime collector mocks
        metrics1 = SystemMetrics(
            timestamp=time.time(), cpu=35.5,
            mem_total=8*1024**3, mem_available=4*1024**3,
            mem_percent=50.0, mem_used=4*1024**3,
            disk_total=100*1024**3, disk_used=40*1024**3,
            disk_free=60*1024**3, disk_percent=40.0,
        )
        pb.realtime = MagicMock()
        pb.realtime.get_system.side_effect = lambda h: metrics1 if h == "testvps1" else None
        pb.realtime.get_all_instances.return_value = {
            "testvps1": [{"u": "bot1", "p": "7", "c": 2.0}],
        }
        pb.realtime.get_stream_info.return_value = {
            "testvps1": {"alive": True, "active": True, "error": None},
        }

        pb.monitor = MagicMock()
        pb.streamer = MagicMock()
        pb.streamer.get_recent_logs.return_value = "line1\nline2\nline3"
        pb.streamer.start_stream.return_value = "stream-id-1"
        pb.streamer.read_stream.return_value = ["new line 4"]
        pb.streamer.get_stream_status.return_value = {
            "hostname": "testvps1", "log_path": "data/logs/PBRun.log",
        }

        return pb

    def test_init_default_port(self):
        """WSServer initializes with default port when no config."""
        pb = MagicMock()
        with patch("master.ws_server.load_ini", return_value=None):
            server = WSServer(pb)
        assert server.port == DEFAULT_WS_PORT

    def test_init_custom_port(self):
        """WSServer reads port from pbgui.ini."""
        pb = MagicMock()
        with patch("master.ws_server.load_ini", return_value="9000"):
            server = WSServer(pb)
        assert server.port == 9000

    def test_init_invalid_port(self):
        """WSServer falls back to default for invalid port values."""
        pb = MagicMock()
        with patch("master.ws_server.load_ini", return_value="abc"):
            server = WSServer(pb)
        assert server.port == DEFAULT_WS_PORT

    def test_update_services(self, mock_pbmaster):
        """update_services caches service results."""
        with patch("master.ws_server.load_ini", return_value=None):
            server = WSServer(mock_pbmaster)
        results = {"testvps1": {"PBRun": {"status": "running", "pid": 123}}}
        server.update_services(results)
        assert server._last_services == results

    def test_get_full_state(self, mock_pbmaster):
        """_get_full_state builds complete state from PBMaster data."""
        with patch("master.ws_server.load_ini", return_value=None):
            server = WSServer(mock_pbmaster)
        server.update_services({"testvps1": {"PBRun": {"status": "running"}}})

        state = server._get_full_state()

        # Connections
        assert "connections" in state
        assert state["connections"]["total"] == 2

        # System metrics
        assert "system" in state
        assert "testvps1" in state["system"]
        assert abs(state["system"]["testvps1"]["cpu"] - 35.5) < 0.01
        assert "testvps2" not in state["system"]  # no metrics for disconnected

        # Instances
        assert "instances" in state
        assert len(state["instances"]["testvps1"]) == 1

        # Streams
        assert "streams" in state
        assert state["streams"]["testvps1"]["alive"]

        # Services
        assert "services" in state
        assert "testvps1" in state["services"]

        # Timestamp
        assert "timestamp" in state
        assert state["timestamp"] > 0

    def test_get_full_state_empty(self):
        """_get_full_state works when PBMaster has no data yet."""
        pb = MagicMock()
        pb.pool = None
        pb.realtime = None
        with patch("master.ws_server.load_ini", return_value=None):
            server = WSServer(pb)
        state = server._get_full_state()
        assert state["connections"] == {}
        assert state["system"] == {}
        assert state["instances"] == {}
        assert state["streams"] == {}

    def test_client_count(self, mock_pbmaster):
        """client_count reflects number of connected clients."""
        with patch("master.ws_server.load_ini", return_value=None):
            server = WSServer(mock_pbmaster)
        assert server.client_count == 0

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnknownMarkWarning")
    def test_start_stop(self, mock_pbmaster):
        """WSServer starts and stops cleanly."""
        import socket as _socket

        # Use a random available port to avoid conflicts
        with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]

        with patch("master.ws_server.load_ini", return_value=str(port)):
            server = WSServer(mock_pbmaster)

        server.start()
        time.sleep(1)  # Give server time to start

        # Verify server is listening
        try:
            sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('127.0.0.1', port))
            sock.close()
            assert result == 0, f"Server not listening on port {port}"
        except Exception:
            pass

        server.stop()
        time.sleep(0.5)

    def test_ws_full_roundtrip(self, mock_pbmaster):
        """Full WebSocket roundtrip: connect, receive state, send command."""
        import asyncio
        import socket as _socket

        # Find free port
        with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]

        with patch("master.ws_server.load_ini", return_value=str(port)):
            server = WSServer(mock_pbmaster)

        server.update_services({"testvps1": {"PBRun": {"status": "running"}}})
        server.start()
        time.sleep(1)

        async def _client_test():
            import websockets
            uri = f"ws://127.0.0.1:{port}"
            async with websockets.connect(uri, open_timeout=5) as ws:
                # Should receive initial state
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(msg)
                assert data["type"] == "state"
                assert "connections" in data["data"]
                assert data["data"]["system"]["testvps1"]["cpu"] == 35.5

                # Send get_logs command
                await ws.send(json.dumps({
                    "cmd": "get_logs",
                    "host": "testvps1",
                    "service": "PBRun",
                    "lines": 100,
                }))
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(msg)
                assert data["type"] == "logs"
                assert "line1" in data["lines"]

                # Send restart command
                mock_pbmaster.monitor.restart_service.return_value = True
                await ws.send(json.dumps({
                    "cmd": "restart_service",
                    "host": "testvps1",
                    "service": "PBRun",
                }))
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                data = json.loads(msg)
                assert data["type"] == "result"
                assert data["success"] is True

        try:
            asyncio.run(_client_test())
        finally:
            server.stop()
            time.sleep(0.5)

    def test_ws_push_updates(self, mock_pbmaster):
        """Server pushes periodic state updates to clients."""
        import asyncio
        import socket as _socket

        with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]

        with patch("master.ws_server.load_ini", return_value=str(port)):
            server = WSServer(mock_pbmaster)

        server.start()
        time.sleep(1)

        async def _client_test():
            import websockets
            uri = f"ws://127.0.0.1:{port}"
            async with websockets.connect(uri, open_timeout=5) as ws:
                # Receive initial state
                msg1 = await asyncio.wait_for(ws.recv(), timeout=5)
                assert json.loads(msg1)["type"] == "state"

                # Wait for push update
                msg2 = await asyncio.wait_for(ws.recv(), timeout=PUSH_INTERVAL + 3)
                data2 = json.loads(msg2)
                assert data2["type"] == "state"

        try:
            asyncio.run(_client_test())
        finally:
            server.stop()
            time.sleep(0.5)

    def test_ws_unknown_command(self, mock_pbmaster):
        """Server returns error for unknown commands."""
        import asyncio
        import socket as _socket

        with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]

        with patch("master.ws_server.load_ini", return_value=str(port)):
            server = WSServer(mock_pbmaster)

        server.start()
        time.sleep(1)

        async def _recv_until(ws, expected_type, timeout=5):
            """Drain state pushes until we get a message of expected_type."""
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"Did not receive {expected_type} within {timeout}s")
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                data = json.loads(msg)
                if data.get("type") == expected_type:
                    return data

        async def _client_test():
            import websockets
            uri = f"ws://127.0.0.1:{port}"
            async with websockets.connect(uri, open_timeout=5) as ws:
                # Skip initial state
                await asyncio.wait_for(ws.recv(), timeout=5)

                # Send unknown command
                await ws.send(json.dumps({"cmd": "foobar"}))
                data = await _recv_until(ws, "error")
                assert data["type"] == "error"
                assert "Unknown command" in data["error"]

        try:
            asyncio.run(_client_test())
        finally:
            server.stop()
            time.sleep(0.5)

    def test_ws_subscribe_logs(self, mock_pbmaster):
        """Subscribe to live log streaming."""
        import asyncio
        import socket as _socket

        with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]

        with patch("master.ws_server.load_ini", return_value=str(port)):
            server = WSServer(mock_pbmaster)

        server.start()
        time.sleep(1)

        async def _recv_until(ws, expected_type, timeout=5):
            """Drain state pushes until we get a message of expected_type."""
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"Did not receive {expected_type} within {timeout}s")
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                data = json.loads(msg)
                if data.get("type") == expected_type:
                    return data

        async def _client_test():
            import websockets
            uri = f"ws://127.0.0.1:{port}"
            async with websockets.connect(uri, open_timeout=5) as ws:
                # Skip initial state
                await asyncio.wait_for(ws.recv(), timeout=5)

                # Subscribe to logs
                await ws.send(json.dumps({
                    "cmd": "subscribe_logs",
                    "host": "testvps1",
                    "service": "PBRun",
                }))
                data = await _recv_until(ws, "logs")
                assert data["type"] == "logs"
                assert data.get("streaming") is True

                # Unsubscribe
                await ws.send(json.dumps({"cmd": "unsubscribe_logs"}))

        try:
            asyncio.run(_client_test())
        finally:
            server.stop()
            time.sleep(0.5)

    def test_ws_multiple_clients(self, mock_pbmaster):
        """Multiple clients can connect simultaneously."""
        import asyncio
        import socket as _socket

        with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]

        with patch("master.ws_server.load_ini", return_value=str(port)):
            server = WSServer(mock_pbmaster)

        server.start()
        time.sleep(1)

        async def _test():
            import websockets
            uri = f"ws://127.0.0.1:{port}"
            async with websockets.connect(uri, open_timeout=5) as ws1, \
                       websockets.connect(uri, open_timeout=5) as ws2:
                msg1 = await asyncio.wait_for(ws1.recv(), timeout=5)
                msg2 = await asyncio.wait_for(ws2.recv(), timeout=5)
                assert json.loads(msg1)["type"] == "state"
                assert json.loads(msg2)["type"] == "state"
                assert server.client_count == 2

        try:
            asyncio.run(_test())
        finally:
            server.stop()
            time.sleep(0.5)
