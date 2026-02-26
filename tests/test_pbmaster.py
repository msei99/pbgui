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


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════

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
