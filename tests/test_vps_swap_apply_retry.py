"""Offline regressions for interrupted swap applies and authoritative settings reads."""

from types import SimpleNamespace

import pytest

import vps_manager_core as core
from vps_manager_service import VPSManagerService


@pytest.mark.parametrize("remote_swap,expected_apply", [("2.5G", True), ("2G", False), ("0", True)])
def test_apply_rechecks_remote_swap_even_when_target_is_already_saved(remote_swap, expected_apply):
    """Retry a saved 2G target only when the active remote swap differs."""
    service = object.__new__(VPSManagerService)
    starts = []
    reads = []
    vps = SimpleNamespace(
        hostname="test-vps", swap="2G", firewall=False,
        firewall_ssh_port=22, firewall_ssh_ips="", save=lambda: None,
    )

    def read_swap():
        """Assert that inventory refresh cannot replace the inspected state."""
        lock = service._host_task_start_lock(vps.hostname)
        assert lock.locked()
        reads.append(True)
        return {"swap": remote_swap}

    vps.fetch_vps_info = read_swap
    service._require_vps = lambda host: vps
    service._store_session_secrets = lambda *args: None
    service._require_user_password = lambda *args: "test-password"
    service._apply_vps_setup_form = lambda token, obj, form: setattr(obj, "swap", form["swap"])
    service._build_vps_config = lambda token, obj: {"swap": obj.swap}
    service._start_vps_config_apply = lambda *args, **kwargs: starts.append(kwargs) or {"started": True}

    result = service.save_vps_config("session", "test-vps", {"swap": "2G", "check_remote_swap": True})

    assert reads == [True]
    assert result["swap_changed"] is expected_apply
    assert result["remote_apply"]["started"] is expected_apply
    assert starts == ([{"apply_firewall": False, "apply_swap": True}] if expected_apply else [])
    assert result["config"]["swap"] == "2G"


def test_read_settings_keeps_detected_swap_through_firewall_read():
    """A concurrent inventory reload cannot restore the previous 2G target."""
    service = object.__new__(VPSManagerService)
    saved = []
    vps = SimpleNamespace(
        hostname="test-vps", swap="2G", can_login_ssh=lambda: True,
        fetch_vps_info=lambda: {"swap": "2.5G"},
        save=lambda: saved.append(vps.swap), write_vps_firewall_info=lambda: True,
    )

    def read_firewall():
        """Simulate the old inventory refresh racing a slower UFW request."""
        lock = service._host_task_start_lock(vps.hostname)
        if lock.acquire(blocking=False):
            try:
                vps.swap = "2G"
            finally:
                lock.release()
        return True, "198.51.100.1"

    vps.fetch_ufw_settings = read_firewall
    service._require_vps = lambda host: vps
    service._store_session_secrets = lambda *args: None
    service._require_user_password = lambda *args: "test-password"
    service._clear_vps_optional_config_pending = lambda obj: None
    service._build_vps_config = lambda token, obj: {"swap": obj.swap}

    assert service.read_vps_settings("session", "test-vps")["swap"] == "2.5G"
    assert saved == ["2.5G"]


@pytest.mark.parametrize("output,status,expected", [
    (b"/swapfile file 2.5G 1.2G -2\n", 0, "2.5G"),
    (b"", 0, "0"),
    (b"", 1, None),
])
def test_remote_swap_read_distinguishes_no_swap_from_command_failure(monkeypatch, output, status, expected):
    """Read fractional sizes and close SSH even if swapon fails."""
    closed = []
    stdout = SimpleNamespace(read=lambda: output, channel=SimpleNamespace(recv_exit_status=lambda: status))
    ssh = SimpleNamespace(
        connect=lambda *args, **kwargs: None,
        exec_command=lambda *args, **kwargs: (None, stdout, None),
        close=lambda: closed.append(True),
    )
    monkeypatch.setattr(core, "_strict_ssh_client", lambda: ssh)
    vps = core.VPS()
    vps.hostname = "test-vps"
    vps.ip = "192.0.2.1"
    if expected is None:
        with pytest.raises(ValueError, match="Could not read active swap"):
            vps.fetch_vps_info()
    else:
        assert vps.fetch_vps_info()["swap"] == expected
    assert closed == [True]
