"""Tests for secure, concurrent central logging helpers."""

from concurrent.futures import ThreadPoolExecutor
import configparser
import json
import multiprocessing
from pathlib import Path
import threading
import time

import pytest

import logging_helpers
from file_lock import advisory_file_lock


TIER_3_SERVICES = {
    "ApiLogging", "ApiKeys", "BalanceCalc", "CoinDataUI", "Dashboard",
    "Services", "V7Instances", "MarketDataAPI", "PB7OhlcvAPI", "PBV7UI",
}
DEDICATED_SERVICES = {
    "OptimizeQueueAPI", "PBRun", "PBData", "PBCoinData", "PBCluster",
    "PBApiServer", "PBMonitorAgent", "VPSMonitor", "MarketData",
}


def test_log_group_ownership_contract():
    """Tier-3 helpers group into PBGui while daemons and pipelines stay dedicated."""
    assert {service for service in TIER_3_SERVICES if logging_helpers.LOG_GROUPS.get(service) == "PBGui"} == TIER_3_SERVICES
    assert DEDICATED_SERVICES.isdisjoint(logging_helpers.LOG_GROUPS)


def test_pb8_backtest_logs_have_a_managed_rotation_scope(isolated_paths):
    """PB8 queue logs must rotate under their declared nested log directory."""
    _ini_path, log_root = isolated_paths
    target = log_root / "backtests_v8" / "job.log"

    assert logging_helpers.resolve_managed_log_scope(target) == "backtests_v8"
    assert logging_helpers.rotate_managed_log_before_open(target, "backtests_v8") == target


def _process_writer(logfile: str, ini_path: str, start: int, count: int) -> None:
    """Write an isolated range from a child process."""
    logging_helpers.PBGUI_INI = Path(ini_path)
    for index in range(start, start + count):
        logging_helpers.human_log("ProcessTest", f"record-{index}", logfile=logfile)


def _slow_process_writer(logfile: str, ini_path: str, start: int, count: int, started) -> None:
    """Write slowly enough for a parent purge to overlap active writers."""
    logging_helpers.PBGUI_INI = Path(ini_path)
    for index in range(start, start + count):
        logging_helpers.human_log("ProcessTest", f"record-{index}", logfile=logfile)
        started.set()
        time.sleep(0.002)


def _process_set_rotation(ini_path: str, service: str, max_bytes: int, backup_count: int) -> None:
    """Persist one isolated service override from a child process."""
    logging_helpers.PBGUI_INI = Path(ini_path)
    logging_helpers.set_rotate_settings(service, max_bytes, backup_count)


@pytest.fixture
def isolated_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """Redirect every canonical logging path away from repository data."""
    root = tmp_path / "project"
    ini_path = root / "pbgui.ini"
    log_root = root / "data" / "logs"
    monkeypatch.setattr(logging_helpers, "PBGDIR", root)
    monkeypatch.setattr(logging_helpers, "PBGUI_INI", ini_path)
    monkeypatch.setattr(logging_helpers, "LOG_ROOT", log_root)
    return ini_path, log_root


def test_defaults_are_independent_of_cwd(isolated_paths, tmp_path, monkeypatch):
    """Default logs and settings should use canonical paths, never cwd."""
    ini_path, log_root = isolated_paths
    ini_path.parent.mkdir(parents=True)
    ini_path.write_text("[logging]\nrotate_default_max_bytes = 1234\n", encoding="utf-8")
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    monkeypatch.chdir(unrelated)

    logging_helpers.human_log("Canonical", "from elsewhere")

    assert logging_helpers.get_rotate_defaults()[0] == 1234
    assert (log_root / "Canonical.log").read_text(encoding="utf-8").endswith("from elsewhere\n")
    assert not (unrelated / "data").exists()


def test_redacts_all_channels_and_nested_metadata(isolated_paths):
    """Credentials should be removed from text fields and nested metadata."""
    _, log_root = isolated_paths
    pem = "-----BEGIN PRIVATE KEY-----\nprivate-material\n-----END PRIVATE KEY-----"
    recursive = []
    recursive.append(recursive)

    class Unserializable:
        """Object whose repr must not leak into metadata."""

        def __str__(self):
            raise RuntimeError("password=from-str")

    logging_helpers.human_log(
        "Secrets",
        "Authorization: Bearer message-token url=https://host/x?token=query-token&ok=1",
        user="cookie=user-cookie",
        tags=["session=tag-session"],
        code="api_key=code-key",
        meta={
            "Password": "nested-password",
            "passwd": "nested-passwd",
            "api_key": "nested-api-key",
            "apikey": "nested-apikey",
            "api_secret": "nested-api-secret",
            "secret": "nested-secret",
            "token": "nested-token",
            "session": "nested-session",
            "cookie": "nested-cookie",
            "authorization": "nested-authorization",
            "bearer": "nested-bearer",
            "private_key": "nested-private-key",
            "passphrase": "nested-passphrase",
            "access_token": "nested-access-token",
            "refreshToken": "nested-refresh-token",
            "client_secret": "nested-client-secret",
            "aws_secret_access_key": "nested-aws-secret",
            "x-api-key": "nested-x-api-key",
            "nested": {
                "traceback": "failure cookie=trace-cookie",
                "exception": "authorization=Bearer exception-token",
                "url": "https://host/?api_secret=url-secret&safe=yes",
                "pem": pem,
            },
            "recursive": recursive,
            "object": Unserializable(),
        },
    )

    content = (log_root / "Secrets.log").read_text(encoding="utf-8")
    for secret in (
        "message-token", "query-token", "user-cookie", "tag-session", "code-key",
        "nested-password", "trace-cookie", "exception-token", "url-secret", "private-material",
        "from-str", "nested-passwd", "nested-api-key", "nested-apikey", "nested-api-secret",
        "nested-secret", "nested-token", "nested-session", "nested-cookie", "nested-authorization",
        "nested-bearer", "nested-private-key", "nested-passphrase",
        "nested-access-token", "nested-refresh-token", "nested-client-secret",
        "nested-aws-secret", "nested-x-api-key",
    ):
        assert secret not in content
    assert content.count("[REDACTED]") >= 8
    assert '"recursive": ["[RECURSIVE]"]' in content
    assert '"object": "<Unserializable>"' in content
    assert "&ok=1" in content


def test_redacts_basic_auth_set_cookie_and_extended_query_secrets(isolated_paths):
    """Common HTTP and OAuth credential variants must be centrally redacted."""
    _, log_root = isolated_paths
    logging_helpers.human_log(
        "Secrets",
        "Authorization: Basic basic-secret Set-Cookie: pbgui_session=cookie-secret "
        "https://host/path?access_token=oauth-secret&safe=yes",
        meta={"proxy_authorization": "proxy-secret", "totp_secret": "totp-value"},
    )

    content = (log_root / "Secrets.log").read_text(encoding="utf-8")
    for secret in ("basic-secret", "cookie-secret", "oauth-secret", "proxy-secret", "totp-value"):
        assert secret not in content
    assert "&safe=yes" in content


def test_redaction_bounds_unicode_tuples_sets_and_depth(isolated_paths):
    """Container variants and bounded Unicode metadata remain JSON-safe."""
    _, log_root = isolated_paths
    nested = {"level": {"level": {"level": {"level": {"level": {"level": {"level": {"level": {"token": "deep-secret"}}}}}}}}}
    logging_helpers.human_log(
        "Secrets",
        "unicode Grüße token=message-secret " + ("x" * (logging_helpers._MAX_REDACT_TEXT + 100)),
        meta={
            "tuple": ("password=tuple-secret", "Grüße"),
            "set": {"api_key=set-secret", "safe"},
            "many": list(range(logging_helpers._MAX_REDACT_ITEMS + 5)),
            "nested": nested,
        },
    )

    content = (log_root / "Secrets.log").read_text(encoding="utf-8")
    for secret in ("message-secret", "tuple-secret", "set-secret", "deep-secret"):
        assert secret not in content
    assert "Grüße" in content
    assert "[TRUNCATED]" in content
    assert "[MAX_DEPTH]" in content


def test_nested_logging_context_resets_and_explicit_metadata_wins(isolated_paths):
    """Nested context should restore its parent and explicit metadata should win."""
    _, log_root = isolated_paths

    with logging_helpers.logging_context(request_id="outer", operation="outer-op"):
        logging_helpers.human_log("Context", "outer")
        with logging_helpers.logging_context(operation="inner-op", instance="bot-a"):
            logging_helpers.human_log("Context", "inner", meta={"operation": "explicit-op"})
        logging_helpers.human_log("Context", "restored")
    logging_helpers.human_log("Context", "cleared")

    lines = (log_root / "Context.log").read_text(encoding="utf-8").splitlines()
    metadata = [json.loads(line[line.index("{"):]) if "{" in line else {} for line in lines]
    assert metadata == [
        {"operation": "outer-op", "request_id": "outer"},
        {"instance": "bot-a", "operation": "explicit-op", "request_id": "outer"},
        {"operation": "outer-op", "request_id": "outer"},
        {},
    ]


def test_grouped_rotation_uses_physical_stem(isolated_paths):
    """Grouped services and their physical logfile should share one override."""
    logging_helpers.set_rotate_defaults(900, 1)
    logging_helpers.set_rotate_settings("VPSManager", 321, 4)

    assert logging_helpers.get_rotate_settings(service="VPSManager") == (321, 4)
    assert logging_helpers.get_rotate_settings(logfile="/tmp/PBGui.log") == (321, 4)
    assert logging_helpers.get_rotate_settings(service="VPSManager", logfile="/tmp/PBGui.log") == (321, 4)


def test_managed_scope_persistence_and_resolution(isolated_paths):
    """Managed scope settings apply by path below exact physical overrides."""
    ini_path, log_root = isolated_paths
    logging_helpers.set_rotate_defaults(900, 1)
    logging_helpers.set_managed_scope_settings("jobs", 500, 3)
    job_log = log_root / "jobs" / "abc.log"
    assert logging_helpers.resolve_managed_log_scope(job_log) == "jobs"
    assert logging_helpers.get_rotate_settings(logfile=str(job_log)) == (500, 3)
    logging_helpers.set_rotate_settings("abc", 250, 7)
    assert logging_helpers.get_rotate_settings(logfile=str(job_log)) == (250, 7)
    cfg = configparser.ConfigParser(); cfg.read(ini_path)
    assert cfg.getint("logging", "managed_jobs_backup_count") == 3


def test_managed_scope_validation_and_external_paths(isolated_paths):
    """Only registered IDs are accepted and declared legacy paths resolve."""
    root = logging_helpers.PBGDIR
    with pytest.raises(ValueError):
        logging_helpers.set_managed_scope_settings("../../bad", 1, 1)
    assert logging_helpers.resolve_managed_log_scope(root / "data/ohlcv_preload/logs/preload_x.log") == "ohlcv_preloads"
    assert logging_helpers.resolve_managed_log_scope(logging_helpers.LOG_ROOT / "ohlcv-preloads" / "preload_x.log") == "ohlcv_preloads"
    assert logging_helpers.resolve_managed_log_scope(logging_helpers.LOG_ROOT / "vps-manager" / "hosts" / "host-a" / "run.log") == "vps_manager_runs"
    assert logging_helpers.resolve_managed_log_scope(logging_helpers.LOG_ROOT / "monitor-agent" / "live_metrics.ndjson") == "monitor_agent_live"
    assert logging_helpers.resolve_managed_log_scope(root / "outside.log") is None


def test_managed_transcript_append_rotates_sanitizes_and_locks(isolated_paths, monkeypatch):
    """Managed transcript writes share one rotate-and-append lock transaction."""
    _, log_root = isolated_paths
    path = log_root / "jobs" / "job-a.log"
    logging_helpers.set_managed_scope_settings("jobs", 8, 2)
    path.parent.mkdir(parents=True)
    path.write_text("old transcript\n", encoding="utf-8")

    logging_helpers.append_managed_transcript_line(path, "token=secret-value", "jobs")

    assert (Path(f"{path}.1")).read_text(encoding="utf-8") == "old transcript\n"
    assert "secret-value" not in path.read_text(encoding="utf-8")
    assert "[REDACTED]" in path.read_text(encoding="utf-8")


def test_atomic_settings_update_preserves_unrelated_values(isolated_paths):
    """Locked atomic updates should retain unrelated INI settings."""
    ini_path, _ = isolated_paths
    ini_path.parent.mkdir(parents=True)
    ini_path.write_text("[main]\nname = keep-me\n[other]\nvalue = 42\n", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(logging_helpers.set_rotate_defaults, 2048, 2),
            pool.submit(logging_helpers.set_rotate_settings, "PBRun", 4096, 3),
        ]
        for future in futures:
            future.result()

    cfg = configparser.ConfigParser()
    cfg.read(ini_path, encoding="utf-8")
    assert cfg.get("main", "name") == "keep-me"
    assert cfg.get("other", "value") == "42"
    assert logging_helpers.get_rotate_defaults() == (2048, 2)
    assert logging_helpers.get_rotate_settings(service="PBRun") == (4096, 3)
    assert not list(ini_path.parent.glob("*.tmp"))


def test_thread_safe_writes_have_complete_lines(isolated_paths, tmp_path):
    """Concurrent threads should append every complete record exactly once."""
    logfile = tmp_path / "thread.log"
    with ThreadPoolExecutor(max_workers=8) as pool:
        for index in range(200):
            pool.submit(logging_helpers.human_log, "ThreadTest", f"record-{index}", logfile=str(logfile))

    lines = logfile.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 200
    assert {line.rsplit(" ", 1)[-1] for line in lines} == {f"record-{index}" for index in range(200)}


@pytest.mark.skipif(multiprocessing.get_start_method(allow_none=True) == "spawn", reason="fork-oriented lock stress test")
def test_process_safe_writes_have_complete_lines(isolated_paths, tmp_path):
    """Concurrent processes should serialize append transactions."""
    ini_path, _ = isolated_paths
    logfile = tmp_path / "process.log"
    context = multiprocessing.get_context("fork")
    processes = [
        context.Process(target=_process_writer, args=(str(logfile), str(ini_path), worker * 50, 50))
        for worker in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0

    lines = logfile.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 200
    assert {line.rsplit(" ", 1)[-1] for line in lines} == {f"record-{index}" for index in range(200)}


@pytest.mark.skipif(multiprocessing.get_start_method(allow_none=True) == "spawn", reason="fork-oriented lock stress test")
def test_process_rotation_preserves_all_records(isolated_paths, tmp_path):
    """Concurrent process writers retain every record across rotations."""
    ini_path, _ = isolated_paths
    logging_helpers.set_rotate_defaults(200, 100)
    logfile = tmp_path / "process-rotate.log"
    context = multiprocessing.get_context("fork")
    processes = [
        context.Process(target=_process_writer, args=(str(logfile), str(ini_path), worker * 20, 20))
        for worker in range(4)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0

    lines = []
    for path in [logfile, *sorted(tmp_path.glob("process-rotate.log.*"))]:
        if path.suffix == ".lock":
            continue
        lines.extend(path.read_text(encoding="utf-8").splitlines())
    assert len(lines) == 80
    assert {line.rsplit(" ", 1)[-1] for line in lines} == {f"record-{index}" for index in range(80)}


@pytest.mark.skipif(multiprocessing.get_start_method(allow_none=True) == "spawn", reason="fork-oriented lock stress test")
def test_process_purge_preserves_records_from_active_writers(isolated_paths, tmp_path):
    """A purge serialized with process writers loses no completed records."""
    ini_path, _ = isolated_paths
    logfile = tmp_path / "process-purge.log"
    context = multiprocessing.get_context("fork")
    started = context.Event()
    processes = [
        context.Process(target=_slow_process_writer, args=(str(logfile), str(ini_path), worker * 100, 100, started))
        for worker in range(2)
    ]
    for process in processes:
        process.start()
    assert started.wait(5)
    success, _message = logging_helpers.purge_log_to_rotated(str(logfile), 1024 * 1024, 2)
    assert success is True
    for process in processes:
        process.join(20)
        assert process.exitcode == 0

    lines = []
    for path in (logfile, Path(f"{logfile}.1"), Path(f"{logfile}.2")):
        if path.exists():
            lines.extend(path.read_text(encoding="utf-8").splitlines())
    assert len(lines) == 200
    assert {line.rsplit(" ", 1)[-1] for line in lines} == {f"record-{index}" for index in range(200)}


@pytest.mark.skipif(multiprocessing.get_start_method(allow_none=True) == "spawn", reason="fork-oriented lock stress test")
def test_process_settings_updates_preserve_unrelated_values(isolated_paths):
    """Concurrent process settings updates share the INI transaction lock."""
    ini_path, _ = isolated_paths
    ini_path.parent.mkdir(parents=True)
    ini_path.write_text("[main]\nname = keep-me\n", encoding="utf-8")
    context = multiprocessing.get_context("fork")
    processes = [
        context.Process(target=_process_set_rotation, args=(str(ini_path), "PBRun", 2048, 2)),
        context.Process(target=_process_set_rotation, args=(str(ini_path), "PBData", 4096, 3)),
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0

    cfg = configparser.ConfigParser()
    cfg.read(ini_path, encoding="utf-8")
    assert cfg.get("main", "name") == "keep-me"
    assert logging_helpers.get_rotate_settings(service="PBRun") == (2048, 2)
    assert logging_helpers.get_rotate_settings(service="PBData") == (4096, 3)


@pytest.mark.parametrize("operation", ["rotate", "purge"])
def test_rotation_and_purge_use_physical_log_lock(isolated_paths, tmp_path, operation):
    """Rotation and purge should wait for the same lock used by writers."""
    logfile = tmp_path / "locked.log"
    logfile.write_text("old content\n", encoding="utf-8")
    started = threading.Event()
    finished = threading.Event()

    def run_operation():
        started.set()
        if operation == "rotate":
            logging_helpers.rotate_logfile_if_oversize(str(logfile), 1, 2)
        else:
            logging_helpers.purge_log_to_rotated(str(logfile), 1024)
        finished.set()

    with advisory_file_lock(logfile):
        thread = threading.Thread(target=run_operation)
        thread.start()
        assert started.wait(1)
        time.sleep(0.05)
        assert not finished.is_set()
    thread.join(2)
    assert finished.is_set()
    assert (tmp_path / "locked.log.1").exists()


def test_rotate_logfile_keeps_configured_backup_count(tmp_path):
    """Rotation should keep multiple generations up to backup_count."""
    log_path = tmp_path / "service.log"
    for index in range(1, 5):
        log_path.write_text(f"entry-{index}\n", encoding="utf-8")
        logging_helpers.rotate_logfile_if_oversize(str(log_path), max_bytes=1, backup_count=3)

    assert (tmp_path / "service.log.1").exists()
    assert (tmp_path / "service.log.2").exists()
    assert (tmp_path / "service.log.3").exists()
    assert not (tmp_path / "service.log.4").exists()


def test_rotation_prunes_generations_above_reduced_count(tmp_path):
    """Rotation removes stale numeric generations even before the next rollover."""
    log_path = tmp_path / "service.log"
    log_path.write_text("current\n", encoding="utf-8")
    for index in range(1, 5):
        Path(f"{log_path}.{index}").write_text(f"old-{index}\n", encoding="utf-8")

    logging_helpers.rotate_logfile_if_oversize(str(log_path), max_bytes=1024, backup_count=2)

    assert Path(f"{log_path}.1").exists()
    assert Path(f"{log_path}.2").exists()
    assert not Path(f"{log_path}.3").exists()
    assert not Path(f"{log_path}.4").exists()


def test_purge_honors_backup_count_size_and_zero_retention(tmp_path):
    """Forced purge shifts configured generations, keeps a bounded tail, and supports zero backups."""
    log_path = tmp_path / "service.log"
    log_path.write_bytes(b"0123456789")
    Path(f"{log_path}.1").write_bytes(b"old-one")
    Path(f"{log_path}.2").write_bytes(b"old-two")
    Path(f"{log_path}.3").write_bytes(b"excess")

    success, _message = logging_helpers.purge_log_to_rotated(str(log_path), max_bytes=4, backup_count=2)

    assert success is True
    assert log_path.read_bytes() == b""
    assert Path(f"{log_path}.1").read_bytes() == b"6789"
    assert Path(f"{log_path}.2").read_bytes() == b"old-one"
    assert not Path(f"{log_path}.3").exists()

    log_path.write_bytes(b"discard")
    success, _message = logging_helpers.purge_log_to_rotated(str(log_path), max_bytes=4, backup_count=0)
    assert success is True
    assert log_path.read_bytes() == b""
    assert not Path(f"{log_path}.1").exists()
    assert not Path(f"{log_path}.2").exists()


def test_purge_redacts_internal_failure_text(tmp_path, monkeypatch):
    """Purge helper failures never return credential text to API callers."""
    log_path = tmp_path / "service.log"
    log_path.touch()

    def fail_lock(_path):
        raise OSError("token=purge-secret")

    monkeypatch.setattr(logging_helpers, "advisory_file_lock", fail_lock)
    success, message = logging_helpers.purge_log_to_rotated(str(log_path), 1024, 1)
    assert success is False
    assert "purge-secret" not in message
    assert "[REDACTED]" in message


def test_rotation_fallback_stderr_is_sanitized(tmp_path, monkeypatch, capsys):
    """Best-effort rotation failures use only redacted fallback stderr."""
    log_path = tmp_path / "service.log"
    log_path.touch()

    def fail_lock(_path):
        raise OSError("password=rotation-secret")

    monkeypatch.setattr(logging_helpers, "advisory_file_lock", fail_lock)
    logging_helpers.rotate_logfile_if_oversize(str(log_path), 1, 1)
    stderr = capsys.readouterr().err
    assert "rotation-secret" not in stderr
    assert "[REDACTED]" in stderr
