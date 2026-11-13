"""Isolated safety checks for PB8 data removal by the VPS cleanup job."""

import os
from pathlib import Path
import subprocess
import sys
import time


SCRIPT = Path(__file__).resolve().parents[1] / "setup" / "vps_cleanup_job.sh"
OLD_TIME = time.time() - 10 * 24 * 60 * 60
AGED_TIME = time.time() - 35 * 24 * 60 * 60
RECENT_TIME = time.time() - 3 * 24 * 60 * 60


def _environment(tmp_path: Path) -> tuple[dict[str, str], Path, Path]:
    """Create an isolated VPS directory layout for the real cleanup script."""
    home = tmp_path / "home"
    pbgui = home / "software" / "pbgui"
    pb8 = home / "software" / "pb8"
    (pbgui / "data" / "run_v8").mkdir(parents=True)
    (pbgui / "data" / "cluster").mkdir()
    (pbgui / "data" / "cluster" / "node_identity.json").write_text('{"role":"vps"}')
    (pbgui / "pbgui.ini").write_text("[main]\n")
    pb8.mkdir()
    environment = os.environ.copy()
    environment.update({"HOME": str(home), "PBGUI_DIR": str(pbgui), "PB8_DIR": str(pb8), "PB7_DIR": str(home / "software" / "pb7")})
    return environment, pbgui, pb8


def _old_monitor(pb8: Path, name: str) -> Path:
    """Create a monitor directory whose complete contents are older than a week."""
    root = pb8 / "monitor" / "bybit" / name
    (root / "events").mkdir(parents=True)
    event = root / "events" / "old.ndjson.gz"
    event.write_bytes(b"old monitor data")
    for path in (event, event.parent, root):
        os.utime(path, (OLD_TIME, OLD_TIME))
    return root


def _old_legacy_monitor(pb8: Path, name: str) -> Path:
    """Create a dormant monitor tree under PB8's former None root."""
    root = pb8 / "None" / "bybit" / name
    (root / "events").mkdir(parents=True)
    event = root / "events" / "old.ndjson.gz"
    event.write_bytes(b"old monitor data")
    for path in (event, event.parent, root):
        os.utime(path, (OLD_TIME, OLD_TIME))
    return root


def _old_log(pb8: Path, name: str) -> Path:
    """Create a timestamped PB8 run log and its stable alias."""
    logs = pb8 / "logs"
    logs.mkdir(exist_ok=True)
    path = logs / f"20260101_000000_passivbot_live__tmp_data_run_v8_{name}_config.json.log"
    path.write_text("old run")
    os.utime(path, (OLD_TIME, OLD_TIME))
    (logs / f"{name}.log").symlink_to(path.name)
    return path


def _run_log(pb8: Path, name: str, stamp: str, modified: float) -> Path:
    """Create one dated PB8 run log with a rotated backup."""
    logs = pb8 / "logs"
    logs.mkdir(exist_ok=True)
    path = logs / f"{stamp}_passivbot_live__tmp_data_run_v8_{name}_config.json.log"
    path.write_text("run output")
    backup = logs / f"{path.name}.1"
    backup.write_text("rotated run output")
    for entry in (path, backup):
        os.utime(entry, (modified, modified))
    return path


def _old_legacy_log(pb8: Path, name: str) -> Path:
    """Create a stale log and alias directly under PB8's former None root."""
    legacy = pb8 / "None"
    legacy.mkdir(exist_ok=True)
    path = legacy / f"20260101_000000_passivbot_live__tmp_data_run_v8_{name}_config.json.log"
    path.write_text("old run")
    os.utime(path, (OLD_TIME, OLD_TIME))
    (legacy / f"{name}.log").symlink_to(path.name)
    return path


def _run(environment: dict[str, str], *args: str) -> None:
    """Execute the job only against temporary directories."""
    subprocess.run(["bash", str(SCRIPT), *args], env=environment, check=True, capture_output=True, text=True)


def test_pb8_cleanup_removes_only_old_unconfigured_bot_data(tmp_path: Path) -> None:
    """Keep configured and recent bots; delete an old orphan's monitor and logs."""
    environment, pbgui, pb8 = _environment(tmp_path)
    old_orphan = _old_monitor(pb8, "old_bot")
    old_log = _old_log(pb8, "old_bot")
    configured = _old_monitor(pb8, "stopped_but_configured")
    configured_log = _old_log(pb8, "stopped_but_configured")
    (pbgui / "data" / "run_v8" / "stopped_but_configured").mkdir()
    config_path = pbgui / "data" / "run_v8" / "stopped_but_configured" / "config.json"
    config_path.write_text("{}")
    os.utime(config_path, (OLD_TIME, OLD_TIME))
    os.utime(config_path.parent, (OLD_TIME, OLD_TIME))
    stale_runtime = pbgui / "data" / "run_v8" / "old_bot"
    stale_runtime.mkdir()
    stale_file = stale_runtime / "passivbot_err.log"
    stale_file.write_text("old run")
    for path in (stale_file, stale_runtime):
        os.utime(path, (OLD_TIME, OLD_TIME))
    recent = _old_monitor(pb8, "recent_orphan")
    os.utime(recent / "events" / "old.ndjson.gz", None)
    recent_partial = _old_monitor(pb8, "recent_partial")
    (pbgui / "data" / "run_v8" / "recent_partial").mkdir()

    _run(environment, "--dry-run")
    assert old_orphan.exists() and old_log.exists()
    _run(environment)

    assert not old_orphan.exists()
    assert not stale_runtime.exists()
    assert not old_log.exists()
    assert not (pb8 / "logs" / "old_bot.log").is_symlink()
    assert configured.exists() and configured_log.exists()
    assert recent.exists()
    assert recent_partial.exists()


def test_pb8_cleanup_keeps_running_bot_without_config_and_linked_monitor(tmp_path: Path) -> None:
    """Do not remove a process-owned bot or traverse a linked monitor directory."""
    environment, _pbgui, pb8 = _environment(tmp_path)
    running = _old_monitor(pb8, "running_bot")
    linked_target = tmp_path / "outside"
    linked_target.mkdir()
    (linked_target / "marker").write_text("keep")
    (pb8 / "monitor" / "bybit" / "linked_bot").symlink_to(linked_target, target_is_directory=True)
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)", "data/run_v8/running_bot/config.json"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _run(environment)
    finally:
        process.terminate()
        process.wait(timeout=5)

    assert running.exists()
    assert (linked_target / "marker").read_text() == "keep"


def test_pb8_cleanup_skips_when_runtime_inventory_is_missing(tmp_path: Path) -> None:
    """A missing desired-state directory must never make every bot look orphaned."""
    environment, pbgui, pb8 = _environment(tmp_path)
    orphan = _old_monitor(pb8, "old_bot")
    (pbgui / "data" / "run_v8").rmdir()

    _run(environment)

    assert orphan.exists()


def test_pb8_cleanup_removes_dormant_none_tree_but_keeps_recent_data(tmp_path: Path) -> None:
    """A configured bot can have unused legacy data; recent legacy data stays."""
    environment, pbgui, pb8 = _environment(tmp_path)
    old = _old_legacy_monitor(pb8, "configured_bot")
    recent = _old_legacy_monitor(pb8, "recent_bot")
    os.utime(recent / "events" / "old.ndjson.gz", None)
    for name in ("configured_bot", "recent_bot"):
        runtime = pbgui / "data" / "run_v8" / name
        runtime.mkdir()
        (runtime / "config.json").write_text("{}")

    _run(environment, "--dry-run")
    assert old.exists() and recent.exists()
    _run(environment)

    assert not old.exists()
    assert recent.exists()


def test_pb8_cleanup_requires_fresh_canonical_monitor_for_running_bot(tmp_path: Path) -> None:
    """Keep a live bot's legacy tree unless its canonical monitor is active."""
    environment, pbgui, pb8 = _environment(tmp_path)
    canonical_bot = _old_legacy_monitor(pb8, "canonical_bot")
    uncertain_bot = _old_legacy_monitor(pb8, "uncertain_bot")
    uncertain_log = _old_legacy_log(pb8, "uncertain_bot")
    for name in ("canonical_bot", "uncertain_bot"):
        runtime = pbgui / "data" / "run_v8" / name
        runtime.mkdir()
        (runtime / "config.json").write_text("{}")
    canonical_state = pb8 / "monitor" / "bybit" / "canonical_bot" / "state.latest.json"
    canonical_state.parent.mkdir(parents=True)
    canonical_state.write_text("{}")
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)", f"data/run_v8/{name}/config.json"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for name in ("canonical_bot", "uncertain_bot")
    ]
    try:
        _run(environment)
    finally:
        for process in processes:
            process.terminate()
            process.wait(timeout=5)

    assert not canonical_bot.exists()
    assert uncertain_bot.exists()
    assert uncertain_log.exists()


def test_pb8_cleanup_removes_final_none_log_and_empty_root(tmp_path: Path) -> None:
    """Legacy log files and aliases are cleared without leaving an empty None tree."""
    environment, _pbgui, pb8 = _environment(tmp_path)
    log = _old_legacy_log(pb8, "old_bot")
    (pb8 / "None" / "bybit").mkdir()

    _run(environment)

    assert not log.exists()
    assert not (pb8 / "None").exists()


def test_pb8_cleanup_expires_previous_logs_but_preserves_current_run(tmp_path: Path) -> None:
    """Prune aged previous invocations, including backups, for a configured bot."""
    environment, pbgui, pb8 = _environment(tmp_path)
    runtime = pbgui / "data" / "run_v8" / "configured_bot"
    runtime.mkdir()
    (runtime / "config.json").write_text('{"monitor": {"retain_days": 7}}')
    expired = _run_log(pb8, "configured_bot", "20260101_000000", AGED_TIME)
    recent = _run_log(pb8, "configured_bot", "20261101_000000", RECENT_TIME)
    current = _run_log(pb8, "configured_bot", "20261113_000000", AGED_TIME)
    (pb8 / "logs" / "configured_bot.log").symlink_to(current.name)

    _run(environment, "--dry-run")
    assert expired.exists() and expired.with_name(expired.name + ".1").exists()
    _run(environment)

    assert not expired.exists()
    assert not expired.with_name(expired.name + ".1").exists()
    for path in (recent, current):
        assert path.exists() and path.with_name(path.name + ".1").exists()
    assert (pb8 / "logs" / "configured_bot.log").is_symlink()


def test_pb8_cleanup_keeps_running_logs_if_current_alias_is_unknown(tmp_path: Path) -> None:
    """A live bot without a reliable log alias cannot identify its current run."""
    environment, pbgui, pb8 = _environment(tmp_path)
    runtime = pbgui / "data" / "run_v8" / "running_bot"
    runtime.mkdir()
    (runtime / "config.json").write_text("{}")
    old = _run_log(pb8, "running_bot", "20260101_000000", AGED_TIME)
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)", "data/run_v8/running_bot/config.json"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _run(environment)
    finally:
        process.terminate()
        process.wait(timeout=5)

    assert old.exists() and old.with_name(old.name + ".1").exists()


def test_pb8_cleanup_keeps_latest_log_for_stopped_configured_bot(tmp_path: Path) -> None:
    """A stopped bot retains its latest run even when every log is old."""
    environment, pbgui, pb8 = _environment(tmp_path)
    runtime = pbgui / "data" / "run_v8" / "configured_bot"
    runtime.mkdir()
    (runtime / "config.json").write_text("{}")
    previous = _run_log(pb8, "configured_bot", "20260101_000000", AGED_TIME)
    latest = _run_log(pb8, "configured_bot", "20260102_000000", AGED_TIME)

    _run(environment)

    assert not previous.exists()
    assert latest.exists() and latest.with_name(latest.name + ".1").exists()


def test_pb8_cleanup_uses_per_bot_retention_and_keeps_unreadable_config(tmp_path: Path) -> None:
    """A longer bot setting wins; malformed config cannot authorize log deletion."""
    environment, pbgui, pb8 = _environment(tmp_path)
    run_dir = pbgui / "data" / "run_v8"
    long_runtime = run_dir / "long_retention"
    long_runtime.mkdir()
    (long_runtime / "config.json").write_text('{"monitor": {"retain_days": 60}}')
    kept = _run_log(pb8, "long_retention", "20260101_000000", AGED_TIME)
    _run_log(pb8, "long_retention", "20260102_000000", RECENT_TIME)
    bad_runtime = run_dir / "bad_config"
    bad_runtime.mkdir()
    (bad_runtime / "config.json").write_text("{broken")
    uncertain = _run_log(pb8, "bad_config", "20260101_000000", AGED_TIME)
    _run_log(pb8, "bad_config", "20260102_000000", RECENT_TIME)

    _run(environment)

    assert kept.exists() and kept.with_name(kept.name + ".1").exists()
    assert uncertain.exists() and uncertain.with_name(uncertain.name + ".1").exists()


def test_pb8_cleanup_keeps_whole_run_when_rotated_part_is_recent(tmp_path: Path) -> None:
    """A recently written backup keeps its run's older base log available."""
    environment, pbgui, pb8 = _environment(tmp_path)
    runtime = pbgui / "data" / "run_v8" / "configured_bot"
    runtime.mkdir()
    (runtime / "config.json").write_text('{"monitor": {"retain_days": 7}}')
    previous = _run_log(pb8, "configured_bot", "20260101_000000", AGED_TIME)
    os.utime(previous.with_name(previous.name + ".1"), (RECENT_TIME, RECENT_TIME))
    _run_log(pb8, "configured_bot", "20260102_000000", RECENT_TIME)

    _run(environment)

    assert previous.exists() and previous.with_name(previous.name + ".1").exists()
