"""Tests for OKX task-worker pipeline scheduling."""

from __future__ import annotations

import json
import threading
import time

import task_worker


class _FakeOkxResult:
    """Minimal result object returned by the fake OKX downloader."""

    def __init__(self, coin: str) -> None:
        """Store the coin name for to_dict()."""

        self.coin = coin

    def to_dict(self) -> dict:
        """Return the result shape consumed by task_worker."""

        return {
            "coin": self.coin,
            "days_checked": 1,
            "archive_daily_downloaded": 0,
            "rest_minutes_fetched": 0,
            "repair_minutes_fetched": 0,
            "minutes_written": 0,
            "notes": [],
        }


def test_okx_pipeline_starts_next_coin_after_archive_stage(monkeypatch, tmp_path) -> None:
    """OKX jobs pipeline the next coin only after the active coin reaches archives."""

    job_path = tmp_path / "okx-job.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    logs: list[str] = []
    started: list[str] = []
    limiter_ids: list[int] = []
    lock = threading.Lock()
    eth_started = threading.Event()

    def fake_improve_best_okx_1m_for_coin(**kwargs):
        """Simulate BTC entering archives before ETH is allowed to start."""

        coin = str(kwargs["coin"])
        progress_cb = kwargs["progress_cb"]
        rest_limiter = kwargs.get("rest_limiter")
        with lock:
            started.append(coin)
            limiter_ids.append(id(rest_limiter))
        if coin == "BTC":
            progress_cb({"stage": "rest_gap", "done": 1, "planned": 2})
            time.sleep(0.2)
            assert not eth_started.is_set()
            progress_cb({"stage": "archive_index", "day": "2021-09-01"})
            assert eth_started.wait(2.0)
        elif coin == "ETH":
            eth_started.set()
        return _FakeOkxResult(coin)

    monkeypatch.setattr(task_worker, "improve_best_okx_1m_for_coin", fake_improve_best_okx_1m_for_coin)
    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, line: logs.append(line))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_refresh_inventory_coin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)

    task_worker._run_okx_best_1m(
        job_path,
        {
            "coins": ["BTC", "ETH"],
            "end_day": "20260626",
            "start_day": "inception",
            "refetch": False,
            "pipeline_workers": 2,
        },
    )

    assert started == ["BTC", "ETH"]
    assert len(set(limiter_ids)) == 1
    assert any("pipeline  workers=2" in line for line in logs)
    assert any("BTC  stage=archive_index" in line for line in logs)


def test_ohlcv_copy_rsync_command_uses_safe_missing_only_defaults(tmp_path) -> None:
    """OHLCV copy rsync command skips existing files and never deletes target files."""

    source_dir = tmp_path / "bybit"
    cmd = task_worker._build_ohlcv_copy_rsync_command(
        source_dir=source_dir,
        target="localhost",
        destination_root="/home/mani/software/pbgui/data/ohlcv",
        storage_name="bybit",
        ssh_args=["ssh", "-J", "user@jump-host", "-p", "2222"],
        mode="missing_only",
    )

    assert cmd[0] == "rsync"
    assert "--ignore-existing" in cmd
    assert "--delete" not in cmd
    assert cmd[cmd.index("-e") + 1] == "ssh -J user@jump-host -p 2222"
    assert f"{source_dir}/" in cmd
    assert "localhost:/home/mani/software/pbgui/data/ohlcv/bybit/" in cmd


def test_ohlcv_copy_rsync_command_update_mode_can_overwrite_changed_files(tmp_path) -> None:
    """Update mode omits --ignore-existing without enabling remote deletion."""

    cmd = task_worker._build_ohlcv_copy_rsync_command(
        source_dir=tmp_path / "okx",
        target="optimizer",
        destination_root="/srv/pbgui/data/ohlcv",
        storage_name="okx",
        ssh_args=["ssh"],
        mode="update",
    )

    assert "--ignore-existing" not in cmd
    assert "--delete" not in cmd
    assert "optimizer:/srv/pbgui/data/ohlcv/okx/" in cmd


def test_ohlcv_copy_dry_run_command_reports_without_writes(tmp_path) -> None:
    """Dry-run rsync commands include stats and itemized output without deletion."""

    cmd = task_worker._build_ohlcv_copy_rsync_command(
        source_dir=tmp_path / "bybit",
        target="optimizer",
        destination_root="/srv/pbgui/data/ohlcv",
        storage_name="bybit",
        ssh_args=["ssh"],
        mode="missing_only",
        dry_run=True,
    )

    assert "--dry-run" in cmd
    assert "--stats" in cmd
    assert "--itemize-changes" in cmd
    assert "--ignore-existing" in cmd
    assert "--delete" not in cmd
    assert "optimizer:/srv/pbgui/data/ohlcv/bybit/" in cmd


def test_ohlcv_copy_rsync_stats_parse_localized_numbers() -> None:
    """Dry-run stat parsing handles German decimal and thousands separators."""

    stats = task_worker._parse_ohlcv_copy_rsync_stats(
        [
            "Number of files: 232.879 (reg: 231.868, dir: 1.011)",
            "Number of regular files transferred: 231.868",
            "Total file size: 3,57G bytes",
            "Total transferred file size: 3,57G bytes",
            "Total bytes sent: 6,64M",
            "Total bytes received: 703,38K",
        ]
    )

    assert stats["files_total"] == 232879
    assert stats["files_transferred"] == 231868
    assert stats["transfer_size_bytes"] == round(3.57 * 1024**3)
    assert stats["total_size_bytes"] == round(3.57 * 1024**3)
    assert stats["bytes_sent"] == round(6.64 * 1024**2)
    assert stats["bytes_received"] == round(703.38 * 1024)


def test_ohlcv_copy_dry_run_skips_remote_mkdir(monkeypatch, tmp_path) -> None:
    """Dry-run jobs never create remote directories before rsync simulation."""

    source_root = tmp_path / "ohlcv"
    (source_root / "bybit").mkdir(parents=True)
    job_path = tmp_path / "dry-run-job.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    commands: list[tuple[str, list[str]]] = []
    logs: list[str] = []

    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: source_root)
    monkeypatch.setattr(task_worker.shutil, "which", lambda _name: "/usr/bin/rsync")
    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, line: logs.append(line))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "update_job_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)
    monkeypatch.setattr(
        task_worker,
        "_run_ohlcv_copy_command",
        lambda _job_path, _job_id, cmd, label: commands.append((label, cmd)),
    )

    task_worker._run_ohlcv_copy(
        job_path,
        {
            "target": "optimizer",
            "ssh_command": "ssh -J user@jump-host -p 2222",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
            "mode": "missing_only",
        },
        dry_run=True,
    )

    assert len(commands) == 1
    label, cmd = commands[0]
    assert "mkdir" not in label.lower()
    assert cmd[0] == "rsync"
    assert "--dry-run" in cmd
    assert "--stats" in cmd
    assert "--itemize-changes" in cmd
    assert "--delete" not in cmd
    assert any("mkdir skipped for dry run" in line for line in logs)


def test_ohlcv_copy_dry_run_aggregates_exchange_stats(monkeypatch, tmp_path) -> None:
    """Dry-run jobs store structured totals across all selected exchanges."""

    source_root = tmp_path / "ohlcv"
    (source_root / "bybit").mkdir(parents=True)
    (source_root / "okx").mkdir(parents=True)
    job_path = tmp_path / "dry-run-job.json"
    job_obj = {"progress": {}, "status": "running"}
    job_path.write_text(json.dumps(job_obj), encoding="utf-8")

    def fake_update_job_file(_path, mutate):
        mutate(job_obj)

    def fake_run_command(_job_path, _job_id, _cmd, label):
        if "Bybit" in label:
            return [
                "Number of files: 10",
                "Number of regular files transferred: 4",
                "Total file size: 1.50G bytes",
                "Total transferred file size: 1.25G bytes",
                "Total bytes sent: 2.00M",
                "Total bytes received: 100.00K",
            ]
        if "OKX" in label:
            return [
                "Number of files: 20",
                "Number of regular files transferred: 6",
                "Total file size: 2,50G bytes",
                "Total transferred file size: 2,25G bytes",
                "Total bytes sent: 3,00M",
                "Total bytes received: 200,00K",
            ]
        return []

    monkeypatch.setattr(task_worker, "get_market_data_root_dir", lambda: source_root)
    monkeypatch.setattr(task_worker.shutil, "which", lambda _name: "/usr/bin/rsync")
    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "update_job_file", fake_update_job_file)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)
    monkeypatch.setattr(task_worker, "_run_ohlcv_copy_command", fake_run_command)

    task_worker._run_ohlcv_copy(
        job_path,
        {
            "target": "optimizer",
            "ssh_command": "ssh",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit", "okx"],
            "mode": "missing_only",
        },
        dry_run=True,
    )

    result = job_obj["progress"]["last_result"]
    assert result["dry_run"] is True
    assert result["exchanges"] == ["bybit", "okx"]
    assert result["files_total"] == 30
    assert result["files_transferred"] == 10
    assert result["total_size_bytes"] == round(4.0 * 1024**3)
    assert result["transfer_size_bytes"] == round(3.5 * 1024**3)
    assert result["bytes_sent"] == round(5.0 * 1024**2)
    assert result["bytes_received"] == round(300.0 * 1024)
    assert result["remote_paths"] == ["optimizer:/srv/pbgui/data/ohlcv/bybit/", "optimizer:/srv/pbgui/data/ohlcv/okx/"]
    assert len(result["exchange_stats"]) == 2
