"""Tests for OKX task-worker pipeline scheduling."""

from __future__ import annotations

import json
import sys
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


def test_run_job_records_actual_run_timestamps(monkeypatch, tmp_path) -> None:
    """Worker records run start and finish times independently from creation time."""

    job_path = tmp_path / "bitget-job.json"
    job_path.write_text(
        json.dumps({"id": "bitget-job", "type": "bitget_best_1m", "status": "running", "created_ts": 100, "updated_ts": 100, "payload": {"coins": ["BTC"]}}),
        encoding="utf-8",
    )

    def fake_run_bitget(_job_path, _payload):
        obj = json.loads(job_path.read_text(encoding="utf-8"))
        assert obj["run_started_ts"] > obj["created_ts"]
        assert obj["finished_ts"] == 0

    monkeypatch.setattr(task_worker, "_run_bitget_best_1m", fake_run_bitget)
    monkeypatch.setattr(task_worker, "move_job_file", lambda path, _state: path)

    task_worker._run_job(job_path)

    obj = json.loads(job_path.read_text(encoding="utf-8"))
    assert obj["status"] == "done"
    assert obj["run_started_ts"] > obj["created_ts"]
    assert obj["finished_ts"] >= obj["run_started_ts"]


def test_ohlcv_copy_rsync_command_updates_changed_files(tmp_path) -> None:
    """OHLCV copy rsync command updates changed files and never deletes target files."""

    source_dir = tmp_path / "bybit"
    cmd = task_worker._build_ohlcv_copy_rsync_command(
        source_dir=source_dir,
        target="localhost",
        destination_root="/home/mani/software/pbgui/data/ohlcv",
        storage_name="bybit",
        ssh_args=["ssh", "-J", "user@jump-host", "-p", "2222"],
        mode="update",
    )

    assert cmd[0] == "rsync"
    assert "--ignore-existing" not in cmd
    assert "--delete" not in cmd
    assert cmd[cmd.index("-e") + 1] == "ssh -J user@jump-host -p 2222"
    assert f"{source_dir}/" in cmd
    assert "localhost:/home/mani/software/pbgui/data/ohlcv/bybit/" in cmd


def test_ohlcv_copy_rsync_command_ignores_legacy_missing_only_mode(tmp_path) -> None:
    """Legacy missing_only payloads are treated as changed-file updates."""

    cmd = task_worker._build_ohlcv_copy_rsync_command(
        source_dir=tmp_path / "okx",
        target="optimizer",
        destination_root="/srv/pbgui/data/ohlcv",
        storage_name="okx",
        ssh_args=["ssh"],
        mode="missing_only",
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
    assert "--ignore-existing" not in cmd
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
            "mode": "update",
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


def test_bitget_distributed_segments_split_each_coin_into_chunks() -> None:
    """Distributed Bitget splits every coin range into fixed-size chunks."""

    segments = task_worker._build_bitget_distributed_segments(
        ["BTC", "ETH"],
        start_day="20260101",
        end_day="20260110",
        chunk_days=4,
    )

    assert segments == [
        {"coin": "BTC", "start_day": "20260101", "end_day": "20260104"},
        {"coin": "BTC", "start_day": "20260105", "end_day": "20260108"},
        {"coin": "BTC", "start_day": "20260109", "end_day": "20260110"},
        {"coin": "ETH", "start_day": "20260101", "end_day": "20260104"},
        {"coin": "ETH", "start_day": "20260105", "end_day": "20260108"},
        {"coin": "ETH", "start_day": "20260109", "end_day": "20260110"},
    ]


def test_bitget_distributed_segments_default_to_30_day_chunks() -> None:
    """Distributed Bitget defaults to larger chunks to reduce SSH startup overhead."""

    segments = task_worker._build_bitget_distributed_segments(
        ["BTC"],
        start_day="20260101",
        end_day="20260205",
    )

    assert segments == [
        {"coin": "BTC", "start_day": "20260101", "end_day": "20260130"},
        {"coin": "BTC", "start_day": "20260131", "end_day": "20260205"},
    ]


def test_bitget_distributed_segments_use_per_coin_inception_starts() -> None:
    """Distributed Bitget skips pre-inception history for each coin."""

    segments = task_worker._build_bitget_distributed_segments(
        ["BTC", "ETH"],
        start_day="",
        end_day="20260205",
        chunk_days=30,
        coin_start_days={"BTC": "20260120", "ETH": "20260201"},
    )

    assert segments == [
        {"coin": "BTC", "start_day": "20260120", "end_day": "20260205"},
        {"coin": "ETH", "start_day": "20260201", "end_day": "20260205"},
    ]


def test_bitget_distributed_days_to_fetch_skip_complete_existing_days(monkeypatch) -> None:
    """Distributed Bitget does not schedule days that are already complete locally."""

    def fake_read(_path, *, day):
        if day == "2026-01-01":
            return {0: {}}
        if day == "2026-01-02":
            return {idx: {} for idx in range(task_worker._BITGET_MIN_DAY_CANDLES)}
        if day == "2026-01-03":
            return {idx: {} for idx in range(100)}
        return {}

    monkeypatch.setattr(task_worker, "_bitget_read_day_npz", fake_read)
    monkeypatch.setattr(task_worker, "_bitget_day_path", lambda _coin, day: day)

    days = task_worker._bitget_distributed_days_to_fetch(
        "1INCH",
        "20260101",
        "20260104",
        refetch=False,
    )

    assert [day.strftime("%Y%m%d") for day in days] == ["20260103", "20260104"]


def test_bitget_distributed_segments_for_days_preserve_gaps() -> None:
    """Distributed Bitget only chunks consecutive missing-day runs."""

    days = [
        task_worker._parse_day("20260101").date(),
        task_worker._parse_day("20260102").date(),
        task_worker._parse_day("20260105").date(),
    ]

    segments = task_worker._build_bitget_distributed_segments_for_days("1INCH", days, chunk_days=30)

    assert segments == [
        {"coin": "1INCH", "start_day": "20260101", "end_day": "20260102"},
        {"coin": "1INCH", "start_day": "20260105", "end_day": "20260105"},
    ]


def test_bitget_distributed_resolves_start_days_from_inception(monkeypatch) -> None:
    """Distributed Bitget resolves empty start ranges from REST inception probes."""

    seen: list[str] = []

    def fake_find_inception_ms(coin, **_kwargs):
        seen.append(coin)
        day = "20260120" if coin == "BTC" else "20260201"
        return task_worker._bitget_day_start_ms(task_worker._parse_day(day).date())

    monkeypatch.setattr(task_worker, "_bitget_find_inception_ms", fake_find_inception_ms)

    starts = task_worker._resolve_bitget_distributed_start_days(
        ["BTC", "ETH"],
        start_day="",
        end_day="20260205",
    )

    assert seen == ["BTC", "ETH"]
    assert starts == {"BTC": "20260120", "ETH": "20260201"}


def test_bitget_distributed_explicit_start_day_skips_inception_probe(monkeypatch) -> None:
    """Explicit Bitget distributed start dates keep overriding per-coin inception."""

    monkeypatch.setattr(
        task_worker,
        "_bitget_find_inception_ms",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected inception probe")),
    )

    starts = task_worker._resolve_bitget_distributed_start_days(
        ["BTC", "ETH"],
        start_day="20260101",
        end_day="20260205",
    )

    assert starts == {"BTC": "20260101", "ETH": "20260101"}


def test_bitget_distributed_hosts_accept_master_downloader() -> None:
    """Distributed Bitget can use the master as a local downloader."""

    hosts = task_worker._normalize_bitget_distributed_hosts(
        [
            {"hostname": "master", "mode": "master"},
            {"hostname": "master", "mode": "master"},
        ]
    )

    assert hosts == [
        {
            "hostname": "master",
            "label": "Master (local downloader)",
            "target": "master",
            "ssh_args": [],
            "ssh_command": "",
            "mode": "master",
        }
    ]


def test_bitget_distributed_hosts_add_default_ssh_timeouts() -> None:
    """Default SSH downloaders get non-interactive timeout and keepalive options."""

    hosts = task_worker._normalize_bitget_distributed_hosts(
        [
            {
                "hostname": "manibot51",
                "label": "manibot51",
                "target": "mani@manibot51",
                "ssh_command": "ssh",
            }
        ]
    )

    assert hosts[0]["ssh_args"] == ["ssh", *task_worker.BITGET_SSH_OPTIONS]
    assert "ConnectTimeout=20" in hosts[0]["ssh_command"]
    assert "ServerAliveInterval=15" in hosts[0]["ssh_command"]


def test_bitget_remote_download_command_uses_only_python3_stdlib() -> None:
    """Remote Bitget commands run a stdlib downloader, not remote PBGui code."""

    host = {
        "target": "pbgui@203.0.113.10",
        "ssh_args": ["ssh", "-p", "2222"],
    }
    segment = {"coin": "BTC", "start_day": "20260101", "end_day": "20260131"}

    cmd = task_worker._build_bitget_remote_download_command(
        host,
        segment,
        symbol="BTCUSDT",
        since_ms=1767225600000,
        end_ms=1769904000000,
    )

    assert cmd[:4] == ["ssh", "-p", "2222", "pbgui@203.0.113.10"]
    remote_cmd = cmd[4]
    assert remote_cmd.startswith("python3 -c ")
    assert "http.client" in remote_cmd
    assert "concurrent.futures" in remote_cmd
    assert "ThreadPoolExecutor" in remote_cmd
    assert "workers" in remote_cmd
    assert "api/v2/mix/market/history-candles" in remote_cmd
    assert "BTCUSDT" in remote_cmd
    assert "1767225600000" in remote_cmd
    assert "1769904000000" in remote_cmd
    assert "enqueue_running_job" not in remote_cmd
    assert "task_worker.py" not in remote_cmd
    assert "rsync" not in remote_cmd
    assert "/home/pbgui/software/pbgui" not in remote_cmd


def test_bitget_remote_download_segment_writes_npz_on_master(monkeypatch, tmp_path) -> None:
    """Remote raw candle rows are bucketed and written by the local master process."""

    job_path = tmp_path / "bitget-dist.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    since_ms = task_worker._bitget_day_start_ms(task_worker._parse_day("20260101").date())
    end_ms = since_ms + task_worker._BITGET_DAY_MS
    rows = [[str(since_ms), "1", "2", "0.5", "1.5", "10"]]
    script = "import json; print(json.dumps({'type':'rows','rows':" + repr(rows) + "}))"
    writes: list[dict] = []
    logs: list[str] = []

    def fake_write(coin, day_s, candles, overwrite=False):
        writes.append({"coin": coin, "day": day_s, "candles": dict(candles), "overwrite": overwrite})
        return len(candles)

    monkeypatch.setattr(task_worker, "_bitget_write_candles_for_day", fake_write)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, msg: logs.append(str(msg)))
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)

    result = task_worker._run_bitget_remote_download_segment(
        job_path,
        "bitget-dist",
        [sys.executable, "-c", script],
        label="local fake raw downloader",
        coin="BTC",
        since_ms=since_ms,
        end_ms=end_ms,
        refetch=False,
    )

    assert result["pages"] == 1
    assert result["rows"] == 1
    assert result["payload_bytes"] > 0
    assert result["days"] == 1
    assert result["minutes_written"] == 1
    assert writes[0]["coin"] == "BTC"
    assert writes[0]["day"] == "2026-01-01"
    assert writes[0]["overwrite"] is False
    assert list(writes[0]["candles"].keys()) == [0]
    joined_logs = "\n".join(logs)
    assert "starting stdlib remote downloader" in joined_logs
    assert "python3 -c" not in joined_logs
    assert script not in joined_logs


def test_bitget_distributed_streams_segments_while_planning_inceptions(monkeypatch, tmp_path) -> None:
    """Distributed Bitget starts downloading planned coins before all inceptions resolve."""

    job_path = tmp_path / "bitget-dist.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    logs: list[str] = []
    find_order: list[str] = []
    remote_calls: list[str] = []
    first_remote_started = threading.Event()

    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, msg: logs.append(str(msg)))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_refresh_inventory_coin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)
    monkeypatch.setattr(task_worker, "_coin_to_bitget_symbol", lambda coin: coin + "USDT")

    def fake_find_inception_ms(coin, **_kwargs):
        find_order.append(coin)
        if coin == "BOB":
            assert first_remote_started.wait(timeout=2.0)
            return task_worker._bitget_day_start_ms(task_worker._parse_day("20260102").date())
        return task_worker._bitget_day_start_ms(task_worker._parse_day("20260101").date())

    def fake_remote(*_args, **kwargs):
        coin = str(kwargs.get("coin") or "")
        remote_calls.append(coin)
        if coin == "ALICE":
            first_remote_started.set()
        return {"pages": 1, "rows": 1, "payload_bytes": 100, "days": 1, "minutes_written": 1}

    monkeypatch.setattr(task_worker, "_bitget_find_inception_ms", fake_find_inception_ms)
    monkeypatch.setattr(
        task_worker,
        "_bitget_distributed_days_to_fetch",
        lambda coin, _start_day, _end_day, **_kwargs: [task_worker._parse_day("20260101" if coin == "ALICE" else "20260102").date()],
    )
    monkeypatch.setattr(task_worker, "_run_bitget_remote_download_segment", fake_remote)

    task_worker._run_bitget_best_1m_distributed(
        job_path,
        {
            "coins": ["ALICE", "BOB"],
            "end_day": "20260103",
            "distributed_hosts": [
                {
                    "hostname": "manibot51",
                    "label": "manibot51",
                    "target": "mani@manibot51",
                    "ssh_command": "ssh",
                    "mode": "ssh",
                }
            ],
        },
    )

    joined_logs = "\n".join(logs)
    assert find_order == ["ALICE", "BOB"]
    assert remote_calls == ["ALICE", "BOB"]
    assert joined_logs.index("segment  coin=ALICE") < joined_logs.index("planning coin queued  coin=BOB")
    job = json.loads(job_path.read_text(encoding="utf-8"))
    assert job["progress"]["last_result"]["segments_done"] == 2


def test_bitget_distributed_skips_unavailable_coin_during_planning(monkeypatch, tmp_path) -> None:
    """Unavailable Bitget symbols are skipped without failing the distributed job."""

    job_path = tmp_path / "bitget-dist.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    logs: list[str] = []
    downloaded: list[str] = []

    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, msg: logs.append(str(msg)))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_refresh_inventory_coin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)
    monkeypatch.setattr(task_worker, "_coin_to_bitget_symbol", lambda coin: coin + "USDT")

    def fake_find_inception_ms(coin, **_kwargs):
        if coin == "HYUN":
            raise task_worker.BitgetUnavailableSymbolError("Bitget code=40034 msg=Symbol not exists")
        return task_worker._bitget_day_start_ms(task_worker._parse_day("20260101").date())

    def fake_master(**kwargs):
        downloaded.append(str(kwargs.get("coin")))
        return {"pages": 1, "rows": 1, "payload_bytes": 0, "days": 1, "minutes_written": 1}

    monkeypatch.setattr(task_worker, "_bitget_find_inception_ms", fake_find_inception_ms)
    monkeypatch.setattr(
        task_worker,
        "_bitget_distributed_days_to_fetch",
        lambda *_args, **_kwargs: [task_worker._parse_day("20260101").date()],
    )
    monkeypatch.setattr(task_worker, "_run_bitget_master_download_segment", fake_master)

    task_worker._run_bitget_best_1m_distributed(
        job_path,
        {
            "coins": ["BTC", "HYUN"],
            "end_day": "20260101",
            "distributed_hosts": [{"hostname": "master", "mode": "master"}],
        },
    )

    joined_logs = "\n".join(logs)
    job = json.loads(job_path.read_text(encoding="utf-8"))
    last_result = job["progress"]["last_result"]
    assert downloaded == ["BTC"]
    assert "planning coin skipped  coin=HYUN  reason=unavailable" in joined_logs
    assert last_result["segments_done"] == 1
    assert last_result["coin_start_days"] == {"BTC": "20260101"}
    assert last_result["skipped_coins"] == [{"coin": "HYUN", "reason": "unavailable", "error": "Bitget code=40034 msg=Symbol not exists"}]


def test_bitget_distributed_retries_ssh_segment_on_same_downloader(monkeypatch, tmp_path) -> None:
    """A transient SSH segment failure retries on the same downloader before requeueing."""

    job_path = tmp_path / "bitget-dist.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    logs: list[str] = []
    attempts = {"remote": 0, "master": 0}

    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, msg: logs.append(str(msg)))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_refresh_inventory_coin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)
    monkeypatch.setattr(task_worker, "_coin_to_bitget_symbol", lambda coin: coin + "USDT")
    monkeypatch.setattr(
        task_worker,
        "_bitget_distributed_days_to_fetch",
        lambda *_args, **_kwargs: [task_worker._parse_day("20210721").date()],
    )
    monkeypatch.setattr(
        task_worker,
        "_build_bitget_distributed_segments_for_days",
        lambda *_args, **_kwargs: [{"coin": "ALICE", "start_day": "20210721", "end_day": "20210819"}],
    )
    monkeypatch.setattr(task_worker, "_resolve_bitget_distributed_start_days", lambda _coins, **_kwargs: {"ALICE": "20210721"})

    def fake_remote(*_args, **_kwargs):
        attempts["remote"] += 1
        if attempts["remote"] == 1:
            raise RuntimeError("ssh failed with exit code 255")
        return {"pages": 101, "rows": 20200, "payload_bytes": 4096, "days": 30, "minutes_written": 43200}

    def fake_master(**_kwargs):
        attempts["master"] += 1
        return {"pages": 1, "rows": 1, "payload_bytes": 0, "days": 1, "minutes_written": 1}

    monkeypatch.setattr(task_worker, "_run_bitget_remote_download_segment", fake_remote)
    monkeypatch.setattr(task_worker, "_run_bitget_master_download_segment", fake_master)

    task_worker._run_bitget_best_1m_distributed(
        job_path,
        {
            "coins": ["ALICE"],
            "start_day": "20210721",
            "end_day": "20260629",
            "distributed_hosts": [
                {
                    "hostname": "manibot51",
                    "label": "manibot51",
                    "target": "mani@manibot51",
                    "ssh_command": "ssh",
                    "mode": "ssh",
                }
            ],
        },
    )

    joined_logs = "\n".join(logs)
    assert "ssh retry 2/3" in joined_logs
    assert "segment failed" not in joined_logs
    assert "Master fallback" not in joined_logs
    assert attempts == {"remote": 2, "master": 0}
    job = json.loads(job_path.read_text(encoding="utf-8"))
    last_result = job["progress"]["last_result"]
    assert job["progress"]["stage"] == "done"
    assert last_result["segments_done"] == 1
    assert last_result["host_results"] == [
        {
            "host": "manibot51",
            "mode": "ssh",
            "status": "done",
            "segments": 1,
            "failed_segments": 0,
            "coins": ["ALICE"],
            "pages": 101,
            "rows": 20200,
            "payload_bytes": 4096,
            "minutes_written": 43200,
        }
    ]


def test_bitget_distributed_requeues_failed_ssh_segment_to_master(monkeypatch, tmp_path) -> None:
    """A transient SSH downloader failure is retried by the local master fallback."""

    job_path = tmp_path / "bitget-dist.json"
    job_path.write_text(json.dumps({"progress": {}, "status": "running"}), encoding="utf-8")
    logs: list[str] = []

    monkeypatch.setattr(task_worker, "_init_job_log", lambda _job_id: None)
    monkeypatch.setattr(task_worker, "_append_to_job_log", lambda _job_id, msg: logs.append(str(msg)))
    monkeypatch.setattr(task_worker, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_refresh_inventory_coin", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(task_worker, "_is_cancel_requested", lambda _job_path: False)
    monkeypatch.setattr(task_worker, "_coin_to_bitget_symbol", lambda coin: coin + "USDT")
    monkeypatch.setattr(
        task_worker,
        "_bitget_distributed_days_to_fetch",
        lambda *_args, **_kwargs: [task_worker._parse_day("20210721").date()],
    )
    monkeypatch.setattr(
        task_worker,
        "_build_bitget_distributed_segments_for_days",
        lambda *_args, **_kwargs: [{"coin": "ALICE", "start_day": "20210721", "end_day": "20210803"}],
    )
    monkeypatch.setattr(task_worker, "_resolve_bitget_distributed_start_days", lambda _coins, **_kwargs: {"ALICE": "20210721"})
    monkeypatch.setattr(
        task_worker,
        "_run_bitget_remote_download_segment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("ssh failed with exit code 255")),
    )
    monkeypatch.setattr(
        task_worker,
        "_run_bitget_master_download_segment",
        lambda **_kwargs: {"pages": 101, "rows": 20200, "payload_bytes": 0, "days": 14, "minutes_written": 20160},
    )

    task_worker._run_bitget_best_1m_distributed(
        job_path,
        {
            "coins": ["ALICE"],
            "start_day": "20210721",
            "end_day": "20260629",
            "distributed_hosts": [
                {
                    "hostname": "manibot55",
                    "label": "manibot55",
                    "target": "mani@manibot55",
                    "ssh_command": "ssh",
                    "mode": "ssh",
                }
            ],
        },
    )

    joined_logs = "\n".join(logs)
    assert "segment failed" in joined_logs
    assert "requeued=1" in joined_logs
    assert "Master fallback done" in joined_logs
    job = json.loads(job_path.read_text(encoding="utf-8"))
    last_result = job["progress"]["last_result"]
    assert job["progress"]["stage"] == "done"
    assert last_result["segments_done"] == 1
    assert last_result["host_results"][0]["status"] == "failed"
    assert last_result["host_results"][1]["host"] == "Master fallback"
