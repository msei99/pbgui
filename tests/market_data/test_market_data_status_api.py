"""Tests for market-data status API filtering."""

from pathlib import Path
import configparser
import importlib
import importlib.util
import sys
from types import SimpleNamespace

import pytest


repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))

pbcoindata_path = repo_root / "PBCoinData.py"
pbcoindata_spec = importlib.util.spec_from_file_location("PBCoinData", pbcoindata_path)
pbcoindata_module = importlib.util.module_from_spec(pbcoindata_spec)
sys.modules["PBCoinData"] = pbcoindata_module
pbcoindata_spec.loader.exec_module(pbcoindata_module)

market_data_api = importlib.import_module("api.market_data")


def test_filter_status_coins_to_enabled_prunes_removed_coin(monkeypatch) -> None:
    """Status payloads keep only currently enabled coins."""

    monkeypatch.setattr(market_data_api, "load_market_data_config", lambda: object())
    monkeypatch.setattr(
        market_data_api,
        "get_effective_enabled_coins",
        lambda exchange, cfg=None: (["BTC", "ETH"], [], True),
    )

    status = {
        "coins": {
            "BTC": {"result": "ok"},
            "OM": {"result": "ok"},
        },
        "coins_total": 3,
        "coins_done": 2,
        "current_coin": "OM",
    }

    filtered = market_data_api._filter_status_coins_to_enabled("hyperliquid", status)

    assert filtered["coins"] == {"BTC": {"result": "ok"}}
    assert filtered["coins_total"] == 2
    assert filtered["coins_done"] == 2
    assert filtered["current_coin"] == ""


def test_okx_status_and_best_1m_wiring() -> None:
    """OKX has status flag keys and Best 1m queue metadata."""

    assert market_data_api._get_exchange_status_key("okx") == "okx_latest_1m"
    assert market_data_api._get_exchange_flag_prefix("okx") == "okx_latest_1m"

    meta = market_data_api._best_1m_exchange_meta("okx")
    assert meta is not None
    assert meta["label"] == "OKX"
    assert meta["job_type"] == "okx_best_1m"
    assert meta["queue_exchange"] == "okx"


def test_save_market_data_settings_queues_refresh_flag(monkeypatch, tmp_path) -> None:
    """Saving settings wakes the PBData latest-1m loop immediately."""

    saved_ini: list[tuple[str, str, str]] = []

    monkeypatch.setattr(market_data_api, "PBGDIR", tmp_path)
    monkeypatch.setattr(market_data_api, "set_enabled_coins", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(market_data_api, "set_auto_enable_new_coins", lambda *_args, **_kwargs: None)
    def capture_update(mutator) -> None:
        parser = configparser.ConfigParser()
        mutator(parser)
        saved_ini.extend(
            (section, key, value)
            for section in parser.sections()
            for key, value in parser.items(section)
        )

    monkeypatch.setattr(market_data_api, "update_ini", capture_update)
    monkeypatch.setattr(
        market_data_api,
        "_build_market_data_settings_payload",
        lambda exchange: {"exchange": exchange},
    )

    result = market_data_api._save_market_data_settings(
        "okx",
        {
            "enabled_coins": ["BTC"],
            "auto_enable_new_coins": True,
            "settings": {
                "interval_seconds": 3600,
                "coin_pause_seconds": 0.5,
                "api_timeout_seconds": 30,
                "min_lookback_days": 2,
                "max_lookback_days": 7,
            },
        },
    )

    assert result == {"exchange": "okx"}
    assert (tmp_path / "data" / "logs" / "okx_latest_1m_run_now.flag").exists()
    assert ("okx_data", "latest_1m_interval_seconds", "3600") in saved_ini


def test_best_1m_available_coins_do_not_require_enabled_settings(monkeypatch) -> None:
    """Manual Best 1m builds list available coins, not auto-refresh enabled coins."""

    monkeypatch.setattr(market_data_api, "get_market_data_coin_options", lambda exchange: ["BTC", "ETH"])
    monkeypatch.setattr(
        market_data_api,
        "get_effective_enabled_coins",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("enabled coins should not be used")),
    )

    assert market_data_api._get_best_1m_available_coins("okx") == ["BTC", "ETH"]


def test_best_1m_request_normalization_preserves_bitget_mapping_coin() -> None:
    """Bitget mapping coins such as HYUNDAI must not be shortened to invalid symbols."""

    available = ["BTC", "HYUNDAI"]

    assert market_data_api._normalize_best_1m_request_coin("HYUNDAI", available_coins=available) == "HYUNDAI"
    assert market_data_api._normalize_best_1m_request_coin("BTCUSDT", available_coins=available) == "BTC"
    assert market_data_api._normalize_best_1m_request_coin("HYUN", available_coins=available) == "HYUN"


def test_bitget_best_1m_queue_rejects_false_coin_after_normalization(monkeypatch, tmp_path) -> None:
    """Bitget Best-1m jobs reject unsupported coins before writing a job payload."""

    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda exchange: ["BTC", "HYUNDAI"])

    result = market_data_api.queue_best_1m_job(
        "bitget",
        {"coins": ["HYUN"], "selected_only": True, "end_day": "20260629"},
        None,
    )

    assert result["success"] is False
    assert "Unsupported coin(s)" in result["error"]
    assert "HYUN" in result["error"]


def test_bitget_best_1m_queue_keeps_hyundai_mapping_coin(monkeypatch, tmp_path) -> None:
    """Bitget Best-1m queue payloads keep the mapping coin instead of symbol-code shortening."""

    enqueued: list[dict] = []
    popen_calls: list[list[str]] = []

    def fake_enqueue_running_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="bitget-1", path=str(tmp_path / "bitget-1.json"))

    def fake_popen(cmd: list[str], **_kwargs):
        popen_calls.append(cmd)
        return SimpleNamespace(pid=12345)

    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda exchange: ["BTC", "HYUNDAI"])
    monkeypatch.setattr("task_queue.enqueue_running_job", fake_enqueue_running_job)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("subprocess.Popen", fake_popen)

    result = market_data_api.queue_best_1m_job(
        "bitget",
        {"coins": ["HYUNDAI"], "selected_only": True, "end_day": "20260629"},
        None,
    )

    assert result["success"] is True
    assert enqueued[0]["payload"]["coins"] == ["HYUNDAI"]
    assert popen_calls


def test_copy_data_queue_payload_updates_changed_files() -> None:
    """Copy Data queue payloads always update changed files."""

    payload = market_data_api._build_copy_data_queue_payload(
        {
            "target": "localhost",
            "ssh_command": "ssh -J user@jump-host -p 2222",
            "exchanges": ["binance", "bybit", "binanceusdm"],
        }
    )

    assert payload["target"] == "localhost"
    assert payload["mode"] == "update"
    assert payload["exchanges"] == ["binance", "bybit"]
    assert payload["exchange_storage"]["binance"] == "binanceusdm"
    assert payload["destination_root"].endswith("/data/ohlcv")


def test_copy_data_queue_payload_rejects_target_inside_ssh_command() -> None:
    """The SSH command field must not include the rsync target host."""

    with pytest.raises(ValueError, match="must not include the target host"):
        market_data_api._build_copy_data_queue_payload(
            {
                "target": "localhost",
                "ssh_command": "ssh -J user@jump-host -p 2222 localhost",
                "exchanges": ["bybit"],
            }
        )


def test_copy_data_queue_payload_rejects_remote_path_metacharacters() -> None:
    """Destination roots reject shell metacharacters before remote mkdir is queued."""

    with pytest.raises(ValueError, match="Destination root contains unsupported characters"):
        market_data_api._build_copy_data_queue_payload(
            {
                "target": "localhost",
                "destination_root": "/tmp/ohlcv;touch-pwned",
                "exchanges": ["bybit"],
            }
        )


def test_copy_data_ssh_test_command_supports_proxy_jump() -> None:
    """The read-only SSH test command keeps ProxyJump options and appends the target separately."""

    payload = market_data_api._build_copy_data_queue_payload(
        {
            "target": "localhost",
            "ssh_command": "ssh -J user@jump-host -p 2222",
            "destination_root": "/home/mani/software/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
        }
    )

    cmd = market_data_api._build_copy_data_ssh_test_command(payload, ["test", "-d", payload["destination_root"]])

    assert cmd == [
        "ssh",
        "-J",
        "user@jump-host",
        "-p",
        "2222",
        "localhost",
        "test",
        "-d",
        "/home/mani/software/pbgui/data/ohlcv",
    ]


def test_copy_data_test_payload_does_not_require_exchange_selection() -> None:
    """The read-only connection test validates target/path without requiring copy exchanges."""

    payload = market_data_api._build_copy_data_test_payload(
        {
            "target": "localhost",
            "ssh_command": "ssh -J user@jump-host -p 2222",
            "destination_root": "/home/mani/software/pbgui/data/ohlcv",
        }
    )

    assert payload == {
        "target": "localhost",
        "ssh_command": "ssh -J user@jump-host -p 2222",
        "destination_root": "/home/mani/software/pbgui/data/ohlcv",
    }


def test_copy_data_connection_payload_reports_writable_root(monkeypatch) -> None:
    """Connection checks report success when SSH, root existence, and root writability pass."""

    calls: list[list[str]] = []

    def fake_probe(cmd: list[str], *, timeout_s: float = 12.0) -> dict:
        calls.append(cmd)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(market_data_api, "_run_copy_data_ssh_probe", fake_probe)
    payload = market_data_api._build_copy_data_queue_payload(
        {
            "target": "optimizer",
            "ssh_command": "ssh",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["okx"],
        }
    )

    result = market_data_api._test_copy_data_connection_payload(payload)

    assert result["success"] is True
    assert "exists and is writable" in result["message"]
    assert calls == [
        ["ssh", "optimizer", "printf", "PBGUI_COPY_TEST_OK"],
        ["ssh", "optimizer", "test", "-d", "/srv/pbgui/data/ohlcv"],
        ["ssh", "optimizer", "test", "-w", "/srv/pbgui/data/ohlcv"],
    ]


def test_copy_data_dry_run_queue_uses_dry_run_job_type(monkeypatch) -> None:
    """The Dry run endpoint queues a write-free OHLCV dry-run worker job."""

    enqueued: list[dict] = []
    popen_calls: list[list[str]] = []

    def fake_enqueue_running_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="dry-run-1", path=str(repo_root / "data" / "ohlcv" / "_tasks" / "running" / "dry-run-1.json"))

    def fake_popen(cmd: list[str], **_kwargs):
        popen_calls.append(cmd)
        return SimpleNamespace(pid=12345)

    monkeypatch.setattr("task_queue.enqueue_running_job", fake_enqueue_running_job)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("subprocess.Popen", fake_popen)

    result = market_data_api.queue_copy_data_dry_run_job(
        {
            "target": "optimizer",
            "ssh_command": "ssh -J user@jump-host -p 2222",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
        },
        None,
    )

    assert result["success"] is True
    assert result["dry_run"] is True
    assert result["runner_started"] is True
    assert result["job_type"] == "ohlcv_copy_dry_run"
    assert enqueued[0]["job_type"] == "ohlcv_copy_dry_run"
    assert enqueued[0]["exchange"] == "ohlcv"
    assert enqueued[0]["manual_parallel"] is True
    assert enqueued[0]["payload"]["dry_run"] is True
    assert enqueued[0]["payload"]["mode"] == "update"
    assert enqueued[0]["payload"]["exchanges"] == ["bybit"]
    assert popen_calls
    assert popen_calls[0][1].endswith("task_worker.py")
    assert popen_calls[0][2] == "--run-job"


def test_copy_data_queue_uses_fresh_one_shot_worker(monkeypatch) -> None:
    """Real Copy Data jobs also use a fresh runner so stale resident workers cannot consume them."""

    enqueued: list[dict] = []
    popen_calls: list[list[str]] = []

    def fake_enqueue_running_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="copy-1", path=str(repo_root / "data" / "ohlcv" / "_tasks" / "running" / "copy-1.json"))

    def fake_popen(cmd: list[str], **_kwargs):
        popen_calls.append(cmd)
        return SimpleNamespace(pid=12345)

    monkeypatch.setattr("task_queue.enqueue_running_job", fake_enqueue_running_job)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("subprocess.Popen", fake_popen)

    result = market_data_api.queue_copy_data_job(
        {
            "target": "optimizer",
            "ssh_command": "ssh -J user@jump-host -p 2222",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["binance", "bybit"],
        },
        None,
    )

    assert result["success"] is True
    assert result["dry_run"] is False
    assert result["runner_started"] is True
    assert result["worker_started"] is False
    assert result["job_type"] == "ohlcv_copy"
    assert enqueued[0]["job_type"] == "ohlcv_copy"
    assert enqueued[0]["exchange"] == "ohlcv"
    assert enqueued[0]["manual_parallel"] is True
    assert "dry_run" not in enqueued[0]["payload"]
    assert enqueued[0]["payload"]["exchanges"] == ["binance", "bybit"]
    assert popen_calls
    assert popen_calls[0][1].endswith("task_worker.py")
    assert popen_calls[0][2] == "--run-job"


def test_bitget_best_1m_queue_uses_fresh_one_shot_worker(monkeypatch) -> None:
    """Bitget Best 1m jobs bypass stale resident workers when queued."""

    enqueued: list[dict] = []
    popen_calls: list[list[str]] = []

    def fake_enqueue_running_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="bitget-1", path=str(repo_root / "data" / "ohlcv" / "_tasks" / "running" / "bitget-1.json"))

    def fake_popen(cmd: list[str], **_kwargs):
        popen_calls.append(cmd)
        return SimpleNamespace(pid=12345)

    monkeypatch.setattr("task_queue.enqueue_running_job", fake_enqueue_running_job)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda _exchange: ["BTC"])

    result = market_data_api.queue_best_1m_job(
        "bitget",
        {"coins": ["BTC"], "end_day": "20260629", "refetch": False},
        None,
    )

    assert result["success"] is True
    assert result["runner_started"] is True
    assert enqueued[0]["job_type"] == "bitget_best_1m"
    assert enqueued[0]["exchange"] == "bitget"
    assert enqueued[0]["payload"]["coins"] == ["BTC"]
    assert popen_calls
    assert popen_calls[0][1].endswith("task_worker.py")
    assert popen_calls[0][2] == "--run-job"


def test_okx_best_1m_queue_enqueues_selected_range(monkeypatch, tmp_path) -> None:
    """OKX Best 1m jobs retain the selected coins, date range, and refetch flag."""

    enqueued: list[dict] = []

    def fake_enqueue_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="okx-1", path=str(tmp_path / "okx-1.json"))

    monkeypatch.setattr("task_queue.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr("task_queue.read_worker_pid", lambda: 12345)
    monkeypatch.setattr("task_queue.is_pid_running", lambda _pid: True)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "subprocess.Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("resident worker should be reused")),
    )
    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda _exchange: ["BTC", "ETH", "SOL"])

    result = market_data_api.queue_best_1m_job(
        "okx",
        {
            "coins": ["BTC", "ETH"],
            "selected_only": True,
            "start_day": "20260101",
            "end_day": "20260131",
            "refetch": True,
        },
        None,
    )

    assert result["success"] is True
    assert result["job_type"] == "okx_best_1m"
    assert enqueued == [
        {
            "job_type": "okx_best_1m",
            "exchange": "okx",
            "payload": {
                "coins": ["BTC", "ETH"],
                "start_day": "20260101",
                "end_day": "20260131",
                "refetch": True,
            },
        }
    ]


def test_bitget_distributed_queue_uses_selected_vps_hosts(monkeypatch) -> None:
    """Distributed Bitget queue requests store selected known VPS hosts in the worker payload."""

    enqueued: list[dict] = []
    popen_calls: list[list[str]] = []

    def fake_enqueue_running_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="bitget-dist-1", path=str(repo_root / "data" / "ohlcv" / "_tasks" / "running" / "bitget-dist-1.json"))

    def fake_popen(cmd: list[str], **_kwargs):
        popen_calls.append(cmd)
        return SimpleNamespace(pid=12345)

    monkeypatch.setattr("task_queue.enqueue_running_job", fake_enqueue_running_job)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("subprocess.Popen", fake_popen)
    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda _exchange: ["BTC"])
    monkeypatch.setattr(
        market_data_api,
        "_load_bitget_distributed_hosts",
        lambda: [
            {
                "hostname": "vps-a",
                "label": "vps-a (pbgui@203.0.113.10)",
                "target": "pbgui@203.0.113.10",
                "ssh_command": "ssh -p 2222",
            }
        ],
    )

    result = market_data_api.queue_best_1m_job(
        "bitget",
        {
            "coins": ["BTC"],
            "start_day": "20260101",
            "end_day": "20260131",
            "distributed": True,
            "distributed_hosts": ["vps-a"],
        },
        None,
    )

    assert result["success"] is True
    assert result["job_type"] == "bitget_best_1m_distributed"
    assert result["distributed"] is True
    assert result["distributed_hosts_count"] == 1
    assert enqueued[0]["job_type"] == "bitget_best_1m_distributed"
    assert enqueued[0]["exchange"] == "bitget"
    assert enqueued[0]["payload"]["distributed_hosts"][0]["hostname"] == "vps-a"
    assert popen_calls[0][1].endswith("task_worker.py")
    assert popen_calls[0][2] == "--run-job"


def test_bitget_distributed_queue_rejects_unknown_host(monkeypatch) -> None:
    """Distributed queue requests can only reference known VPS hosts."""

    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda _exchange: ["BTC"])
    monkeypatch.setattr(market_data_api, "_load_bitget_distributed_hosts", lambda: [])

    result = market_data_api.queue_best_1m_job(
        "bitget",
        {"coins": ["BTC"], "end_day": "20260131", "distributed": True, "distributed_hosts": ["missing"]},
        None,
    )

    assert result["success"] is False
    assert "Unknown or unsupported Bitget downloader" in result["error"]


def test_bitget_distributed_queue_accepts_master_downloader(monkeypatch) -> None:
    """Distributed queue requests can target the master downloader without SSH."""

    enqueued: list[dict] = []

    def fake_enqueue_running_job(**kwargs):
        enqueued.append(kwargs)
        return SimpleNamespace(job_id="bitget-dist-master", path=str(repo_root / "data" / "ohlcv" / "_tasks" / "running" / "bitget-dist-master.json"))

    monkeypatch.setattr("task_queue.enqueue_running_job", fake_enqueue_running_job)
    monkeypatch.setattr("market_data.append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("subprocess.Popen", lambda *_args, **_kwargs: SimpleNamespace(pid=12345))
    monkeypatch.setattr(market_data_api, "_get_best_1m_available_coins", lambda _exchange: ["BTC"])
    monkeypatch.setattr(
        market_data_api,
        "_load_bitget_distributed_hosts",
        lambda: [
            {
                "hostname": "master",
                "label": "Master (local downloader)",
                "target": "master",
                "ssh_command": "",
                "mode": "master",
            }
        ],
    )

    result = market_data_api.queue_best_1m_job(
        "bitget",
        {
            "coins": ["BTC"],
            "start_day": "20260101",
            "end_day": "20260131",
            "distributed": True,
            "distributed_hosts": ["master"],
        },
        None,
    )

    assert result["success"] is True
    assert result["distributed_hosts_count"] == 1
    assert enqueued[0]["payload"]["distributed_hosts"][0]["mode"] == "master"
    assert enqueued[0]["payload"]["distributed_hosts"][0]["target"] == "master"


def test_bitget_failed_job_retry_starts_fresh_runner(monkeypatch) -> None:
    """Retrying failed Bitget jobs starts the current one-shot worker immediately."""

    jobs_api = importlib.import_module("api.jobs")
    started: list[str] = []

    monkeypatch.setattr(
        jobs_api,
        "list_jobs",
        lambda **_kwargs: [{"id": "bitget-1", "type": "bitget_best_1m", "status": "failed"}],
    )
    monkeypatch.setattr(jobs_api, "retry_failed_job", lambda job_id: job_id == "bitget-1")

    def fake_start_pending_job(job_id: str):
        started.append(job_id)
        return True, ""

    monkeypatch.setattr(jobs_api, "start_pending_job", fake_start_pending_job)

    result = jobs_api.retry_job("bitget-1", None)

    assert result == {"success": True, "job_id": "bitget-1", "runner_started": True}
    assert started == ["bitget-1"]


def test_bitget_distributed_failed_job_retry_starts_fresh_runner(monkeypatch) -> None:
    """Retrying failed distributed Bitget jobs starts a one-shot worker immediately."""

    jobs_api = importlib.import_module("api.jobs")
    started: list[str] = []

    monkeypatch.setattr(
        jobs_api,
        "list_jobs",
        lambda **_kwargs: [{"id": "bitget-dist-1", "type": "bitget_best_1m_distributed", "status": "failed"}],
    )
    monkeypatch.setattr(jobs_api, "retry_failed_job", lambda job_id: job_id == "bitget-dist-1")
    monkeypatch.setattr(jobs_api, "start_pending_job", lambda job_id: (started.append(job_id) or True, ""))

    result = jobs_api.retry_job("bitget-dist-1", None)

    assert result == {"success": True, "job_id": "bitget-dist-1", "runner_started": True}
    assert started == ["bitget-dist-1"]
