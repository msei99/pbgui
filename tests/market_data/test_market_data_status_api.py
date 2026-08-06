"""Tests for market-data status API filtering."""

import asyncio
from pathlib import Path
import configparser
import importlib
import importlib.util
import json
import sys
import threading
import time
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


def test_internal_status_decodes_large_payload_off_event_loop(monkeypatch) -> None:
    """PBData snapshot decoding must not block unrelated API requests."""
    calls = []

    class Request:
        client = SimpleNamespace(host="127.0.0.1")

        async def body(self) -> bytes:
            return b'{"latest_1m":{"coins_done":42}}'

    async def run_in_thread(function, *args):
        calls.append(function)
        return function(*args)

    monkeypatch.setattr(market_data_api.asyncio, "to_thread", run_in_thread)
    monkeypatch.setattr(market_data_api, "_market_data_status_snapshot", {"binance_latest_1m": {"coins_done": 3}})
    result = asyncio.run(market_data_api.update_market_data_status_snapshot(Request()))

    assert result == {"ok": True}
    assert calls == [market_data_api.json.loads]
    assert market_data_api._market_data_status_snapshot["latest_1m"]["coins_done"] == 42
    assert market_data_api._market_data_status_snapshot["binance_latest_1m"]["coins_done"] == 3


def test_internal_status_decode_keeps_event_loop_responsive(monkeypatch) -> None:
    """A slow snapshot decode must not delay unrelated event-loop work."""
    real_loads = market_data_api.json.loads

    class Request:
        client = SimpleNamespace(host="127.0.0.1")

        async def body(self) -> bytes:
            return b'{"latest_1m":{"coins_done":42}}'

    def slow_loads(payload):
        time.sleep(0.05)
        return real_loads(payload)

    async def scenario() -> None:
        task = asyncio.create_task(market_data_api.update_market_data_status_snapshot(Request()))
        await asyncio.sleep(0.005)
        assert task.done() is False
        assert await task == {"ok": True}

    monkeypatch.setattr(market_data_api.json, "loads", slow_loads)
    asyncio.run(scenario())


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


def test_copy_data_queue_uses_fresh_one_shot_worker(monkeypatch, tmp_path) -> None:
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
    monkeypatch.setattr(market_data_api, "_copy_data_dispatch_lock_file", lambda: tmp_path / "copy-dispatch")
    monkeypatch.setattr(market_data_api, "_copy_data_payload_has_active_job", lambda *_args, **_kwargs: False)

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


def test_copy_data_schedule_persists_validated_copy_payload(monkeypatch, tmp_path) -> None:
    """Copy schedules persist their sanitized target and recurring interval atomically."""

    schedule_path = tmp_path / "market_data" / "copy_data_schedules.json"
    monkeypatch.setattr(market_data_api, "_copy_data_schedules_file", lambda: schedule_path)
    market_data_api._copy_data_schedules.clear()

    result = market_data_api.save_copy_data_schedule(
        {
            "name": "Optimizer refresh",
            "target": "optimizer",
            "ssh_command": "ssh -p 2222",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["binance", "okx"],
            "interval_hours": 6,
            "enabled": True,
        },
        None,
    )

    assert result["success"] is True
    assert result["schedule"]["interval_hours"] == 6
    assert result["schedule"]["next_run"]
    assert schedule_path.exists()
    assert schedule_path.stat().st_mode & 0o777 == 0o600

    market_data_api._copy_data_schedules.clear()
    loaded = market_data_api.get_copy_data_schedules(None)

    assert len(loaded["schedules"]) == 1
    assert loaded["schedules"][0]["target"] == "optimizer"
    assert loaded["schedules"][0]["exchanges"] == ["binance", "okx"]
    market_data_api._copy_data_schedules.clear()


def test_copy_data_schedule_store_error_blocks_mutation(monkeypatch, tmp_path) -> None:
    """A damaged schedule file is preserved and blocks writes instead of becoming an empty store."""

    schedule_path = tmp_path / "market_data" / "copy_data_schedules.json"
    schedule_path.parent.mkdir(parents=True)
    damaged = b'{"not": "a schedule list"}\n'
    schedule_path.write_bytes(damaged)
    monkeypatch.setattr(market_data_api, "_copy_data_schedules_file", lambda: schedule_path)
    market_data_api._copy_data_schedules.clear()

    with pytest.raises(market_data_api.HTTPException) as exc_info:
        market_data_api.save_copy_data_schedule(
            {
                "target": "optimizer",
                "ssh_command": "ssh",
                "destination_root": "/srv/pbgui/data/ohlcv",
                "exchanges": ["bybit"],
                "interval_hours": 6,
                "enabled": True,
            },
            None,
        )

    assert exc_info.value.status_code == 500
    assert schedule_path.read_bytes() == damaged


def test_copy_data_schedule_store_rejects_duplicate_ids(monkeypatch, tmp_path) -> None:
    """Duplicate canonical IDs fail the whole load instead of silently dropping a schedule."""

    schedule_path = tmp_path / "market_data" / "copy_data_schedules.json"
    monkeypatch.setattr(market_data_api, "_copy_data_schedules_file", lambda: schedule_path)
    market_data_api._copy_data_schedules.clear()
    schedule = market_data_api.save_copy_data_schedule(
        {
            "target": "optimizer",
            "ssh_command": "ssh",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
            "interval_hours": 6,
            "enabled": True,
        },
        None,
    )["schedule"]
    duplicate_payload = json.dumps([schedule, schedule], indent=4) + "\n"
    schedule_path.write_text(duplicate_payload, encoding="utf-8")

    with pytest.raises(market_data_api.HTTPException) as exc_info:
        market_data_api.get_copy_data_schedules(None)

    assert exc_info.value.status_code == 500
    assert schedule_path.read_text(encoding="utf-8") == duplicate_payload
    market_data_api._copy_data_schedules.clear()


def test_copy_data_manual_queue_rejects_overlapping_destination(monkeypatch, tmp_path) -> None:
    """Regular Copy Data requests share the scheduled destination overlap guard."""

    enqueued: list[dict] = []
    monkeypatch.setattr("task_queue.enqueue_running_job", lambda **kwargs: enqueued.append(kwargs))
    monkeypatch.setattr(market_data_api, "_copy_data_dispatch_lock_file", lambda: tmp_path / "copy-dispatch")
    monkeypatch.setattr(market_data_api, "_copy_data_payload_has_active_job", lambda *_args, **_kwargs: True)

    result = market_data_api.queue_copy_data_job(
        {
            "target": "optimizer",
            "ssh_command": "ssh",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
        },
        None,
    )

    assert result["success"] is False
    assert "overlapping active work" in result["error"]
    assert enqueued == []


def test_copy_data_scheduler_queues_due_schedule_once(monkeypatch, tmp_path) -> None:
    """A due recurring schedule launches one tagged Copy Data worker and advances its next run."""

    schedule_path = tmp_path / "market_data" / "copy_data_schedules.json"
    monkeypatch.setattr(market_data_api, "_copy_data_schedules_file", lambda: schedule_path)
    monkeypatch.setattr(market_data_api, "_copy_data_schedule_has_active_job", lambda *_args: False)
    queued: list[dict] = []

    def fake_queue(
        request: dict,
        *,
        dry_run: bool,
        schedule_id: str = "",
        dispatch_id: str = "",
    ) -> dict:
        queued.append(
            {
                "request": dict(request),
                "dry_run": dry_run,
                "schedule_id": schedule_id,
                "dispatch_id": dispatch_id,
            }
        )
        return {"success": True, "job_id": "scheduled-copy-1"}

    monkeypatch.setattr(market_data_api, "_queue_copy_data_job_response", fake_queue)
    market_data_api._copy_data_schedules.clear()
    saved = market_data_api.save_copy_data_schedule(
        {
            "name": "Optimizer refresh",
            "target": "optimizer",
            "ssh_command": "ssh",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
            "interval_hours": 1,
            "enabled": True,
        },
        None,
    )["schedule"]
    market_data_api._copy_data_schedules[saved["id"]]["next_run"] = "2000-01-01T00:00:00+00:00"
    market_data_api._save_copy_data_schedules()

    market_data_api._run_copy_data_scheduler_tick()

    assert len(queued) == 1
    assert queued[0]["schedule_id"] == saved["id"]
    assert queued[0]["dispatch_id"]
    assert queued[0]["dry_run"] is False
    assert market_data_api._copy_data_schedules[saved["id"]]["last_job_id"] == "scheduled-copy-1"
    assert market_data_api._copy_data_schedules[saved["id"]]["dispatch_pending_at"] == ""
    assert market_data_api._copy_data_schedules[saved["id"]]["next_run"] > saved["next_run"]
    market_data_api._copy_data_schedules.clear()


@pytest.mark.parametrize("interval_hours", [0, 169, 1.5, "invalid"])
def test_copy_data_schedule_rejects_invalid_intervals(monkeypatch, tmp_path, interval_hours) -> None:
    """Recurring copy intervals must be whole hours inside the supported range."""

    schedule_path = tmp_path / "market_data" / "copy_data_schedules.json"
    monkeypatch.setattr(market_data_api, "_copy_data_schedules_file", lambda: schedule_path)
    market_data_api._copy_data_schedules.clear()

    result = market_data_api.save_copy_data_schedule(
        {
            "target": "optimizer",
            "ssh_command": "ssh",
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
            "interval_hours": interval_hours,
            "enabled": True,
        },
        None,
    )

    assert result["success"] is False
    assert "interval" in result["error"].lower()
    market_data_api._copy_data_schedules.clear()


@pytest.mark.parametrize(
    "ssh_command",
    ["/tmp/ssh -p 22", "ssh -o ProxyCommand=malicious", "ssh -F /tmp/config"],
)
def test_copy_data_schedule_rejects_unsafe_ssh_commands(monkeypatch, tmp_path, ssh_command) -> None:
    """Persisted Copy Data commands reject executable paths and shell-capable SSH options."""

    monkeypatch.setattr(
        market_data_api,
        "_copy_data_schedules_file",
        lambda: tmp_path / "market_data" / "copy_data_schedules.json",
    )
    market_data_api._copy_data_schedules.clear()

    result = market_data_api.save_copy_data_schedule(
        {
            "target": "optimizer",
            "ssh_command": ssh_command,
            "destination_root": "/srv/pbgui/data/ohlcv",
            "exchanges": ["bybit"],
            "interval_hours": 6,
            "enabled": True,
        },
        None,
    )

    assert result["success"] is False
    assert "ssh command" in result["error"].lower()


@pytest.mark.parametrize("target", ["-E", "user@-host"])
def test_copy_data_rejects_option_like_targets(target) -> None:
    """Remote targets cannot be interpreted as additional SSH options."""

    with pytest.raises(ValueError, match="Remote target is invalid"):
        market_data_api._normalize_copy_data_target(target)


def test_copy_data_scheduler_shutdown_waits_for_active_tick(monkeypatch) -> None:
    """Scheduler shutdown waits for its active thread-backed tick before returning."""

    started = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(market_data_api, "_load_copy_data_schedules", lambda: True)

    def blocking_tick() -> None:
        started.set()
        release.wait(timeout=2)

    monkeypatch.setattr(market_data_api, "_run_copy_data_scheduler_tick", blocking_tick)

    async def scenario() -> None:
        market_data_api.startup()
        assert await asyncio.to_thread(started.wait, 1)
        shutdown_task = asyncio.create_task(market_data_api.shutdown())
        await asyncio.sleep(0.01)
        assert shutdown_task.done() is False
        release.set()
        await asyncio.wait_for(shutdown_task, timeout=1)
        assert market_data_api._copy_data_scheduler_task is None

    try:
        asyncio.run(scenario())
    finally:
        release.set()


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


def test_checksum_settings_keep_publish_and_reference_archives_independent(monkeypatch) -> None:
    """A writable own archive and a public comparison archive may differ."""
    monkeypatch.setattr(
        market_data_api,
        "list_github_archives",
        lambda: [
            {"name": "mine", "can_publish": True, "can_reference": True},
            {"name": "community", "can_publish": False, "can_reference": True},
        ],
    )
    saved = {}

    def update(mutator):
        parser = configparser.ConfigParser()
        mutator(parser)
        saved.update(dict(parser["market_data"]))

    monkeypatch.setattr(market_data_api, "update_ini", update)
    monkeypatch.setattr(market_data_api, "_checksum_settings_payload", lambda: {"saved": True})

    result = market_data_api._save_checksum_settings(
        {
            "publish_enabled": True,
            "publish_archive": "mine",
            "reference_archive": "community",
        }
    )

    assert result == {"saved": True}
    assert saved == {
        "checksum_publish_enabled": "true",
        "checksum_publish_archive": "mine",
        "checksum_reference_archive": "community",
    }


def test_checksum_settings_reject_non_writable_publish_archive(monkeypatch) -> None:
    """Reference-only community archives cannot be selected as publishers."""
    monkeypatch.setattr(
        market_data_api,
        "list_github_archives",
        lambda: [{"name": "community", "can_publish": False, "can_reference": True}],
    )

    with pytest.raises(ValueError, match="writable own archive"):
        market_data_api._save_checksum_settings(
            {
                "publish_enabled": True,
                "publish_archive": "community",
                "reference_archive": "community",
            }
        )


def test_integrity_repair_job_payload_contains_only_identifiers(monkeypatch) -> None:
    """Repair API queues validated identifiers without paths or credentials."""
    calls = []

    def enqueue(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(job_id="repair-job", created=True)

    monkeypatch.setattr(market_data_api, "enqueue_unique_job", enqueue)
    result = market_data_api.queue_integrity_repair(
        {"exchange": "bybit", "coin": "BTC_USDT:USDT", "day": "2026-07-13"},
        session=None,
    )

    assert result == {"success": True, "job_id": "repair-job", "created": True}
    assert calls[0]["payload"] == {
        "exchange": "bybit",
        "coin": "BTC_USDT:USDT",
        "day": "2026-07-13",
    }


def test_integrity_repair_all_queues_one_batch_job(monkeypatch) -> None:
    """Repair All uses one durable job rather than one queue file per damaged day."""
    monkeypatch.setattr(
        market_data_api,
        "list_integrity_issues",
        lambda **_kwargs: {
            "total": 3,
            "rows": [
                {"coin": "BTC_USDT:USDT", "market_status": "available"},
                {"coin": "ETH_USDT:USDT", "market_status": "available"},
                {"coin": "OLD_USDT:USDT", "market_status": "removed"},
            ],
        },
    )
    calls = []

    def enqueue(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(job_id="repair-all-job", created=True)

    monkeypatch.setattr(market_data_api, "enqueue_unique_job", enqueue)
    result = market_data_api.queue_integrity_repair_all(session=None)

    assert result == {
        "success": True,
        "job_id": "repair-all-job",
        "created": True,
        "total": 2,
        "coin": "",
    }
    assert calls == [{
        "job_type": "ohlcv_integrity_repair_all",
        "payload": {"exchange": "bybit"},
        "exchange": "bybit",
        "dedupe_key": "ohlcv-integrity-repair-all:bybit:all",
    }]


def test_integrity_repair_all_can_scope_batch_to_one_coin(monkeypatch) -> None:
    """Grouped Repair coin queues one sequential batch for that exact coin."""
    monkeypatch.setattr(
        market_data_api,
        "list_integrity_issues",
        lambda **_kwargs: {
            "rows": [
                {"coin": "KORU_USDT:USDT", "market_status": "available"},
                {"coin": "KORU_USDT:USDT", "market_status": "available"},
                {"coin": "BTC_USDT:USDT", "market_status": "available"},
            ]
        },
    )
    calls = []
    monkeypatch.setattr(
        market_data_api,
        "enqueue_unique_job",
        lambda **kwargs: calls.append(kwargs) or SimpleNamespace(job_id="coin-job", created=True),
    )

    result = market_data_api.queue_integrity_repair_all(
        session=None,
        body={"coin": "KORU_USDT:USDT"},
    )

    assert result["total"] == 2
    assert result["coin"] == "KORU_USDT:USDT"
    assert calls[0]["payload"] == {"exchange": "bybit", "coin": "KORU_USDT:USDT"}
    assert calls[0]["dedupe_key"] == "ohlcv-integrity-repair-all:bybit:KORU_USDT:USDT"


def test_hyperliquid_integrity_repair_all_queues_exchange_scoped_batch(monkeypatch) -> None:
    """Hyperliquid uses the same durable coin-grouped repair queue as Bybit."""
    monkeypatch.setattr(
        market_data_api,
        "list_integrity_issues",
        lambda **kwargs: {
            "rows": [{"coin": "BLAST_USDC:USDC", "market_status": "available"}]
            if kwargs["exchange"] == "hyperliquid"
            else [],
        },
    )
    calls = []
    monkeypatch.setattr(
        market_data_api,
        "enqueue_unique_job",
        lambda **kwargs: calls.append(kwargs) or SimpleNamespace(job_id="hl-repair", created=True),
    )

    result = market_data_api.queue_integrity_repair_all(
        session=None,
        body={"exchange": "hyperliquid", "coin": "BLAST_USDC:USDC"},
    )

    assert result["total"] == 1
    assert calls[0]["payload"] == {"exchange": "hyperliquid", "coin": "BLAST_USDC:USDC"}
    assert calls[0]["exchange"] == "hyperliquid"
    assert calls[0]["dedupe_key"] == "ohlcv-integrity-repair-all:hyperliquid:BLAST_USDC:USDC"


def test_removed_integrity_coin_queues_revalidated_delete_job(monkeypatch) -> None:
    """Removed-coin deletion queues identifiers only after a safe preview."""
    preview = {"exchange": "bybit", "coin": "OLD_USDT:USDT", "files": 12, "bytes": 500}
    monkeypatch.setattr(market_data_api, "unavailable_coin_data_preview", lambda **_kwargs: preview)
    calls = []

    def enqueue(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(job_id="remove-job", created=True)

    monkeypatch.setattr(market_data_api, "enqueue_unique_job", enqueue)
    result = market_data_api.queue_removed_integrity_coin(
        {"exchange": "bybit", "coin": "OLD_USDT:USDT"},
        session=None,
    )

    assert result["job_id"] == "remove-job"
    assert result["preview"] == preview
    assert calls == [{
        "job_type": "ohlcv_removed_coin_delete",
        "payload": {"exchange": "bybit", "coin": "OLD_USDT:USDT"},
        "exchange": "bybit",
        "dedupe_key": "ohlcv-removed-coin-delete:bybit:OLD_USDT:USDT",
    }]


def test_removed_integrity_coin_batch_queues_one_revalidated_job(monkeypatch) -> None:
    """Selected or all unavailable markets become one exact restart-persistent batch job."""
    preview = {
        "exchange": "bybit",
        "coins": ["OLD_A_USDT:USDT", "OLD_B_USDT:USDT"],
        "coin_count": 2,
        "files": 20,
        "bytes": 500,
    }
    preview_calls = []
    monkeypatch.setattr(
        market_data_api,
        "unavailable_coin_data_batch_preview",
        lambda **kwargs: preview_calls.append(kwargs) or preview,
    )
    queued = []
    monkeypatch.setattr(
        market_data_api,
        "enqueue_unique_job",
        lambda **kwargs: queued.append(kwargs) or SimpleNamespace(job_id="batch-remove", created=True),
    )

    shown = market_data_api.preview_removed_integrity_coins(
        {"exchange": "bybit", "coins": ["OLD_B_USDT:USDT", "OLD_A_USDT:USDT"]},
        session=None,
    )
    result = market_data_api.queue_removed_integrity_coins(
        {"exchange": "bybit", "all": True},
        session=None,
    )

    assert shown == preview
    assert preview_calls == [
        {"exchange": "bybit", "coins": ["OLD_B_USDT:USDT", "OLD_A_USDT:USDT"]},
        {"exchange": "bybit", "coins": None},
    ]
    assert result["job_id"] == "batch-remove"
    assert queued == [{
        "job_type": "ohlcv_removed_coins_delete",
        "payload": {"exchange": "bybit", "coins": preview["coins"]},
        "exchange": "bybit",
        "dedupe_key": "ohlcv-removed-coins-delete:bybit",
    }]


def test_checksum_publish_requires_completed_idle_catalog(monkeypatch) -> None:
    """Manual publishing cannot replace a good release from an incomplete catalog."""
    monkeypatch.setattr(
        market_data_api,
        "_checksum_settings_payload",
        lambda: {
            "publish_archive": "mine",
            "catalogs": {"bybit": {"initial_scan_complete": False}},
        },
    )
    with pytest.raises(Exception) as incomplete:
        market_data_api.queue_checksum_publish(session=None)
    assert incomplete.value.status_code == 409

    monkeypatch.setattr(
        market_data_api,
        "_checksum_settings_payload",
        lambda: {
            "publish_archive": "mine",
            "catalogs": {"bybit": {"initial_scan_complete": True}},
        },
    )
    monkeypatch.setattr(
        market_data_api,
        "list_jobs",
        lambda **_kwargs: [{"type": "ohlcv_integrity_repair", "status": "pending"}],
    )
    with pytest.raises(Exception) as updating:
        market_data_api.queue_checksum_publish(session=None)
    assert updating.value.status_code == 409


@pytest.mark.parametrize(
    ("requested", "storage"),
    [
        ("binance", "binanceusdm"),
        ("binanceusdm", "binanceusdm"),
        ("bybit", "bybit"),
        ("okx", "okx"),
        ("bitget", "bitget"),
        ("hyperliquid", "hyperliquid"),
    ],
)
def test_integrity_scan_queues_selected_storage_exchange(monkeypatch, requested: str, storage: str) -> None:
    """Every supported GUI exchange gets an exchange-scoped scan job."""
    calls = []
    monkeypatch.setattr(
        market_data_api,
        "enqueue_unique_job",
        lambda **kwargs: calls.append(kwargs) or SimpleNamespace(job_id="scan-job", created=True),
    )

    result = market_data_api.queue_integrity_scan(session=None, body={"exchange": requested})

    assert result["exchange"] == storage
    assert calls[0]["payload"]["exchange"] == storage
    assert calls[0]["exchange"] == storage
    assert calls[0]["dedupe_key"] == f"ohlcv-integrity-scan:{storage}:v{market_data_api.INITIAL_SCAN_VERSION}"


def test_hyperliquid_fallback_normalization_queues_one_scoped_job(monkeypatch) -> None:
    """The maintenance endpoint queues one deduplicated Hyperliquid job."""
    calls = []
    monkeypatch.setattr(
        market_data_api,
        "enqueue_unique_job",
        lambda **kwargs: calls.append(kwargs) or SimpleNamespace(job_id="normalize-job", created=True),
    )

    result = market_data_api.queue_hyperliquid_fallback_normalization(session=None)

    assert result["job_id"] == "normalize-job"
    assert calls == [{
        "job_type": "ohlcv_hyperliquid_normalize_fallback",
        "payload": {"exchange": "hyperliquid", "dry_run": False},
        "exchange": "hyperliquid",
        "dedupe_key": "ohlcv-hyperliquid-normalize-fallback:v1",
    }]


def test_removed_coin_listing_uses_selected_storage_exchange(monkeypatch) -> None:
    """Unavailable-market listing follows the global exchange selection."""
    calls = []
    monkeypatch.setattr(
        market_data_api,
        "list_removed_coin_data",
        lambda **kwargs: calls.append(kwargs) or {"exchange": kwargs["exchange"], "rows": []},
    )

    result = market_data_api.get_removed_integrity_coins(exchange="hyperliquid", session=None)

    assert result["exchange"] == "hyperliquid"
    assert calls == [{"exchange": "hyperliquid"}]


def test_integrity_day_details_normalizes_exchange_and_forwards_identifiers(monkeypatch) -> None:
    """The authenticated detail endpoint exposes only validated storage identifiers."""
    calls = []
    monkeypatch.setattr(
        market_data_api,
        "daily_gap_details",
        lambda **kwargs: calls.append(kwargs) or {"coverage": "p" * 1440},
    )

    result = market_data_api.get_integrity_day_details(
        exchange="binance",
        coin="BTC_USDT:USDT",
        day="2019-09-08",
        session=None,
    )

    assert len(result["coverage"]) == 1440
    assert calls == [{
        "exchange": "binanceusdm",
        "coin": "BTC_USDT:USDT",
        "day": "2019-09-08",
    }]


def test_integrity_status_scopes_catalog_and_comparison(monkeypatch) -> None:
    """Selected-exchange status cannot leak another exchange's comparison rows."""
    catalogs = {
        exchange: {"exchange": exchange, "initial_scan_complete": True, "counts": {}}
        for exchange in market_data_api.SUPPORTED_EXCHANGES
    }
    monkeypatch.setattr(
        market_data_api,
        "_checksum_settings_payload",
        lambda: {
            "catalogs": catalogs,
            "reference": {"available": True, "matches_selected": True},
        },
    )
    monkeypatch.setattr(market_data_api, "reference_database_path", lambda: Path(__file__))
    compared = []
    monkeypatch.setattr(
        market_data_api,
        "compare_catalogs_readonly",
        lambda **kwargs: compared.append(kwargs) or {"counts": {}, "differences": []},
    )

    result = market_data_api.get_integrity_status(exchange="binance", session=None)

    assert result["exchange"] == "binanceusdm"
    assert result["catalog"]["exchange"] == "binanceusdm"
    assert result["repair_supported"] is True
    assert compared[0]["exchange"] == "binanceusdm"


def test_unknown_mutation_exchanges_are_rejected(monkeypatch) -> None:
    """Repair and removal reject exchange identifiers outside the integrity boundary."""
    monkeypatch.setattr(market_data_api, "enqueue_unique_job", lambda **_kwargs: pytest.fail("job was queued"))

    with pytest.raises(Exception) as repair:
        market_data_api.queue_integrity_repair_all(session=None, body={"exchange": "unknown"})
    with pytest.raises(Exception) as removal:
        market_data_api.queue_removed_integrity_coin(
            {"exchange": "unknown", "coin": "BTC_USDT:USDT"},
            session=None,
        )

    assert repair.value.status_code == 422
    assert removal.value.status_code == 422


def test_checksum_settings_identify_stale_reference_archive(monkeypatch) -> None:
    """Changing reference selection suppresses comparison until that archive is refreshed."""
    values = {
        ("market_data", "checksum_publish_archive"): "",
        ("market_data", "checksum_reference_archive"): "community",
        ("market_data", "checksum_publish_enabled"): "false",
    }
    monkeypatch.setattr(market_data_api, "load_ini", lambda section, key: values.get((section, key), ""))
    monkeypatch.setattr(
        market_data_api,
        "list_github_archives",
        lambda: [{"name": "community", "repository": "owner/new", "can_reference": True}],
    )
    monkeypatch.setattr(market_data_api, "catalog_summary", lambda **_kwargs: {"initial_scan_complete": True})
    monkeypatch.setattr(
        market_data_api,
        "reference_status",
        lambda: {"available": True, "source": "https://github.com/owner/old"},
    )

    payload = market_data_api._checksum_settings_payload()

    assert payload["reference"]["selected_repository"] == "owner/new"
    assert payload["reference"]["matches_selected"] is False
