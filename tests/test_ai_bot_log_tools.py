"""Offline coverage for scoped historical bot log evidence."""
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

import ai_bot_log_tools as logs
from ai_capabilities import AICapabilityError, AICapabilityService


@pytest.fixture
def inventory(tmp_path, monkeypatch):
    """Use only isolated native log paths and an in-memory monitor."""
    from api import vps, v8_instances
    import pbgui_purefunc
    runtime = tmp_path / "pb8"
    (runtime / "logs").mkdir(parents=True)
    monkeypatch.setattr(pbgui_purefunc, "pb8dir", lambda: str(runtime))
    monkeypatch.setattr(pbgui_purefunc, "PBGDIR", tmp_path)
    monkeypatch.setattr(v8_instances, "_last_active_v8_host", lambda name: {})
    monkeypatch.setattr(vps, "_monitor", SimpleNamespace(store=SimpleNamespace(bot_logs={})))
    return runtime / "logs"


def test_historical_search_returns_real_older_evidence(inventory):
    """A whole-file search finds fills older than the returned tail."""
    path = inventory / "bot.log"
    path.write_text('2026-09-01T01:00:00Z [fill] SOL token=secret123\n' + '2026-10-03T12:00:00Z tick\n' * 600)
    listed = asyncio.run(logs.query({"name": "bot"}))
    file_id = listed["files"][0]["file_id"]
    result = asyncio.run(logs.query({"name": "bot", "file_id": file_id, "contains": "[fill]"}, read=True))
    assert result["scan_complete_verified"]
    assert result["returned_lines"] == 1
    assert "secret123" not in str(result)
    assert result["returned_first_timestamp"] == "2026-09-01T01:00:00Z"
    tail = asyncio.run(logs.query({"name": "bot", "file_id": file_id, "lines": 2}, read=True))
    assert tail["matches_truncated"] and tail["all_history_reviewed"] is False
    assert all("[fill]" not in row for row in tail["untrusted_log_lines"])


@pytest.mark.parametrize("path,expected", [
    ("pb8/logs/bot.log", True),
    ("pb8/logs/2026-01-01_run_v8_bot_config.json.log", True),
    ("data/run_v8/bot/passivbot_err.log.old", True),
    ("pb8/logs/other.log", False),
    ("data/run_v8/other/passivbot_err.log", False),
    ("data/logs/PBGui.log", False),
    ("pb7/logs/bot.log", False),
    ("pb8/logs/../../pbgui.ini", False),
])
def test_monitor_file_scope(path, expected):
    """Even discovered file identifiers must stay within the selected bot."""
    assert logs._belongs(path, "bot", "8") is expected


def test_inventory_lists_multiple_hosts_and_excludes_other_bots(inventory, monkeypatch):
    """Historical placement on another VPS is included without path exposure."""
    from api import vps
    vps._monitor.store.bot_logs = {
        "host1": {"8:bot": ["pb8/logs/bot.log", "pb8/logs/other.log"]},
        "host2": {"8:bot": ["pb8/logs/old_run_v8_bot_config.json.log"]},
    }
    (inventory / "other.log").write_text("unrelated")
    (inventory / "bot.log").symlink_to(inventory / "other.log")
    result = asyncio.run(logs.query({"name": "bot"}))
    assert result["total_files"] == 2
    assert {row["host"] for row in result["files"]} == {"host1", "host2"}
    assert all("path" not in row for row in result["files"])


def test_remote_search_uses_one_scoped_file_and_reports_limits(inventory, monkeypatch):
    """A remote full-file search must not claim independently verified coverage."""
    from api import vps
    vps._monitor.store.bot_logs = {"host": {"8:bot": ["pb8/logs/bot.log"]}}
    class Streamer:
        """Fake daemon transport; no real SSH or runtime files."""
        async def get_log_info(self, host, path):
            """Prove a readable file exists."""
            assert (host, path) == ("host", "pb8/logs/bot.log")
            return {"size": 100}
        async def get_recent_log_files(self, host, paths, lines, *, contains):
            """Prove the literal search is sent to the server."""
            assert paths == ["pb8/logs/bot.log"] and contains == "ERROR"
            assert lines == 3
            return 'ERROR token=privatevalue\nERROR two\nERROR three'
    monkeypatch.setattr(vps, "_streamer", Streamer())
    entry = asyncio.run(logs.query({"name": "bot"}))["files"][0]
    result = asyncio.run(logs.query({"name": "bot", "file_id": entry["file_id"], "contains": "ERROR", "lines": 2}, read=True))
    assert result["matches_truncated"] and not result["scan_complete_verified"]
    assert len(result["untrusted_log_lines"]) == 2


def test_invalid_handles_and_read_limits_fail(inventory):
    """Arbitrary paths, stale IDs, invalid versions and control needles fail."""
    (inventory / "bot.log").write_text("safe")
    entry = asyncio.run(logs.query({"name": "bot"}))["files"][0]
    for args in ({"name": "../bot"}, {"name": "bot", "version": "v6"},
                 {"name": "bot", "file_id": "/etc/passwd"},
                 {"name": "bot", "file_id": entry["file_id"], "lines": 0},
                 {"name": "bot", "file_id": entry["file_id"], "contains": "x\ny"}):
        with pytest.raises(AICapabilityError):
            asyncio.run(logs.query(args, read=True))


def test_new_tools_dispatch_for_all_providers_in_analysis_mode(tmp_path, inventory):
    """Provider catalogs and read-only conversations share the same tools."""
    async def scenario():
        """Exercise the actual asynchronous capability dispatcher."""
        service = AICapabilityService(tmp_path / "capabilities")
        service.analysis_only = lambda owner, cid: True
        names = {item["function"]["name"] for item in service.chat_completion_tools()}
        assert {"list_bot_logs", "read_bot_log"} <= names
        assert names == {item["name"] for item in service.responses_tools()}
        assert names == {item["name"] for item in service.messages_tools()}
        result = await service.dispatch("owner", "chat", "list_bot_logs", {"name": "bot"})
        assert result["total_files"] == 0
        await service.shutdown()
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["transport", "missing", "read"])
def test_remote_failures_are_not_empty_evidence(inventory, monkeypatch, failure):
    """Disconnected and unreadable files produce errors rather than no-event claims."""
    from api import vps
    vps._monitor.store.bot_logs = {"host": {"8:bot": ["pb8/logs/bot.log"]}}
    class Streamer:
        """Simulate an unavailable daemon or read failure."""
        async def get_log_info(self, host, path):
            """Return no metadata for inaccessible files."""
            return None if failure == "missing" else {"size": 100}
        async def get_recent_log_files(self, *args, **kwargs):
            """Return explicit transport failure."""
            return None
    monkeypatch.setattr(vps, "_streamer", None if failure == "transport" else Streamer())
    entry = asyncio.run(logs.query({"name": "bot"}))["files"][0]
    with pytest.raises(AICapabilityError):
        asyncio.run(logs.query({"name": "bot", "file_id": entry["file_id"]}, read=True))


def test_inventory_pagination_covers_historical_files(inventory):
    """The model can enumerate more than one bounded page of archives."""
    for index in range(105):
        (inventory / f"{index}_run_v8_bot_config.json.log").write_text("entry")
    first = asyncio.run(logs.query({"name": "bot"}))
    second = asyncio.run(logs.query({"name": "bot", "offset": first["next_offset"]}))
    assert len(first["files"]) == 100 and len(second["files"]) == 5
    assert second["next_offset"] is None
    assert len({r["file_id"] for r in first["files"] + second["files"]}) == 105


def test_oversized_local_lines_mark_scan_incomplete(inventory):
    """Bound memory for malformed logs rather than claiming a complete search."""
    path = inventory / "bot.log"
    path.write_text("x" * 70000 + "\n")
    entry = asyncio.run(logs.query({"name": "bot"}))["files"][0]
    result = asyncio.run(logs.query({"name": "bot", "file_id": entry["file_id"], "contains": "ERROR"}, read=True))
    assert not result["scan_complete_verified"]
    assert not result["all_history_reviewed"]
