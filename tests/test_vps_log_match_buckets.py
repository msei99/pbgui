"""Selected-day bot log matching and WebSocket regressions without remote I/O."""

import asyncio
from datetime import datetime
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import api.vps as vps_api
import api.vps_manager as manager_api


@pytest.mark.parametrize("bucket, target", [("today", "2026-01-01"), ("yesterday", "2025-12-31")])
@pytest.mark.parametrize("kind", ["errors", "tracebacks"])
@pytest.mark.parametrize("version", ["7", "8"])
def test_selected_utc_day(monkeypatch, bucket, target, kind, version):
    """Both runtimes filter mixed-day entries correctly across a year boundary."""
    clock = SimpleNamespace(now=lambda tz: datetime(2026, 1, 1, 0, 1, tzinfo=tz))
    monkeypatch.setattr(vps_api, "datetime", clock)
    dates = ["2025-12-30", "2025-12-31", "2026-01-01"]
    if kind == "errors":
        output = "".join(f"{day}T23:59:00Z ERROR {day}\n{day}T23:59:01Z INFO ignored\n" for day in dates)
        expected = [f"{target}T23:59:00Z ERROR {target}"]
    else:
        output = "".join(f"{day}T23:59:00Z stderr\nTraceback (most recent call last):\nValueError: {day}\n" for day in dates)
        expected = [f"{target}T23:59:00Z stderr", "Traceback (most recent call last):", f"ValueError: {target}"]
    reader = AsyncMock(return_value=output)
    monkeypatch.setattr(vps_api, "_streamer", SimpleNamespace(get_recent_log_files=reader))
    monkeypatch.setattr(vps_api, "_monitor", None)
    result = asyncio.run(vps_api.get_bot_log_matches(
        "host", "bot", pb_version=version, kind=kind, bucket=bucket,
    ))
    assert result == expected
    reader.assert_awaited_once()
    assert all(f"v{version}/" in path or f"pb{version}/" in path for path in reader.call_args.args[1])


@pytest.mark.parametrize("bucket", ["", "last_week", "Today"])
def test_invalid_bucket_does_not_read_logs(monkeypatch, bucket):
    """Unknown day selectors are rejected before any remote reads."""
    reader = AsyncMock()
    monkeypatch.setattr(vps_api, "_streamer", SimpleNamespace(get_recent_log_files=reader))
    assert asyncio.run(vps_api.get_bot_log_matches("host", "bot", bucket=bucket)) == []
    reader.assert_not_called()


@pytest.mark.parametrize("bucket", ["today", "yesterday", "invalid"])
def test_websocket_bucket_dispatch(monkeypatch, bucket):
    """The real WebSocket handler accepts both days and correlates successful replies."""
    message = {"cmd": "fetch_bot_log_matches", "request_id": "request-7", "hostname": "host",
               "bot_name": "bot", "pb_version": "8", "kind": "errors", "bucket": bucket}

    class Socket:
        """Supply one message without opening sockets or contacting a VPS."""

        async def iter_text(self):
            """End the handler after one command."""
            yield json.dumps(message)

        send_json = AsyncMock()

    reader = AsyncMock(return_value=["matching entry"])
    monkeypatch.setattr(manager_api, "authenticate_websocket", AsyncMock(return_value=SimpleNamespace(token="test")))
    monkeypatch.setattr(manager_api, "_get_service", lambda: object())
    monkeypatch.setattr(manager_api, "_push_loop", AsyncMock())
    monkeypatch.setattr(manager_api, "get_bot_log_matches", reader)
    socket = Socket()
    asyncio.run(manager_api.ws_vps_manager(socket))
    reply = socket.send_json.call_args.args[0]
    if bucket == "invalid":
        reader.assert_not_called()
        assert reply["type"] == "error"
        assert reply["error"] == "bucket must be today or yesterday"
    else:
        reader.assert_awaited_once()
        assert reader.call_args.kwargs["bucket"] == bucket
        assert reply["type"] == "bot_log_matches"
        assert reply["request_id"] == "request-7"
        assert reply["bucket"] == bucket
        assert reply["lines"] == ["matching entry"]
