"""Tests for async log path resolution helpers."""

import asyncio
import os
import shlex
import signal
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import api.vps as vps_api
import PBRun as pbrun_module
from master import async_logs
from master.async_monitor import VPSMonitor
from master.async_store import VPSStore
from master.vps_monitor_client import RemoteLogStreamerProxy
from master.vps_monitor_daemon import VPSMonitorRPCDaemon
from Exchange import Exchange
from file_lock import advisory_file_lock


class FakePool:
    """Minimal pool stub for PB7 log path resolution tests."""

    def __init__(self, pb7dir: str = "", pb8dir: str = ""):
        """Store fake host runtime directory values."""
        self._pb7dir = pb7dir
        self._pb8dir = pb8dir

    def get_connection(self, hostname: str):
        """Return a fake connection entry with cached host data."""
        return SimpleNamespace(data={"pb7dir": self._pb7dir, "pb8dir": self._pb8dir})


class FakeMonitorPool:
    """Capture fixed remote commands and return configured process output."""

    def __init__(self, process_output: str = "") -> None:
        """Store the fake process listing."""
        self.process_output = process_output
        self.commands: list[str] = []

    async def run(self, hostname: str, command: str, timeout: int = 15):
        """Return process output for ps and success for a numeric kill command."""
        del hostname, timeout
        self.commands.append(command)
        if command == "ps -eo pid=,args=":
            return SimpleNamespace(exit_status=0, stdout=self.process_output)
        return SimpleNamespace(exit_status=0, stdout="")

    def get_remote_pbgui_dir(self, hostname: str) -> str:
        """Return the canonical test PBGui checkout path."""

        del hostname
        return "/home/pbgui/software/pbgui"


class FakeTask:
    """Minimal cancellable task used to verify stream registry cleanup."""

    def __init__(self) -> None:
        self.cancelled = False

    def done(self) -> bool:
        """Report the task as running until cancellation."""
        return self.cancelled

    def cancel(self) -> None:
        """Record cancellation without requiring an event loop."""
        self.cancelled = True


class FakeWsClient:
    """Track asynchronous CCXT-Pro close calls."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        """Record that the shared client was released."""
        self.closed = True


def test_pb7_logs_path_is_home_relative():
    """Treat sidebar PB7 log paths as host-home/PB7 paths, not PBGui paths."""
    assert async_logs._is_home_relative_log_path("pb7/logs/20260610_215403__bot_config_run.json.log") is True


def test_pbcluster_service_resolves_to_remote_pbgui_log_path():
    """Resolve PBCluster sidebar selection to its PBGui data log file."""

    assert async_logs._resolve_log_path("PBCluster") == "data/logs/PBCluster.log"


def test_remote_pb7_logs_path_uses_cached_pb7dir():
    """Resolve remote sidebar PB7 log paths through cached pb7dir."""
    path = async_logs._remote_log_shell_path(
        FakePool("/srv/passivbot7"),
        "manibot52",
        "pb7/logs/20260610_215403__bot_config_run.json.log",
    )

    assert path == "/srv/passivbot7/logs/20260610_215403__bot_config_run.json.log"


def test_remote_pb7_logs_path_defaults_to_home_software_pb7():
    """Resolve remote sidebar PB7 log paths to ~/software/pb7 when pb7dir is unknown."""
    path = async_logs._remote_log_shell_path(
        FakePool(),
        "manibot52",
        "pb7/logs/20260610_215403__bot_config_run.json.log",
    )

    assert path == '"$HOME"/software/pb7/logs/20260610_215403__bot_config_run.json.log'


def test_remote_pb8_logs_path_uses_cached_pb8dir():
    """Resolve PB8 logical logs only through the cached remote runtime directory."""

    path = async_logs._remote_log_shell_path(
        FakePool(pb8dir="/srv/passivbot8"),
        "manibot52",
        "pb8/logs/live-bot.log",
    )

    assert path == "/srv/passivbot8/logs/live-bot.log"


def test_regular_remote_log_follow_command_uses_direct_tail():
    """Keep ordinary daemon log streams on the lightweight direct tail path."""
    command = async_logs._remote_log_follow_command(
        "data/logs/PBRun.log",
        '"$HOME"/software/pbgui/data/logs/PBRun.log',
    )

    assert command == 'tail -F -n 0 "$HOME"/software/pbgui/data/logs/PBRun.log 2>/dev/null'


def test_native_bot_log_stream_follows_replaced_symlink(tmp_path):
    """Show new startup lines promptly when PB7 replaces its current-log alias."""

    async def exercise_stream() -> None:
        old_log = tmp_path / "old.log"
        new_log = tmp_path / "new.log"
        alias = tmp_path / "bot.log"
        replacement = tmp_path / "bot.log.next"
        old_log.write_text("old history\n", encoding="utf-8")
        alias.symlink_to(old_log)
        command = async_logs._remote_log_follow_command(
            "software/pb7/logs/bot.log",
            shlex.quote(str(alias)),
        )
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        assert process.stdout is not None
        try:
            await asyncio.sleep(1.2)
            with old_log.open("a", encoding="utf-8") as handle:
                handle.write("old live\n")
                handle.flush()
            assert (await asyncio.wait_for(process.stdout.readline(), 3)).decode().strip() == "old live"

            new_log.write_text("new startup\n", encoding="utf-8")
            replacement.symlink_to(new_log)
            replacement.replace(alias)
            assert (await asyncio.wait_for(process.stdout.readline(), 3)).decode().strip() == "new startup"
        finally:
            if process.returncode is None:
                os.killpg(process.pid, signal.SIGTERM)
            await asyncio.wait_for(process.wait(), 3)

    asyncio.run(exercise_stream())


@pytest.mark.parametrize(
    "path",
    [
        "../../.ssh/id_ed25519",
        "data/logs/../../../etc/passwd",
        "/etc/passwd",
        "~/secret.log",
        "data/logs/config.json",
        "data/run_v7/bot/config.json",
    ],
)
def test_remote_log_path_rejects_traversal_and_non_log_files(path):
    """Keep remote reads inside supported log roots and file types."""
    with pytest.raises(ValueError):
        async_logs._resolve_log_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "data/logs/PBRun.log",
        "data/logs/PBRun.log.1",
        "data/run_v7/bybit_SOLUSDT/passivbot_err.log",
        "data/run_v7/bybit_SOLUSDT/passivbot_err.log.old",
        "data/run_v8/bybit_SOLUSDT/passivbot_err.log",
        "data/run_v8/bybit_SOLUSDT/passivbot_err.log.old",
        "pb7/logs/20260610_215403__bot_config_run.json.log",
        "pb8/logs/bybit_SOLUSDT.log",
        "software/pb7/logs/bybit_SOLUSDT.log",
        "software/pb8/logs/bybit_SOLUSDT.log",
    ],
)
def test_remote_log_path_accepts_supported_log_locations(path):
    """Preserve all remote log locations used by the monitor UI."""
    assert async_logs._resolve_log_path(path) == path


def test_remote_bot_log_rejects_traversal_name():
    """Prevent bot names from escaping the configured PB7 log directory."""
    with pytest.raises(ValueError):
        async_logs.resolve_bot_log_path("../../.ssh/id_ed25519", "7")


def test_remote_bot_log_resolves_pb8_logical_identifier():
    """PB8 bot logs use an allowlisted logical path, never a request path."""

    assert async_logs.resolve_bot_log_path("same", "8") == "software/pb8/logs/same.log"


@pytest.mark.parametrize("value", ["1; touch /tmp/pwn", "$(id)", -1, 50_001, True])
def test_remote_log_line_count_rejects_unsafe_values(value):
    """Reject values which could alter a remote shell command or exhaust output."""
    with pytest.raises(ValueError):
        async_logs.normalize_remote_log_lines(value)


@pytest.mark.parametrize(("value", "expected"), [(0, 0), (200, 200), ("5000", 5000)])
def test_remote_log_line_count_accepts_supported_values(value, expected):
    """Accept integer line counts used by the shared log viewer."""
    assert async_logs.normalize_remote_log_lines(value) == expected


@pytest.mark.parametrize(
    ("next_stream_id", "next_sid"),
    [("stream-2", "sid-2"), ("stream-1", "sid-2")],
)
def test_remote_log_push_discards_data_when_subscription_changes_during_read(
    monkeypatch: pytest.MonkeyPatch, next_stream_id: str, next_sid: str,
) -> None:
    """An awaited old-stream read must never be emitted under a newer subscription SID."""

    state = {"stream_id": "stream-1", "sid": "sid-1"}

    class RaceStreamer:
        """Change the active subscription while the first async read is suspended."""

        def __init__(self) -> None:
            self.read_count = 0

        async def read_stream_async(self, stream_id: str, max_lines: int) -> list[str]:
            """Return stale data once, then data belonging to the current pair."""
            assert max_lines == 50
            self.read_count += 1
            if self.read_count == 1:
                assert stream_id == "stream-1"
                state.update(stream_id=next_stream_id, sid=next_sid)
                await asyncio.sleep(0)
                return ["stale"]
            assert stream_id == next_stream_id
            return ["fresh"]

        async def get_stream_status_async(self, stream_id: str) -> dict[str, str]:
            """Describe the stream whose lines are about to be emitted."""
            return {"hostname": "host", "log_path": stream_id}

    class CaptureWebSocket:
        """Stop the infinite push loop after the first emitted payload."""

        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send_json(self, message: dict) -> None:
            """Capture one message and cancel the loop."""
            self.messages.append(message)
            raise asyncio.CancelledError

    streamer = RaceStreamer()
    websocket = CaptureWebSocket()
    monkeypatch.setattr(vps_api, "_streamer", streamer)
    monkeypatch.setattr(vps_api, "LOG_PUSH_INTERVAL", 0)

    asyncio.run(vps_api._push_log_loop(
        websocket,
        lambda: (state["stream_id"], state["sid"]),
    ))

    assert streamer.read_count == 2
    assert websocket.messages == [{
        "type": "log_lines",
        "lines": ["fresh"],
        "host": "host",
        "service": next_stream_id,
        "sid": next_sid,
    }]


@pytest.mark.parametrize("value", [-1, 50_001, True, "not-a-number"])
def test_local_log_requests_reject_unbounded_or_invalid_line_counts(
    monkeypatch: pytest.MonkeyPatch, value: object,
) -> None:
    """Local one-shot reads must reject the same unsafe counts as remote reads."""

    class CaptureStreamer:
        """Record local reads without touching runtime log files."""

        def __init__(self) -> None:
            self.calls: list[tuple[str, int]] = []

        def get_local_logs(self, filename: str, lines: int) -> tuple[list[str], int]:
            """Capture validated arguments."""
            self.calls.append((filename, lines))
            return [], 0

    streamer = CaptureStreamer()
    monkeypatch.setattr(vps_api, "_streamer", streamer)

    response = vps_api._cmd_get_local_logs({"file": "PBRun.log", "lines": value})

    assert response["type"] == "error"
    assert streamer.calls == []


def test_local_log_subscription_rejects_oversized_line_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local subscriptions must reject oversized snapshots before resolving a file."""

    class CaptureWebSocket:
        """Capture validation errors returned by the subscription command."""

        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send_json(self, message: dict) -> None:
            """Record the WebSocket response."""
            self.messages.append(message)

    websocket = CaptureWebSocket()
    resolved: list[str] = []
    monkeypatch.setattr(
        vps_api,
        "resolve_local_log_path",
        lambda filename: resolved.append(filename),
    )

    subscription = asyncio.run(vps_api._cmd_subscribe_local_logs(
        websocket,
        {"file": "PBRun.log", "lines": 50_001},
    ))

    assert subscription is None
    assert websocket.messages == [{
        "type": "error",
        "error": "lines must be between 0 and 50000",
    }]
    assert resolved == []


def test_legacy_all_log_request_is_bounded_to_remote_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy local and remote zero values now request only 50,000 lines."""

    class CaptureStreamer:
        """Record the bounded local log request."""

        def __init__(self) -> None:
            self.local_lines: int | None = None
            self.remote_lines: int | None = None

        def get_local_logs(self, filename: str, lines: int) -> tuple[list[str], int]:
            """Capture the normalized line limit."""
            del filename
            self.local_lines = lines
            return ["newest"], 1

        async def get_recent_logs(self, hostname: str, service: str, lines: int) -> str:
            """Capture the normalized remote line limit."""
            del hostname, service
            self.remote_lines = lines
            return "newest"

    streamer = CaptureStreamer()
    monkeypatch.setattr(vps_api, "_streamer", streamer)

    response = vps_api._cmd_get_local_logs({"file": "PBRun.log", "lines": 0})
    remote_response = asyncio.run(vps_api._cmd_get_logs({
        "host": "host",
        "service": "PBRun",
        "lines": 0,
    }))

    assert response["lines"] == ["newest"]
    assert remote_response["lines"] == ["newest"]
    assert streamer.local_lines == streamer.remote_lines == async_logs.MAX_REMOTE_LOG_LINES == 50_000


def test_remote_log_info_echoes_subscription_sid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Metadata responses carry the selection SID supplied by the viewer."""

    class InfoStreamer:
        """Return one fixed remote file size."""

        async def get_log_info(self, _host: str, _service: str, _version: str | None) -> dict:
            return {"size": 42}

    monkeypatch.setattr(vps_api, "_streamer", InfoStreamer())

    response = asyncio.run(vps_api._cmd_get_log_info({
        "host": "host", "service": "PBRun", "sid": "selection-7",
    }))

    assert response == {
        "type": "log_info",
        "host": "host",
        "service": "PBRun",
        "size": 42,
        "sid": "selection-7",
    }


def test_remote_subscription_is_inactive_while_previous_stop_is_suspended(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement clears the old stream/SID pair before awaiting its stop."""

    class SuspendedStopStreamer:
        """Suspend the first stop while recording any push-loop reads."""

        def __init__(self) -> None:
            self.stream_count = 0
            self.stop_started = asyncio.Event()
            self.release_stop = asyncio.Event()
            self.push_reads: list[str] = []
            self.reads_at_stop = 0
            self.detached: list[str] = []

        async def start_stream(self, _host: str, _service: str) -> str:
            self.stream_count += 1
            return f"stream-{self.stream_count}"

        async def get_recent_logs(self, _host: str, _service: str, _lines: int) -> str:
            return "snapshot\n"

        async def read_stream_async(self, stream_id: str, max_lines: int) -> list[str]:
            if max_lines == 50:
                self.push_reads.append(stream_id)
            return []

        async def stop_stream_async(self, stream_id: str) -> None:
            assert stream_id == "stream-1"
            self.reads_at_stop = len(self.push_reads)
            self.stop_started.set()
            await self.release_stop.wait()

        def detach_stream(self, stream_id: str) -> None:
            self.detached.append(stream_id)

    class TwoSubscriptionWebSocket:
        """Issue a replacement after the first subscription snapshot arrives."""

        client = "test-client"

        def __init__(self) -> None:
            self.first_sent = asyncio.Event()
            self.messages: list[dict] = []

        async def iter_text(self):
            yield '{"cmd":"subscribe_logs","host":"host","service":"PBRun","sid":"one"}'
            await self.first_sent.wait()
            yield '{"cmd":"subscribe_logs","host":"host","service":"PBRun","sid":"two"}'

        async def send_json(self, message: dict) -> None:
            self.messages.append(message)
            if message.get("type") == "logs" and message.get("sid") == "one":
                self.first_sent.set()

    async def exercise() -> tuple[SuspendedStopStreamer, TwoSubscriptionWebSocket]:
        streamer = SuspendedStopStreamer()
        websocket = TwoSubscriptionWebSocket()
        monkeypatch.setattr(vps_api, "_streamer", streamer)
        monkeypatch.setattr(vps_api, "LOG_PUSH_INTERVAL", 0)
        task = asyncio.create_task(vps_api.ws_vps(websocket))
        await asyncio.wait_for(streamer.stop_started.wait(), 1)
        await asyncio.sleep(0.01)
        assert streamer.push_reads[streamer.reads_at_stop:] == []
        streamer.release_stop.set()
        await asyncio.wait_for(task, 1)
        return streamer, websocket

    async def authenticate(_websocket):
        return object()

    async def send_no_state(_websocket):
        return None

    monkeypatch.setattr(vps_api, "authenticate_websocket", authenticate)
    monkeypatch.setattr(vps_api, "_send_full_state", send_no_state)
    monkeypatch.setattr(vps_api, "_log", lambda *_args, **_kwargs: None)
    streamer, websocket = asyncio.run(exercise())

    assert [message.get("sid") for message in websocket.messages if message.get("type") == "logs"] == ["one", "two"]
    assert streamer.detached == ["stream-2"]


def test_failed_initial_remote_snapshot_send_releases_registered_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Disconnect cleanup sees a stream registered before its initial send."""

    class InitialSendStreamer:
        """Create one stream and record disconnect detachment."""

        def __init__(self) -> None:
            self.detached: list[str] = []

        async def start_stream(self, _host: str, _service: str) -> str:
            return "stream-created"

        async def get_recent_logs(self, _host: str, _service: str, _lines: int) -> str:
            return "snapshot\n"

        def detach_stream(self, stream_id: str) -> None:
            self.detached.append(stream_id)

    class FailedSendWebSocket:
        """Disconnect while sending the first remote snapshot."""

        client = "test-client"

        async def iter_text(self):
            yield '{"cmd":"subscribe_logs","host":"host","service":"PBRun","sid":"current"}'

        async def send_json(self, _message: dict) -> None:
            raise vps_api.WebSocketDisconnect

    async def authenticate(_websocket):
        return object()

    async def send_no_state(_websocket):
        return None

    streamer = InitialSendStreamer()
    monkeypatch.setattr(vps_api, "_streamer", streamer)
    monkeypatch.setattr(vps_api, "authenticate_websocket", authenticate)
    monkeypatch.setattr(vps_api, "_send_full_state", send_no_state)
    monkeypatch.setattr(vps_api, "_log", lambda *_args, **_kwargs: None)

    asyncio.run(vps_api.ws_vps(FailedSendWebSocket()))

    assert streamer.detached == ["stream-created"]


def test_local_log_delta_buffers_split_writes(tmp_path) -> None:
    """Emit records only after a newline joins writes split across polling chunks."""
    path = tmp_path / "split.log"
    path.write_bytes(b"")
    _content, _size, sub = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        path, "split.log",
    )

    with path.open("ab") as handle:
        handle.write(b"hel")
    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub, max_bytes=2) == []
    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub, max_bytes=2) == []

    with path.open("ab") as handle:
        handle.write(b"lo\nnext")
    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub, max_bytes=64) == ["hello"]
    assert sub.partial == b"next"

    with path.open("ab") as handle:
        handle.write(b"\n")
    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["next"]


def test_local_log_delta_preserves_blank_lines(tmp_path) -> None:
    """Keep intentional empty records between newline delimiters."""
    path = tmp_path / "blank.log"
    path.write_bytes(b"")
    _content, _size, sub = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        path, "blank.log",
    )
    with path.open("ab") as handle:
        handle.write(b"first\n\nthird\n")

    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["first", "", "third"]


def test_local_snapshot_cap_includes_unterminated_final_line(tmp_path) -> None:
    """Appending a partial final record must not make an N-line snapshot N+1."""
    path = tmp_path / "partial.log"
    path.write_bytes(b"first\nsecond\npartial")

    assert async_logs.tail_file(path, 2) == ["second", "partial"]


def test_remote_stream_preserves_blank_lines_and_normalizes_crlf() -> None:
    """Remote live output retains empty records without leaking CR terminators."""

    async def exercise() -> list[str]:
        streamer = async_logs.AsyncLogStreamer(FakePool())
        stream = async_logs.LogStream("stream", "host", "data/logs/PBRun.log")

        class Output:
            """Yield fixed remote records and then stop the worker cleanly."""

            def __init__(self) -> None:
                self.lines = iter(["first\r\n", "\r\n", "third\n"])

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(self.lines)
                except StopIteration:
                    stream.active = False
                    raise StopAsyncIteration

        process = SimpleNamespace(stdout=Output(), close=lambda: None)

        async def start_process(_hostname: str, _command: str):
            return process

        streamer._pool.start_process = start_process
        await streamer._stream_worker(stream, "/tmp/test.log")
        return list(stream.buffer)

    assert asyncio.run(exercise()) == ["first", "", "third"]


def test_local_log_delta_resets_on_file_replacement(tmp_path) -> None:
    """Read a replacement from byte zero after its descriptor identity changes."""
    path = tmp_path / "replace.log"
    path.write_bytes(b"old\n")
    _content, _size, sub = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        path, "replace.log",
    )
    old_identity = sub.identity
    replacement = tmp_path / "replacement.log"
    replacement.write_bytes(b"new\n")
    replacement.replace(path)

    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["new"]
    assert sub.identity != old_identity


def test_local_log_delta_resets_on_in_place_truncation(tmp_path) -> None:
    """Reset cursor and partial state when the same inode shrinks below the cursor."""
    path = tmp_path / "truncate.log"
    path.write_bytes(b"before\npending")
    _content, _size, sub = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        path, "truncate.log",
    )
    identity = sub.identity
    path.write_bytes(b"new\n")

    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["new"]
    assert sub.identity == identity
    assert sub.partial == b""


def test_local_log_subscription_snapshot_handoff_has_no_gap(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An append during initial snapshot delivery remains after the atomic cursor."""
    path = tmp_path / "handoff.log"
    path.write_bytes(b"before\n")
    monkeypatch.setattr(vps_api, "resolve_local_log_path", lambda _filename: path)

    class AppendDuringSendWebSocket:
        """Append once while the initial WebSocket snapshot is being delivered."""

        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send_json(self, message: dict) -> None:
            """Capture the snapshot and append before subscription setup returns."""
            self.messages.append(message)
            with path.open("ab") as handle:
                handle.write(b"during-send\n")
            await asyncio.sleep(0)

    websocket = AppendDuringSendWebSocket()
    sub = asyncio.run(vps_api._cmd_subscribe_local_logs(
        websocket, {"file": "handoff.log", "lines": 200, "sid": "current"},
    ))

    assert sub is not None
    assert sub.identity is not None
    assert websocket.messages[0]["lines"] == ["before"]
    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["during-send"]


@pytest.mark.parametrize(
    "value",
    [
        {"mode": "literal", "query": "x" * (vps_api.MAX_LOCAL_FILTER_LENGTH + 1)},
        {"mode": "terms", "terms": ["ok"] * (vps_api.MAX_LOCAL_FILTER_TERMS + 1)},
        {"mode": "terms", "terms": ["bad\nterm"]},
        {"mode": "regex", "query": ".*"},
        {"mode": "literal", "query": "ok", "context": 21},
    ],
)
def test_local_log_filter_rejects_unbounded_or_executable_patterns(value: dict) -> None:
    """Client filters accept only bounded literals and literal OR-term presets."""
    with pytest.raises(ValueError):
        vps_api._parse_local_log_filter(value)


def test_local_log_filter_preserves_context_and_sliding_match_count() -> None:
    """Filtered deltas emit only match context and expire counts with the source window."""
    config = vps_api._parse_local_log_filter({
        "mode": "literal", "query": "HIT", "context": 1,
    })
    assert config is not None
    state, records = vps_api._initialize_local_filter(
        ["old hit", "old context", "quiet", "leading", "new hit"], 5, config,
    )
    assert [record["line_no"] for record in records] == [1, 2, 4, 5]
    assert state.match_count == 2

    delta, source_start = vps_api._filter_local_log_delta(
        state, ["after", "hidden", "leading two", "\x1b[31mHIT two\x1b[0m", "after two"],
    )

    assert [record["line_no"] for record in delta] == [6, 8, 9, 10]
    assert [record["match"] for record in delta] == [False, False, True, False]
    assert source_start == 6
    assert state.match_count == 1


def test_local_log_subscription_returns_compact_filtered_snapshot(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A filtered subscription returns exact source positions without raw nonmatches."""
    path = tmp_path / "filtered.log"
    path.write_text("before\nneedle\nafter\nhidden\n", encoding="utf-8")
    monkeypatch.setattr(vps_api, "resolve_local_log_path", lambda _filename: path)

    class CaptureWebSocket:
        """Capture one filtered snapshot response."""

        def __init__(self) -> None:
            self.messages: list[dict] = []

        async def send_json(self, message: dict) -> None:
            """Store the response without external I/O."""
            self.messages.append(message)

    websocket = CaptureWebSocket()
    sub = asyncio.run(vps_api._cmd_subscribe_local_logs(websocket, {
        "file": "filtered.log", "lines": 50000, "sid": "filter-1",
        "filter": {"mode": "literal", "query": "needle", "context": 1},
    }))

    assert sub is not None
    response = websocket.messages[0]
    assert response["type"] == "local_logs_filtered"
    assert "lines" not in response
    assert [record["line_no"] for record in response["records"]] == [1, 2, 3]
    assert response["match_count"] == 1
    assert getattr(sub, "_filter_state").next_line_no == 5


def test_local_log_delta_bounds_unterminated_line(tmp_path) -> None:
    """Retain only the viewer-sized tail of a line that never terminates."""
    path = tmp_path / "long.log"
    path.write_bytes(b"")
    _content, _size, sub = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        path, "long.log",
    )
    limit = async_logs.MAX_LOCAL_LOG_PARTIAL_BYTES
    with path.open("ab") as handle:
        handle.write(b"a" * 100 + b"b" * limit)

    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub, max_bytes=limit + 100) == []
    assert sub.partial == b"b" * limit
    with path.open("ab") as handle:
        handle.write(b"\n")
    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["b" * limit]
    assert sub.partial == b""


def test_local_log_unsubscribe_clears_cursor_state(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The WebSocket unsubscribe command releases identity and buffered content."""
    sub = async_logs.LocalLogSub(
        file=tmp_path / "cleanup.log",
        name="cleanup.log",
        pos=42,
        sid="old",
        identity=(1, 2),
        partial=b"unfinished",
    )

    class UnsubscribeWebSocket:
        """Provide one subscribe command followed by its matching unsubscribe."""

        client = "test-client"

        async def iter_text(self):
            """Yield the command sequence consumed by the endpoint."""
            yield '{"cmd":"subscribe_local_logs"}'
            yield '{"cmd":"unsubscribe_local_logs"}'

        async def send_json(self, _message: dict) -> None:
            """Accept endpoint messages without external I/O."""

    async def authenticate(_websocket):
        """Authenticate the isolated fake socket."""
        return object()

    async def send_no_state(_websocket):
        """Avoid reading monitor runtime state in this focused test."""

    async def subscribe(_websocket, _request):
        """Return the prepared cursor whose cleanup is under test."""
        return sub

    monkeypatch.setattr(vps_api, "authenticate_websocket", authenticate)
    monkeypatch.setattr(vps_api, "_send_full_state", send_no_state)
    monkeypatch.setattr(vps_api, "_cmd_subscribe_local_logs", subscribe)
    monkeypatch.setattr(vps_api, "_log", lambda *_args, **_kwargs: None)

    asyncio.run(vps_api.ws_vps(UnsubscribeWebSocket()))

    assert sub.pos == 0
    assert sub.identity is None
    assert sub.partial == b""


def test_state_opt_out_keeps_explicit_local_log_protocol(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A state-free socket still lists, snapshots, and streams local logs."""
    sub = async_logs.LocalLogSub(file=tmp_path / "local.log", name="PBGui.log", sid="local-1")
    messages: list[dict] = []

    class LocalOnlyWebSocket:
        """Exercise the local protocol with full VPS state disabled."""

        client = "test-client"
        query_params = {"state": "0"}

        async def iter_text(self):
            """Request the explicit list and a local subscription."""
            yield '{"cmd":"list_local_logs"}'
            yield '{"cmd":"subscribe_local_logs","file":"PBGui.log","sid":"local-1"}'
            await asyncio.sleep(0.01)

        async def send_json(self, message: dict) -> None:
            """Capture aggregate message types only."""
            messages.append(message)
            await asyncio.sleep(0)

    async def authenticate(_websocket):
        """Authenticate the isolated fake socket."""
        return object()

    async def reject_state(_websocket):
        """Fail if an initial full state crosses the opt-out boundary."""
        raise AssertionError("state must be suppressed")

    async def reject_state_loop(_websocket):
        """Fail if the periodic full-state task is created."""
        raise AssertionError("state loop must be suppressed")

    async def idle_remote_loop(_websocket, _get_subscription):
        """Remain cancellable without producing remote traffic."""
        await asyncio.Event().wait()

    async def local_loop(websocket, get_sub):
        """Emit one filtered-log-style delta after subscription activation."""
        while get_sub() is None:
            await asyncio.sleep(0)
        await websocket.send_json({
            "type": "local_log_filtered_lines", "sid": "local-1", "records": [],
            "source_start": 1, "source_end": 1, "match_count": 0,
        })
        await asyncio.Event().wait()

    async def subscribe(websocket, _request):
        """Return one local snapshot through the normal command boundary."""
        await websocket.send_json({
            "type": "local_logs_filtered", "sid": "local-1", "records": [],
            "streaming": True, "source_start": 1, "source_end": 0, "match_count": 0,
        })
        return sub

    monkeypatch.setattr(vps_api, "authenticate_websocket", authenticate)
    monkeypatch.setattr(vps_api, "_send_full_state", reject_state)
    monkeypatch.setattr(vps_api, "_push_state_loop", reject_state_loop)
    monkeypatch.setattr(vps_api, "_push_log_loop", idle_remote_loop)
    monkeypatch.setattr(vps_api, "_push_local_log_loop", local_loop)
    monkeypatch.setattr(vps_api, "_cmd_subscribe_local_logs", subscribe)
    monkeypatch.setattr(vps_api, "_streamer", SimpleNamespace(list_local_logs=lambda: ["PBGui.log"]))
    monkeypatch.setattr(vps_api, "_log", lambda *_args, **_kwargs: None)

    asyncio.run(vps_api.ws_vps(LocalOnlyWebSocket()))

    assert [message["type"] for message in messages] == [
        "local_logs_list", "local_logs_filtered", "local_log_filtered_lines",
    ]


def test_default_vps_socket_preserves_initial_and_periodic_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sockets without the opt-out retain the existing full-state behavior."""
    events: list[str] = []

    class DefaultWebSocket:
        """Open a default socket long enough for its state task to start."""

        client = "test-client"
        query_params = {}

        async def iter_text(self):
            """Yield no commands after allowing background tasks to run."""
            await asyncio.sleep(0.01)
            if False:
                yield ""

        async def send_json(self, _message: dict) -> None:
            """Accept state output without external I/O."""
            await asyncio.sleep(0)

    async def authenticate(_websocket):
        """Authenticate the isolated fake socket."""
        return object()

    async def send_state(_websocket):
        """Record initial state delivery."""
        events.append("initial")
        await asyncio.sleep(0)

    async def state_loop(_websocket):
        """Record periodic task creation and remain cancellable."""
        events.append("periodic")
        await asyncio.Event().wait()

    async def idle_loop(*_args):
        """Remain cancellable without producing log traffic."""
        await asyncio.Event().wait()

    monkeypatch.setattr(vps_api, "authenticate_websocket", authenticate)
    monkeypatch.setattr(vps_api, "_send_full_state", send_state)
    monkeypatch.setattr(vps_api, "_push_state_loop", state_loop)
    monkeypatch.setattr(vps_api, "_push_log_loop", idle_loop)
    monkeypatch.setattr(vps_api, "_push_local_log_loop", idle_loop)
    monkeypatch.setattr(vps_api, "_log", lambda *_args, **_kwargs: None)

    asyncio.run(vps_api.ws_vps(DefaultWebSocket()))

    assert events == ["initial", "periodic"]


def test_local_log_reads_use_managed_file_physical_lock(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapshot and delta reads coordinate on the writer's path lock target."""
    path = tmp_path / "data" / "logs" / "managed.log"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"current\n")
    rotated_path = Path(f"{path}.1")
    rotated_path.write_bytes(b"before\n")
    lock_targets: list[object] = []

    @contextmanager
    def observed_lock(target):
        lock_targets.append(target)
        yield

    monkeypatch.setattr(async_logs, "_project_root", lambda: tmp_path)
    monkeypatch.setattr(async_logs, "advisory_file_lock", observed_lock)
    _content, _size, sub = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        rotated_path, "managed.log.1",
    )
    with rotated_path.open("ab") as handle:
        handle.write(b"after\n")

    assert async_logs.AsyncLogStreamer.read_local_log_delta(sub) == ["after"]
    assert lock_targets == [path, path]


def test_rotated_log_read_contends_on_current_log_writer_lock(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Foo.log.1 read waits on the canonical Foo.log writer lock."""
    path = tmp_path / "data" / "logs" / "managed.log"
    path.parent.mkdir(parents=True)
    rotated_path = Path(f"{path}.1")
    rotated_path.write_text("rotated\n", encoding="utf-8")
    started = threading.Event()
    finished = threading.Event()
    result: list[str] = []
    monkeypatch.setattr(async_logs, "_project_root", lambda: tmp_path)

    def read_rotated() -> None:
        started.set()
        result.extend(async_logs.tail_file(rotated_path, 1))
        finished.set()

    with advisory_file_lock(path):
        thread = threading.Thread(target=read_rotated)
        thread.start()
        assert started.wait(1)
        time.sleep(0.05)
        assert not finished.is_set()
    thread.join(2)

    assert finished.is_set()
    assert result == ["rotated"]


def test_get_recent_logs_rejects_injection_before_remote_command():
    """Never pass a malicious line-count value to the SSH pool."""
    streamer = async_logs.AsyncLogStreamer(FakePool())

    with pytest.raises(ValueError):
        asyncio.run(streamer.get_recent_logs("host", "PBRun", "1; touch /tmp/pwn"))


def test_grouped_bot_log_fetch_uses_one_remote_command_for_all_files() -> None:
    """Error and traceback popups fetch monitor-discovered files in one SSH round trip."""

    class GroupedPool(FakePool):
        """Capture the grouped remote command."""

        def __init__(self) -> None:
            super().__init__("/srv/pb7")
            self.commands: list[str] = []

        def get_remote_pbgui_dirs(self, hostname: str) -> list[str]:
            """Return one known PBGui root."""
            del hostname
            return ["software/pbgui"]

        async def run(self, hostname: str, command: str, timeout: int = 30):
            """Record one grouped SSH command."""
            del hostname, timeout
            self.commands.append(command)
            return SimpleNamespace(exit_status=0, stdout="2026-07-17T12:00:00Z ERROR boom\n")

    pool = GroupedPool()
    streamer = async_logs.AsyncLogStreamer(pool)
    output = asyncio.run(streamer.get_recent_log_files(
        "host",
        ["pb7/logs/bot.log", "data/run_v7/bot/passivbot_err.log.old"],
        500,
        contains=" ERROR ",
    ))

    assert output == "2026-07-17T12:00:00Z ERROR boom\n"
    assert len(pool.commands) == 1
    assert "/srv/pb7/logs/bot.log" in pool.commands[0]
    assert '"$HOME"/software/pbgui/data/run_v7/bot/passivbot_err.log.old' in pool.commands[0]


def test_today_error_matches_use_monitor_discovered_current_and_rotated_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The popup reads the same PB7 log set which produced the monitor count."""

    class MatchStreamer:
        """Return two matching lines and capture the grouped request."""

        def __init__(self) -> None:
            self.calls: list[tuple[list[str], int, str | None]] = []

        async def get_recent_log_files(self, hostname, paths, lines, *, contains=None):
            """Capture the selected files without performing SSH."""
            del hostname
            self.calls.append((list(paths), int(lines), contains))
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            return f"{today}T01:00:00Z ERROR old\n{today}T02:00:00Z ERROR current\n"

    streamer = MatchStreamer()
    monkeypatch.setattr(vps_api, "_streamer", streamer)
    monkeypatch.setattr(
        vps_api,
        "_monitor",
        SimpleNamespace(store=SimpleNamespace(bot_logs={
            "host": {"bot": ["pb7/logs/archived-bot.log", "pb7/logs/bot.log"]}
        })),
    )

    matches = asyncio.run(vps_api.get_bot_log_matches(
        "host",
        "bot",
        pb_version="7",
        kind="errors",
        bucket="today",
        expected_count=2,
        lines=0,
    ))

    assert len(matches) == 2
    assert streamer.calls == [
        (["pb7/logs/archived-bot.log", "pb7/logs/bot.log"], 500, " ERROR ")
    ]


@pytest.mark.parametrize("file_count", [32, 33, 65])
@pytest.mark.parametrize("version", ["7", "8"])
@pytest.mark.parametrize("transport", ["direct", "rpc"])
def test_today_errors_survive_restart_with_many_log_files(
    tmp_path, monkeypatch: pytest.MonkeyPatch, file_count: int, version: str, transport: str,
) -> None:
    """Read pre-restart errors beyond the RPC file limit using only isolated logs."""
    runtime = tmp_path / f"pb{version}"
    logs = runtime / "logs"
    logs.mkdir(parents=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    expected = [f"{today}T01:00:{index:02d}Z ERROR before restart" for index in range(37)]
    paths = []
    for index in range(file_count):
        filename = f"{index:04d}_bot_config_run.json.log"
        content = "2000-01-01T00:00:00Z ERROR historical\n"
        if index == 0:
            content += "\n".join(expected[:5]) + "\n"
        if index == file_count - 2:
            content += "\n".join(expected[5:]) + "\n"
        if index == file_count - 1:
            content = f"{today}T02:00:00Z INFO restarted\n"
        (logs / filename).write_text(content, encoding="utf-8")
        paths.append(f"pb{version}/logs/{filename}")

    class IsolatedPool(FakePool):
        """Execute the real read-only log command against temporary files, never SSH."""

        async def run(self, hostname, command, timeout=30):
            """Capture output from a bounded local subprocess."""
            del hostname
            process = await asyncio.create_subprocess_shell(command, stdout=asyncio.subprocess.PIPE)
            try:
                stdout, _ = await asyncio.wait_for(process.communicate(), timeout)
                return SimpleNamespace(exit_status=process.returncode, stdout=stdout.decode())
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()

    pool = IsolatedPool(pb7dir=str(runtime), pb8dir=str(runtime))
    store = VPSStore()
    store.update_bot_logs("host", {f"{version}:bot": {"sidebar": paths}})
    monitor = SimpleNamespace(store=store)
    streamer = async_logs.AsyncLogStreamer(pool)
    daemon = VPSMonitorRPCDaemon(tmp_path / "unused.sock", monitor=monitor, streamer=streamer)

    class DirectRPCClient:
        """Exercise the real RPC dispatcher without opening a socket or starting daemons."""

        async def call_async(self, method, params):
            """Apply production RPC validation to each grouped log request."""
            return await daemon.dispatch(method, params)

    monkeypatch.setattr(vps_api, "_monitor", monitor)
    monkeypatch.setattr(vps_api, "_streamer", RemoteLogStreamerProxy(DirectRPCClient()) if transport == "rpc" else streamer)
    matches = asyncio.run(vps_api.get_bot_log_matches(
        "host", "bot", pb_version=version, kind="errors", bucket="today", expected_count=37,
    ))
    assert matches == expected


def test_today_traceback_matches_use_current_and_old_stderr_in_one_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The traceback popup uses both stderr rotations without sequential SSH probes."""

    class MatchStreamer:
        """Return one wrapper-timestamped traceback block."""

        def __init__(self) -> None:
            self.paths: list[str] = []

        async def get_recent_log_files(self, hostname, paths, lines, *, contains=None):
            """Capture one grouped request and return a traceback."""
            del hostname, lines
            assert contains is None
            self.paths = list(paths)
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            return f"{today}T03:00:00Z stderr\nTraceback (most recent call last):\nValueError: boom\n"

    streamer = MatchStreamer()
    monkeypatch.setattr(vps_api, "_streamer", streamer)
    monkeypatch.setattr(
        vps_api,
        "_monitor",
        SimpleNamespace(store=SimpleNamespace(bot_logs={
            "host": {"bot": [
                "data/run_v7/bot/passivbot_err.log.old",
                "data/run_v7/bot/passivbot_err.log",
            ]}
        })),
    )

    matches = asyncio.run(vps_api.get_bot_log_matches(
        "host",
        "bot",
        pb_version="7",
        kind="tracebacks",
        bucket="today",
        expected_count=1,
        lines=0,
    ))

    assert "Traceback (most recent call last):" in matches
    assert streamer.paths == [
        "data/run_v7/bot/passivbot_err.log.old",
        "data/run_v7/bot/passivbot_err.log",
    ]


def test_kill_instance_never_interpolates_name_into_remote_shell():
    """Keep shell metacharacters in a bot name out of every remote command."""
    pool = FakeMonitorPool()
    monitor = SimpleNamespace(pool=pool)
    malicious_name = "bot';touch_pwn;#"

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", malicious_name))

    assert result == {"success": False, "pid": ""}
    assert pool.commands == ["ps -eo pid=,args="]
    assert all(malicious_name not in command for command in pool.commands)


def test_kill_instance_uses_only_parsed_numeric_pid():
    """Kill the process whose config path exactly identifies the requested bot."""
    process_output = (
        " 123 /usr/bin/python /srv/pb7/src/main.py "
        "/home/pbgui/software/pbgui/data/run_v7/bybit_SOLUSDT/config_run.json\n"
    )
    pool = FakeMonitorPool(process_output)
    monitor = SimpleNamespace(pool=pool)

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", "bybit_SOLUSDT"))

    assert result == {"success": True, "pid": "123"}
    assert pool.commands == ["ps -eo pid=,args=", "kill -- 123"]


def test_kill_pb8_instance_uses_one_exact_pidfd_capable_remote_helper():
    """Remote PB8 stop validates identity and escalates until the process exits."""

    class PB8Pool(FakeMonitorPool):
        """Return cached PB8 runtime paths and process metadata."""

        def get_connection(self, hostname: str):
            del hostname
            return SimpleNamespace(data={"pb8dir": "/srv/pb8", "pb8venv": "/srv/venv_pb8/bin/python"})

        async def run(self, hostname: str, command: str, timeout: int = 15):
            del hostname, timeout
            self.commands.append(command)
            return SimpleNamespace(exit_status=0, stdout="321\n")

    pool = PB8Pool()
    monitor = SimpleNamespace(pool=pool)

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", "same", "8"))

    assert result == {"success": True, "pid": "321"}
    assert len(pool.commands) == 1
    arguments = shlex.split(pool.commands[0])
    assert arguments[:2] == ["python3", "-c"]
    assert arguments[3:] == [
        "/home/pbgui/software/pbgui/data/run_v8/same/config.json",
        "/srv/pb8",
        "/srv/venv_pb8/bin/python",
    ]
    assert "pidfd_open" in arguments[2]
    assert "pidfd_send_signal" in arguments[2]
    assert 'argv not in expected_commands' in arguments[2]
    assert 'cwd != expected_cwd' in arguments[2]
    assert "snapshot(pid) == identity" in arguments[2]
    assert "signal.SIGINT, 5" in arguments[2]
    assert "signal.SIGTERM, 3" in arguments[2]
    assert "signal.SIGKILL, 2" in arguments[2]
    assert "wait_stopped(timeout)" in arguments[2]


def test_kill_pb8_instance_reports_remote_exact_match_rejection():
    """The single remote helper reports no target when exact validation fails."""

    class PB8Pool(FakeMonitorPool):
        """Expose cached paths for an otherwise invalid process command."""

        def get_connection(self, hostname: str):
            del hostname
            return SimpleNamespace(data={"pb8dir": "/srv/pb8", "pb8venv": "/srv/venv_pb8/bin/python"})

        async def run(self, hostname: str, command: str, timeout: int = 15):
            del hostname, timeout
            self.commands.append(command)
            return SimpleNamespace(exit_status=1, stdout="")

    pool = PB8Pool()
    monitor = SimpleNamespace(pool=pool)

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", "same", "8"))

    assert result == {"success": False, "pid": ""}
    assert len(pool.commands) == 1


def test_kill_pb8_instance_shell_quotes_validated_arguments():
    """A valid unusual instance name remains one inert remote helper argument."""

    class PB8Pool(FakeMonitorPool):
        """Expose PB8 paths and reject the helper after command capture."""

        def get_connection(self, hostname: str):
            del hostname
            return SimpleNamespace(data={"pb8dir": "/srv/pb 8", "pb8venv": "/srv/venv pb8/bin/python"})

        async def run(self, hostname: str, command: str, timeout: int = 15):
            del hostname, timeout
            self.commands.append(command)
            return SimpleNamespace(exit_status=1, stdout="")

    name = "bot';touch_pwn;#"
    pool = PB8Pool()
    monitor = SimpleNamespace(pool=pool)

    result = asyncio.run(VPSMonitor.kill_instance(monitor, "host", name, "8"))

    assert result == {"success": False, "pid": ""}
    arguments = shlex.split(pool.commands[0])
    assert arguments[3] == f"/home/pbgui/software/pbgui/data/run_v8/{name}/config.json"
    assert arguments[4:] == ["/srv/pb 8", "/srv/venv pb8/bin/python"]


def test_local_kill_pb8_delegates_to_runv8_stop(tmp_path, monkeypatch):
    """Local PB8 restart requests use the supervisor's graceful stop method."""

    instance_dir = tmp_path / "data" / "run_v8" / "same"
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_text("{}", encoding="utf-8")
    events = []

    class FakePBRun:
        """Provide local runtime paths without starting the supervisor."""

        v7_path = str(tmp_path / "data" / "run_v7")
        v8_path = str(tmp_path / "data" / "run_v8")
        name = "node-a"
        pbgdir = tmp_path
        pb7dir = str(tmp_path / "pb7")
        pb7venv = str(tmp_path / "venv_pb7" / "bin" / "python")
        pb8dir = str(tmp_path / "pb8")
        pb8venv = str(tmp_path / "venv_pb8" / "bin" / "python")

    class FakeRunV8:
        """Record the runtime-aware stop delegation."""

        def load(self):
            return True

        def pid(self):
            return SimpleNamespace(pid=4321)

        def stop(self):
            events.append((self.user, self.path, self.pb8dir, self.pb8venv))

    monkeypatch.setattr(pbrun_module, "PBRun", FakePBRun)
    monkeypatch.setattr(pbrun_module, "RunV8", FakeRunV8)

    result = asyncio.run(vps_api._local_kill_instance("same", "8"))

    assert result["success"] is True
    assert result["pid"] == 4321
    assert events == [(
        "same",
        str(instance_dir),
        str(tmp_path / "pb8"),
        str(tmp_path / "venv_pb8" / "bin" / "python"),
    )]


def test_stop_stream_removes_registry_entry_immediately():
    """A disconnected remote-log subscriber must not leave its stream object cached."""
    streamer = async_logs.AsyncLogStreamer(FakePool())
    task = FakeTask()
    stream = async_logs.LogStream("stream-1", "host", "data/logs/PBRun.log", task=task)
    streamer._streams[stream.stream_id] = stream

    streamer.stop_stream(stream.stream_id)

    assert stream.stream_id not in streamer._streams
    assert stream.active is False
    assert task.cancelled is True


def test_stop_all_streams_clears_registry_immediately():
    """Shutdown of remote-log streaming must release every cached stream object."""
    streamer = async_logs.AsyncLogStreamer(FakePool())
    tasks = [FakeTask(), FakeTask()]
    for index, task in enumerate(tasks):
        stream = async_logs.LogStream(f"stream-{index}", "host", "data/logs/PBRun.log", task=task)
        streamer._streams[stream.stream_id] = stream

    streamer.stop_all_streams()

    assert streamer._streams == {}
    assert all(task.cancelled for task in tasks)


def test_private_ws_client_closes_only_after_last_owner_releases(monkeypatch):
    """Dashboard disconnects must not close a private client still used by Live SSE."""
    client = FakeWsClient()
    user = SimpleNamespace(name="alice")
    monkeypatch.setattr(Exchange, "_private_ws_clients", {"bybit:alice": client})
    monkeypatch.setattr(Exchange, "_private_ws_owners", {"bybit:alice": {"dashboard_positions", "live_session.positions"}})
    monkeypatch.setattr(Exchange, "_private_ws_locks", {"bybit:alice": object()})

    async def release_owners():
        first = await Exchange.release_private_ws_client("bybit", user, caller="dashboard_positions")
        second = await Exchange.release_private_ws_client("bybit", user, caller="live_session.positions")
        return first, second

    first, second = asyncio.run(release_owners())

    assert first is False
    assert client.closed is True
    assert second is True
    assert Exchange._private_ws_clients == {}
    assert Exchange._private_ws_owners == {}
    assert Exchange._private_ws_locks == {}


def test_shared_ws_client_closes_only_after_last_owner_releases(monkeypatch):
    """One candle watcher must not close a shared client used by another chart."""
    client = FakeWsClient()
    monkeypatch.setattr(Exchange, "_shared_ws_clients", {"bybit": client})
    monkeypatch.setattr(Exchange, "_shared_ws_owners", {"bybit": {"chart-a", "chart-b"}})
    monkeypatch.setattr(Exchange, "_shared_ws_markets_loaded", {"bybit"})
    monkeypatch.setattr(Exchange, "_shared_ws_locks", {"bybit": object()})

    async def release_owners():
        first = await Exchange.release_shared_ws_client("bybit", caller="chart-a")
        second = await Exchange.release_shared_ws_client("bybit", caller="chart-b")
        return first, second

    first, second = asyncio.run(release_owners())

    assert first is False
    assert client.closed is True
    assert second is True
    assert Exchange._shared_ws_clients == {}
    assert Exchange._shared_ws_owners == {}
    assert Exchange._shared_ws_markets_loaded == set()
    assert Exchange._shared_ws_locks == {}
