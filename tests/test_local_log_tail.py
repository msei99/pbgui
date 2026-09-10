"""Exact local tails with variable-size records and bounded chunk reads."""

from io import BytesIO

import pytest

from master import async_logs


@pytest.mark.parametrize("count", [200, 10000])
def test_tail_returns_requested_newest_records_not_byte_estimate(tmp_path, count):
    """Long records must not reduce the requested count or omit the latest saves."""
    records = [f"2026-09-09 [ApiKeys] [INFO] record-{i} " + "x" * 800 for i in range(count + 100)]
    records[-2:] = [
        "2026-09-09T19:00:37.598 [ApiKeys] [INFO] Updated API key user: synthetic16",
        "2026-09-09T19:02:31.042 [ApiKeys] [INFO] Updated API key user: synthetic17",
    ]
    path = tmp_path / "PBGui.log"
    path.write_text("\n".join(records) + "\n", encoding="utf-8")
    assert async_logs.tail_file(path, count) == records[-count:]
    content, size, subscription = async_logs.AsyncLogStreamer.initialize_local_log_subscription(
        path, "PBGui.log", count,
    )
    assert content == records[-count:]
    assert subscription.pos == size == path.stat().st_size
    assert subscription.partial == b""


@pytest.mark.parametrize("data", [b"", b"one", b"one\n", b"\n", b"one\n\nthree\npartial", b"one\r\ntwo\r\n"])
@pytest.mark.parametrize("count", [0, 1, 2, 10000])
def test_tail_record_boundaries_and_partial(data, count):
    """Preserve blank/CRLF records and keep incomplete final writes separate."""
    parts = data.split(b"\n")
    expected = [async_logs._decode_local_log_line(part) for part in parts[:-1]]
    lines, partial = async_logs._tail_open_file(BytesIO(data), len(data), count)
    assert lines == (expected[-count:] if count else expected)
    assert partial == parts[-1]


@pytest.mark.parametrize("terminated", [False, True])
def test_tail_bounds_multichunk_records_and_reads(terminated):
    """Cross-chunk lines retain their bounded ends without a whole-file read."""
    limit = async_logs.MAX_LOCAL_LOG_PARTIAL_BYTES
    record = b"x" * 200000 + b"z" * limit
    data = b"older\n" + record + (b"\n" if terminated else b"")

    class BoundedReader(BytesIO):
        """Reject unbounded or oversized reads from the snapshot descriptor."""

        def read(self, size=-1):
            """Require fixed-size I/O even for very long individual records."""
            assert 0 < size <= 65536
            return super().read(size)

    lines, partial = async_logs._tail_open_file(BoundedReader(data), len(data), 2)
    assert lines == (["older", "z" * limit] if terminated else ["older"])
    assert partial == (b"" if terminated else b"z" * limit)


def test_tail_keeps_full_bounded_text_before_multichunk_crlf():
    """The CR terminator must not consume one character of the retained record."""
    limit = async_logs.MAX_LOCAL_LOG_PARTIAL_BYTES
    data = b"old\n" + b"x" * 200000 + b"z" * limit + b"\r\n"
    lines, partial = async_logs._tail_open_file(BytesIO(data), len(data), 1)
    assert lines == ["z" * limit]
    assert partial == b""
