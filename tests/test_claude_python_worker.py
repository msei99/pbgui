"""Offline rejection checks for the Python worker's untrusted pipe boundary."""
import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("payload", [
    b"[]\n", b'{"mode":\n', b'{"mode":"models","value":NaN}\n',
    b'{"mode":"models","value":1e999}\n', b'{"mode":"not-a-mode"}\n',
    b'{"mode":"models","output_schema":{"type":"array"}}\n',
    b'{"mode":"chat","tools":[{"name":"bad/tool","input_schema":{"type":"object"}}]}\n',
    b"x" * (2 * 1024 * 1024 + 1) + b"\n",
])
def test_invalid_pipe_input_fails_without_native_execution_or_secret_output(tmp_path, payload):
    """Malformed input cannot submit a prompt, dispatch a tool or reflect its payload."""
    worker = Path(__file__).resolve().parents[1] / "runtime/claude_agent/worker.py"
    response = subprocess.run([sys.executable, "-I", str(worker)], input=payload, capture_output=True,
        cwd=tmp_path, env={"PATH": "", "HOME": str(tmp_path), "CLAUDE_CONFIG_DIR": str(tmp_path)}, timeout=10)
    assert response.returncode == 1
    events = [json.loads(line) for line in response.stdout.splitlines()]
    assert len(events) == 1 and events[0]["type"] == "error"
    assert events[0]["message"] in {
        "Claude runtime protocol failed.", "Claude did not return the required structured decision.",
        "Claude enabled an unexpected tool.", "Claude runtime failed. Check dependencies and subscription sign-in.",
    }
    assert not any(tmp_path.iterdir()), "Rejected input started native profile work"
