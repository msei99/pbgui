"""Offline behavior checks for mount-aware shared navigation in a Node VM."""

import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("prefix", [
    "<absent>", "", "/pbgui", "/team/pbgui", "/team%20space/pbgui",
    "/a%20b/%3C%3E%26%22%25/%C3%A4", "/literal%252f",
])
@pytest.mark.parametrize("origin", ["http://example.test:8080", "https://example.test", "http://[::1]:8080", "https://[2001:db8::1]:8443"])
def test_shared_nav_mount_behavior(prefix: str, origin: str) -> None:
    """Exercise real nav startup, handlers and URL boundaries without browser I/O."""
    result = subprocess.run(
        ["node", str(ROOT / "tests" / "nav_mount_paths.cjs"), prefix, origin],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
