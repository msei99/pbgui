"""Offline DB Tools confirmation keyboard regression."""

import subprocess
from pathlib import Path


def test_escape_cancels_db_tools_confirmation():
    """Escape cancels once and removes the temporary keyboard listener."""
    script = Path(__file__).with_name("db_tools_modal_keyboard.cjs")
    subprocess.run(["node", str(script)], check=True, capture_output=True, text=True)
