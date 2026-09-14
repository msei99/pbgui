"""Interactively save a separate GHCR read-only credential without exposing it."""

from getpass import getpass
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "VastGpuRegistrySetup"


def main() -> None:
    """Store only a user-entered registry credential; make no network calls."""
    folder = ensure_private_directory(ROOT / ".local-work/vast-gpu-test/credentials")
    target = folder / "ghcr_pull_token"
    if target.exists():
        raise SystemExit("A registry pull credential is already stored")
    token = getpass("GitHub classic PAT with read:packages only (hidden): ").strip()
    if not token or any(char.isspace() for char in token):
        raise SystemExit("Invalid input; nothing saved")
    atomic_write_private_text(target, token + "\n")
    print("Registry pull credential saved privately. Its permissions still need verification.")


if __name__ == "__main__":
    main()
