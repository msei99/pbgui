"""Read-only process-reference checks shared by native Run and Cluster cleanup."""

from pathlib import Path

import psutil

SERVICE = "NativeLiveProcess"


def config_is_referenced(config_path: Path | str) -> bool:
    """Detect absolute, symlink and cwd-relative references without signaling a process.

    Unreadable process metadata propagates to the caller's fail-closed boundary.
    Only candidate config arguments require a cwd lookup.
    """
    expected = Path(config_path).resolve()
    for process in psutil.process_iter():
        try:
            for argument in process.cmdline():
                candidate = Path(argument)
                if candidate.name != expected.name:
                    continue
                if not candidate.is_absolute():
                    candidate = Path(process.cwd()) / candidate
                if candidate.resolve() == expected:
                    return True
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
    return False
