"""Server-selected PB9 config library with no request-controlled directory paths."""

from contextlib import contextmanager
import os
from pathlib import Path
import stat

from pb9_config import PB9ConfigError
from pb9_config_storage import PB9FileContext, _config_name, _root_directory
from secure_files import private_directory_at


SERVICE = "PB9Config"
MAX_LIBRARY_ENTRIES = 1000


def config_name(value) -> str:
    """Validate a visible UTF-8 JSON basename before accessing settings or storage."""
    name = _config_name(value)
    try:
        if len(name.encode("utf-8")) > 255:
            raise PB9ConfigError("invalid_config_name")
    except UnicodeError:
        raise PB9ConfigError("invalid_config_name") from None
    return name


@contextmanager
def config_library(application_root: Path, paths: dict, cancel_event, *, create: bool = False):
    """Select only data/configs_v9, without chmod or symlink-following creation."""
    root = Path(application_root) / "data" / "configs_v9"
    try:
        with _root_directory(Path(application_root)) as application_fd:
            with private_directory_at(application_fd, "data", create=create, private=False) as data_fd:
                with private_directory_at(data_fd, "configs_v9", create=create):
                    yield PB9FileContext(root, paths["pb9dir"], paths["pb9venv"],
                                         {key: paths[key] for key in ("pb7dir", "pb7venv", "pb8dir", "pb8venv")},
                                         cancel_event=cancel_event)
    except FileNotFoundError:
        raise PB9ConfigError("config_file_not_found") from None
    except ValueError:
        raise PB9ConfigError("config_directory_not_private") from None
    except OSError:
        raise PB9ConfigError("config_file_unavailable") from None


def list_configs(context: PB9FileContext) -> list[str]:
    """List bounded regular JSON basenames; never follow links or load configs."""
    names = []
    with _root_directory(Path(context.approved_root)) as descriptor:
        with os.scandir(descriptor) as entries:
            for count, entry in enumerate(entries, 1):
                if context.cancel_event.is_set():
                    raise PB9ConfigError("helper_cancelled")
                if count > MAX_LIBRARY_ENTRIES:
                    raise PB9ConfigError("config_library_limit")
                try:
                    name = _config_name(entry.name)
                    name.encode("utf-8")
                except (PB9ConfigError, UnicodeError):
                    continue
                try:
                    info = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISREG(info.st_mode):
                    names.append(name)
    return sorted(names, key=lambda name: (name.casefold(), name))


def require_config(context: PB9FileContext, name: str) -> str:
    """Check a visible basename with no path traversal or link following."""
    filename = _config_name(name)
    with _root_directory(Path(context.approved_root)) as descriptor:
        try:
            info = os.stat(filename, dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError:
            raise PB9ConfigError("config_file_not_found") from None
        if not stat.S_ISREG(info.st_mode):
            raise PB9ConfigError("config_file_unavailable")
    return filename
