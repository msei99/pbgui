"""Read-only PB9 file boundary, selected explicitly through pb7_config."""

from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import stat

from logging_helpers import human_log as _log
from pb9_config import MAX_DOCUMENT_BYTES, PB9ConfigError, validate_pb9_document


SERVICE = "PB9Config"
MAX_FILE_BYTES = MAX_DOCUMENT_BYTES


@dataclass(frozen=True)
class PB9FileContext:
    """Caller-owned selection of an approved config directory and native runtime.

    The root is supplied by trusted application code, never inferred from a
    filename or document metadata. No default production data root is used.
    """

    approved_root: str | Path
    pb9dir: str | Path | None
    pb9venv: str | Path | None
    other_slots: dict | None = None
    timeout: float = 60


def _approved_root(context: PB9FileContext) -> Path:
    """Require an explicit absolute root before any file is opened."""
    if not isinstance(context, PB9FileContext) or not isinstance(context.approved_root, (str, Path)):
        raise PB9ConfigError("invalid_storage_context")
    if context.other_slots is not None and not isinstance(context.other_slots, dict):
        raise PB9ConfigError("invalid_storage_context")
    raw = str(context.approved_root)
    if not raw.strip() or any(ord(char) < 32 or ord(char) == 127 for char in raw):
        raise PB9ConfigError("invalid_storage_context")
    root = Path(raw)
    if not root.is_absolute() or ".." in root.parts:
        raise PB9ConfigError("invalid_storage_context")
    return root


def _config_name(name: str | Path) -> str:
    """Accept only visible JSON basenames, never paths or traversal segments."""
    if not isinstance(name, (str, Path)):
        raise PB9ConfigError("invalid_config_name")
    name = str(name)
    if (not name or name != name.strip() or name.startswith(".")
            or any(char in name for char in ("/", "\\", ":"))
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or Path(name).suffix.lower() != ".json"):
        raise PB9ConfigError("invalid_config_name")
    return name


def _read_config_bytes(root: Path, name: str) -> bytes:
    """Read a bounded regular file via no-follow descriptors, including ancestors."""
    if os.name != "posix" or any(not hasattr(os, flag) for flag in ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")):
        raise PB9ConfigError("unsupported_platform")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptors = []
    try:
        directory_fd = os.open(root.anchor, directory_flags)
        descriptors.append(directory_fd)
        for part in root.parts[1:]:
            directory_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            descriptors.append(directory_fd)
        file_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
        descriptors.append(file_fd)
        info = os.fstat(file_fd)
        if not stat.S_ISREG(info.st_mode):
            raise PB9ConfigError("config_file_unavailable")
        if info.st_size > MAX_FILE_BYTES:
            raise PB9ConfigError("config_file_too_large")
        chunks, remaining = [], MAX_FILE_BYTES + 1
        while remaining:
            chunk = os.read(file_fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > MAX_FILE_BYTES:
            raise PB9ConfigError("config_file_too_large")
        return data
    finally:
        close_error = None
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError as exc:
                if close_error is None:
                    close_error = exc
        if close_error is not None:
            raise close_error


def _parse_document(data: bytes) -> dict:
    """Decode strict UTF-8 JSON without duplicate keys or nonfinite numbers."""
    def unique_object(pairs):
        """Reject duplicate members at every object depth."""
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = value
        return result

    def reject_constant(_value):
        """Do not permit Python's nonstandard JSON constants."""
        raise ValueError("nonfinite JSON constant")

    def finite_float(value):
        """Reject exponent overflow as well as explicit nonfinite constants."""
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("nonfinite JSON number")
        return parsed

    try:
        document = json.loads(data.decode("utf-8"), object_pairs_hook=unique_object,
                              parse_constant=reject_constant, parse_float=finite_float)
        if not isinstance(document, dict):
            raise ValueError("document must be an object")
        return document
    except (ValueError, RecursionError):
        pass
    # Raise outside the handler so JSONDecodeError.doc is not retained as context.
    raise PB9ConfigError("invalid_document") from None


def load_pb9_config_file(name: str | Path, context: PB9FileContext, *,
                         verbose: bool = False, neutralize_added: bool = False) -> dict:
    """Load an inline v9 source through the explicit pb7_config dispatch.

    This synchronous operation never writes, locks, migrates or invokes PB7.
    File bytes and the existing document/transport budgets are separate limits.
    Approved roots must contain no symlink components; only a direct JSON file
    is supported initially. A future API owner must offload and own native work.
    """
    error_code = None
    try:
        if verbose is not False or neutralize_added is not False:
            raise PB9ConfigError("invalid_request")
        root = _approved_root(context)
        filename = _config_name(name)
        document = _parse_document(_read_config_bytes(root, filename))
    except (OSError, RuntimeError, ValueError) as exc:
        error_code = exc.error_codes[0] if isinstance(exc, PB9ConfigError) else "config_file_unavailable"
    if error_code is not None:
        _log(SERVICE, "PB9 config file rejected", level="ERROR", meta={"error_codes": [error_code]})
        # A fresh exception outside the handler retains no raw file/decoder cause.
        raise PB9ConfigError(error_code) from None
    validated = validate_pb9_document(
        document, context.pb9dir, context.pb9venv, context.other_slots, timeout=context.timeout,
    )
    config = validated["config"]
    if validated["pbgui"] is not None:
        config["pbgui"] = validated["pbgui"]
    return config
