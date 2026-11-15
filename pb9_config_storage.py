"""PB9 loading and exclusive creation, selected explicitly through pb7_config."""

from dataclasses import dataclass
from contextlib import contextmanager
import json
import hashlib
import math
import os
import re
from pathlib import Path
import stat
import threading

from logging_helpers import human_log as _log
from file_lock import advisory_directory_lock
from pb9_config import MAX_DOCUMENT_BYTES, PB9CancelSignal, PB9ConfigError, validate_pb9_document
from secure_files import (PRIVATE_DIR_MODE, PrivateFileChangedError, PrivateFilePublishedError,
                          create_private_file_at, replace_private_file_at)


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
    cancel_event: threading.Event | PB9CancelSignal | None = None
    validation_target: str = "config"


def _approved_root(context: PB9FileContext) -> Path:
    """Require an explicit absolute root before any file is opened."""
    if not isinstance(context, PB9FileContext) or not isinstance(context.approved_root, (str, Path)):
        raise PB9ConfigError("invalid_storage_context")
    if context.validation_target not in {"config", "backtest"}:
        raise PB9ConfigError("invalid_storage_context")
    if context.other_slots is not None and not isinstance(context.other_slots, dict):
        raise PB9ConfigError("invalid_storage_context")
    if context.cancel_event is not None and not isinstance(context.cancel_event, (threading.Event, PB9CancelSignal)):
        raise PB9ConfigError("invalid_storage_context")
    _check_cancel(context)
    raw = str(context.approved_root)
    if not raw.strip() or any(ord(char) < 32 or ord(char) == 127 for char in raw):
        raise PB9ConfigError("invalid_storage_context")
    root = Path(raw)
    if not root.is_absolute() or ".." in root.parts:
        raise PB9ConfigError("invalid_storage_context")
    return root


def _check_cancel(context: PB9FileContext) -> None:
    """Stop before native work or a file publication commit point."""
    if context.cancel_event is not None and context.cancel_event.is_set():
        raise PB9ConfigError("helper_cancelled")


def _native_options(context: PB9FileContext) -> dict:
    """Keep legacy keyword shapes intact when no owner supplies a stop flag."""
    options = {"cancel_event": context.cancel_event} if context.cancel_event is not None else {}
    if context.validation_target != "config":
        options["validation_target"] = context.validation_target
    return options


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


@contextmanager
def _root_directory(root: Path):
    """Own no-follow directory descriptors through every approved root component."""
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
        yield directory_fd
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


@contextmanager
def _config_descriptor_at(directory_fd: int, name: str):
    """Own a regular source FD, optionally retained through a guarded replacement."""
    file_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    try:
        info = os.fstat(file_fd)
        if not stat.S_ISREG(info.st_mode):
            raise PB9ConfigError("config_file_unavailable")
        if info.st_size > MAX_FILE_BYTES:
            raise PB9ConfigError("config_file_too_large")
        yield file_fd
    finally:
        os.close(file_fd)


def _read_descriptor_bytes(file_fd: int) -> bytes:
    """Read at most the file budget plus one byte, including growth after stat."""
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


def _read_config_bytes_at(directory_fd: int, name: str) -> bytes:
    """Read one no-follow source below an already owned directory descriptor."""
    with _config_descriptor_at(directory_fd, name) as file_fd:
        return _read_descriptor_bytes(file_fd)


def _read_config_bytes(root: Path, name: str) -> bytes:
    """Read a bounded regular file via no-follow descriptors, including ancestors."""
    with _root_directory(root) as directory_fd:
        return _read_config_bytes_at(directory_fd, name)


def _expect_revision(data: bytes, expected: str) -> None:
    """Compare the exact current source bytes, without canonicalization."""
    if hashlib.sha256(data).hexdigest() != expected:
        raise PB9ConfigError("config_revision_conflict")


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
        json.dumps(document, ensure_ascii=False, allow_nan=False).encode("utf-8")
        return document
    except (ValueError, RecursionError):
        pass
    # Raise outside the handler so JSONDecodeError.doc is not retained as context.
    raise PB9ConfigError("invalid_document") from None


def inspect_pb9_config_revision(name: str, context: PB9FileContext) -> dict:
    """Read bounded exact-byte provenance without native validation or imports."""
    error = None
    try:
        root = _approved_root(context)
        filename = _config_name(name)
        data = _read_config_bytes(root, filename)
        _check_cancel(context)
        return {"revision": hashlib.sha256(data).hexdigest()}
    except FileNotFoundError:
        error = "config_file_not_found"
    except (OSError, ValueError, RuntimeError) as exc:
        error = exc.error_codes[0] if isinstance(exc, PB9ConfigError) else "config_file_unavailable"
    _log(SERVICE, "PB9 revision read failed", level="WARNING", meta={"error_codes": [error]})
    raise PB9ConfigError(error) from None


def reconcile_pb9_config_file(name: str, document: dict, context: PB9FileContext) -> dict:
    """Compare stored authored values exactly, without JavaScript number coercion.

    This read-only check neither validates nor executes native code and never
    acknowledges that a write occurred; it reports whether the requested values
    currently exist. Revision and text belong to the same bounded source read.
    """
    error = None
    try:
        root = _approved_root(context)
        filename = _config_name(name)
        if not isinstance(document, dict):
            raise PB9ConfigError("invalid_document")
        data = _read_config_bytes(root, filename)
        stored = _parse_document(data)
        _check_cancel(context)
        options = {"sort_keys": True, "ensure_ascii": False, "allow_nan": False}
        matches = json.dumps(stored, **options) == json.dumps(document, **options)
        result = {"matches": matches, "revision": hashlib.sha256(data).hexdigest()}
        if matches:
            result["config_text"] = json.dumps(stored, ensure_ascii=False, indent=4, allow_nan=False) + "\n"
        return result
    except FileNotFoundError:
        error = "config_file_not_found"
    except OSError:
        error = "config_file_unavailable"
    except (ValueError, RuntimeError, TypeError) as exc:
        error = exc.error_codes[0] if isinstance(exc, PB9ConfigError) else "invalid_document"
    _log(SERVICE, "PB9 saved-value check failed", level="WARNING", meta={"error_codes": [error]})
    raise PB9ConfigError(error) from None


def load_pb9_config_file(name: str | Path, context: PB9FileContext, *,
                         verbose: bool = False, neutralize_added: bool = False,
                         with_revision: bool = False) -> dict:
    """Load an inline v9 source through the explicit pb7_config dispatch.

    This synchronous operation never writes, locks, migrates or invokes PB7.
    File bytes and the existing document/transport budgets are separate limits.
    Approved roots must contain no symlink components; only a direct JSON file
    is supported initially. A future API owner must offload and own native work.
    With an explicit revision request, return a config/revision envelope bound
    to the same bytes read once for validation, never a reserialized digest.
    """
    error_code = None
    try:
        if (verbose is not False or neutralize_added is not False or type(with_revision) is not bool
                or with_revision and context is None):
            raise PB9ConfigError("invalid_request")
        root = _approved_root(context)
        filename = _config_name(name)
        data = _read_config_bytes(root, filename)
        document = _parse_document(data)
    except (OSError, RuntimeError, ValueError) as exc:
        error_code = exc.error_codes[0] if isinstance(exc, PB9ConfigError) else "config_file_unavailable"
    if error_code is not None:
        _log(SERVICE, "PB9 config file rejected", level="ERROR", meta={"error_codes": [error_code]})
        # A fresh exception outside the handler retains no raw file/decoder cause.
        raise PB9ConfigError(error_code) from None
    validated = validate_pb9_document(
        document, context.pb9dir, context.pb9venv, context.other_slots, timeout=context.timeout,
        **_native_options(context),
    )
    config = validated["config"]
    if validated["pbgui"] is not None:
        config["pbgui"] = validated["pbgui"]
    if with_revision:
        return {"config": config, "revision": hashlib.sha256(data).hexdigest()}
    return config


def save_pb9_config_file(document: dict, name: str | Path, context: PB9FileContext, *,
                         expected_revision: str | None = None) -> None:
    """Validate a sparse source, then create exclusively or update its exact revision.

    The approved root must already exist with owner-only 0700 permissions.
    Without a revision, creation is exclusive: existing entries are refused.
    That inserts a complete snapshot without a read/modify/write transaction;
    atomic no-replace publication provides its cross-process conflict boundary.
    Updates compare SHA-256 under an inode-bound directory lock shared by all
    PBGui updaters. Native validation is outside the lock. Noncooperating same-UID
    writers are outside that guarantee; retained source FDs and inode rechecks
    narrow external rename races, but cannot implement OS-level byte CAS.
    Directory-lock acquisition and filesystem sync can block; future API owners
    must offload and own this synchronous operation through completion/shutdown.
    ``config_file_created_io_failed`` means publication completed but its I/O
    acknowledgement failed; the target must be checked rather than rolled back.
    ``config_file_updated_io_failed`` has the same meaning for a guarded update.
    """
    error_code, published = None, False
    try:
        if expected_revision is not None and context is None:
            raise PB9ConfigError("invalid_request")
        root = _approved_root(context)
        filename = _config_name(name)
        if expected_revision is not None:
            if not isinstance(expected_revision, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_revision):
                raise PB9ConfigError("invalid_revision")
            with _root_directory(root) as precheck_fd:
                try:
                    current_data = _read_config_bytes_at(precheck_fd, filename)
                except FileNotFoundError:
                    raise PB9ConfigError("config_revision_conflict") from None
                _expect_revision(current_data, expected_revision)
    except (OSError, RuntimeError, ValueError) as exc:
        error_code = exc.error_codes[0] if isinstance(exc, PB9ConfigError) else "config_file_unavailable"
    if error_code is not None:
        _log(SERVICE, "PB9 config save rejected", level="ERROR", meta={"error_codes": [error_code]})
        raise PB9ConfigError(error_code) from None
    validated = validate_pb9_document(
        document, context.pb9dir, context.pb9venv, context.other_slots, timeout=context.timeout,
        **_native_options(context),
    )
    config = validated["config"]
    if validated["pbgui"] is not None:
        config["pbgui"] = validated["pbgui"]
    try:
        _check_cancel(context)
        encoded = bytearray()
        encoder = json.JSONEncoder(indent=4, ensure_ascii=False, allow_nan=False)
        for chunk in encoder.iterencode(config):
            chunk = chunk.encode("utf-8")
            if len(encoded) + len(chunk) + 1 > MAX_FILE_BYTES:
                error_code = "config_file_too_large"
                break
            encoded.extend(chunk)
        if error_code is None:
            encoded.extend(b"\n")
            data = bytes(encoded)
    except (ValueError, TypeError, RecursionError):
        error_code = "invalid_document"
    if error_code is not None:
        _log(SERVICE, "PB9 config save rejected", level="ERROR", meta={"error_codes": [error_code]})
        raise PB9ConfigError(error_code) from None
    try:
        with _root_directory(root) as directory_fd:
            info = os.fstat(directory_fd)
            if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != PRIVATE_DIR_MODE:
                raise PB9ConfigError("config_directory_not_private")
            if expected_revision is None:
                try:
                    os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise PB9ConfigError("config_file_exists")
                try:
                    options = {"cancel_requested": context.cancel_event.is_set} if context.cancel_event is not None else {}
                    create_private_file_at(directory_fd, filename, data, **options)
                    published = True
                except PrivateFilePublishedError:
                    published = True
                    raise
            else:
                options = ({"cancel_requested": context.cancel_event.is_set, "timeout": 2}
                           if context.cancel_event is not None else {})
                with advisory_directory_lock(directory_fd, **options):
                    _check_cancel(context)
                    try:
                        with _config_descriptor_at(directory_fd, filename) as current_fd:
                            _expect_revision(_read_descriptor_bytes(current_fd), expected_revision)
                            current = os.fstat(current_fd)
                            try:
                                replace_private_file_at(directory_fd, filename, data,
                                                        expected_identity=(current.st_dev, current.st_ino),
                                                        **({"cancel_requested": context.cancel_event.is_set}
                                                           if context.cancel_event is not None else {}))
                                published = True
                            except PrivateFilePublishedError:
                                published = True
                                raise
                    except FileNotFoundError:
                        raise PB9ConfigError("config_revision_conflict") from None
    except (OSError, RuntimeError, ValueError) as exc:
        if published:
            error_code = "config_file_updated_io_failed" if expected_revision is not None else "config_file_created_io_failed"
        elif isinstance(exc, PB9ConfigError):
            error_code = exc.error_codes[0]
        elif isinstance(exc, InterruptedError):
            error_code = "helper_cancelled"
        elif isinstance(exc, TimeoutError):
            error_code = "config_storage_busy"
        elif expected_revision is not None and isinstance(exc, PrivateFileChangedError):
            error_code = "config_revision_conflict"
        elif isinstance(exc, FileExistsError):
            error_code = "config_file_exists"
        else:
            error_code = "config_file_unavailable"
    if error_code is not None:
        message = ("PB9 config updated; I/O acknowledgement failed" if error_code == "config_file_updated_io_failed" else
                   "PB9 config created; I/O acknowledgement failed" if error_code == "config_file_created_io_failed"
                   else "PB9 config save rejected")
        _log(SERVICE, message, level="ERROR", meta={"error_codes": [error_code]})
        raise PB9ConfigError(error_code) from None
