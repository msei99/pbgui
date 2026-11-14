"""One-request native PB9 template protocol, run only in the selected environment."""

from __future__ import annotations

from contextlib import redirect_stdout
import importlib
import json
import math
from pathlib import Path
import re
import sys


SERVICE = "PB9ConfigHelper"
MAX_REQUEST_BYTES = 16384
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


class _HelperError(RuntimeError):
    """Carry a fixed protocol error code without native exception contents."""


def _require_json_value(value) -> None:
    """Reject silent JSON coercion of tuples, non-string keys and nonfinite values."""
    if value is None or type(value) in (str, bool, int):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _require_json_value(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _require_json_value(item)
        return
    raise _HelperError("invalid_template")


def _module_path(module, src: Path) -> str:
    """Require imported source modules to belong to the selected checkout."""
    path = Path(module.__file__).resolve()
    if not path.is_relative_to(src):
        raise _HelperError("runtime_identity_mismatch")
    return str(path)


def handle(request: dict) -> dict:
    """Return authored native defaults only; never prepare, migrate or save configs."""
    required = {"operation", "pb9_dir", "config_schema", "package_version"}
    if not isinstance(request, dict) or set(request) != required:
        raise _HelperError("invalid_request")
    if request["operation"] != "template" or any(
        not isinstance(request[key], str) or not request[key]
        for key in ("pb9_dir", "config_schema", "package_version")
    ):
        raise _HelperError("invalid_request")
    root = Path(request["pb9_dir"])
    if not root.is_absolute():
        raise _HelperError("invalid_request")
    root = root.resolve()
    src = (root / "src").resolve()
    if not src.is_relative_to(root) or not src.is_dir():
        raise _HelperError("invalid_request")
    sys.path.insert(0, str(src))
    with redirect_stdout(sys.stderr):
        schema = importlib.import_module("config.schema")
        version = importlib.import_module("passivbot_version")
        schema_file = _module_path(schema, src)
        version_file = _module_path(version, src)
        schema_version = schema.CONFIG_SCHEMA_VERSION
        package_version = version.__version__
        if (not isinstance(schema_version, str) or not re.fullmatch(r"v9\.\d+\.\d+", schema_version)
                or schema_version != request["config_schema"]
                or package_version != request["package_version"]):
            raise _HelperError("runtime_identity_mismatch")
        config = schema.get_template_config()
    if not isinstance(config, dict) or config.get("config_version") != schema_version:
        raise _HelperError("runtime_identity_mismatch")
    try:
        _require_json_value(config)
    except RecursionError as exc:
        raise _HelperError("invalid_template") from exc
    return {"config": config, "config_schema": schema_version, "package_version": package_version,
            "schema_module_file": schema_file, "version_module_file": version_file}


def main() -> int:
    """Emit one bounded JSON response; native diagnostics stay off protocol stdout."""
    try:
        source = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(source) > MAX_REQUEST_BYTES:
            raise _HelperError("invalid_request")
        response = {"ok": True, "result": handle(json.loads(source))}
    except _HelperError as exc:
        response = {"ok": False, "error_code": str(exc)}
    except (UnicodeError, json.JSONDecodeError):
        response = {"ok": False, "error_code": "invalid_request"}
    except Exception:
        # The PBGui owner logs this fixed error code, never native exception data.
        response = {"ok": False, "error_code": "native_template_failed"}
    try:
        encoded = json.dumps(response, allow_nan=False).encode("utf-8")
        if len(encoded) > MAX_RESPONSE_BYTES:
            encoded = b'{"ok":false,"error_code":"helper_output_limit"}'
    except (TypeError, ValueError, RecursionError):
        encoded = b'{"ok":false,"error_code":"invalid_template"}'
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
