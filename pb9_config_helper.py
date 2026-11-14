"""One-request native PB9 config protocol, run only in the selected environment."""

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
MAX_VALIDATION_BYTES = 1024 * 1024
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


def check_inline_config(config: dict, expected_schema: str | None = None) -> None:
    """Reject external-data and metadata inputs before any native config import.

    Shared with the client; this helper module has standard-library imports only.
    Audited against v9 ref ad9d6ede: universe lists use resolve_files=False;
    scalar file discovery, HSL override loading and symbolmap reads are excluded.
    """
    try:
        _require_json_value(config)
    except (_HelperError, RecursionError) as exc:
        raise _HelperError("invalid_config") from exc
    if not isinstance(config, dict):
        raise _HelperError("invalid_config")
    version = config.get("config_version")
    if (not isinstance(version, str) or not re.fullmatch(r"v9\.\d+\.\d+", version)
            or expected_schema is not None and version != expected_schema):
        raise _HelperError("config_version_mismatch")

    def walk(node) -> None:
        """Check all contexts, including scenarios and dotted optimizer patches."""
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for key, value in node.items():
                parts = key.split(".")
                leaf = parts[-1]
                if any(part.startswith("_") or part == "pbgui" for part in parts):
                    raise _HelperError("invalid_config")
                if key.endswith("override_config_path"):
                    raise _HelperError("external_reference_rejected")
                if "coin_overrides" in parts and (leaf != "coin_overrides" or value != {}):
                    raise _HelperError("external_reference_rejected")
                if any(part in {"base_config_path", "organillo_csv", "fake_scenario_path"} for part in parts) and value not in (None, ""):
                    raise _HelperError("external_reference_rejected")
                if "coin_sources" in parts and (leaf != "coin_sources" or value not in (None, {})):
                    # Native hydration uses symbol_to_coin(), which reads a map.
                    raise _HelperError("external_reference_rejected")
                if any(part in {"approved_coins", "ignored_coins"} for part in parts) and leaf not in {"approved_coins", "ignored_coins"}:
                    raise _HelperError("external_reference_rejected")
                if leaf in {"approved_coins", "ignored_coins"}:
                    if value is None:
                        continue  # Native inheritance never discovers a file.
                    if leaf == "approved_coins" and isinstance(value, str) and value.strip().lower() == "all":
                        if any(ord(char) < 32 for char in value):
                            raise _HelperError("invalid_config")
                        continue  # Explicit native no-I/O sentinel.
                    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                        raise _HelperError("external_reference_rejected")
                    for item in value:
                        # Native file discovery checks the whole list item before
                        # splitting commas; check both representations.
                        for token in [item, *item.split(",")]:
                            token = token.strip()
                            if (any(ord(char) < 32 or ord(char) == 127 for char in item)
                                    or token.startswith(("./", "../", "/", "~"))
                                    or "\\" in token or "://" in token
                                    or ".." in token.split("/")
                                    # pathlib removes trailing slash/dot segments
                                    # before native suffix checks. Match without
                                    # depending on the two Python versions.
                                    or token.rstrip("/.").lower().endswith((".json", ".hjson", ".txt"))):
                                raise _HelperError("external_reference_rejected")
                walk(value)

    try:
        walk(config)
    except RecursionError as exc:
        raise _HelperError("invalid_config") from exc


def handle(request: dict) -> dict:
    """Return templates or validate in-memory native configs, never save or migrate."""
    required = {"operation", "pb9_dir", "config_schema", "package_version"}
    if isinstance(request, dict) and request.get("operation") == "validate":
        required.add("config")
    if not isinstance(request, dict) or set(request) != required:
        raise _HelperError("invalid_request")
    if not isinstance(request["operation"], str) or request["operation"] not in {"template", "validate"} or any(
        not isinstance(request[key], str) or not request[key]
        for key in ("pb9_dir", "config_schema", "package_version")
    ):
        raise _HelperError("invalid_request")
    if request["operation"] == "validate":
        check_inline_config(request["config"], request["config_schema"])
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
        if request["operation"] == "validate":
            try:
                loader = importlib.import_module("config.load")
                load_file = _module_path(loader, src)
            except _HelperError:
                raise
            except Exception as exc:
                raise _HelperError("native_validation_failed") from exc
            try:
                prepared = loader.prepare_config(
                    request["config"], base_config_path="", verbose=False,
                    target="canonical", runtime=None,
                )
                if not isinstance(prepared, dict) or prepared.get("config_version") != schema_version:
                    raise _HelperError("runtime_identity_mismatch")
            except _HelperError:
                raise
            except (ValueError, TypeError, KeyError) as exc:
                raise _HelperError("native_config_rejected") from exc
            except Exception as exc:
                raise _HelperError("native_validation_failed") from exc
            return {"valid": True, "config_schema": schema_version, "package_version": package_version,
                    "schema_module_file": schema_file, "version_module_file": version_file,
                    "load_module_file": load_file}
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
    request = None
    try:
        source = sys.stdin.buffer.read(MAX_VALIDATION_BYTES + 1)
        if len(source) > MAX_VALIDATION_BYTES:
            raise _HelperError("invalid_request")
        request = json.loads(source)
        if isinstance(request, dict) and request.get("operation") == "template" and len(source) > MAX_REQUEST_BYTES:
            raise _HelperError("invalid_request")
        response = {"ok": True, "result": handle(request)}
    except _HelperError as exc:
        response = {"ok": False, "error_code": str(exc)}
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        response = {"ok": False, "error_code": "invalid_request"}
    except Exception:
        # The PBGui owner logs this fixed error code, never native exception data.
        code = "native_validation_failed" if isinstance(request, dict) and request.get("operation") == "validate" else "native_template_failed"
        response = {"ok": False, "error_code": code}
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
