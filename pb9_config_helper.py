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


def check_inline_config(config: dict, expected_schema: str | None = None, *, allow_inline_overrides: bool = False) -> None:
    """Reject external-data and metadata inputs before any native config import.

    Shared with the client; this helper module has standard-library imports only.
    Base paths were audited at ad9d6ede; the canonical override path was
    inspected with ec20de4 normalization/override modules. Canonical acceptance
    is not launch acceptance: runtime projection owns override key/effective
    checks. No runtime compilation or file-backed overrides are requested.
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
                    if not (allow_inline_overrides and node is config and key == "coin_overrides"
                            and isinstance(value, dict) and all(isinstance(patch, dict) for patch in value.values())):
                        raise _HelperError("external_reference_rejected")
                    for coin in value:
                        if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", coin)
                                or "://" in coin or ".." in coin.split("/")
                                or coin.lower().endswith((".json", ".hjson", ".txt"))):
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


def _editor_metadata(schema, src: Path) -> dict:
    """Describe native fields and strategies without coupling to the PB8 schema."""
    def arrays(value):
        """Rust tuples serialize as arrays; preserve every other JSON type exactly."""
        if isinstance(value, (tuple, list)):
            return [arrays(item) for item in value]
        if isinstance(value, dict):
            return {key: arrays(item) for key, item in value.items()}
        return value

    registry = importlib.import_module("config.field_registry")
    field_spec = importlib.import_module("config.field_spec")
    strategy = importlib.import_module("config.strategy_spec")
    rust = importlib.import_module("passivbot_rust")
    registry_file = _module_path(registry, src)
    strategy_file = _module_path(strategy, src)
    _module_path(field_spec, src)
    rust_file = Path(rust.__file__).resolve()
    if not rust_file.is_relative_to(Path(sys.prefix).resolve()):
        raise _HelperError("runtime_identity_mismatch")
    fields = []
    for field in registry.get_field_registry():
        item = {
            "path": list(field.path), "types": [kind.value for kind in field.types],
            "nullable": bool(field.nullable), "description": field.description,
            "unit": field.unit, "scope": field.scope, "order": field.order,
        }
        if field.default is not field_spec.NO_DEFAULT:
            default = arrays(field.default)
            _require_json_value(default)
            item["default"] = default
        if field.domain is not None:
            domain = field.domain
            item["domain"] = {key: getattr(domain, key) for key in (
                "minimum", "maximum", "exclusive_minimum", "exclusive_maximum", "finite", "string_pattern")}
            item["domain"]["choices"] = list(domain.choices)
        if field.coin_override is not None:
            item["coin_override"] = field.coin_override.value
        fields.append(item)
    kinds = list(strategy.get_supported_strategy_kinds())
    metadata = {
        "contract_version": 1, "template": schema.get_template_config(),
        "prototype": schema.get_expanded_config_prototype(), "fields": fields,
        "strategies": kinds, "strategy_specs": {kind: strategy.get_strategy_spec(kind) for kind in kinds},
        "strategy_defaults": strategy.get_all_strategy_defaults(),
    }
    metadata = arrays(metadata)
    _require_json_value(metadata)
    return {"metadata": metadata, "registry_module_file": registry_file, "strategy_module_file": strategy_file,
            "rust_module_file": str(rust_file)}


def handle(request: dict) -> dict:
    """Return templates or validate in-memory native configs, never save or migrate."""
    if not isinstance(request, dict) or not isinstance(request.get("operation"), str):
        raise _HelperError("invalid_request")
    required = {"operation", "pb9_dir", "config_schema", "package_version"}
    if isinstance(request, dict) and request.get("operation") in {"validate", "validate_live", "validate_backtest"}:
        required.add("config")
    if not isinstance(request, dict) or set(request) != required:
        raise _HelperError("invalid_request")
    if not isinstance(request["operation"], str) or request["operation"] not in {"template", "metadata", "validate", "validate_live", "validate_backtest"} or any(
        not isinstance(request[key], str) or not request[key]
        for key in ("pb9_dir", "config_schema", "package_version")
    ):
        raise _HelperError("invalid_request")
    if request["operation"] in {"validate", "validate_live", "validate_backtest"}:
        check_inline_config(request["config"], request["config_schema"],
                            allow_inline_overrides=request["operation"] in {"validate_live", "validate_backtest"})
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
        if request["operation"] == "metadata":
            try:
                result = _editor_metadata(schema, src)
            except _HelperError:
                raise
            except Exception as exc:
                raise _HelperError("native_metadata_failed") from exc
            return {**result, "config_schema": schema_version, "package_version": package_version,
                    "schema_module_file": schema_file, "version_module_file": version_file}
        if request["operation"] in {"validate", "validate_live", "validate_backtest"}:
            try:
                loader = importlib.import_module("config.load")
                load_file = _module_path(loader, src)
            except _HelperError:
                raise
            except Exception as exc:
                raise _HelperError("native_validation_failed") from exc
            try:
                options = {"live_only": True} if request["operation"] == "validate_live" else {}
                prepared = loader.prepare_config(
                    request["config"], base_config_path="", verbose=False,
                    target="canonical", runtime=None, **options,
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
        if isinstance(request, dict) and isinstance(request.get("operation"), str) and request.get("operation") in {"template", "metadata"} and len(source) > MAX_REQUEST_BYTES:
            raise _HelperError("invalid_request")
        response = {"ok": True, "result": handle(request)}
    except _HelperError as exc:
        response = {"ok": False, "error_code": str(exc)}
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        response = {"ok": False, "error_code": "invalid_request"}
    except Exception:
        # The PBGui owner logs this fixed error code, never native exception data.
        operation = request.get("operation") if isinstance(request, dict) else None
        code = "native_validation_failed" if isinstance(operation, str) and operation in {"validate", "validate_live", "validate_backtest"} else "native_template_failed"
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
