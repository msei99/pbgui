"""Run PB8 config operations inside the isolated PB8 virtual environment."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path


def _load_pb8_modules(pb8_dir: Path):
    """Import PB8 modules only after its source directory is selected."""
    src_dir = pb8_dir / "src"
    if not src_dir.is_dir():
        raise RuntimeError(f"PB8 source directory not found: {src_dir}")
    sys.path.insert(0, str(src_dir))

    from config.load import load_prepared_config, prepare_config
    from config.metrics import ANALYSIS_SHARED_KEYS, CURRENCY_METRICS
    from config.schema import CONFIG_SCHEMA_VERSION, get_template_config
    from config.migrations.trailing_grid_v7 import migrate_v7_trailing_grid_file
    from config_utils import sanitize_prepared_config_for_dump
    from passivbot_version import __version__

    return {
        "load_prepared_config": load_prepared_config,
        "prepare_config": prepare_config,
        "schema_version": CONFIG_SCHEMA_VERSION,
        "get_template_config": get_template_config,
        "migrate_v7": migrate_v7_trailing_grid_file,
        "result_metrics": sorted(
            set(ANALYSIS_SHARED_KEYS)
            | set(CURRENCY_METRICS)
            | {
                f"{metric}_{currency}"
                for metric in CURRENCY_METRICS
                for currency in ("usd", "btc")
            }
        ),
        "sanitize": sanitize_prepared_config_for_dump,
        "version": __version__,
    }


def _prepare(modules: dict, config: dict, base_config_path: str = "") -> dict:
    """Return a clean canonical PB8 config suitable for persistence."""
    candidate = copy.deepcopy(config)
    pbgui_metadata = candidate.pop("pbgui", None)
    if pbgui_metadata is not None and not isinstance(pbgui_metadata, dict):
        raise TypeError("pbgui must be an object")
    prepared = modules["prepare_config"](
        candidate,
        base_config_path=base_config_path,
        verbose=False,
        target="canonical",
        runtime=None,
        raw_snapshot=candidate,
        effective_snapshot=candidate,
    )
    sanitized = modules["sanitize"](prepared)
    if pbgui_metadata is not None:
        sanitized["pbgui"] = copy.deepcopy(pbgui_metadata)
    return sanitized


def handle(payload: dict) -> dict:
    """Dispatch one JSON request and return a JSON-compatible result."""
    pb8_dir = Path(str(payload.get("pb8_dir") or "")).resolve()
    modules = _load_pb8_modules(pb8_dir)
    operation = str(payload.get("operation") or "")

    if operation == "status":
        return {
            "version": modules["version"],
            "config_schema": modules["schema_version"],
        }
    if operation == "default":
        return {"config": _prepare(modules, modules["get_template_config"]())}
    if operation == "result_metrics":
        return {"metrics": modules["result_metrics"]}
    if operation == "prepare":
        config = payload.get("config")
        if not isinstance(config, dict):
            raise TypeError("config must be an object")
        return {
            "config": _prepare(
                modules,
                config,
                str(payload.get("base_config_path") or ""),
            )
        }
    if operation == "load":
        config_path = Path(str(payload.get("config_path") or "")).resolve()
        raw_config = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(raw_config, dict):
            raise TypeError("config must be an object")
        pbgui_metadata = raw_config.get("pbgui")
        if pbgui_metadata is not None and not isinstance(pbgui_metadata, dict):
            raise TypeError("pbgui must be an object")
        prepared = modules["load_prepared_config"](
            str(config_path),
            verbose=False,
            target="canonical",
            runtime=None,
            log_info=False,
        )
        sanitized = modules["sanitize"](prepared)
        if pbgui_metadata is not None:
            sanitized["pbgui"] = copy.deepcopy(pbgui_metadata)
        return {"config": sanitized}
    if operation == "migrate_v7":
        source_path = Path(str(payload.get("source_path") or "")).resolve()
        output_path = Path(str(payload.get("output_path") or "")).resolve()
        allow_manual_review = bool(payload.get("allow_manual_review_output", False))
        migrated, report = modules["migrate_v7"](
            source_path,
            output_path,
            allow_manual_review_output=allow_manual_review,
        )
        result = {"report": report}
        if report.get("output_written") and isinstance(migrated, dict):
            result["config"] = _prepare(modules, migrated, str(output_path))
        return result
    raise ValueError(f"Unsupported operation: {operation}")


def main() -> int:
    """Read one request from stdin and write one response to stdout."""
    try:
        payload = json.load(sys.stdin)
        if not isinstance(payload, dict):
            raise TypeError("request must be an object")
        response = {"ok": True, "result": handle(payload)}
    except Exception as exc:
        response = {
            "ok": False,
            "error": type(exc).__name__,
            "detail": str(exc),
        }
    json.dump(response, sys.stdout, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
