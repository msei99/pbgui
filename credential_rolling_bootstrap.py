"""Local-only bootstrap for order-independent credential-store upgrades."""

from __future__ import annotations

import configparser
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import time
from typing import Any, Mapping

from credential_store import CredentialStore, credential_mutation_lock
from file_lock import advisory_file_lock
from pb7_api_keys import PB7ApiKeysMergeWriter
from secure_files import (
    atomic_write_private_text,
    ensure_private_directory,
    read_regular_file_nofollow,
    secure_private_file,
)


SERVICE = "CredentialRollingBootstrap"
STATE_VERSION = 1
TRADFI_PROVIDERS = ("alpaca", "polygon", "finnhub", "alphavantage", "tiingo")
TRADFI_SUFFIXES = (
    "api_key",
    "api_secret",
    "api_key_id",
    "api_secret_key",
    "key",
    "secret",
    "secret_key",
    "api_token",
    "access_token",
    "token",
)


def bootstrap_local_legacy_credentials(pbgdir: Path | str | None = None) -> dict[str, Any]:
    """Copy this node's legacy credentials into active local shadow records.

    This function never creates cluster identity or operations and never mutates a
    legacy source. Once a replicated freeze or cutoff is visible, it does not open
    a legacy credential source.
    """

    root = _absolute(pbgdir or Path(__file__).resolve().parent)
    store = CredentialStore(root / "data" / "credentials")
    state_root = store.root / "legacy_shadow"
    state_path = state_root / "state.json"
    ensure_private_directory(state_root)
    with advisory_file_lock(state_path):
        frozen, cutoff = _migration_flags(root)
        if cutoff:
            with credential_mutation_lock(store.root):
                retired = _retire_remaining_shadows(store)
            state = _read_state_or_rebuild(state_path)
            state["cutoff_seen"] = True
            _write_state(state_path, state)
            return {"status": "cutoff", "retired": retired, "credentials": _credential_count(state)}
        state = _read_state_or_rebuild(state_path)
        if frozen:
            return {"status": "frozen", "credentials": _credential_count(state)}

        with credential_mutation_lock(store.root):
            frozen, cutoff = _migration_flags(root)
            if frozen or cutoff:
                retired = _retire_remaining_shadows(store) if cutoff else 0
                return {"status": "cutoff" if cutoff else "frozen", "retired": retired, "credentials": _credential_count(state)}
            current_sources: dict[str, dict[str, Any]] = {}
            created_ids: set[str] = set()
            for source in _read_local_sources(root, store):
                source_id = str(source["source_id"])
                fingerprint = str(source["fingerprint"])
                previous = state["sources"].get(source_id)
                if (
                    isinstance(previous, dict)
                    and previous.get("fingerprint") == fingerprint
                    and set((previous.get("credentials") or {}).keys())
                    == {str(item["item_id"]) for item in source["items"]}
                    and _state_records_are_live(store, previous)
                ):
                    current_sources[source_id] = previous
                    continue
                record = (
                    dict(previous)
                    if isinstance(previous, dict) and previous.get("fingerprint") == fingerprint
                    else {"source_id": source_id, "fingerprint": fingerprint, "credentials": {}}
                )
                record["credentials"] = dict(record.get("credentials") or {})
                state["sources"][source_id] = record
                _write_state(state_path, state)
                for item in source["items"]:
                    item_id = str(item["item_id"])
                    frozen, cutoff = _migration_flags(root)
                    if frozen or cutoff:
                        retired = (
                            _retire_remaining_shadows(store)
                            if cutoff
                            else _deactivate_shadow_ids(store, created_ids)
                        )
                        return {
                            "status": "cutoff" if cutoff else "frozen",
                            "retired": retired,
                            "credentials": _credential_count(state),
                        }
                    credential_id = _find_equal_credential(store, item)
                    created_shadow = False
                    if not credential_id:
                        prefix = "cmc" if item["kind"] == "cmc_api_key" else "tradfi"
                        planned_id = str(record["credentials"].get(item_id) or "")
                        credential_id = (
                            planned_id
                            if planned_id.startswith(f"{prefix}_")
                            else f"{prefix}_{secrets.token_hex(16)}"
                        )
                        record["credentials"][item_id] = credential_id
                        _write_state(state_path, state)
                        active = True
                        if item["kind"] == "tradfi_profile":
                            provider = str(item.get("provider") or "unknown").strip().lower()
                            active = not any(
                                candidate.get("active")
                                and str(candidate.get("provider") or "").strip().lower() == provider
                                for candidate in store.list_tradfi(active_only=False)
                            )
                        store.materialize_legacy_shadow(
                            credential_id,
                            str(item["kind"]),
                            item["value"],
                            metadata={
                                "provider": str(item.get("provider") or "unknown"),
                                "label": str(item.get("label") or "Local rolling-upgrade credential"),
                                "active": active,
                            },
                        )
                        created_shadow = True
                        created_ids.add(credential_id)
                    record["credentials"][item_id] = credential_id
                    _write_state(state_path, state)
                    frozen, cutoff = _migration_flags(root)
                    if frozen or cutoff:
                        if cutoff:
                            retired = _retire_remaining_shadows(store)
                        else:
                            retired = _deactivate_shadow_ids(store, created_ids) if created_shadow else 0
                        return {
                            "status": "cutoff" if cutoff else "frozen",
                            "retired": retired,
                            "credentials": _credential_count(state),
                        }
                current_sources[source_id] = record

            stale_ids = _state_credential_ids(state) - _state_credential_ids({"sources": current_sources})
            _deactivate_shadow_ids(store, stale_ids)
            state["sources"] = current_sources
            _write_state(state_path, state)
            frozen, cutoff = _migration_flags(root)
            if frozen or cutoff:
                retired = (
                    _retire_remaining_shadows(store)
                    if cutoff
                    else _deactivate_shadow_ids(store, created_ids)
                )
                return {"status": "cutoff" if cutoff else "frozen", "retired": retired, "credentials": _credential_count(state)}
            return {"status": "ready", "credentials": _credential_count(state)}


def legacy_shadow_credential_id(
    pbgdir: Path | str,
    source_id: str,
    fingerprint: str,
    item_id: str,
) -> str:
    """Return the fixed local shadow ID for one already inventoried source item."""

    root = _absolute(pbgdir)
    state_path = root / "data" / "credentials" / "legacy_shadow" / "state.json"
    if not state_path.is_file() or state_path.is_symlink():
        return ""
    secure_private_file(state_path)
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    source = (state.get("sources") or {}).get(str(source_id)) if isinstance(state, dict) else None
    if not isinstance(source, dict) or source.get("fingerprint") != str(fingerprint):
        return ""
    return str((source.get("credentials") or {}).get(str(item_id)) or "")


def _read_local_sources(root: Path, store: CredentialStore) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    ini_path = root / "pbgui.ini"
    parser = configparser.ConfigParser()
    if ini_path.is_file() and not ini_path.is_symlink():
        with advisory_file_lock(ini_path):
            raw = read_regular_file_nofollow(ini_path, root)
        try:
            parser.read_string(raw.decode("utf-8"))
        except (UnicodeDecodeError, configparser.Error) as exc:
            raise ValueError("Unable to parse local legacy INI") from exc
        items = _ini_items(parser)
        if items:
            sources.append({
                "source_id": "ini:pbgui.ini",
                "fingerprint": hashlib.sha256(raw).hexdigest(),
                "items": items,
            })

    pb7_value = parser.get("main", "pb7dir", fallback="").strip() if parser.has_section("main") else ""
    if pb7_value:
        pb7_root = _absolute(pb7_value)
        path = pb7_root / "api-keys.json"
        if path.is_file() and not path.is_symlink():
            writer = PB7ApiKeysMergeWriter(path, store.root / "pb7_projection.json")
            raw = writer.read_bytes()
            items = _pb7_items(raw)
            if items:
                sources.append({
                    "source_id": "pb7:api-keys.json",
                    "fingerprint": hashlib.sha256(raw).hexdigest(),
                    "items": items,
                })
    return sources


def _ini_items(parser: configparser.ConfigParser) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cmc_key = parser.get("coinmarketcap", "api_key", fallback="") if parser.has_section("coinmarketcap") else ""
    if _usable_secret(cmc_key):
        items.append({
            "item_id": "ini:coinmarketcap.api_key",
            "kind": "cmc_api_key",
            "value": cmc_key,
            "label": "Rolling pbgui.ini CoinMarketCap key",
        })
    for provider in TRADFI_PROVIDERS:
        credentials: dict[str, str] = {}
        for suffix in TRADFI_SUFFIXES:
            value = (
                parser.get("tradfi_profiles", f"{provider}_{suffix}", fallback="")
                if parser.has_section("tradfi_profiles")
                else ""
            )
            if not _usable_secret(value):
                continue
            field = "api_secret" if suffix in {"api_secret", "api_secret_key", "secret", "secret_key"} else "api_key"
            credentials.setdefault(field, value)
        if credentials:
            items.append({
                "item_id": f"ini:tradfi_profiles.{provider}",
                "kind": "tradfi_profile",
                "provider": provider,
                "value": credentials,
                "label": f"Rolling INI {provider} profile",
            })
    return items


def _pb7_items(raw: bytes) -> list[dict[str, Any]]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Unable to parse local PB7 API keys") from exc
    tradfi = payload.get("tradfi") if isinstance(payload, dict) else None
    if not isinstance(tradfi, dict) or (
        "_projection_generation" in tradfi and "_source_fingerprint" in tradfi
    ):
        return []
    profiles: list[tuple[str, str, Mapping[str, Any]]] = []
    nested = tradfi.get("profiles") if isinstance(tradfi.get("profiles"), dict) else None
    if nested:
        for profile_id, profile in sorted(nested.items()):
            if isinstance(profile, Mapping):
                profiles.append((str(profile_id), str(profile.get("provider") or "unknown"), profile))
    else:
        for provider in TRADFI_PROVIDERS:
            if isinstance(tradfi.get(provider), Mapping):
                profiles.append((provider, provider, tradfi[provider]))
        profiles.append(("top-level", str(tradfi.get("provider") or "unknown"), tradfi))
    result = []
    for profile_id, provider, profile in profiles:
        credentials = _tradfi_secrets(profile)
        if credentials:
            result.append({
                "item_id": f"pb7:tradfi:{profile_id}",
                "kind": "tradfi_profile",
                "provider": provider.strip().lower() or "unknown",
                "value": credentials,
                "label": f"Rolling PB7 {provider} profile",
            })
    return result


def _find_equal_credential(store: CredentialStore, item: Mapping[str, Any]) -> str:
    if item["kind"] == "cmc_api_key":
        records = store.list_cmc(active_only=False)
        loader = store.load_cmc_key
    else:
        provider = str(item.get("provider") or "unknown").strip().lower()
        records = [
            record for record in store.list_tradfi(active_only=False)
            if str(record.get("provider") or "").strip().lower() == provider
        ]
        loader = store.load_tradfi_credentials
    for record in records:
        try:
            value = loader(str(record["id"]), int(record["generation"]))
        except (KeyError, ValueError):
            continue
        if _values_equal(value, item["value"]):
            return str(record["id"])
    return ""


def _state_records_are_live(store: CredentialStore, source: Mapping[str, Any]) -> bool:
    for credential_id in (source.get("credentials") or {}).values():
        try:
            record = store.get_cmc(credential_id) if str(credential_id).startswith("cmc_") else store.get_tradfi(credential_id)
        except (KeyError, ValueError):
            return False
        if record.get("origin") == "legacy_shadow" and not record.get("active"):
            return False
    return True


def _migration_flags(root: Path) -> tuple[bool, bool]:
    path = root / "data" / "cluster" / "desired_state.json"
    if not path.is_file() or path.is_symlink():
        return False, False
    try:
        desired = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return True, False
    migration = desired.get("credential_migration") if isinstance(desired, dict) else None
    migration = migration if isinstance(migration, dict) else {}
    return migration.get("frozen") is True, isinstance(migration.get("cutoff"), dict)


def _retire_remaining_shadows(store: CredentialStore) -> int:
    records = store.list_cmc(active_only=False) + store.list_tradfi(active_only=False)
    stale = {
        str(record["id"])
        for record in records
        if record.get("origin") == "legacy_shadow"
    }
    return _deactivate_shadow_ids(store, stale)


def _deactivate_shadow_ids(store: CredentialStore, credential_ids: set[str]) -> int:
    changed = 0
    for credential_id in sorted(credential_ids):
        try:
            record = store.get_cmc(credential_id) if credential_id.startswith("cmc_") else store.get_tradfi(credential_id)
        except (KeyError, ValueError):
            continue
        if record.get("origin") != "legacy_shadow" or not record.get("active"):
            continue
        if credential_id.startswith("cmc_"):
            store.update_cmc(credential_id, active=False)
        else:
            store.update_tradfi(credential_id, active=False)
        changed += 1
    return changed


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": STATE_VERSION, "sources": {}}
    secure_private_file(path)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Unable to read legacy shadow state") from exc
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION or not isinstance(state.get("sources"), dict):
        raise ValueError("Unsupported legacy shadow state")
    return state


def _read_state_or_rebuild(path: Path) -> dict[str, Any]:
    try:
        return _read_state(path)
    except ValueError:
        quarantine = path.with_name(f"state.quarantine-{int(time.time())}-{secrets.token_hex(4)}.json")
        try:
            os.replace(path, quarantine)
            secure_private_file(quarantine)
        except FileNotFoundError:
            pass
        return {"version": STATE_VERSION, "sources": {}}


def _write_state(path: Path, state: Mapping[str, Any]) -> None:
    atomic_write_private_text(path, json.dumps(dict(state), indent=4, sort_keys=True) + "\n")


def _state_credential_ids(state: Mapping[str, Any]) -> set[str]:
    return {
        str(credential_id)
        for source in (state.get("sources") or {}).values()
        if isinstance(source, Mapping)
        for credential_id in (source.get("credentials") or {}).values()
        if credential_id
    }


def _credential_count(state: Mapping[str, Any]) -> int:
    return len(_state_credential_ids(state))


def _tradfi_secrets(profile: Mapping[str, Any]) -> dict[str, str]:
    metadata = {
        "provider", "label", "generation", "active", "active_profile_id", "profiles",
        "enabled", "name", "_projection_generation", "_source_fingerprint",
    }
    return {
        str(key): value
        for key, value in profile.items()
        if key not in metadata and isinstance(value, str) and _usable_secret(value)
    }


def _usable_secret(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    stripped = value.strip()
    return stripped.lower() not in {"none", "null", "false", "<api_key>"} and not (
        stripped.startswith("<") and stripped.endswith(">")
    )


def _values_equal(left: Any, right: Any) -> bool:
    try:
        left_raw = json.dumps(left, sort_keys=True, separators=(",", ":")).encode("utf-8")
        right_raw = json.dumps(right, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError):
        return False
    return hmac.compare_digest(left_raw, right_raw)


def _absolute(path: Path | str) -> Path:
    return Path(os.path.abspath(Path(path).expanduser()))


__all__ = ["bootstrap_local_legacy_credentials", "legacy_shadow_credential_id"]
