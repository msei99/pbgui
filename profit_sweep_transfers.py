"""Sealed exchange write adapters for profit-sweep internal transfers.

The module accepts only fixed internal account routes.  Request descriptors are
safe to persist as JSON and never contain credentials or signatures; credentials
are attached server-side by :class:`Exchange.Exchange` immediately before one
allowlisted call.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any

import requests

from Exchange import Exchange


SERVICE = "ProfitSweepTransfers"
_HYPERLIQUID_EXCHANGE_URL = "https://api.hyperliquid.xyz/exchange"
_HYPERLIQUID_USD_QUANTUM = Decimal("0.000001")


class TransferRequestError(ValueError):
    """Report an input or descriptor outside the fixed transfer contract."""


_ROUTES = {
    "hyperliquid": {
        "perp_to_spot": "hyperliquid_agent",
        "perps_to_spot": "hyperliquid_agent",
        "spot_to_perp": "hyperliquid_agent",
        "vault_to_main": "hyperliquid_vault",
        "vault_to_main_perps": "hyperliquid_vault",
        "main_perps_to_vault": "hyperliquid_vault",
        "main_perp_to_spot": "hyperliquid_vault_spot",
        "main_perps_to_spot": "hyperliquid_vault_spot",
        "vault_main_to_spot": "hyperliquid_vault_spot",
    },
    "bybit": {
        "unified_to_fund": "bybit_v5",
        "unified_to_funding": "bybit_v5",
        "fund_to_unified": "bybit_v5",
    },
    "binance": {
        "umfuture_to_funding": "binance_um",
        "futures_to_funding": "binance_um",
        "funding_to_umfuture": "binance_um",
    },
    "bitget": {
        "usdt_futures_to_p2p": "bitget_classic",
        "futures_to_p2p": "bitget_classic",
        "p2p_to_usdt_futures": "bitget_classic",
        "usdt_futures_to_spot": "bitget_classic",
        "spot_to_usdt_futures": "bitget_classic",
        "uta_to_spot": "bitget_uta",
        "spot_to_uta": "bitget_uta",
    },
}

_REVERSE_ROUTES = {
    "perp_to_spot": "spot_to_perp",
    "vault_to_main_perps": "main_perps_to_vault",
    "unified_to_fund": "fund_to_unified",
    "umfuture_to_funding": "funding_to_umfuture",
    "usdt_futures_to_p2p": "p2p_to_usdt_futures",
    "usdt_futures_to_spot": "spot_to_usdt_futures",
    "uta_to_spot": "spot_to_uta",
}

_CANONICAL_ROUTES = {
    "hyperliquid": ["perp_to_spot", "vault_to_main_perps", "main_perps_to_spot"],
    "bybit": ["unified_to_fund"],
    "binance": ["umfuture_to_funding"],
    "bitget_classic": ["usdt_futures_to_spot"],
    "bitget_uta": ["uta_to_spot"],
}
BINANCE_TRANSFER_PERMISSION_REASON = (
    "Binance API key does not permit Universal Transfer. Enable Internal/Universal Transfer "
    "for this API key in Binance API Management; Withdrawals are not required."
)
BYBIT_TRANSFER_PERMISSION_REASON = (
    "Bybit API key does not permit internal account transfers. Enable Account Transfer permission "
    "for this key; withdrawals are not required."
)
BITGET_TRANSFER_PERMISSION_REASON = (
    "Bitget API key does not permit internal account transfers. Enable Transfer permission "
    "for this key; Withdraw permission is not required."
)

_METHODS = {
    "hyperliquid_agent": "privatePostExchange",
    "hyperliquid_vault": "privatePostExchange",
    "hyperliquid_vault_spot": "privatePostExchange",
    "bybit_v5": "privatePostV5AssetTransferInterTransfer",
    "binance_um": "sapiPostAssetTransfer",
    "bitget_classic": "privateSpotPostV2SpotWalletTransfer",
    "bitget_uta": "privateUtaPostV3AccountTransfer",
}

_ASSETS = {
    "hyperliquid_agent": frozenset({"USDC"}),
    "hyperliquid_vault": frozenset({"USDC"}),
    "hyperliquid_vault_spot": frozenset({"USDC"}),
    "bybit_v5": frozenset({"USDT", "USDC"}),
    "binance_um": frozenset({"USDT", "USDC"}),
    "bitget_classic": frozenset({"USDT"}),
    "bitget_uta": frozenset({"USDT"}),
}

_TOP_LEVEL_KEYS = frozenset({
    "schema_version",
    "adapter",
    "exchange",
    "operation_id",
    "route",
    "amount",
    "asset",
    "idempotency",
    "source",
    "destination",
    "prepared_at_ms",
    "request",
    "fingerprint",
})
_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_HISTORY_WINDOW_MS = 5 * 60 * 1_000


def _now_ms() -> int:
    """Return current Unix time in milliseconds."""

    return int(time.time() * 1_000)


def _exchange_name(user: Any) -> str:
    """Return a normalized exchange name from a server-side user object."""

    return str(getattr(user, "exchange", "") or "").strip().lower()


def _mode(snapshot: dict[str, Any]) -> str:
    """Resolve an account mode from fixed snapshot locations."""

    candidates = [
        snapshot.get("account_mode"),
        snapshot.get("mode"),
        snapshot.get("account", {}).get("mode") if isinstance(snapshot.get("account"), dict) else None,
        snapshot.get("capability", {}).get("account_mode")
        if isinstance(snapshot.get("capability"), dict)
        else None,
    ]
    if snapshot.get("uta") is True:
        candidates.insert(0, "uta")
    for candidate in candidates:
        value = str(candidate or "").strip().lower().replace("-", "_")
        if value:
            return value
    return ""


def _bitget_mode(snapshot: dict[str, Any]) -> str:
    """Map a persisted Bitget account-mode snapshot to one adapter."""

    value = _mode(snapshot)
    if value in {"classic", "standard", "legacy"}:
        return "classic"
    if value in {"uta", "unified", "unified_trading", "unified_trading_account"}:
        return "uta"
    return ""


def transfer_capability(user: Any, snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return fail-closed write capability for one exchange snapshot."""

    exchange = _exchange_name(user)
    result: dict[str, Any] = {
        "exchange": exchange or "unknown",
        "supported": False,
        "writes_available": False,
        "external_withdrawal": False,
        "adapter": None,
        "routes": [],
    }
    if not isinstance(snapshot, dict):
        result["reason"] = "A server-side exchange snapshot is required"
        return result
    if snapshot.get("complete") is False:
        result["reason"] = "The exchange snapshot is incomplete"
        return result
    snapshot_exchange = str(snapshot.get("exchange") or "").strip().lower()
    if snapshot_exchange and snapshot_exchange != exchange:
        result["reason"] = "The exchange snapshot belongs to a different exchange"
        return result
    if exchange == "hyperliquid":
        if not getattr(user, "private_key", None):
            result["reason"] = "A configured Hyperliquid API agent is required"
            return result
        account_kind = str(snapshot.get("account_kind") or "normal").lower()
        if account_kind == "vault":
            leader = snapshot.get("leader") if isinstance(snapshot.get("leader"), dict) else {}
            agent = leader.get("agent") if isinstance(leader.get("agent"), dict) else {}
            if agent and not agent.get("relationship_valid"):
                result["reason"] = "The configured leader agent is not active"
                return result
            routes = ["vault_to_main_perps"]
            if str(leader.get("account_mode") or "standard_manual") == "standard_manual":
                routes.append("main_perps_to_spot")
            result.update({
                "supported": True,
                "writes_available": True,
                "adapter": "hyperliquid_vault",
                "routes": routes,
            })
            return result
        if _mode(snapshot) not in {"standard", "manual", "standard_manual", "disabled"}:
            result["reason"] = "Hyperliquid requires Standard/Manual account mode"
            return result
        result.update({
            "supported": True,
            "writes_available": True,
            "adapter": "hyperliquid_agent",
            "routes": _CANONICAL_ROUTES["hyperliquid"][:1],
        })
        return result
    if exchange == "bybit":
        if not getattr(user, "key", None) or not getattr(user, "secret", None):
            result["reason"] = "Bybit API credentials are incomplete"
            return result
        permissions = snapshot.get("transfer_permissions")
        if isinstance(permissions, dict) and permissions.get("internal_transfer") is False:
            result["supported"] = True
            result["reason"] = BYBIT_TRANSFER_PERMISSION_REASON
            return result
        result.update({
            "supported": True,
            "writes_available": True,
            "adapter": "bybit_v5",
            "routes": _CANONICAL_ROUTES["bybit"],
        })
        return result
    if exchange == "binance":
        if not getattr(user, "key", None) or not getattr(user, "secret", None):
            result["reason"] = "Binance API credentials are incomplete"
            return result
        permissions = snapshot.get("transfer_permissions")
        if isinstance(permissions, dict) and permissions.get("universal_transfer") is False:
            result["supported"] = True
            result["reason"] = BINANCE_TRANSFER_PERMISSION_REASON
            return result
        result.update({
            "supported": True,
            "writes_available": True,
            "adapter": "binance_um",
            "routes": _CANONICAL_ROUTES["binance"],
        })
        return result
    if exchange == "bitget":
        if (
            not getattr(user, "key", None)
            or not getattr(user, "secret", None)
            or not getattr(user, "passphrase", None)
        ):
            result["reason"] = "Bitget API credentials are incomplete"
            return result
        account_mode = _bitget_mode(snapshot)
        if not account_mode:
            result["reason"] = "Bitget account mode was not resolved by the snapshot"
            return result
        adapter = f"bitget_{account_mode}"
        result.update({
            "supported": True,
            "writes_available": True,
            "adapter": adapter,
            "routes": _CANONICAL_ROUTES[adapter],
        })
        return result
    result["reason"] = "Exchange has no profit-sweep write adapter"
    return result


def reverse_transfer_route(user: Any, snapshot: dict[str, Any], forward_route: Any) -> str:
    """Return the fixed reverse of a currently supported forward route."""

    capability = transfer_capability(user, snapshot)
    if not capability.get("supported") or not capability.get("writes_available"):
        raise TransferRequestError(str(capability.get("reason") or "transfer is not supported"))
    if not isinstance(forward_route, str) or forward_route not in capability.get("routes", []):
        raise TransferRequestError("forward route is not available for this account snapshot")
    reverse = _REVERSE_ROUTES.get(forward_route)
    if reverse is None or _ROUTES.get(_exchange_name(user), {}).get(reverse) != capability.get("adapter"):
        raise TransferRequestError("reverse route is not available for this account snapshot")
    return reverse


def _decimal(value: Any) -> Decimal:
    """Parse one positive finite monetary value without binary floating point."""

    if isinstance(value, bool):
        raise TransferRequestError("amount must be a positive decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TransferRequestError("amount must be a positive decimal") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise TransferRequestError("amount must be a positive decimal")
    return parsed


def _precision(snapshot: dict[str, Any], asset: str) -> Any:
    """Read optional amount precision from fixed snapshot fields."""

    asset_data = snapshot.get("asset") if isinstance(snapshot.get("asset"), dict) else {}
    precision_data = snapshot.get("precision") if isinstance(snapshot.get("precision"), dict) else {}
    for value in (
        asset_data.get("amount_precision"),
        asset_data.get("size_decimals"),
        asset_data.get("szDecimals"),
        precision_data.get(asset),
        snapshot.get("amount_precision"),
    ):
        if value is not None and value != "":
            return value
    return None


def _amount_string(value: Any, precision: Any = None) -> str:
    """Return an exact decimal string, rounding down only with precision."""

    amount = _decimal(value)
    if precision is not None:
        if isinstance(precision, bool):
            raise TransferRequestError("amount precision is invalid")
        try:
            if isinstance(precision, int) or str(precision).isdigit():
                places = int(precision)
                if places < 0 or places > 18:
                    raise TransferRequestError("amount precision is invalid")
                step = Decimal(1).scaleb(-places)
            else:
                step = Decimal(str(precision))
                if not step.is_finite() or step <= 0:
                    raise TransferRequestError("amount precision is invalid")
            units = (amount / step).to_integral_value(rounding=ROUND_DOWN)
            amount = units * step
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise TransferRequestError("amount precision is invalid") from exc
        if amount <= 0:
            raise TransferRequestError("amount rounds to zero at exchange precision")
    return format(amount, "f")


def _identifier(value: Any, field: str) -> str:
    """Validate one bounded persisted operation identifier."""

    text = str(value or "")
    if not _ID_PATTERN.fullmatch(text):
        raise TransferRequestError(f"{field} is invalid")
    return text


def _address(value: Any, field: str) -> str:
    """Validate and normalize one 20-byte hexadecimal address."""

    text = str(value or "").lower()
    if len(text) != 42 or not text.startswith("0x"):
        raise TransferRequestError(f"{field} is invalid")
    try:
        int(text[2:], 16)
    except ValueError as exc:
        raise TransferRequestError(f"{field} is invalid") from exc
    return text


def _token(snapshot: dict[str, Any]) -> str:
    """Return canonical Hyperliquid USDC wire token from the snapshot."""

    asset_data = snapshot.get("asset") if isinstance(snapshot.get("asset"), dict) else {}
    if str(asset_data.get("symbol") or "").upper() != "USDC":
        raise TransferRequestError("snapshot does not contain canonical USDC metadata")
    token_id = str(asset_data.get("token_id") or asset_data.get("tokenId") or "")
    if not token_id or any(character.isspace() for character in token_id) or len(token_id) > 128:
        raise TransferRequestError("snapshot USDC token identifier is invalid")
    return f"USDC:{token_id}"


def _prepared_time(snapshot: dict[str, Any]) -> int:
    """Use a persisted snapshot time when available, otherwise current time."""

    value = snapshot.get("collected_at_ms")
    if isinstance(value, bool):
        return _now_ms()
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return _now_ms()
    return parsed if parsed > 0 else _now_ms()


def _descriptor_fingerprint(descriptor: dict[str, Any]) -> str:
    """Hash the non-secret descriptor fields to detect accidental mutation."""

    payload = {key: value for key, value in descriptor.items() if key != "fingerprint"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def prepare_transfer(
    user: Any,
    *,
    operation_id: Any,
    amount: Any,
    asset: Any,
    route: Any,
    snapshot: dict[str, Any],
    nonce: Any = None,
) -> dict[str, Any]:
    """Build one JSON-safe descriptor for a fixed internal transfer route."""

    if not isinstance(snapshot, dict):
        raise TransferRequestError("snapshot must be an object")
    exchange = _exchange_name(user)
    if not isinstance(route, str) or route not in _ROUTES.get(exchange, {}):
        raise TransferRequestError("route is not allowlisted for this exchange")
    adapter = _ROUTES[exchange][route]
    capability = transfer_capability(user, snapshot)
    if not capability["supported"] or not capability["writes_available"]:
        raise TransferRequestError(str(capability.get("reason") or "transfer is not supported"))
    if adapter == "hyperliquid_vault_spot" and route not in capability.get("routes", []):
        raise TransferRequestError("Vault leader Main Perps-to-Spot requires Standard/Manual account mode")
    if exchange == "bitget" and capability["adapter"] != adapter:
        raise TransferRequestError("route does not match the snapshotted Bitget account mode")

    operation = _identifier(operation_id, "operation_id")
    asset_name = str(asset or "").strip().upper()
    snapshot_asset = snapshot.get("asset") if isinstance(snapshot.get("asset"), dict) else {}
    snapshot_symbol = str(snapshot_asset.get("symbol") or "").strip().upper()
    if not snapshot_symbol or asset_name != snapshot_symbol:
        raise TransferRequestError("asset does not match the snapshotted settlement asset")
    if asset_name not in _ASSETS[adapter]:
        raise TransferRequestError("asset is not allowlisted for this adapter")
    amount_precision = 6 if adapter == "hyperliquid_vault" else _precision(snapshot, asset_name)
    amount_text = _amount_string(amount, amount_precision)
    prepared_at_ms = _prepared_time(snapshot)

    request: dict[str, Any]
    source: str
    destination: str
    idempotency: dict[str, Any]
    if adapter.startswith("hyperliquid_"):
        if isinstance(nonce, bool):
            raise TransferRequestError("nonce must be a positive integer")
        try:
            nonce_value = _now_ms() if nonce is None else int(nonce)
        except (TypeError, ValueError) as exc:
            raise TransferRequestError("nonce must be a positive integer") from exc
        if nonce_value <= 0:
            raise TransferRequestError("nonce must be a positive integer")
        if adapter == "hyperliquid_agent":
            wallet = _address(getattr(user, "wallet_address", None), "wallet_address")
            reverse = route == "spot_to_perp"
            source = "spot" if reverse else "default_perps"
            destination = "default_perps" if reverse else wallet
            action = {
                "type": "agentSendAsset",
                "destination": wallet,
                "sourceDex": "spot" if reverse else "",
                "destinationDex": "" if reverse else "spot",
                "token": _token(snapshot),
                "amount": amount_text,
                "fromSubAccount": "",
                "nonce": nonce_value,
            }
        else:
            vault = snapshot.get("vault") if isinstance(snapshot.get("vault"), dict) else {}
            leader = snapshot.get("leader") if isinstance(snapshot.get("leader"), dict) else {}
            vault_address = _address(vault.get("address") or getattr(user, "wallet_address", None), "vault address")
            leader_address = _address(leader.get("address"), "leader address")
            if adapter == "hyperliquid_vault":
                is_deposit = route == "main_perps_to_vault"
                source = leader_address if is_deposit else vault_address
                destination = vault_address if is_deposit else leader_address
                action = {
                    "type": "vaultTransfer",
                    "vaultAddress": vault_address,
                    "isDeposit": is_deposit,
                    "usd": int(_decimal(amount_text) * Decimal("1000000")),
                }
            else:
                source = "leader_default_perps"
                destination = leader_address
                action = {
                    "type": "agentSendAsset",
                    "destination": leader_address,
                    "sourceDex": "",
                    "destinationDex": "spot",
                    "token": _token(snapshot),
                    "amount": amount_text,
                    "fromSubAccount": "",
                    "nonce": nonce_value,
                }
        request = {"method": _METHODS[adapter], "action": action, "nonce": nonce_value}
        idempotency = {"kind": "nonce", "value": str(nonce_value), "replay_safe": True}
    elif adapter == "bybit_v5":
        try:
            transfer_id = str(uuid.UUID(operation))
        except (ValueError, AttributeError) as exc:
            raise TransferRequestError("Bybit operation_id must be a UUID") from exc
        if transfer_id != operation.lower():
            raise TransferRequestError("Bybit operation_id must be a canonical UUID")
        source, destination = (
            ("FUND", "UNIFIED") if route == "fund_to_unified" else ("UNIFIED", "FUND")
        )
        request = {
            "method": _METHODS[adapter],
            "params": {
                "transferId": transfer_id,
                "coin": asset_name,
                "amount": amount_text,
                "fromAccountType": source,
                "toAccountType": destination,
            },
        }
        idempotency = {"kind": "transferId", "value": transfer_id, "replay_safe": True}
    elif adapter == "binance_um":
        source, destination = (
            ("FUNDING", "UMFUTURE") if route == "funding_to_umfuture" else ("UMFUTURE", "FUNDING")
        )
        request = {
            "method": _METHODS[adapter],
            "params": {"type": f"{source}_{destination}", "asset": asset_name, "amount": amount_text},
        }
        idempotency = {"kind": "none", "value": None, "replay_safe": False}
    elif adapter == "bitget_classic":
        source, destination = (
            ("p2p", "usdt_futures") if route == "p2p_to_usdt_futures"
            else ("usdt_futures", "p2p") if route in {"usdt_futures_to_p2p", "futures_to_p2p"}
            else ("spot", "usdt_futures") if route == "spot_to_usdt_futures"
            else ("usdt_futures", "spot")
        )
        request = {
            "method": _METHODS[adapter],
            "params": {
                "fromType": source,
                "toType": destination,
                "amount": amount_text,
                "coin": asset_name,
                "clientOid": operation,
            },
        }
        idempotency = {"kind": "clientOid", "value": operation, "replay_safe": True}
    else:
        source, destination = ("spot", "uta") if route == "spot_to_uta" else ("uta", "spot")
        request = {
            "method": _METHODS[adapter],
            "params": {
                "fromType": source,
                "toType": destination,
                "coin": asset_name,
                "amount": amount_text,
                "allowBorrow": "no",
            },
        }
        idempotency = {"kind": "none", "value": None, "replay_safe": False}

    descriptor = {
        "schema_version": 1,
        "adapter": adapter,
        "exchange": exchange,
        "operation_id": operation,
        "route": route,
        "amount": amount_text,
        "asset": asset_name,
        "idempotency": idempotency,
        "source": source,
        "destination": destination,
        "prepared_at_ms": prepared_at_ms,
        "request": request,
    }
    descriptor["fingerprint"] = _descriptor_fingerprint(descriptor)
    return descriptor


def _validate_descriptor(user: Any, descriptor: Any) -> dict[str, Any]:
    """Validate a persisted descriptor against every fixed adapter field."""

    if not isinstance(descriptor, dict) or set(descriptor) != _TOP_LEVEL_KEYS:
        raise TransferRequestError("descriptor shape is invalid")
    if descriptor.get("schema_version") != 1:
        raise TransferRequestError("descriptor schema is unsupported")
    if descriptor.get("fingerprint") != _descriptor_fingerprint(descriptor):
        raise TransferRequestError("descriptor integrity check failed")
    exchange = _exchange_name(user)
    adapter = descriptor.get("adapter")
    route = descriptor.get("route")
    if exchange != descriptor.get("exchange") or adapter != _ROUTES.get(exchange, {}).get(route):
        raise TransferRequestError("descriptor does not match the exchange route")
    if _identifier(descriptor.get("operation_id"), "operation_id") != descriptor.get("operation_id"):
        raise TransferRequestError("descriptor operation_id is invalid")
    if not isinstance(descriptor.get("source"), str) or not isinstance(descriptor.get("destination"), str):
        raise TransferRequestError("descriptor accounts are invalid")
    request = descriptor.get("request")
    if not isinstance(request, dict) or request.get("method") != _METHODS.get(adapter):
        raise TransferRequestError("descriptor method is not allowlisted")
    if descriptor.get("asset") not in _ASSETS.get(adapter, frozenset()):
        raise TransferRequestError("descriptor asset is not allowlisted")
    if _amount_string(descriptor.get("amount")) != descriptor.get("amount"):
        raise TransferRequestError("descriptor amount is invalid")
    if not isinstance(descriptor.get("idempotency"), dict) or set(descriptor["idempotency"]) != {
        "kind",
        "value",
        "replay_safe",
    }:
        raise TransferRequestError("descriptor idempotency shape is invalid")
    try:
        if isinstance(descriptor.get("prepared_at_ms"), bool) or int(descriptor["prepared_at_ms"]) <= 0:
            raise TransferRequestError("descriptor timestamp is invalid")
    except (TypeError, ValueError) as exc:
        raise TransferRequestError("descriptor timestamp is invalid") from exc
    if adapter.startswith("hyperliquid_"):
        if set(request) != {"method", "action", "nonce"} or not isinstance(request.get("action"), dict):
            raise TransferRequestError("Hyperliquid descriptor shape is invalid")
        expected_type = "vaultTransfer" if adapter == "hyperliquid_vault" else "agentSendAsset"
        action = request["action"]
        expected_keys = (
            {"type", "vaultAddress", "isDeposit", "usd"}
            if expected_type == "vaultTransfer"
            else {"type", "destination", "sourceDex", "destinationDex", "token", "amount", "fromSubAccount", "nonce"}
        )
        if set(action) != expected_keys or action.get("type") != expected_type:
            raise TransferRequestError("Hyperliquid action is not allowlisted")
        if (
            isinstance(request.get("nonce"), bool)
            or not isinstance(request.get("nonce"), int)
            or request["nonce"] <= 0
            or action.get("nonce", request["nonce"]) != request["nonce"]
            or descriptor["idempotency"] != {
                "kind": "nonce",
                "value": str(request["nonce"]),
                "replay_safe": True,
            }
        ):
            raise TransferRequestError("Hyperliquid nonce mismatch")
        if expected_type == "vaultTransfer":
            vault_address = str(getattr(user, "wallet_address", "") or "").lower()
            reverse_vault = route == "main_perps_to_vault"
            expected_accounts = (
                (_address(descriptor.get("source"), "vault leader"), vault_address)
                if reverse_vault
                else (vault_address, _address(descriptor.get("destination"), "vault leader"))
            )
            if (
                action.get("isDeposit") is not reverse_vault
                or action.get("vaultAddress") != vault_address
                or (descriptor.get("source"), descriptor.get("destination")) != expected_accounts
                or not isinstance(action.get("usd"), int)
                or action.get("usd") <= 0
                or action.get("usd") != int(Decimal(descriptor["amount"]) * Decimal("1000000"))
            ):
                raise TransferRequestError("vault transfer account, direction, or amount is invalid")
        reverse_agent = adapter == "hyperliquid_agent" and route == "spot_to_perp"
        if expected_type == "agentSendAsset" and (
            action.get("sourceDex") != ("spot" if reverse_agent else "")
            or action.get("destinationDex") != ("" if reverse_agent else "spot")
            or action.get("fromSubAccount") != ""
            or action.get("amount") != descriptor.get("amount")
            or not re.fullmatch(r"USDC:\S{1,128}", str(action.get("token") or ""))
        ):
            raise TransferRequestError("Hyperliquid agent route is invalid")
        if adapter == "hyperliquid_agent":
            wallet = str(getattr(user, "wallet_address", "") or "").lower()
            expected_accounts = ("spot", "default_perps") if reverse_agent else ("default_perps", wallet)
            if (
                action.get("destination") != wallet
                or (descriptor.get("source"), descriptor.get("destination")) != expected_accounts
            ):
                raise TransferRequestError("Hyperliquid route is not bound to the user's own wallet")
        elif adapter == "hyperliquid_vault_spot" and (
            descriptor.get("source") != "leader_default_perps"
            or action.get("destination") != descriptor.get("destination")
            or _address(descriptor.get("destination"), "Vault forwarding destination")
            != descriptor.get("destination")
        ):
            raise TransferRequestError("Hyperliquid Vault forwarding route is invalid")
    else:
        if set(request) != {"method", "params"} or not isinstance(request.get("params"), dict):
            raise TransferRequestError("exchange descriptor shape is invalid")
        expected_param_keys = {
            "bybit_v5": {"transferId", "coin", "amount", "fromAccountType", "toAccountType"},
            "binance_um": {"type", "asset", "amount"},
            "bitget_classic": {"fromType", "toType", "amount", "coin", "clientOid"},
            "bitget_uta": {"fromType", "toType", "coin", "amount", "allowBorrow"},
        }[adapter]
        if set(request["params"]) != expected_param_keys:
            raise TransferRequestError("exchange request parameters are invalid")
        params = request["params"]
        expected_idempotency = {
            "bybit_v5": {"kind": "transferId", "value": descriptor["operation_id"].lower(), "replay_safe": True},
            "binance_um": {"kind": "none", "value": None, "replay_safe": False},
            "bitget_classic": {"kind": "clientOid", "value": descriptor["operation_id"], "replay_safe": True},
            "bitget_uta": {"kind": "none", "value": None, "replay_safe": False},
        }[adapter]
        if descriptor["idempotency"] != expected_idempotency:
            raise TransferRequestError("descriptor idempotency value is invalid")
        if adapter == "bybit_v5":
            try:
                canonical_id = str(uuid.UUID(descriptor["operation_id"]))
            except (ValueError, AttributeError) as exc:
                raise TransferRequestError("Bybit operation_id must be a UUID") from exc
            if canonical_id != descriptor["operation_id"]:
                raise TransferRequestError("Bybit operation_id must be a canonical UUID")
        bybit_accounts = (
            ("FUND", "UNIFIED") if route == "fund_to_unified" else ("UNIFIED", "FUND")
        )
        if adapter == "bybit_v5" and params != {
            "transferId": descriptor["idempotency"]["value"],
            "coin": descriptor["asset"],
            "amount": descriptor["amount"],
            "fromAccountType": bybit_accounts[0],
            "toAccountType": bybit_accounts[1],
        }:
            raise TransferRequestError("Bybit transfer parameters are invalid")
        binance_accounts = (
            ("FUNDING", "UMFUTURE")
            if route == "funding_to_umfuture"
            else ("UMFUTURE", "FUNDING")
        )
        if adapter == "binance_um" and params != {
            "type": f"{binance_accounts[0]}_{binance_accounts[1]}",
            "asset": descriptor["asset"],
            "amount": descriptor["amount"],
        }:
            raise TransferRequestError("Binance transfer parameters are invalid")
        classic_accounts = (
            ("p2p", "usdt_futures") if route == "p2p_to_usdt_futures"
            else ("usdt_futures", "p2p") if route in {"usdt_futures_to_p2p", "futures_to_p2p"}
            else ("spot", "usdt_futures") if route == "spot_to_usdt_futures"
            else ("usdt_futures", "spot")
        )
        if adapter == "bitget_classic" and params != {
            "fromType": classic_accounts[0],
            "toType": classic_accounts[1],
            "amount": descriptor["amount"],
            "coin": descriptor["asset"],
            "clientOid": descriptor["idempotency"]["value"],
        }:
            raise TransferRequestError("Bitget Classic transfer parameters are invalid")
        uta_accounts = ("spot", "uta") if route == "spot_to_uta" else ("uta", "spot")
        if adapter == "bitget_uta" and params != {
            "fromType": uta_accounts[0],
            "toType": uta_accounts[1],
            "coin": descriptor["asset"],
            "amount": descriptor["amount"],
            "allowBorrow": "no",
        }:
            raise TransferRequestError("Bitget UTA borrowing is forbidden")
        if (descriptor.get("source"), descriptor.get("destination")) != {
            "bybit_v5": bybit_accounts,
            "binance_um": binance_accounts,
            "bitget_classic": classic_accounts,
            "bitget_uta": uta_accounts,
        }[adapter]:
            raise TransferRequestError("descriptor account direction is invalid")
    return descriptor


def _owned_client(user: Any, exchange: str) -> tuple[Exchange, Any]:
    """Create one credentialed client owned by the current operation."""

    owner = Exchange(exchange, user)
    try:
        owner.connect()
        if owner.instance is None:
            raise RuntimeError("exchange client unavailable")
    except Exception:
        owner.close()
        raise
    return owner, owner.instance


def _is_timeout(exc: Exception) -> bool:
    """Classify timeout exceptions without exposing their messages."""

    return isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower()


def _error_result(
    exc: Exception,
    *,
    adapter: str | None = None,
    reconciliation: bool = False,
) -> dict[str, Any]:
    """Return a bounded secret-free failure result."""

    status = "unknown" if reconciliation or _is_timeout(exc) else "failed"
    error = {
        "category": "timeout" if _is_timeout(exc) else "exchange_error",
        "type": type(exc).__name__[:64],
    }
    if adapter == "binance_um" and type(exc).__name__ == "AuthenticationError":
        error["reason"] = BINANCE_TRANSFER_PERMISSION_REASON
    return {
        "status": status,
        "error": error,
    }


def _response_status(adapter: str, response: Any) -> tuple[str, str | None]:
    """Reduce an exchange response to bounded acceptance fields."""

    if not isinstance(response, dict):
        return "unknown", None
    if adapter.startswith("hyperliquid_"):
        status = str(response.get("status") or "").lower()
        return ("submitted" if status == "ok" else "failed" if status in {"err", "error"} else "unknown", None)
    if adapter == "bybit_v5":
        code = response.get("retCode")
        result = response.get("result") if isinstance(response.get("result"), dict) else {}
        transfer_id = str(result.get("transferId") or "") or None
        return (
            "submitted" if str(code) == "0" and transfer_id else "failed" if code is not None and str(code) != "0" else "unknown",
            transfer_id,
        )
    if adapter == "binance_um":
        transfer_id = response.get("tranId")
        return ("submitted" if transfer_id is not None else "unknown", str(transfer_id) if transfer_id is not None else None)
    code = str(response.get("code") or "")
    data = response.get("data") if isinstance(response.get("data"), dict) else {}
    transfer_id = data.get("transferId") or data.get("clientOid")
    return (
        "submitted" if code in {"00000", "0"} and transfer_id else "failed" if code and code not in {"00000", "0"} else "unknown",
        str(transfer_id) if transfer_id else None,
    )


def _post_hyperliquid_exchange(payload: dict[str, Any], *, timeout_s: float = 30.0) -> dict[str, Any]:
    """Post one sealed signed action without allowing CCXT to expose its request body on errors."""

    response = requests.post(_HYPERLIQUID_EXCHANGE_URL, json=payload, timeout=timeout_s)
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict):
        raise TransferRequestError("Hyperliquid returned an unexpected exchange response")
    return result


def _canonical_hyperliquid_action(descriptor: dict[str, Any]) -> dict[str, Any]:
    """Restore schema-defined key order after canonical JSON persistence sorted object keys."""

    action = descriptor["request"]["action"]
    action_type = action.get("type")
    if action_type == "agentSendAsset":
        return {
            "type": "agentSendAsset",
            "destination": action["destination"],
            "sourceDex": action["sourceDex"],
            "destinationDex": action["destinationDex"],
            "token": action["token"],
            "amount": action["amount"],
            "fromSubAccount": action["fromSubAccount"],
            "nonce": action["nonce"],
        }
    if action_type == "vaultTransfer":
        return {
            "type": "vaultTransfer",
            "vaultAddress": action["vaultAddress"],
            "isDeposit": action["isDeposit"],
            "usd": action["usd"],
        }
    raise TransferRequestError("Hyperliquid action type is not supported")


def _typed_data_for_descriptor(client: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
    """Build the exact Hyperliquid phantom-agent typed data for one descriptor."""

    action = _canonical_hyperliquid_action(descriptor)
    nonce = descriptor["request"]["nonce"]
    connection_id = client.action_hash(action, None, nonce)
    return {
        "domain": {
            "name": "Exchange",
            "version": "1",
            "chainId": 1337,
            "verifyingContract": "0x0000000000000000000000000000000000000000",
        },
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
        },
        "primaryType": "Agent",
        "message": {
            "source": "a",
            "connectionId": "0x" + bytes(connection_id).hex(),
        },
    }


def browser_signing_request(user: Any, descriptor: dict[str, Any], expected_signer: str) -> dict[str, Any]:
    """Return secret-free typed data for a Leader-wallet Vault signature."""

    validated = _validate_descriptor(user, descriptor)
    if validated["adapter"] != "hyperliquid_vault":
        raise TransferRequestError("browser wallet signing is available only for Hyperliquid Vault transfers")
    signer = _address(expected_signer, "expected signer")
    owner: Exchange | None = None
    try:
        owner, client = _owned_client(user, validated["exchange"])
        return {"account": signer, "typed_data": _typed_data_for_descriptor(client, validated)}
    finally:
        if owner is not None:
            owner.close()


def verify_browser_signature(
    user: Any,
    descriptor: dict[str, Any],
    signature: Any,
    expected_signer: str,
) -> dict[str, Any]:
    """Validate one wallet signature shape for Hyperliquid's authoritative verification."""

    validated = _validate_descriptor(user, descriptor)
    if validated["adapter"] != "hyperliquid_vault":
        raise TransferRequestError("browser wallet signature does not match a Hyperliquid Vault transfer")
    if not isinstance(signature, str) or re.fullmatch(r"0x[0-9a-fA-F]{130}", signature) is None:
        raise TransferRequestError("browser wallet signature must be a 65-byte hexadecimal value")
    raw = bytes.fromhex(signature[2:])
    reported_recovery_id = raw[64] - 27 if raw[64] >= 27 else raw[64]
    if reported_recovery_id not in {0, 1}:
        raise TransferRequestError("browser wallet signature recovery id is invalid")
    _address(expected_signer, "expected signer")
    return {
        "r": "0x" + raw[:32].hex(),
        "s": "0x" + raw[32:64].hex(),
        "v": reported_recovery_id + 27,
    }


def submit_browser_signed_transfer(
    user: Any,
    descriptor: dict[str, Any],
    signature: dict[str, Any],
) -> dict[str, Any]:
    """Submit one preverified Leader-wallet signature without persisting it."""

    validated = _validate_descriptor(user, descriptor)
    if validated["adapter"] != "hyperliquid_vault":
        raise TransferRequestError("browser wallet submission does not match a Hyperliquid Vault transfer")
    submitted_at_ms = _now_ms()
    submission_started = False
    try:
        payload = {
            "action": _canonical_hyperliquid_action(validated),
            "nonce": validated["request"]["nonce"],
            "signature": dict(signature),
        }
        submission_started = True
        response = _post_hyperliquid_exchange(payload)
        status, exchange_id = _response_status(validated["adapter"], response)
        result: dict[str, Any] = {
            "status": status,
            "operation_id": validated["operation_id"],
            "exchange_id": exchange_id,
            "submitted_at_ms": submitted_at_ms,
            "retry_safe": bool(validated["idempotency"]["replay_safe"] and status == "submitted"),
        }
        if status == "failed":
            result["error"] = {
                "category": "exchange_rejected",
                "type": "HyperliquidError",
                "reason": _hyperliquid_rejection_reason(response, "vaultTransfer"),
            }
        return result
    except Exception as exc:
        result = _error_result(exc, reconciliation=submission_started)
        result.update({
            "operation_id": validated["operation_id"],
            "submitted_at_ms": submitted_at_ms,
            "retry_safe": False,
        })
        return result


def _hyperliquid_rejection_reason(response: dict[str, Any], action_type: str) -> str:
    """Reduce one provider rejection to bounded, secret-free user guidance."""

    action_name = action_type if action_type in {"agentSendAsset", "vaultTransfer"} else "signed action"
    message = str(response.get("response") or "").strip()
    lower = message.lower()
    if "user or api wallet" in lower and "does not exist" in lower:
        return (
            f"Hyperliquid did not recognize the signing API wallet for {action_name}. Regenerate or reauthorize "
            "the Leader API wallet in Hyperliquid, then update its private key in PBGui."
        )
    if "minimum" in lower or "too small" in lower:
        return "Hyperliquid rejected the test amount because it is below the provider minimum."
    if "insufficient vault equity for withdrawal" in lower:
        return (
            "Hyperliquid reports insufficient personal Vault equity for this withdrawal. The Leader must retain "
            "more than 100 USDC and more than 5% of Vault shares. Refresh the snapshot and reduce the amount."
        )
    if "insufficient" in lower:
        return f"Hyperliquid rejected {action_name} because the available balance or margin is insufficient."
    safe = re.sub(r"0x[0-9a-fA-F]{8,}", "[address]", message)
    safe = re.sub(r"[^\x20-\x7E]", " ", safe).strip()[:240]
    return f"Hyperliquid rejected {action_name}: {safe}" if safe else f"Hyperliquid rejected {action_name}."


def submit_transfer(user: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
    """Submit one allowlisted request exactly once and close its owned client."""

    validated = _validate_descriptor(user, descriptor)
    adapter = validated["adapter"]
    submitted_at_ms = _now_ms()
    owner: Exchange | None = None
    submission_started = False
    try:
        owner, client = _owned_client(user, validated["exchange"])
        request = validated["request"]
        if adapter.startswith("hyperliquid_"):
            action = _canonical_hyperliquid_action(validated)
            nonce = request["nonce"]
            signature = client.sign_l1_action(action, nonce)
            submission_started = True
            response = _post_hyperliquid_exchange({
                "action": action,
                "nonce": nonce,
                "signature": signature,
            })
        elif adapter == "bybit_v5":
            submission_started = True
            response = client.privatePostV5AssetTransferInterTransfer(request["params"])
        elif adapter == "binance_um":
            submission_started = True
            response = client.sapiPostAssetTransfer(request["params"])
        elif adapter == "bitget_classic":
            submission_started = True
            response = client.privateSpotPostV2SpotWalletTransfer(request["params"])
        else:
            submission_started = True
            response = client.privateUtaPostV3AccountTransfer(request["params"])
        status, exchange_id = _response_status(adapter, response)
        result = {
            "status": status,
            "operation_id": validated["operation_id"],
            "exchange_id": exchange_id,
            "submitted_at_ms": submitted_at_ms,
            "retry_safe": bool(validated["idempotency"]["replay_safe"] and status == "submitted"),
        }
        if adapter.startswith("hyperliquid_") and status == "failed":
            result["error"] = {
                "category": "exchange_rejected",
                "type": "HyperliquidError",
                "reason": _hyperliquid_rejection_reason(response, str(action.get("type") or "")),
            }
        return result
    except Exception as exc:
        result = _error_result(exc, adapter=adapter, reconciliation=submission_started)
        result.update({
            "operation_id": validated["operation_id"],
            "submitted_at_ms": submitted_at_ms,
            "retry_safe": False,
        })
        return result
    finally:
        if owner is not None:
            owner.close()


def _items(response: Any) -> list[dict[str, Any]]:
    """Extract records only through known exchange response containers."""

    value = response
    for _depth in range(4):
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if not isinstance(value, dict):
            return []
        next_value = None
        for key in ("result", "data", "list", "rows"):
            if isinstance(value.get(key), (dict, list)):
                next_value = value[key]
                break
        if next_value is None:
            return []
        value = next_value
    return []


def _field(record: dict[str, Any], *names: str) -> Any:
    """Read a field from a record or its fixed nested detail objects."""

    for container in (record, record.get("delta"), record.get("details")):
        if not isinstance(container, dict):
            continue
        for name in names:
            if name in container:
                return container[name]
    return None


def _same_amount(value: Any, expected: str) -> bool:
    """Compare exchange monetary values exactly as decimals."""

    try:
        return Decimal(str(value)) == Decimal(expected)
    except (InvalidOperation, TypeError, ValueError):
        return False


def _same_integer(value: Any, expected: int) -> bool:
    """Compare one provider identifier as an exact integer without raising."""

    if isinstance(value, bool):
        return False
    try:
        return int(value) == expected
    except (TypeError, ValueError):
        return False


def _hyperliquid_vault_match(
    record: dict[str, Any],
    descriptor: dict[str, Any],
    *,
    deposit: bool,
) -> bool:
    """Match one Vault ledger event by direction, Vault, and requested amount."""

    expected_type = "vaultDeposit" if deposit else "vaultWithdraw"
    expected_vault = descriptor["request"]["action"]["vaultAddress"]
    vault = _field(record, "vaultAddress", "vault")
    requested = _field(record, "usdc") if deposit else _field(record, "requestedUsd", "grossUsd")
    amount_matches = _same_amount(requested, descriptor["amount"])
    if not deposit and requested is not None and not amount_matches:
        try:
            amount_matches = Decimal(str(requested)) == Decimal(descriptor["amount"]) - _HYPERLIQUID_USD_QUANTUM
        except (InvalidOperation, TypeError, ValueError):
            amount_matches = False
    return (
        str(_field(record, "type") or "") == expected_type
        and vault is not None
        and str(vault).lower() == expected_vault
        and requested is not None
        and amount_matches
    )


def _hyperliquid_agent_match(record: dict[str, Any], descriptor: dict[str, Any]) -> bool:
    """Match one agent send only when every provider identifier is present and exact."""

    action = descriptor["request"]["action"]
    event_type = str(_field(record, "type") or "")
    destination = _field(record, "destination")
    source_dex = _field(record, "sourceDex")
    destination_dex = _field(record, "destinationDex")
    token = _field(record, "token")
    amount = _field(record, "usdc", "amount")
    nonce = _field(record, "nonce")
    expected_token = str(action["token"])
    return (
        event_type in {"agentSendAsset", "spotTransfer", "send"}
        and destination is not None
        and str(destination).lower() == str(action["destination"]).lower()
        and source_dex is not None
        and str(source_dex) == action["sourceDex"]
        and destination_dex is not None
        and str(destination_dex) == action["destinationDex"]
        and token is not None
        and (
            str(token) == expected_token
            or str(token).upper() == expected_token.split(":", 1)[0].upper()
        )
        and amount is not None
        and _same_amount(amount, descriptor["amount"])
        and nonce is not None
        and _same_integer(nonce, descriptor["request"]["nonce"])
    )


def _record_time(record: dict[str, Any]) -> int | None:
    """Read one integer millisecond timestamp from a record."""

    value = _field(record, "timestamp", "time", "cTime", "createdTime", "createdAt")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _time_bounds(descriptor: dict[str, Any], submission: dict[str, Any]) -> tuple[int, int]:
    """Build a conservative bounded reconciliation interval."""

    try:
        center = int(submission.get("submitted_at_ms") or descriptor["prepared_at_ms"])
    except (TypeError, ValueError):
        center = int(descriptor["prepared_at_ms"])
    return max(0, center - _HISTORY_WINDOW_MS), center + _HISTORY_WINDOW_MS


def _normalized_status(record: dict[str, Any]) -> str:
    """Map known exchange record statuses to the public result vocabulary."""

    value = str(_field(record, "status", "state", "transferStatus") or "").strip().lower()
    if value in {"success", "successful", "confirmed", "completed", "complete", "ok", "2"}:
        return "confirmed"
    if value in {"failed", "fail", "rejected", "cancelled", "canceled", "3"}:
        return "failed"
    if value in {"pending", "processing", "created", "wait", "1", ""}:
        return "pending"
    return "unknown"


def _matched_result(records: list[dict[str, Any]], *, amount_field: str | None = None) -> dict[str, Any]:
    """Return a conservative result for zero, one, or ambiguous records."""

    if not records:
        return {"status": "pending", "matched_records": 0}
    if len(records) != 1:
        return {"status": "unknown", "matched_records": len(records), "reason": "ambiguous_history"}
    record = records[0]
    result: dict[str, Any] = {"status": _normalized_status(record), "matched_records": 1}
    transfer_id = _field(record, "transferId", "tranId", "clientOid", "id")
    if transfer_id is not None:
        result["exchange_id"] = str(transfer_id)[:128]
    if amount_field:
        received = _field(record, amount_field)
        if received is not None:
            try:
                result["received_amount"] = format(Decimal(str(received)), "f")
            except (InvalidOperation, TypeError, ValueError):
                result["status"] = "unknown"
    return result


def reconcile_transfer(
    user: Any,
    descriptor: dict[str, Any],
    submission: dict[str, Any],
) -> dict[str, Any]:
    """Reconcile one submitted transfer through a fixed exchange history query."""

    validated = _validate_descriptor(user, descriptor)
    if not isinstance(submission, dict):
        raise TransferRequestError("submission must be an object")
    if submission.get("status") == "failed":
        result: dict[str, Any] = {"status": "failed", "matched_records": 0}
        if isinstance(submission.get("error"), dict):
            result["error"] = dict(submission["error"])
        return result
    adapter = validated["adapter"]
    start_ms, end_ms = _time_bounds(validated, submission)
    owner: Exchange | None = None
    try:
        owner, client = _owned_client(user, validated["exchange"])
        if adapter.startswith("hyperliquid_"):
            ledger_user = (
                validated["source"]
                if adapter == "hyperliquid_vault"
                else validated["destination"]
                if adapter == "hyperliquid_vault_spot"
                else str(getattr(user, "wallet_address", "") or "").lower()
            )
            response = client.publicPostInfo({
                "type": "userNonFundingLedgerUpdates",
                "user": ledger_user,
                "startTime": start_ms,
                "endTime": end_ms,
            })
            vault_deposit = adapter == "hyperliquid_vault" and validated["route"] == "main_perps_to_vault"
            matches = [
                record
                for record in _items(response)
                if (
                    _hyperliquid_vault_match(record, validated, deposit=vault_deposit)
                    if adapter == "hyperliquid_vault"
                    else _hyperliquid_agent_match(record, validated)
                )
                and start_ms <= (_record_time(record) or -1) <= end_ms
            ]
            result = _matched_result(
                matches,
                amount_field="usdc" if vault_deposit else "netWithdrawnUsd" if adapter == "hyperliquid_vault" else "amount",
            )
            if len(matches) == 1 and result["status"] == "pending":
                result["status"] = "confirmed"
            if adapter == "hyperliquid_vault" and not vault_deposit and len(matches) == 1:
                closing_cost = _field(matches[0], "closingCost")
                if closing_cost is not None and not _same_amount(closing_cost, "0"):
                    result.update({"status": "unknown", "reason": "vault_closing_cost"})
        elif adapter == "bybit_v5":
            transfer_id = validated["idempotency"]["value"]
            response = client.privateGetV5AssetTransferQueryInterTransferList({"transferId": transfer_id})
            matches = [
                record
                for record in _items(response)
                if str(_field(record, "transferId") or "") == transfer_id
                and (_field(record, "coin") is None or str(_field(record, "coin")).upper() == validated["asset"])
                and (_field(record, "amount") is None or _same_amount(_field(record, "amount"), validated["amount"]))
                and (
                    _field(record, "fromAccountType") is None
                    or str(_field(record, "fromAccountType")).upper() == validated["source"]
                )
                and (
                    _field(record, "toAccountType") is None
                    or str(_field(record, "toAccountType")).upper() == validated["destination"]
                )
            ]
            result = _matched_result(matches, amount_field="amount")
        elif adapter == "binance_um":
            exchange_id = str(submission.get("exchange_id") or "")
            response = client.sapiGetAssetTransfer({
                "type": validated["request"]["params"]["type"],
                "startTime": start_ms,
                "endTime": end_ms,
            })
            matches = [
                record
                for record in _items(response)
                if str(_field(record, "type") or "") == validated["request"]["params"]["type"]
                and str(_field(record, "asset") or "").upper() == validated["asset"]
                and _same_amount(_field(record, "amount"), validated["amount"])
                and (
                    not exchange_id
                    or str(_field(record, "tranId", "transferId", "id") or "") == exchange_id
                )
                and start_ms <= (_record_time(record) or -1) <= end_ms
            ]
            result = _matched_result(matches, amount_field="amount")
        elif adapter == "bitget_classic":
            client_oid = validated["idempotency"]["value"]
            response = client.privateSpotGetV2SpotAccountTransferRecords({
                "clientOid": client_oid,
                "coin": validated["asset"],
                "fromType": validated["source"],
            })
            matches = [
                record
                for record in _items(response)
                if str(_field(record, "clientOid") or "") == client_oid
                and (_field(record, "coin") is None or str(_field(record, "coin")).upper() == validated["asset"])
                and (_field(record, "amount", "size") is None or _same_amount(_field(record, "amount", "size"), validated["amount"]))
                and (_field(record, "fromType") is None or str(_field(record, "fromType")).lower() == validated["source"])
                and (_field(record, "toType") is None or str(_field(record, "toType")).lower() == validated["destination"])
            ]
            result = _matched_result(matches, amount_field="size")
        else:
            exchange_id = str(submission.get("exchange_id") or "")
            response = client.privateUtaGetV3AccountFinancialRecords({
                "coin": validated["asset"],
                "startTime": start_ms,
                "endTime": end_ms,
            })
            matches = []
            for record in _items(response):
                record_type = str(_field(record, "type", "businessType", "category") or "").lower()
                from_type = _field(record, "fromType", "fromAccountType")
                to_type = _field(record, "toType", "toAccountType")
                if (
                    "transfer" in record_type
                    and str(_field(record, "coin", "asset") or "").upper() == validated["asset"]
                    and _same_amount(_field(record, "amount", "size"), validated["amount"])
                    and (
                        not exchange_id
                        or str(_field(record, "transferId", "id", "tranId") or "") == exchange_id
                    )
                    and start_ms <= (_record_time(record) or -1) <= end_ms
                    and (from_type is None or str(from_type).lower() == validated["source"])
                    and (to_type is None or str(to_type).lower() == validated["destination"])
                ):
                    matches.append(record)
            result = _matched_result(matches, amount_field="amount")
        result["operation_id"] = validated["operation_id"]
        return result
    except Exception as exc:
        if (
            adapter in {"bitget_classic", "bitget_uta"}
            and "permission" in type(exc).__name__.lower()
            and submission.get("status") == "submitted"
            and submission.get("exchange_id")
        ):
            return {
                "status": "confirmed",
                "operation_id": validated["operation_id"],
                "exchange_id": str(submission["exchange_id"]),
                "matched_records": 0,
                "received_amount": validated["amount"],
                "reason": "exchange_acknowledged_history_permission_unavailable",
            }
        result = _error_result(exc, reconciliation=True)
        result.update({"operation_id": validated["operation_id"], "matched_records": 0})
        return result
    finally:
        if owner is not None:
            owner.close()
