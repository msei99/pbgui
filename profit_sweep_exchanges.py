"""Read-only exchange snapshots for the profit-sweep dry-run phase.

Read-only collection deliberately exposes no exchange actions, signer, nonce
allocation, or generic request forwarding. Hyperliquid reads use a fixed allowlist.
"""

from __future__ import annotations

import hashlib
import json
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from hyperliquid_api import hyperliquid_info_post


SERVICE = "ProfitSweepExchanges"

# Imported lazily because lightweight read-only tooling does not always install
# CCXT. Tests replace this with an offline owner factory.
Exchange: Any = None

HYPERLIQUID_READ_TYPES = frozenset({
    "userAbstraction",
    "clearinghouseState",
    "spotClearinghouseState",
    "userFillsByTime",
    "userFunding",
    "userNonFundingLedgerUpdates",
    "openOrders",
    "vaultDetails",
    "userVaultEquities",
    "extraAgents",
    "userRole",
    "spotMeta",
})
CCXT_READ_METHODS = frozenset({
    "privateGetV5AssetTransferQueryTransferCoinList",
    "privateGetV5AssetTransferQueryAccountCoinBalance",
    "privateGetV5AssetTransferQueryAccountCoinsBalance",
    "privateGetV5AccountWalletBalance",
    "privateGetV5AccountTransactionLog",
    "fapiPrivateV3GetBalance",
    "fapiPrivateGetIncome",
    # Binance implements this balance query as POST, but it is a documented
    # read-only endpoint and is the only allowlisted POST in this module.
    "sapiPostAssetGetFundingAsset",
    "sapiGetAccountApiRestrictions",
    "privateUtaGetV3AccountSettings",
    "privateUtaGetV3AccountAssets",
    "privateUtaGetV3AccountFundingAssets",
    "privateUtaGetV3AccountMaxTransferable",
    "privateUtaGetV3AccountFinancialRecords",
    "privateMixGetV2MixAccountAccounts",
    "privateMixGetV2MixAccountBill",
    "privateSpotGetV2AccountFundingAssets",
    "privateSpotGetV2SpotAccountAssets",
})
_TIMED_READ_TYPES = frozenset({
    "userFillsByTime",
    "userFunding",
    "userNonFundingLedgerUpdates",
})
_USER_READ_TYPES = frozenset({
    "userAbstraction",
    "clearinghouseState",
    "spotClearinghouseState",
    "openOrders",
    "userVaultEquities",
    "extraAgents",
    "userRole",
})
_HISTORY_PAGE_SIZES = {
    "userFillsByTime": 2_000,
    "userFunding": 500,
    "userNonFundingLedgerUpdates": 500,
}
_MAX_HISTORY_PAGES = 20
_MAX_FRESHNESS_MS = 5 * 60 * 1_000
_SUPPORTED_ASSETS = {
    "hyperliquid": frozenset({"USDC"}),
    "bybit": frozenset({"USDT", "USDC"}),
    "binance": frozenset({"USDT", "USDC"}),
    "bitget": frozenset({"USDT"}),
}
_DEFAULT_ASSETS = {"hyperliquid": "USDC", "bybit": "USDT", "binance": "USDT", "bitget": "USDT"}
_MONEY_KEYS = frozenset({
    "amount",
    "basis",
    "closingCost",
    "commission",
    "fee",
    "nativeTokenFee",
    "netWithdrawnUsd",
    "pnl",
    "usdc",
    "vaultEquity",
})
_SAFE_LEDGER_KEYS = frozenset({"coin", "isDeposit", "type"})


class ReadOnlyRequestError(ValueError):
    """Report a request that is outside the fixed read-only contract."""


def _now_ms() -> int:
    """Return current Unix time in milliseconds."""

    return int(time.time() * 1_000)


def _is_address(value: Any) -> bool:
    """Return whether a value is a canonical 20-byte hexadecimal address."""

    text = str(value or "")
    if len(text) != 42 or not text.startswith("0x"):
        return False
    try:
        int(text[2:], 16)
    except ValueError:
        return False
    return True


def _validated_address(value: Any, field: str) -> str:
    """Return a normalized address or reject the malformed boundary value."""

    if not _is_address(value):
        raise ReadOnlyRequestError(f"{field} must be a 20-byte hexadecimal address")
    return str(value).lower()


def hyperliquid_readonly_info(
    request_type: str,
    *,
    user: str | None = None,
    vault_address: str | None = None,
    start_ms: int | None = None,
    end_ms: int | None = None,
    timeout_s: float = 30.0,
) -> Any:
    """Execute one explicitly shaped Hyperliquid info request.

    The caller chooses only an allowlisted read type and its documented scalar
    arguments. There is intentionally no payload or arbitrary keyword channel.
    """

    if not isinstance(request_type, str) or request_type not in HYPERLIQUID_READ_TYPES:
        raise ReadOnlyRequestError(f"Hyperliquid info type {request_type!r} is not read-only allowlisted")
    if not isinstance(timeout_s, (int, float)) or isinstance(timeout_s, bool) or timeout_s <= 0:
        raise ReadOnlyRequestError("timeout_s must be positive")

    payload: dict[str, Any] = {"type": request_type}
    if request_type == "spotMeta":
        if any(value is not None for value in (user, vault_address, start_ms, end_ms)):
            raise ReadOnlyRequestError("spotMeta does not accept account or time arguments")
    elif request_type == "vaultDetails":
        if vault_address is None:
            raise ReadOnlyRequestError("vaultDetails requires vault_address")
        payload["vaultAddress"] = _validated_address(vault_address, "vault_address")
        if user is not None:
            payload["user"] = _validated_address(user, "user")
        if start_ms is not None or end_ms is not None:
            raise ReadOnlyRequestError("vaultDetails does not accept time arguments")
    elif request_type in _USER_READ_TYPES or request_type in _TIMED_READ_TYPES:
        if user is None:
            raise ReadOnlyRequestError(f"{request_type} requires user")
        payload["user"] = _validated_address(user, "user")
        if vault_address is not None:
            raise ReadOnlyRequestError(f"{request_type} does not accept vault_address")
        if request_type in _TIMED_READ_TYPES:
            if start_ms is None or end_ms is None:
                raise ReadOnlyRequestError(f"{request_type} requires start_ms and end_ms")
            if isinstance(start_ms, bool) or isinstance(end_ms, bool):
                raise ReadOnlyRequestError("history bounds must be integer milliseconds")
            try:
                start_value = int(start_ms)
                end_value = int(end_ms)
            except (TypeError, ValueError) as exc:
                raise ReadOnlyRequestError("history bounds must be integer milliseconds") from exc
            if start_value < 0 or end_value < start_value:
                raise ReadOnlyRequestError("history bounds are invalid")
            payload["startTime"] = start_value
            payload["endTime"] = end_value
        elif start_ms is not None or end_ms is not None:
            raise ReadOnlyRequestError(f"{request_type} does not accept time arguments")
    else:
        raise ReadOnlyRequestError(f"No fixed request builder exists for {request_type}")

    return hyperliquid_info_post(payload, timeout_s=float(timeout_s))


def readonly_capability(user: Any) -> dict[str, Any]:
    """Return the read-only capability for an exchange user."""

    exchange = str(getattr(user, "exchange", "") or "").strip().lower()
    if exchange == "hyperliquid":
        return {
            "exchange": exchange,
            "supported": True,
            "read_only": True,
            "writes_available": False,
            "account_types": ["standard_manual", "legacy_vault"],
        }
    account_types = {
        "bybit": ["unified"],
        "binance": ["usd_m_futures"],
        "bitget": ["classic", "uta"],
    }
    if exchange in account_types:
        return {
            "exchange": exchange,
            "supported": True,
            "read_only": True,
            "writes_available": False,
            "account_types": account_types[exchange],
        }
    return {
        "exchange": exchange or "unknown",
        "supported": False,
        "read_only": True,
        "writes_available": False,
        "reason": "Exchange has no read-only Profit Sweep snapshot adapter",
    }


def _add_error(errors: list[dict[str, Any]], code: str, source: str, message: str) -> None:
    """Append one secret-free fatal completeness error."""

    errors.append({"code": code, "source": source, "message": message, "fatal": True})


def _read(
    errors: list[dict[str, Any]],
    request_type: str,
    *,
    expected_type: type | tuple[type, ...],
    timeout_s: float,
    user: str | None = None,
    vault_address: str | None = None,
) -> Any:
    """Run one fixed read and turn transport or shape failures into fatal errors."""

    try:
        result = hyperliquid_readonly_info(
            request_type,
            user=user,
            vault_address=vault_address,
            timeout_s=timeout_s,
        )
    except Exception as exc:
        _add_error(errors, "read_failed", request_type, f"Read failed ({type(exc).__name__})")
        return None
    if not isinstance(result, expected_type):
        _add_error(errors, "invalid_response", request_type, "Exchange returned an unexpected response shape")
        return None
    return result


def _item_time(item: Any) -> int | None:
    """Extract an event timestamp as integer milliseconds."""

    if not isinstance(item, dict):
        return None
    try:
        return int(item.get("time"))
    except (TypeError, ValueError):
        return None


def _canonical_hash(value: Any) -> str:
    """Return a deterministic short hash for a JSON-like value."""

    encoded = json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def _fetch_history(
    errors: list[dict[str, Any]],
    request_type: str,
    user: str,
    since_ms: int,
    until_ms: int,
    timeout_s: float,
) -> tuple[list[dict[str, Any]], bool]:
    """Fetch a bounded time series and fail closed if pagination is ambiguous."""

    cursor = since_ms
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _page in range(_MAX_HISTORY_PAGES):
        try:
            result = hyperliquid_readonly_info(
                request_type,
                user=user,
                start_ms=cursor,
                end_ms=until_ms,
                timeout_s=timeout_s,
            )
        except Exception as exc:
            _add_error(errors, "read_failed", request_type, f"History read failed ({type(exc).__name__})")
            return items, False
        if not isinstance(result, list) or any(not isinstance(item, dict) for item in result):
            _add_error(errors, "invalid_response", request_type, "Exchange returned an unexpected history shape")
            return items, False
        for item in result:
            identity = _canonical_hash(item)
            if identity not in seen:
                seen.add(identity)
                items.append(item)
        page_size = _HISTORY_PAGE_SIZES[request_type]
        if len(result) < page_size:
            items.sort(key=lambda item: (_item_time(item) or 0, _canonical_hash(item)))
            return items, True
        timestamps = [_item_time(item) for item in result]
        valid_timestamps = [timestamp for timestamp in timestamps if timestamp is not None]
        if not valid_timestamps:
            _add_error(errors, "pagination_ambiguous", request_type, "Full history page has no usable cursor")
            return items, False
        last_time = max(valid_timestamps)
        if last_time < cursor or last_time >= until_ms:
            _add_error(errors, "pagination_ambiguous", request_type, "Full history page cannot advance safely")
            return items, False
        if sum(1 for timestamp in valid_timestamps if timestamp == last_time) > 1:
            _add_error(
                errors,
                "pagination_ambiguous",
                request_type,
                "Full history page ends with multiple events at the cursor timestamp",
            )
            return items, False
        cursor = last_time + 1
    _add_error(errors, "history_page_limit", request_type, "History exceeded the conservative page limit")
    return items, False


def _decimal(value: Any) -> Decimal:
    """Parse one finite exchange monetary value as Decimal."""

    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid decimal") from exc
    if not parsed.is_finite():
        raise ValueError("non-finite decimal")
    return parsed


def _money(value: Any) -> str | None:
    """Normalize an optional monetary value without binary floating point."""

    if value is None or value == "":
        return None
    try:
        return format(_decimal(value), "f")
    except ValueError:
        return None


def _required_money(
    errors: list[dict[str, Any]],
    value: Any,
    source: str,
    field: str,
) -> str | None:
    """Normalize a required monetary field and record malformed data."""

    normalized = _money(value)
    if normalized is None:
        _add_error(errors, "invalid_monetary_value", source, f"Missing or invalid {field}")
    return normalized


def _sum_money(values: list[str | None]) -> str:
    """Sum normalized monetary strings as an exact Decimal string."""

    total = sum((_decimal(value) for value in values if value is not None), Decimal("0"))
    return format(total, "f")


def _event_id(kind: str, source_user: str, item: dict[str, Any]) -> str:
    """Build a stable composite event identifier without exposing source addresses."""

    timestamp = _item_time(item) or 0
    tx_hash = str(item.get("hash") or "no-hash")
    delta = item.get("delta") if isinstance(item.get("delta"), dict) else item
    event_type = str(delta.get("type") or kind)
    fingerprint = _canonical_hash({"user": source_user.lower(), "payload": item})
    return f"hyperliquid:{kind}:{timestamp}:{tx_hash}:{event_type}:{fingerprint}"


def _normalize_fill(item: dict[str, Any], source_user: str, errors: list[dict[str, Any]]) -> dict[str, Any]:
    """Normalize one fill into a JSON-safe realized-PnL event."""

    closed_pnl = _required_money(errors, item.get("closedPnl"), "userFillsByTime", "closedPnl")
    fee = _required_money(errors, item.get("fee"), "userFillsByTime", "fee")
    net = None
    if closed_pnl is not None and fee is not None:
        net = format(_decimal(closed_pnl) - _decimal(fee), "f")
    return {
        "id": _event_id("fill", source_user, item),
        "kind": "fill",
        "time_ms": _item_time(item),
        "hash": str(item.get("hash") or ""),
        "trade_id": str(item.get("tid") or ""),
        "order_id": str(item.get("oid") or ""),
        "coin": str(item.get("coin") or ""),
        "side": str(item.get("side") or ""),
        "price": _money(item.get("px")),
        "size": _money(item.get("sz")),
        "closed_pnl": closed_pnl,
        "fee": fee,
        "net_closed_pnl": net,
    }


def _normalize_funding(item: dict[str, Any], source_user: str, errors: list[dict[str, Any]]) -> dict[str, Any]:
    """Normalize one funding delta into a JSON-safe event."""

    delta = item.get("delta") if isinstance(item.get("delta"), dict) else item
    amount = _required_money(errors, delta.get("usdc"), "userFunding", "usdc")
    return {
        "id": _event_id("funding", source_user, item),
        "kind": "funding",
        "time_ms": _item_time(item),
        "hash": str(item.get("hash") or ""),
        "coin": str(delta.get("coin") or ""),
        "amount": amount,
        "rate": _money(delta.get("fundingRate")),
        "position_size": _money(delta.get("szi")),
    }


def _normalize_ledger(item: dict[str, Any], source_user: str, source: str) -> dict[str, Any]:
    """Normalize one non-funding ledger update using a fixed safe field set."""

    delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
    details: dict[str, Any] = {}
    for key in sorted(_SAFE_LEDGER_KEYS):
        if key in delta:
            details[key] = delta[key]
    for key in sorted(_MONEY_KEYS):
        if key in delta:
            details[key] = _money(delta[key])
    return {
        "id": _event_id("ledger", source_user, item),
        "kind": "ledger",
        "source": source,
        "time_ms": _item_time(item),
        "hash": str(item.get("hash") or ""),
        "event_type": str(delta.get("type") or "unknown"),
        "details": details,
    }


def _open_positions(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Return only non-flat positions with fixed safe fields."""

    positions: list[dict[str, Any]] = []
    for item in state.get("assetPositions", []):
        if not isinstance(item, dict):
            continue
        position = item.get("position") if isinstance(item.get("position"), dict) else item
        size = _money(position.get("szi"))
        try:
            if size is None or _decimal(size) == 0:
                continue
        except ValueError:
            continue
        positions.append({
            "coin": str(position.get("coin") or ""),
            "size": size,
            "entry_price": _money(position.get("entryPx")),
            "position_value": _money(position.get("positionValue")),
            "unrealized_pnl": _money(position.get("unrealizedPnl")),
            "liquidation_price": _money(position.get("liquidationPx")),
            "leverage_type": str(
                position.get("leverage", {}).get("type", "")
                if isinstance(position.get("leverage"), dict)
                else ""
            ),
        })
    return positions


def _open_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize open orders without returning raw exchange payloads."""

    normalized: list[dict[str, Any]] = []
    for order in orders:
        normalized.append({
            "order_id": str(order.get("oid") or ""),
            "coin": str(order.get("coin") or ""),
            "side": str(order.get("side") or ""),
            "price": _money(order.get("limitPx")),
            "size": _money(order.get("sz")),
            "timestamp_ms": _item_time(order) or order.get("timestamp"),
            "reduce_only": bool(order.get("reduceOnly", False)),
        })
    return normalized


def _account_mode(value: Any) -> str:
    """Map Hyperliquid account-abstraction output to a conservative mode."""

    raw = value.get("type") if isinstance(value, dict) else value
    normalized = str(raw or "unknown").strip().lower().replace("_", "").replace("-", "")
    if normalized in {"default", "disabled", "manual", "standard", "none"}:
        return "standard_manual"
    if "portfolio" in normalized:
        return "portfolio_margin"
    if "unified" in normalized:
        return "unified"
    return "unknown"


def _role_name(value: Any) -> str:
    """Extract only the non-address role label from a userRole response."""

    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return str(value.get("role") or value.get("type") or "unknown")
    return "unknown"


def _state_balances(state: dict[str, Any], errors: list[dict[str, Any]], source: str) -> dict[str, Any]:
    """Extract account value and withdrawable balance as Decimal strings."""

    margin = state.get("marginSummary") if isinstance(state.get("marginSummary"), dict) else {}
    account_value = _required_money(errors, margin.get("accountValue"), source, "accountValue")
    return {
        "balance": account_value,
        "account_value": account_value,
        "withdrawable": _required_money(errors, state.get("withdrawable"), source, "withdrawable"),
    }


def _empty_account_balance(label: str | None, asset: str | None) -> dict[str, Any]:
    """Return a stable unavailable account-balance record."""

    return {"label": label, "balance": None, "available": False, "asset": asset}


def _source_account_balance(
    label: str,
    balance: Any,
    withdrawable: Any,
    asset: str,
) -> dict[str, Any]:
    """Return a stable source balance without raw exchange fields."""

    normalized_balance = _money(balance)
    return {
        "label": label,
        "balance": normalized_balance,
        "available": normalized_balance is not None,
        "withdrawable": _money(withdrawable),
        "asset": asset,
    }


def _destination_account_balance(label: str, balance: Any, asset: str) -> dict[str, Any]:
    """Return a stable destination balance or an explicit unavailable record."""

    normalized = _money(balance)
    return {
        "label": label,
        "balance": normalized,
        "available": normalized is not None,
        "asset": asset,
    }


def _vault_destination_balances(
    leader_mode: str,
    leader_margin: dict[str, Any],
    leader_spot: dict[str, Any] | None,
    leader_withdrawable: Any = None,
) -> dict[str, dict[str, Any]]:
    """Return separated or unified Leader destination balances by account mode."""

    spot_total = leader_spot.get("total") if leader_spot is not None else None
    spot_available = None
    try:
        spot_available = max(
            Decimal("0"),
            Decimal(str(spot_total)) - Decimal(str((leader_spot or {}).get("hold") or "0")),
        )
    except (InvalidOperation, TypeError, ValueError):
        pass
    if leader_mode in {"unified", "portfolio_margin"}:
        unified = _destination_account_balance("Main Unified", spot_total, "USDC")
        unified["withdrawable"] = _money(spot_available)
        return {"main_perps": dict(unified), "main_spot": dict(unified)}
    main_perps = _destination_account_balance("Main Perps", leader_margin.get("accountValue"), "USDC")
    main_perps["withdrawable"] = _money(leader_withdrawable)
    main_spot = _destination_account_balance("Main Spot", spot_total, "USDC")
    main_spot["withdrawable"] = _money(spot_available)
    return {
        "main_perps": main_perps,
        "main_spot": main_spot,
    }


def _optional_hyperliquid_read(
    request_type: str,
    *,
    expected_type: type | tuple[type, ...],
    timeout_s: float,
    user: str,
) -> Any:
    """Run an optional fixed Hyperliquid target-balance read."""

    try:
        result = hyperliquid_readonly_info(request_type, user=user, timeout_s=timeout_s)
    except Exception:
        return None
    return result if isinstance(result, expected_type) else None


def _spot_usdc_balance(state: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the USDC spot balance from a spot clearinghouse state."""

    balances = state.get("balances")
    if not isinstance(balances, list):
        return None
    for balance in balances:
        if isinstance(balance, dict) and str(balance.get("coin") or "").upper() == "USDC":
            return {
                "coin": "USDC",
                "total": _money(balance.get("total")),
                "hold": _money(balance.get("hold")),
            }
    return {"coin": "USDC", "total": "0", "hold": "0"}


def _spot_usdc_meta(meta: dict[str, Any]) -> dict[str, Any] | None:
    """Extract only canonical USDC token metadata needed by later phases."""

    for token in meta.get("tokens", []):
        if isinstance(token, dict) and str(token.get("name") or "").upper() == "USDC":
            return {
                "symbol": "USDC",
                "token_id": str(token.get("tokenId") or token.get("index") or ""),
                "size_decimals": token.get("szDecimals"),
            }
    return None


def _configured_agent_address(user: Any) -> str | None:
    """Resolve the configured agent address without exposing private-key material."""

    candidate = getattr(user, "agent_address", None)
    extra = getattr(user, "extra", None)
    if candidate is None and isinstance(extra, dict):
        candidate = extra.get("agent_address")
    if not _is_address(candidate):
        private_key = str(getattr(user, "private_key", "") or "")
        if private_key:
            try:
                import ccxt

                signer = ccxt.hyperliquid()
                candidate = signer.privateKeyToAddress(private_key.removeprefix("0x"))
            except Exception:
                candidate = None
    return str(candidate).lower() if _is_address(candidate) else None


def _agent_status(
    agents: list[dict[str, Any]],
    configured_address: str | None,
    now_ms: int,
) -> dict[str, Any]:
    """Validate configured public-agent membership and expiry without returning its address."""

    if configured_address is None:
        return {
            "configured": False,
            "relationship_valid": False,
            "matched": False,
            "valid_until_ms": None,
            "expired": None,
        }
    matched = next(
        (
            agent
            for agent in agents
            if isinstance(agent, dict)
            and str(agent.get("address") or "").lower() == configured_address.lower()
        ),
        None,
    )
    if matched is None:
        return {
            "configured": True,
            "relationship_valid": False,
            "matched": False,
            "valid_until_ms": None,
            "expired": None,
        }
    try:
        valid_until = int(matched["validUntil"]) if matched.get("validUntil") is not None else None
    except (TypeError, ValueError):
        valid_until = 0
    expired = valid_until is not None and valid_until <= now_ms
    return {
        "configured": True,
        "relationship_valid": not expired,
        "matched": True,
        "valid_until_ms": valid_until,
        "expired": expired,
        "name": str(matched.get("name") or ""),
    }


def _matching_vault_equity(equities: list[dict[str, Any]], vault_address: str) -> dict[str, Any] | None:
    """Return the leader-owned equity entry for the configured vault."""

    for equity in equities:
        if str(equity.get("vaultAddress") or "").lower() == vault_address.lower():
            return {
                "equity": _money(equity.get("equity") or equity.get("vaultEquity")),
                "locked_until_ms": equity.get("lockedUntilTimestamp") or equity.get("lockupUntil"),
            }
    return None


def _attribute_vault_commissions(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    """Attribute leader commissions only on exact hash, time, and amount matches."""

    withdrawals = [event for event in events if event.get("event_type") == "vaultWithdraw"]
    commissions = [event for event in events if event.get("event_type") == "vaultLeaderCommission"]
    results: list[dict[str, Any]] = []
    attributed_amounts: list[str | None] = []
    for commission in commissions:
        amount = commission.get("details", {}).get("usdc") or commission.get("details", {}).get("commission")
        matches = []
        for withdrawal in withdrawals:
            expected = withdrawal.get("details", {}).get("commission")
            if (
                commission.get("hash")
                and commission.get("hash") == withdrawal.get("hash")
                and commission.get("time_ms") == withdrawal.get("time_ms")
                and amount is not None
                and expected is not None
                and _money(amount) == _money(expected)
            ):
                matches.append(withdrawal)
        status = "exact" if len(matches) == 1 else "ambiguous" if len(matches) > 1 else "unmatched"
        if status == "exact":
            attributed_amounts.append(_money(amount))
        results.append({
            "commission_event_id": commission["id"],
            "vault_withdraw_event_id": matches[0]["id"] if status == "exact" else None,
            "hash": commission.get("hash"),
            "time_ms": commission.get("time_ms"),
            "amount": _money(amount),
            "status": status,
        })
    return results, _sum_money(attributed_amounts)


def _collect_normal(
    snapshot: dict[str, Any],
    user_address: str,
    since_ms: int,
    until_ms: int,
    timeout_s: float,
) -> None:
    """Populate a normal Hyperliquid Standard/Manual read-only snapshot."""

    errors = snapshot["errors"]
    abstraction = _read(errors, "userAbstraction", expected_type=(dict, str), timeout_s=timeout_s, user=user_address)
    role = _read(errors, "userRole", expected_type=(dict, str), timeout_s=timeout_s, user=user_address)
    state = _read(errors, "clearinghouseState", expected_type=dict, timeout_s=timeout_s, user=user_address)
    spot_state = _read(errors, "spotClearinghouseState", expected_type=dict, timeout_s=timeout_s, user=user_address)
    orders = _read(errors, "openOrders", expected_type=list, timeout_s=timeout_s, user=user_address)
    spot_meta = _read(errors, "spotMeta", expected_type=dict, timeout_s=timeout_s)
    fills, fills_complete = _fetch_history(errors, "userFillsByTime", user_address, since_ms, until_ms, timeout_s)
    funding, funding_complete = _fetch_history(errors, "userFunding", user_address, since_ms, until_ms, timeout_s)
    ledger, ledger_complete = _fetch_history(
        errors,
        "userNonFundingLedgerUpdates",
        user_address,
        since_ms,
        until_ms,
        timeout_s,
    )

    mode = _account_mode(abstraction)
    if mode != "standard_manual":
        _add_error(errors, "unsupported_account_mode", "userAbstraction", "Account is not Standard/Manual")
    normalized_fills = [_normalize_fill(item, user_address, errors) for item in fills]
    normalized_funding = [_normalize_funding(item, user_address, errors) for item in funding]
    normalized_ledger = [_normalize_ledger(item, user_address, "account") for item in ledger]
    fill_net = _sum_money([event["net_closed_pnl"] for event in normalized_fills])
    funding_total = _sum_money([event["amount"] for event in normalized_funding])
    events = [*normalized_fills, *normalized_funding, *normalized_ledger]
    events.sort(key=lambda event: (event.get("time_ms") or 0, event["id"]))
    state_balance = _state_balances(state, errors, "clearinghouseState") if state is not None else {}
    spot_balance = _spot_usdc_balance(spot_state) if spot_state is not None else None
    withdrawable = state_balance.get("withdrawable")

    snapshot.update({
        "account_kind": "normal",
        "account": {
            "mode": mode,
            "role": _role_name(role),
            **state_balance,
            "spot_usdc": spot_balance,
        },
        "account_balances": {
            "source": _source_account_balance(
                "Perps",
                state_balance.get("balance"),
                withdrawable,
                "USDC",
            ),
            "destination": _destination_account_balance(
                "Spot",
                spot_balance.get("total") if spot_balance is not None else None,
                "USDC",
            ),
            "max_transferable": _money(withdrawable),
        },
        "asset": _spot_usdc_meta(spot_meta) if spot_meta is not None else None,
        "positions": _open_positions(state) if state is not None else [],
        "orders": _open_orders(orders) if orders is not None else [],
        "fills": {
            "events": normalized_fills,
            "closed_pnl_less_fees": fill_net,
            "complete": fills_complete,
        },
        "funding": {"events": normalized_funding, "total": funding_total, "complete": funding_complete},
        "ledger": {"events": normalized_ledger, "complete": ledger_complete},
        "realized_net_pnl": format(_decimal(fill_net) + _decimal(funding_total), "f"),
        "events": events,
    })


def _collect_vault(
    snapshot: dict[str, Any],
    user: Any,
    vault_address: str,
    since_ms: int,
    until_ms: int,
    timeout_s: float,
    now_ms: int,
) -> None:
    """Populate a legacy Hyperliquid vault read-only snapshot."""

    errors = snapshot["errors"]
    snapshot["account_balances"] = {
        "source": {
            **_empty_account_balance("Vault", "USDC"),
            "withdrawable": None,
        },
        "destination": {
            "main_perps": _empty_account_balance("Main Perps", "USDC"),
            "main_spot": _empty_account_balance("Main Spot", "USDC"),
        },
        "max_transferable": None,
    }
    initial_details = _read(
        errors,
        "vaultDetails",
        expected_type=dict,
        timeout_s=timeout_s,
        vault_address=vault_address,
    )
    leader_value = initial_details.get("leader") if initial_details is not None else None
    if not _is_address(leader_value):
        _add_error(errors, "leader_unresolved", "vaultDetails", "Vault leader could not be resolved")
        snapshot.update({"account_kind": "vault", "vault": None})
        return
    leader = str(leader_value).lower()
    details = _read(
        errors,
        "vaultDetails",
        expected_type=dict,
        timeout_s=timeout_s,
        vault_address=vault_address,
        user=leader,
    )
    role = _read(errors, "userRole", expected_type=(dict, str), timeout_s=timeout_s, user=vault_address)
    abstraction = _read(errors, "userAbstraction", expected_type=(dict, str), timeout_s=timeout_s, user=leader)
    agents = _read(errors, "extraAgents", expected_type=list, timeout_s=timeout_s, user=leader)
    state = _read(errors, "clearinghouseState", expected_type=dict, timeout_s=timeout_s, user=vault_address)
    leader_state = _optional_hyperliquid_read(
        "clearinghouseState",
        expected_type=dict,
        timeout_s=timeout_s,
        user=leader,
    )
    leader_spot_state = _optional_hyperliquid_read(
        "spotClearinghouseState",
        expected_type=dict,
        timeout_s=timeout_s,
        user=leader,
    )
    orders = _read(errors, "openOrders", expected_type=list, timeout_s=timeout_s, user=vault_address)
    equities = _read(errors, "userVaultEquities", expected_type=list, timeout_s=timeout_s, user=leader)
    spot_meta = _read(errors, "spotMeta", expected_type=dict, timeout_s=timeout_s)
    vault_ledger, vault_ledger_complete = _fetch_history(
        errors,
        "userNonFundingLedgerUpdates",
        vault_address,
        since_ms,
        until_ms,
        timeout_s,
    )
    leader_ledger, leader_ledger_complete = _fetch_history(
        errors,
        "userNonFundingLedgerUpdates",
        leader,
        since_ms,
        until_ms,
        timeout_s,
    )

    if details is None:
        details = {}
    if str(details.get("leader") or "").lower() != leader:
        _add_error(errors, "leader_mismatch", "vaultDetails", "User-scoped vault details changed leader")
    agent = _agent_status(agents or [], _configured_agent_address(user), now_ms)
    if not agent["relationship_valid"]:
        _add_error(errors, "agent_relationship_invalid", "extraAgents", "Configured public agent is absent or expired")
    follower = details.get("followerState") if isinstance(details.get("followerState"), dict) else {}
    ledger_events = [
        *[_normalize_ledger(item, vault_address, "vault") for item in vault_ledger],
        *[_normalize_ledger(item, leader, "leader") for item in leader_ledger],
    ]
    ledger_events.sort(key=lambda event: (event.get("time_ms") or 0, event["id"]))
    attributions, attributed_total = _attribute_vault_commissions(ledger_events)
    vault_equity = _required_money(
        errors,
        follower.get("vaultEquity"),
        "vaultDetails",
        "followerState.vaultEquity",
    )
    max_withdrawable = _required_money(
        errors,
        details.get("maxWithdrawable"),
        "vaultDetails",
        "maxWithdrawable",
    )
    leader_margin = (
        leader_state.get("marginSummary")
        if isinstance(leader_state, dict) and isinstance(leader_state.get("marginSummary"), dict)
        else {}
    )
    leader_spot = _spot_usdc_balance(leader_spot_state) if leader_spot_state is not None else None
    vault_balances = _state_balances(state, errors, "clearinghouseState") if state is not None else {}
    leader_mode = _account_mode(abstraction)

    snapshot.update({
        "account_kind": "vault",
        "account_balances": {
            "source": _source_account_balance(
                "Vault",
                vault_equity,
                vault_balances.get("withdrawable"),
                "USDC",
            ),
            "destination": _vault_destination_balances(
                leader_mode,
                leader_margin,
                leader_spot,
                leader_state.get("withdrawable") if isinstance(leader_state, dict) else None,
            ),
            "max_transferable": _money(max_withdrawable),
        },
        "asset": _spot_usdc_meta(spot_meta) if spot_meta is not None else None,
        "leader": {
            "address": leader,
            "account_mode": leader_mode,
            "agent": agent,
        },
        "vault": {
            "address": vault_address,
            "role": _role_name(role),
            "all_time_pnl": _required_money(errors, follower.get("allTimePnl"), "vaultDetails", "followerState.allTimePnl"),
            "pnl": _money(follower.get("pnl")),
            "vault_equity": vault_equity,
            "leader_fraction": _required_money(errors, details.get("leaderFraction"), "vaultDetails", "leaderFraction"),
            "max_withdrawable": max_withdrawable,
            "lockup_until_ms": follower.get("lockupUntil") or details.get("lockupUntil"),
            "always_close_on_withdraw": bool(details.get("alwaysCloseOnWithdraw", False)),
            "closed": bool(details.get("isClosed", False)),
            "balances": vault_balances,
            "positions": _open_positions(state) if state is not None else [],
            "orders": _open_orders(orders) if orders is not None else [],
            "user_vault_equity": _matching_vault_equity(equities or [], vault_address),
        },
        "ledger": {
            "events": ledger_events,
            "vault_complete": vault_ledger_complete,
            "leader_complete": leader_ledger_complete,
        },
        "vault_leader_commissions": {
            "attributions": attributions,
            "exact_attributed_total": attributed_total,
        },
        "events": ledger_events,
    })


def _owned_client(user: Any, exchange: str, timeout_s: float) -> tuple[Any, Any]:
    """Create one credentialed CCXT client owned by this snapshot."""

    factory = Exchange
    if factory is None:
        from Exchange import Exchange as factory

    owner = factory(exchange, user)
    try:
        owner.connect()
        client = owner.instance
        if client is None:
            raise RuntimeError("exchange client unavailable")
        client.timeout = int(timeout_s * 1_000)
    except Exception:
        owner.close()
        raise
    return owner, client


def _asset_symbol(user: Any, exchange: str, settlement_asset: str | None = None) -> str:
    """Resolve the configured settlement asset without accepting arbitrary coins."""

    candidate = str(
        settlement_asset
        if settlement_asset is not None
        else getattr(user, "quote", "") or _DEFAULT_ASSETS[exchange]
    ).strip().upper()
    if candidate in _SUPPORTED_ASSETS[exchange]:
        return candidate
    if settlement_asset is not None:
        raise ReadOnlyRequestError(f"{candidate or 'Empty asset'} is not supported by the {exchange} snapshot adapter")
    return _DEFAULT_ASSETS[exchange]


def _client_read(client: Any, method: str, params: dict[str, Any]) -> Any:
    """Invoke one explicitly allowlisted credentialed exchange read."""

    if method not in CCXT_READ_METHODS:
        raise ReadOnlyRequestError(f"CCXT method {method!r} is not read-only allowlisted")
    function = getattr(client, method, None)
    if not callable(function):
        raise ReadOnlyRequestError(f"CCXT read method {method!r} is unavailable")
    return function(dict(params))


def _optional_client_read(client: Any, method: str, params: dict[str, Any]) -> Any:
    """Run an optional allowlisted target-balance read without failing a snapshot."""

    try:
        return _client_read(client, method, params)
    except Exception:
        return None


def _response_data(response: Any, source: str, *, bybit: bool = False) -> Any:
    """Return a successful native response payload or reject its envelope."""

    if not isinstance(response, dict):
        raise ReadOnlyRequestError(f"{source} returned an unexpected response shape")
    if bybit:
        if str(response.get("retCode")) != "0" or not isinstance(response.get("result"), dict):
            raise ReadOnlyRequestError(f"{source} returned an unsuccessful response")
        return response["result"]
    if str(response.get("code")) != "00000" or "data" not in response:
        raise ReadOnlyRequestError(f"{source} returned an unsuccessful response")
    return response["data"]


def _single_asset(items: Any, asset: str, source: str, *fields: str) -> dict[str, Any]:
    """Select exactly one asset record with all required fields."""

    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise ReadOnlyRequestError(f"{source} returned an unexpected asset list")
    matches = [
        item
        for item in items
        if str(item.get("coin") or item.get("asset") or item.get("marginCoin") or "").upper() == asset
    ]
    if len(matches) != 1 or any(field not in matches[0] for field in fields):
        raise ReadOnlyRequestError(f"{source} did not return one complete {asset} balance")
    return matches[0]


def _optional_asset(items: Any, asset: str, source: str, *fields: str) -> dict[str, Any] | None:
    """Select an optional target asset without making its absence fatal."""

    try:
        return _single_asset(items, asset, source, *fields)
    except ReadOnlyRequestError:
        return None


def _sum_asset_fields(item: dict[str, Any] | None, required: str, *optional: str) -> str | None:
    """Sum a required monetary field and any present optional balance fields."""

    if item is None or required not in item:
        return None
    values = [item.get(required), *(item.get(field) for field in optional if field in item)]
    normalized = [_money(value) for value in values]
    if any(value is None for value in normalized):
        return None
    return _sum_money(normalized)


def _native_time(item: dict[str, Any]) -> int | None:
    """Extract a timestamp from one supported native history record."""

    for key in ("transactionTime", "time", "ts", "cTime", "timestamp"):
        if item.get(key) is not None:
            try:
                return int(item[key])
            except (TypeError, ValueError):
                return None
    return None


def _native_id(exchange: str, kind: str, item: dict[str, Any]) -> str:
    """Build a stable event ID from safe native fields and a payload hash."""

    identity = (
        item.get("id")
        or item.get("tradeId")
        or item.get("tranId")
        or item.get("billId")
        or item.get("orderId")
        or _canonical_hash(item)
    )
    event_type = item.get("incomeType") or item.get("type") or item.get("businessType") or "event"
    return f"{exchange}:{kind}:{event_type}:{_native_time(item) or 0}:{identity}"


def _base_coin(item: dict[str, Any]) -> str:
    """Return a normalized base coin from a native symbol or coin field."""

    symbol = str(item.get("symbol") or item.get("coin") or "").upper()
    for suffix in ("USDT", "USDC"):
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            return symbol[: -len(suffix)]
    return symbol


def _native_fill(
    exchange: str,
    item: dict[str, Any],
    *,
    closed_pnl: Any,
    fee: Any,
    errors: list[dict[str, Any]],
    source: str,
) -> dict[str, Any]:
    """Normalize one native realized-PnL or fee record as a fill event."""

    normalized_pnl = _required_money(errors, closed_pnl, source, "closed_pnl")
    normalized_fee = _required_money(errors, fee, source, "fee")
    net = None
    if normalized_pnl is not None and normalized_fee is not None:
        net = format(_decimal(normalized_pnl) - _decimal(normalized_fee), "f")
    return {
        "id": _native_id(exchange, "fill", item),
        "kind": "fill",
        "time_ms": _native_time(item),
        "trade_id": str(item.get("tradeId") or item.get("id") or item.get("billId") or ""),
        "order_id": str(item.get("orderId") or ""),
        "coin": _base_coin(item),
        "side": str(item.get("side") or item.get("businessType") or item.get("type") or ""),
        "price": _money(item.get("tradePrice") or item.get("price")),
        "size": _money(item.get("qty") or item.get("size") or item.get("positionAmount")),
        "closed_pnl": normalized_pnl,
        "fee": normalized_fee,
        "net_closed_pnl": net,
    }


def _native_funding(
    exchange: str,
    item: dict[str, Any],
    *,
    amount: Any,
    errors: list[dict[str, Any]],
    source: str,
) -> dict[str, Any]:
    """Normalize one native funding record."""

    return {
        "id": _native_id(exchange, "funding", item),
        "kind": "funding",
        "time_ms": _native_time(item),
        "coin": _base_coin(item),
        "amount": _required_money(errors, amount, source, "amount"),
        "rate": _money(item.get("feeRate") or item.get("fundingRate")),
        "position_size": _money(item.get("size") or item.get("positionAmount")),
    }


def _set_native_snapshot(
    snapshot: dict[str, Any],
    *,
    mode: str,
    asset: str,
    balance: Any,
    account_value: Any,
    withdrawable: Any,
    fills: list[dict[str, Any]],
    funding: list[dict[str, Any]],
) -> None:
    """Install the common normal-account snapshot shape consumed by the API."""

    errors = snapshot["errors"]
    account = {
        "mode": mode,
        "balance": _required_money(errors, balance, "account", "balance"),
        "account_value": _required_money(errors, account_value, "account", "account_value"),
        "withdrawable": _required_money(errors, withdrawable, "account", "withdrawable"),
    }
    fill_total = _sum_money([event.get("net_closed_pnl") for event in fills])
    funding_total = _sum_money([event.get("amount") for event in funding])
    events = [*fills, *funding]
    events.sort(key=lambda event: (event.get("time_ms") or 0, event["id"]))
    snapshot.update({
        "account_kind": "normal",
        "account_mode": mode,
        "account": account,
        "asset": {"symbol": asset, "amount_precision": 8},
        "positions": [],
        "orders": [],
        "fills": {"events": fills, "closed_pnl_less_fees": fill_total, "complete": True},
        "funding": {"events": funding, "total": funding_total, "complete": True},
        "ledger": {"events": [], "complete": True},
        "realized_net_pnl": format(_decimal(fill_total) + _decimal(funding_total), "f"),
        "events": events,
    })


def _bybit_history(client: Any, since_ms: int, until_ms: int, asset: str) -> list[dict[str, Any]]:
    """Read all Bybit UTA transaction-log pages in fixed seven-day windows."""

    records: list[dict[str, Any]] = []
    window_start = since_ms
    seven_days = 7 * 24 * 60 * 60 * 1_000
    pages = 0
    while window_start <= until_ms:
        window_end = min(until_ms, window_start + seven_days)
        cursor: str | None = None
        while True:
            if pages >= _MAX_HISTORY_PAGES:
                raise ReadOnlyRequestError("bybit_transaction_log exceeded the page limit")
            pages += 1
            params: dict[str, Any] = {
                "accountType": "UNIFIED",
                "category": "linear",
                "currency": asset,
                "startTime": window_start,
                "endTime": window_end,
                "limit": 50,
            }
            if cursor:
                params["cursor"] = cursor
            data = _response_data(
                _client_read(client, "privateGetV5AccountTransactionLog", params),
                "bybit_transaction_log",
                bybit=True,
            )
            items = data.get("list")
            if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
                raise ReadOnlyRequestError("bybit_transaction_log returned an unexpected history shape")
            records.extend(items)
            next_cursor = data.get("nextPageCursor")
            if not next_cursor:
                break
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise ReadOnlyRequestError("bybit_transaction_log returned an invalid cursor")
            cursor = next_cursor
        if window_end >= until_ms:
            break
        window_start = window_end + 1
    return records


def _collect_bybit(snapshot: dict[str, Any], client: Any, asset: str, since_ms: int, until_ms: int) -> None:
    """Collect a Bybit Unified account snapshot using fixed V5 GET calls."""

    errors = snapshot["errors"]
    route = {"fromAccountType": "UNIFIED", "toAccountType": "FUND"}
    supported_response = _optional_client_read(client, "privateGetV5AssetTransferQueryTransferCoinList", route)
    supported: Any = None
    if supported_response is not None:
        try:
            supported = _response_data(supported_response, "bybit_transferable_coins", bybit=True).get("list")
        except ReadOnlyRequestError:
            supported = None
    transfer_response = _optional_client_read(
        client,
        "privateGetV5AssetTransferQueryAccountCoinBalance",
        {"accountType": "UNIFIED", "toAccountType": "FUND", "coin": asset},
    )
    transfer_balance: dict[str, Any] | None = None
    if transfer_response is not None:
        try:
            transfer = _response_data(
                transfer_response,
                "bybit_coin_balance",
                bybit=True,
            )
            candidate = transfer.get("balance")
            if isinstance(candidate, dict) and str(candidate.get("coin") or "").upper() == asset:
                transfer_balance = candidate
        except ReadOnlyRequestError:
            transfer_balance = None
    transfer_available = (
        isinstance(supported, list)
        and asset in [str(item).upper() for item in supported]
        and transfer_balance is not None
        and _money(transfer_balance.get("transferBalance")) is not None
    )
    wallet = _response_data(
        _client_read(
            client,
            "privateGetV5AccountWalletBalance",
            {"accountType": "UNIFIED", "coin": asset},
        ),
        "bybit_wallet_balance",
        bybit=True,
    )
    accounts = wallet.get("list")
    if not isinstance(accounts, list) or len(accounts) != 1 or not isinstance(accounts[0], dict):
        raise ReadOnlyRequestError("bybit_wallet_balance returned an unexpected account list")
    account = accounts[0]
    if account.get("accountType") != "UNIFIED":
        raise ReadOnlyRequestError("bybit_wallet_balance did not confirm Unified mode")
    coin = _single_asset(account.get("coin"), asset, "bybit_wallet_balance", "walletBalance", "equity")
    funding_response = _optional_client_read(
        client,
        "privateGetV5AssetTransferQueryAccountCoinsBalance",
        {"accountType": "FUND", "coin": asset},
    )
    funding_data: Any = None
    if funding_response is not None:
        try:
            funding_data = _response_data(funding_response, "bybit_funding_balance", bybit=True)
        except ReadOnlyRequestError:
            funding_data = None
    funding_coin = _optional_asset(
        funding_data.get("balance") if isinstance(funding_data, dict) else None,
        asset,
        "bybit_funding_balance",
        "walletBalance",
    )

    fills: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []
    for item in _bybit_history(client, since_ms, until_ms, asset):
        event_type = str(item.get("type") or "").upper()
        funding_amount = _money(item.get("funding"))
        if event_type == "TRADE":
            fills.append(_native_fill(
                "bybit",
                item,
                closed_pnl=item.get("cashFlow") or "0",
                fee=item.get("fee") or "0",
                errors=errors,
                source="bybit_transaction_log",
            ))
        if event_type == "SETTLEMENT" and funding_amount is not None and _decimal(funding_amount) != 0:
            funding.append(_native_funding(
                "bybit",
                item,
                amount=funding_amount,
                errors=errors,
                source="bybit_transaction_log",
            ))
        if event_type in {"TRADE", "SETTLEMENT"} and _native_time(item) is None:
            _add_error(errors, "invalid_response", "bybit_transaction_log", "History event has no valid timestamp")
    _set_native_snapshot(
        snapshot,
        mode="unified",
        asset=asset,
        balance=coin.get("walletBalance"),
        account_value=coin.get("equity"),
        withdrawable=transfer_balance.get("transferBalance") if transfer_available else "0",
        fills=fills,
        funding=funding,
    )
    funding_destination = _destination_account_balance(
        "Funding",
        funding_coin.get("walletBalance") if funding_coin is not None else None,
        asset,
    )
    funding_destination["withdrawable"] = _money(
        funding_coin.get("transferBalance") if funding_coin is not None else None
    )
    snapshot["account_balances"] = {
        "source": _source_account_balance(
            "Unified",
            coin.get("walletBalance"),
            transfer_balance.get("transferBalance") if transfer_available else None,
            asset,
        ),
        "destination": funding_destination,
        "max_transferable": _money(transfer_balance.get("transferBalance")) if transfer_available else None,
    }
    snapshot["transfer_permissions"] = {
        "internal_transfer": transfer_available,
        "reason": (
            None
            if transfer_available
            else "Bybit API key does not permit internal account transfers. Enable Account Transfer permission; withdrawals are not required."
        ),
    }


def _binance_income(client: Any, since_ms: int, until_ms: int, income_type: str) -> list[dict[str, Any]]:
    """Read one Binance USD-M income class without mixing accounting types."""

    records: list[dict[str, Any]] = []
    window_start = since_ms
    seven_days = 7 * 24 * 60 * 60 * 1_000
    pages = 0
    while window_start <= until_ms:
        window_end = min(until_ms, window_start + seven_days)
        cursor = window_start
        while True:
            if pages >= _MAX_HISTORY_PAGES:
                raise ReadOnlyRequestError(f"binance_{income_type.lower()} exceeded the page limit")
            pages += 1
            params = {"incomeType": income_type, "startTime": cursor, "endTime": window_end, "limit": 1000}
            page = _client_read(client, "fapiPrivateGetIncome", params)
            if not isinstance(page, list) or any(not isinstance(item, dict) for item in page):
                raise ReadOnlyRequestError(f"binance_{income_type.lower()} returned an unexpected history shape")
            records.extend(page)
            if len(page) < 1000:
                break
            times = [_native_time(item) for item in page]
            if any(value is None for value in times):
                raise ReadOnlyRequestError(f"binance_{income_type.lower()} returned a page without timestamps")
            last_time = max(value for value in times if value is not None)
            if last_time < cursor or sum(value == last_time for value in times) > 1:
                raise ReadOnlyRequestError(f"binance_{income_type.lower()} pagination is ambiguous")
            cursor = last_time + 1
        if window_end >= until_ms:
            return records
        window_start = window_end + 1
    raise ReadOnlyRequestError(f"binance_{income_type.lower()} exceeded the page limit")


def _collect_binance(snapshot: dict[str, Any], client: Any, asset: str, since_ms: int, until_ms: int) -> None:
    """Collect Binance USD-M balance and separated native income histories."""

    errors = snapshot["errors"]
    balances = _client_read(client, "fapiPrivateV3GetBalance", {})
    balance = _single_asset(
        balances,
        asset,
        "binance_futures_balance",
        "balance",
        "maxWithdrawAmount",
    )
    funding_response = _optional_client_read(
        client,
        "sapiPostAssetGetFundingAsset",
        {"asset": asset},
    )
    restrictions = _optional_client_read(client, "sapiGetAccountApiRestrictions", {})
    if isinstance(restrictions, dict):
        snapshot["transfer_permissions"] = {
            "internal_transfer": restrictions.get("enableInternalTransfer") is True,
            "universal_transfer": restrictions.get("permitsUniversalTransfer") is True,
        }
    funding_asset = _optional_asset(
        funding_response,
        asset,
        "binance_funding_wallet",
        "free",
    )
    if funding_asset is None and funding_response == []:
        funding_balance = "0"
    else:
        funding_balance = _sum_asset_fields(funding_asset, "free", "locked", "freeze", "withdrawing")
    realized = _binance_income(client, since_ms, until_ms, "REALIZED_PNL")
    commissions = _binance_income(client, since_ms, until_ms, "COMMISSION")
    funding_rows = _binance_income(client, since_ms, until_ms, "FUNDING_FEE")
    fills = [
        _native_fill(
            "binance",
            item,
            closed_pnl=item.get("income"),
            fee="0",
            errors=errors,
            source="binance_realized_pnl",
        )
        for item in realized
    ]
    for item in commissions:
        try:
            fee = format(-_decimal(item.get("income")), "f")
        except ValueError:
            fee = None
        fills.append(_native_fill(
            "binance",
            item,
            closed_pnl="0",
            fee=fee,
            errors=errors,
            source="binance_commission",
        ))
    funding = [
        _native_funding(
            "binance",
            item,
            amount=item.get("income"),
            errors=errors,
            source="binance_funding_fee",
        )
        for item in funding_rows
    ]
    _set_native_snapshot(
        snapshot,
        mode="usd_m_futures",
        asset=asset,
        balance=balance.get("balance"),
        account_value=balance.get("balance"),
        withdrawable=balance.get("maxWithdrawAmount"),
        fills=fills,
        funding=funding,
    )
    funding_destination = _destination_account_balance("Funding Wallet", funding_balance, asset)
    funding_destination["withdrawable"] = _money(
        funding_asset.get("free") if funding_asset is not None else "0" if funding_response == [] else None
    )
    snapshot["account_balances"] = {
        "source": _source_account_balance(
            "USD-M Futures",
            balance.get("balance"),
            balance.get("maxWithdrawAmount"),
            asset,
        ),
        "destination": funding_destination,
        "max_transferable": _money(balance.get("maxWithdrawAmount")),
    }


def _bitget_max_transferable(client: Any, asset: str) -> Any:
    """Call Bitget's fixed max-transferable GET on old or new CCXT."""

    params = {"coin": asset}
    function = getattr(client, "privateUtaGetV3AccountMaxTransferable", None)
    if callable(function):
        return _client_read(client, "privateUtaGetV3AccountMaxTransferable", params)
    return client.request(
        "v3/account/max-transferable",
        ["private", "uta"],
        "GET",
        params,
        {"cost": 1},
    )


def _bitget_history(
    client: Any,
    mode: str,
    since_ms: int,
    until_ms: int,
    asset: str,
) -> list[dict[str, Any]]:
    """Read Bitget Classic bills or UTA financial records in fixed windows."""

    records: list[dict[str, Any]] = []
    window_start = since_ms
    thirty_days = 30 * 24 * 60 * 60 * 1_000
    pages = 0
    while window_start <= until_ms:
        window_end = min(until_ms, window_start + thirty_days)
        cursor: str | None = None
        while True:
            if pages >= _MAX_HISTORY_PAGES:
                raise ReadOnlyRequestError("Bitget financial history exceeded the page limit")
            pages += 1
            if mode == "uta":
                params: dict[str, Any] = {
                    "category": "USDT-FUTURES",
                    "coin": asset,
                    "startTime": window_start,
                    "endTime": window_end,
                    "limit": "100",
                }
                if cursor:
                    params["cursor"] = cursor
                data = _response_data(
                    _client_read(client, "privateUtaGetV3AccountFinancialRecords", params),
                    "bitget_uta_financial_records",
                )
                items = data.get("list") if isinstance(data, dict) else None
                next_cursor = data.get("cursor") if isinstance(data, dict) else None
            else:
                params = {
                    "productType": "USDT-FUTURES",
                    "startTime": window_start,
                    "endTime": window_end,
                    "limit": "100",
                }
                if cursor:
                    params["idLessThan"] = cursor
                data = _response_data(
                    _client_read(client, "privateMixGetV2MixAccountBill", params),
                    "bitget_classic_account_bill",
                )
                items = data.get("bills") if isinstance(data, dict) else None
                next_cursor = data.get("endId") if isinstance(data, dict) and len(items or []) == 100 else None
            if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
                raise ReadOnlyRequestError("Bitget returned an unexpected financial history shape")
            records.extend(items)
            if not next_cursor:
                break
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise ReadOnlyRequestError("Bitget returned an invalid financial-history cursor")
            cursor = next_cursor
        if window_end >= until_ms:
            break
        window_start = window_end + 1
    return records


def _signed_direction_amount(value: Any, event_type: str) -> Any:
    """Apply Bitget IN/OUT direction where the API amount is unsigned."""

    try:
        amount = _decimal(value)
    except ValueError:
        return None
    if event_type.endswith("_OUT"):
        amount = -abs(amount)
    elif event_type.endswith("_IN"):
        amount = abs(amount)
    return format(amount, "f")


def _normalize_bitget_history(
    mode: str,
    records: list[dict[str, Any]],
    errors: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Normalize Bitget PnL, commission, and funding records."""

    fills: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []
    for item in records:
        event_type = str(item.get("type") or item.get("businessType") or "").upper()
        is_funding = "SETTLE_FEE" in event_type
        is_trade = any(token in event_type for token in ("OPEN", "CLOSE", "BUY", "SELL", "ORDER_DEALT"))
        if is_funding:
            amount = _signed_direction_amount(item.get("amount"), event_type)
            funding.append(_native_funding(
                "bitget",
                item,
                amount=amount,
                errors=errors,
                source=f"bitget_{mode}_history",
            ))
        elif is_trade or _money(item.get("fee")) not in {None, "0"}:
            raw_fee = item.get("fee") or "0"
            try:
                fee = format(-_decimal(raw_fee), "f")
            except ValueError:
                fee = None
            closed_pnl = item.get("amount") if "OPEN" not in event_type else "0"
            fills.append(_native_fill(
                "bitget",
                item,
                closed_pnl=closed_pnl or "0",
                fee=fee,
                errors=errors,
                source=f"bitget_{mode}_history",
            ))
        if _native_time(item) is None:
            _add_error(errors, "invalid_response", f"bitget_{mode}_history", "History event has no valid timestamp")
    return fills, funding


def _collect_bitget(snapshot: dict[str, Any], client: Any, asset: str, since_ms: int, until_ms: int) -> None:
    """Detect Bitget UTA/Classic mode and collect its fixed account reads."""

    classic_accounts: Any = None
    destination_read_denied = False
    try:
        settings_response = _client_read(client, "privateUtaGetV3AccountSettings", {})
    except Exception:
        mode = "classic"
        classic_accounts = _response_data(
            _client_read(
                client,
                "privateMixGetV2MixAccountAccounts",
                {"productType": "USDT-FUTURES"},
            ),
            "bitget_classic_accounts",
        )
    else:
        settings = _response_data(settings_response, "bitget_uta_settings")
        if not isinstance(settings, dict) or settings.get("accountMode") not in {"unified", "hybrid"}:
            raise ReadOnlyRequestError("bitget_uta_settings returned an unknown account mode")
        mode = "uta"

    if mode == "uta":
        account_data = _response_data(
            _client_read(client, "privateUtaGetV3AccountAssets", {}),
            "bitget_uta_assets",
        )
        if not isinstance(account_data, dict):
            raise ReadOnlyRequestError("bitget_uta_assets returned an unexpected account shape")
        balance = _single_asset(account_data.get("assets"), asset, "bitget_uta_assets", "balance", "equity")
        maximum = _response_data(
            _bitget_max_transferable(client, asset),
            "bitget_uta_max_transferable",
        )
        if not isinstance(maximum, dict) or maximum.get("coin") != asset or "maxTransfer" not in maximum:
            raise ReadOnlyRequestError("bitget_uta_max_transferable returned an unexpected response")
        balance_value = balance.get("balance")
        account_value = balance.get("equity")
        withdrawable = maximum.get("maxTransfer")
        source_balance = balance_value
        try:
            destination_response = _client_read(client, "privateUtaGetV3AccountFundingAssets", {"coin": asset})
        except Exception as exc:
            destination_read_denied = "permission" in type(exc).__name__.lower()
            destination_response = None
        destination_data: Any = None
        if destination_response is not None:
            try:
                destination_data = _response_data(destination_response, "bitget_uta_funding_assets")
            except ReadOnlyRequestError:
                destination_data = None
        destination_asset = _optional_asset(
            destination_data,
            asset,
            "bitget_uta_funding_assets",
            "balance",
        )
        destination_balance = destination_asset.get("balance") if destination_asset is not None else None
        destination_withdrawable = (
            destination_asset.get("available")
            if destination_asset is not None and destination_asset.get("available") is not None
            else destination_asset.get("maxTransfer") if destination_asset is not None else None
        )
        source_label = "UTA"
        destination_label = "Funding"
    else:
        balance = _single_asset(
            classic_accounts,
            asset,
            "bitget_classic_accounts",
            "available",
            "accountEquity",
            "maxTransferOut",
        )
        balance_value = balance.get("available")
        account_value = balance.get("accountEquity")
        withdrawable = balance.get("maxTransferOut")
        source_balance = account_value
        try:
            destination_response = _client_read(client, "privateSpotGetV2SpotAccountAssets", {"coin": asset})
        except Exception as exc:
            destination_read_denied = "permission" in type(exc).__name__.lower()
            destination_response = None
        destination_data = None
        if destination_response is not None:
            try:
                destination_data = _response_data(destination_response, "bitget_classic_spot_assets")
            except ReadOnlyRequestError:
                destination_data = None
        destination_asset = _optional_asset(
            destination_data,
            asset,
            "bitget_classic_spot_assets",
            "available",
        )
        destination_balance = (
            destination_asset.get("balance")
            if destination_asset is not None and "balance" in destination_asset
            else _sum_asset_fields(destination_asset, "available", "frozen", "locked")
        )
        destination_withdrawable = destination_asset.get("available") if destination_asset is not None else None
        source_label = "Classic Futures"
        destination_label = "Spot"

    rows = _bitget_history(client, mode, since_ms, until_ms, asset)
    fills, funding = _normalize_bitget_history(mode, rows, snapshot["errors"])
    _set_native_snapshot(
        snapshot,
        mode=mode,
        asset=asset,
        balance=balance_value,
        account_value=account_value,
        withdrawable=withdrawable,
        fills=fills,
        funding=funding,
    )
    destination_record = _destination_account_balance(destination_label, destination_balance, asset)
    destination_record["withdrawable"] = _money(destination_withdrawable)
    snapshot["account_balances"] = {
        "source": _source_account_balance(source_label, source_balance, withdrawable, asset),
        "destination": destination_record,
        "max_transferable": _money(withdrawable),
    }
    snapshot["transfer_permissions"] = {
        "internal_transfer": True,
        "destination_read": not destination_read_denied,
        "reason": (
            "Bitget Spot account read is unavailable. Enable Spot read permission to display the destination balance and transfer history; Wallet Transfer permission is sufficient to move funds."
            if destination_read_denied
            else None
        ),
    }


def _json_safe(value: Any) -> Any:
    """Recursively convert supported values into JSON-safe primitives."""

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def collect_readonly_snapshot(
    user: Any,
    since_ms: int,
    until_ms: int,
    timeout_s: float = 30.0,
    settlement_asset: str | None = None,
) -> dict[str, Any]:
    """Collect a fail-closed, JSON-safe exchange snapshot.

    Unsupported exchanges return an explicit capability result. Transport,
    response-shape, pagination, account-mode, or freshness uncertainty is
    retained as fatal errors and sets ``complete`` to false.
    """

    capability = readonly_capability(user)
    now_ms = _now_ms()
    exchange = capability["exchange"]
    asset_error: str | None = None
    if exchange in _SUPPORTED_ASSETS:
        try:
            balance_asset: str | None = _asset_symbol(user, exchange, settlement_asset)
        except ReadOnlyRequestError as exc:
            balance_asset = str(settlement_asset or "").strip().upper() or None
            asset_error = str(exc)
    else:
        balance_asset = str(getattr(user, "quote", "") or "").strip().upper() or None
    snapshot: dict[str, Any] = {
        "schema_version": 1,
        "read_only": True,
        "capability": capability,
        "exchange": capability["exchange"],
        "user_name": str(getattr(user, "name", "") or ""),
        "collected_at_ms": now_ms,
        "history": {
            "since_ms": since_ms,
            "until_ms": until_ms,
            "freshness_age_ms": None,
            "fresh": False,
        },
        "complete": False,
        "errors": [],
        "account_balances": {
            "source": {
                **_empty_account_balance(None, balance_asset),
                "withdrawable": None,
            },
            "destination": _empty_account_balance(None, balance_asset),
            "max_transferable": None,
        },
    }
    if not capability["supported"]:
        _add_error(snapshot["errors"], "unsupported_exchange", "capability", capability["reason"])
        return snapshot
    if asset_error is not None:
        _add_error(snapshot["errors"], "unsupported_asset", "asset", asset_error)
        return snapshot
    try:
        since_value = int(since_ms)
        until_value = int(until_ms)
    except (TypeError, ValueError):
        _add_error(snapshot["errors"], "invalid_time_range", "history", "History bounds must be integer milliseconds")
        return snapshot
    if isinstance(since_ms, bool) or isinstance(until_ms, bool) or since_value < 0 or until_value < since_value:
        _add_error(snapshot["errors"], "invalid_time_range", "history", "History bounds are invalid")
        return snapshot
    if not isinstance(timeout_s, (int, float)) or isinstance(timeout_s, bool) or timeout_s <= 0:
        _add_error(snapshot["errors"], "invalid_timeout", "transport", "timeout_s must be positive")
        return snapshot
    freshness_age = max(0, now_ms - until_value)
    snapshot["history"] = {
        "since_ms": since_value,
        "until_ms": until_value,
        "freshness_age_ms": freshness_age,
        "fresh": freshness_age <= _MAX_FRESHNESS_MS and until_value <= now_ms + _MAX_FRESHNESS_MS,
    }
    if not snapshot["history"]["fresh"]:
        _add_error(snapshot["errors"], "stale_snapshot", "history", "Requested history end is outside the freshness window")

    if exchange == "hyperliquid":
        try:
            user_address = _validated_address(getattr(user, "wallet_address", None), "wallet_address")
        except ReadOnlyRequestError as exc:
            _add_error(snapshot["errors"], "invalid_wallet_address", "user", str(exc))
            return snapshot
        if bool(getattr(user, "is_vault", False)):
            _collect_vault(snapshot, user, user_address, since_value, until_value, float(timeout_s), now_ms)
        else:
            _collect_normal(snapshot, user_address, since_value, until_value, float(timeout_s))
    else:
        owner: Any = None
        try:
            owner, client = _owned_client(user, exchange, float(timeout_s))
            if exchange == "bybit":
                _collect_bybit(snapshot, client, balance_asset, since_value, until_value)
            elif exchange == "binance":
                _collect_binance(snapshot, client, balance_asset, since_value, until_value)
            elif exchange == "bitget":
                _collect_bitget(snapshot, client, balance_asset, since_value, until_value)
        except ReadOnlyRequestError as exc:
            _add_error(snapshot["errors"], "invalid_response", exchange, str(exc))
        except Exception as exc:
            _add_error(snapshot["errors"], "read_failed", exchange, f"Read failed ({type(exc).__name__})")
        finally:
            if owner is not None:
                owner.close()
    snapshot["complete"] = not snapshot["errors"]
    return _json_safe(snapshot)
