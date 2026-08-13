"""Read-only Bitunix and WEEX account operations inside PB8's runtime."""

from __future__ import annotations

import asyncio
from contextlib import redirect_stdout
import json
import math
import sys
from pathlib import Path


MAX_REQUEST_BYTES = 64 * 1024
MAX_SYMBOLS = 32
MAX_POSITIONS = 100
MAX_ORDERS = 5000
MAX_FILLS = 10000
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
SUPPORTED_PB8_VERSION = "8.1.0"


def _load_modules(pb8_dir: Path) -> dict:
    src_dir = pb8_dir / "src"
    if not src_dir.is_dir():
        raise RuntimeError("PB8 source directory is unavailable")
    sys.path.insert(0, str(src_dir))
    from exchanges.bitunix import BitunixClient
    from exchanges.weex import AsyncWeex
    from fill_events_manager import BitunixFetcher, WeexFetcher
    from passivbot_version import __version__

    if __version__ != SUPPORTED_PB8_VERSION:
        raise RuntimeError(
            f"PB8 exchange bridge requires {SUPPORTED_PB8_VERSION}; installed runtime is {__version__}"
        )

    return {"BitunixClient": BitunixClient, "AsyncWeex": AsyncWeex, "BitunixFetcher": BitunixFetcher, "WeexFetcher": WeexFetcher, "version": __version__}


def _credentials(payload: dict, exchange: str) -> dict:
    credentials = payload.get("credentials")
    if not isinstance(credentials, dict):
        raise TypeError("credentials must be an object")
    key = str(credentials.get("key") or "")
    secret = str(credentials.get("secret") or "")
    passphrase = str(credentials.get("passphrase") or "")
    if not key or not secret:
        raise ValueError("key and secret are required")
    if exchange == "weex" and not passphrase:
        raise ValueError("WEEX passphrase is required")
    return {"key": key, "secret": secret, "passphrase": passphrase}


async def _build_client(modules: dict, exchange: str, credentials: dict):
    if exchange == "bitunix":
        return modules["BitunixClient"]({
            "apiKey": credentials["key"],
            "secret": credentials["secret"],
            "timeout": 30_000,
            "enableRateLimit": True,
            "wsEnabled": False,
        })
    client = modules["AsyncWeex"]({
        "apiKey": credentials["key"],
        "secret": credentials["secret"],
        "password": credentials["passphrase"],
        "timeout": 30_000,
        "enableRateLimit": True,
    })
    client.options["defaultType"] = "swap"
    return client


def _weex_wallet_balance(fetched: dict) -> float:
    raw = fetched.get("info") if isinstance(fetched, dict) else None
    rows = raw if isinstance(raw, list) else [raw]
    matches = [row for row in rows if isinstance(row, dict) and str(row.get("asset") or "").upper() == "USDT"]
    if len(matches) != 1:
        raise ValueError("WEEX balance response missing unique USDT asset row")
    equity = float(matches[0]["balance"])
    upnl = float(matches[0]["unrealizePnl"])
    wallet = equity - upnl
    if not all(math.isfinite(value) for value in (equity, upnl, wallet)):
        raise ValueError("WEEX balance response contains non-finite values")
    return wallet


async def _weex_tickers(client, symbols: list[str]) -> dict:
    markets_by_id = {str(market.get("id") or ""): symbol for symbol, market in client.markets.items()}
    rows = await client.contract_get_capi_v3_market_ticker_bookticker()
    rows = rows if isinstance(rows, list) else [rows]
    tickers = {}
    wanted = set(symbols)
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = markets_by_id.get(str(row.get("symbol") or ""))
        if symbol not in wanted:
            continue
        try:
            bid = float(row["bidPrice"])
            ask = float(row["askPrice"])
            timestamp = int(row["time"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(bid) and math.isfinite(ask) and 0.0 < bid <= ask and timestamp > 0:
            tickers[symbol] = {"bid": bid, "ask": ask, "last": (bid + ask) / 2.0, "timestamp": timestamp, "source": "weex_book_ticker_mid"}
    return tickers


async def _dispatch(payload: dict) -> dict:
    pb8_dir = Path(str(payload.get("pb8_dir") or "")).resolve()
    modules = _load_modules(pb8_dir)
    exchange = str(payload.get("exchange") or "").strip().lower()
    if exchange not in {"bitunix", "weex"}:
        raise ValueError("exchange must be bitunix or weex")
    operation = str(payload.get("operation") or "")
    if operation not in {"account_snapshot", "ohlcv", "fills"}:
        raise ValueError("unsupported operation")
    credentials = _credentials(payload, exchange)
    client = await _build_client(modules, exchange, credentials)
    try:
        await client.load_markets()
        if operation == "fills":
            since = payload.get("since")
            until = payload.get("until")
            fetcher = modules["BitunixFetcher"](client) if exchange == "bitunix" else modules["WeexFetcher"](client)
            events = await fetcher.fetch(
                int(since) if since is not None else None,
                int(until) if until is not None else None,
                {},
            )
            if exchange == "bitunix" and len(events) >= int(getattr(fetcher, "trade_limit", 100)):
                return {"version": modules["version"], "fills": [], "too_many": True}
            if len(events) > MAX_FILLS:
                return {"version": modules["version"], "fills": [], "too_many": True}
            return {"version": modules["version"], "fills": events}
        if operation == "ohlcv":
            symbol = str(payload.get("symbol") or "")
            timeframe = str(payload.get("timeframe") or "1m")
            limit = max(1, min(int(payload.get("limit") or 100), 500))
            since = payload.get("since")
            rows = await client.fetch_ohlcv(symbol, timeframe=timeframe, since=int(since) if since is not None else None, limit=limit)
            return {"version": modules["version"], "ohlcv": rows}

        balance_raw, positions, orders = await asyncio.gather(
            client.fetch_balance(params={"type": "swap"}),
            client.fetch_positions(),
            client.fetch_open_orders(),
        )
        if len(positions) > MAX_POSITIONS:
            raise ValueError(f"position result exceeds maximum of {MAX_POSITIONS} rows")
        if len(orders) > MAX_ORDERS:
            raise ValueError(f"order result exceeds maximum of {MAX_ORDERS} rows")
        symbols = list(dict.fromkeys(str(row.get("symbol") or "") for row in positions if isinstance(row, dict) and row.get("symbol")))
        if len(symbols) > MAX_SYMBOLS:
            raise ValueError("account snapshot has too many open-position symbols")
        if exchange == "bitunix":
            wallet = float(balance_raw["total"]["USDT"])
            if len(symbols) > client.MAX_DEPTH_FALLBACK_SYMBOLS:
                raise ValueError(
                    "Bitunix REST-only account snapshot cannot price more than "
                    f"{client.MAX_DEPTH_FALLBACK_SYMBOLS} open-position symbols"
                )
            tickers = await client.fetch_tickers(symbols) if symbols else {}
        else:
            wallet = _weex_wallet_balance(balance_raw)
            tickers = await _weex_tickers(client, symbols) if symbols else {}
        if not math.isfinite(wallet) or wallet < 0.0:
            raise ValueError("wallet balance is invalid")
        return {
            "version": modules["version"],
            "balance": wallet,
            "positions": positions,
            "orders": orders,
            "tickers": tickers,
        }
    finally:
        await client.close()


def main() -> int:
    try:
        raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError("request is too large")
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("request must be an object")
        with redirect_stdout(sys.stderr):
            response = {"ok": True, "result": asyncio.run(_dispatch(payload))}
    except Exception as exc:
        response = {"ok": False, "error": type(exc).__name__, "detail": str(exc)[:1000]}
    serialized = json.dumps(response, separators=(",", ":"), allow_nan=False)
    if len(serialized.encode("utf-8")) > MAX_RESPONSE_BYTES:
        serialized = json.dumps({"ok": False, "error": "ResponseTooLarge", "detail": "helper response exceeds maximum size"}, separators=(",", ":"))
    sys.stdout.write(serialized + "\n")
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
