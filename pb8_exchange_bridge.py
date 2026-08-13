"""Synchronous read-only facade for PB8-native Bitunix and WEEX clients."""

from __future__ import annotations

import copy
import json
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pbgui_purefunc import PBGDIR, pb8_runtime_status


class PB8ExchangeError(RuntimeError):
    """Raised when a PB8-native read-only exchange request fails."""


class PB8ExchangeInstance:
    """Expose a small CCXT-like read surface backed by isolated PB8 clients."""

    _SNAPSHOT_TTL_SECONDS = 10.0
    _MAX_RESPONSE_BYTES = 16 * 1024 * 1024
    _FILL_WINDOW_MS = 24 * 60 * 60 * 1000

    def __init__(self, exchange_id: str, user):
        self.id = str(exchange_id).lower()
        self._user = user
        self._snapshot = None
        self._snapshot_deadline = 0.0
        self._lock = threading.RLock()

    def _secrets(self) -> list[str]:
        return [str(value) for value in (getattr(self._user, "key", ""), getattr(self._user, "secret", ""), getattr(self._user, "passphrase", "")) if value]

    def _redact(self, value: str) -> str:
        result = str(value or "")
        for secret in self._secrets():
            result = result.replace(secret, "[redacted]")
        return result[-1000:]

    def _call(self, operation: str, **payload) -> dict:
        lease = None
        try:
            lease = acquire_master_runtime_lock(Path(PBGDIR))
            runtime = pb8_runtime_status()
            if not runtime.get("ready"):
                raise PB8ExchangeError("PB8 runtime is unavailable")
            request = {
                "pb8_dir": runtime["pb8dir"],
                "operation": operation,
                "exchange": self.id,
                "credentials": {
                    "key": str(getattr(self._user, "key", "") or ""),
                    "secret": str(getattr(self._user, "secret", "") or ""),
                    "passphrase": str(getattr(self._user, "passphrase", "") or ""),
                },
                **payload,
            }
            helper = Path(__file__).resolve().with_name("pb8_exchange_helper.py")
            request_text = json.dumps(request, separators=(",", ":"), allow_nan=False)
            with tempfile.TemporaryFile(mode="w+b") as output:
                proc = subprocess.Popen(
                    [runtime["pb8venv"], str(helper)],
                    cwd=runtime["pb8dir"],
                    stdin=subprocess.PIPE,
                    stdout=output,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    close_fds=True,
                )
                try:
                    proc.communicate(input=request_text, timeout=90)
                except subprocess.TimeoutExpired as exc:
                    proc.kill()
                    proc.wait()
                    raise PB8ExchangeError("PB8 exchange request timed out") from exc
                size = output.tell()
                if size > self._MAX_RESPONSE_BYTES:
                    raise PB8ExchangeError("PB8 exchange helper response exceeded the size limit")
                output.seek(0)
                stdout = output.read().decode("utf-8")
            returncode = proc.returncode
        except MasterUpdateBusyError as exc:
            raise PB8ExchangeError("PB8 is being updated; retry later") from exc
        except OSError as exc:
            raise PB8ExchangeError("PB8 exchange request failed") from exc
        finally:
            if lease is not None:
                lease.release()
        try:
            response = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise PB8ExchangeError("PB8 exchange helper returned an invalid response") from exc
        if returncode != 0 or not response.get("ok"):
            raise PB8ExchangeError(self._redact(response.get("detail") or "PB8 exchange request failed"))
        result = response.get("result")
        if not isinstance(result, dict):
            raise PB8ExchangeError("PB8 exchange helper returned invalid data")
        return result

    def _account_snapshot(self) -> dict:
        with self._lock:
            now = time.monotonic()
            if self._snapshot is None or now >= self._snapshot_deadline:
                self._snapshot = self._call("account_snapshot")
                self._snapshot_deadline = time.monotonic() + self._SNAPSHOT_TTL_SECONDS
            return copy.deepcopy(self._snapshot)

    def fetch_balance(self, params=None) -> dict:
        balance = float(self._account_snapshot()["balance"])
        return {"USDT": {"total": balance}, "total": {"USDT": balance}}

    def fetch_positions(self, symbols=None, params=None) -> list[dict]:
        positions = self._account_snapshot().get("positions") or []
        if symbols:
            wanted = set(symbols)
            positions = [row for row in positions if row.get("symbol") in wanted]
        return positions

    def fetch_open_orders(self, symbol=None, since=None, limit=None, params=None) -> list[dict]:
        orders = self._account_snapshot().get("orders") or []
        if symbol:
            orders = [row for row in orders if row.get("symbol") == symbol]
        return orders

    def fetch_tickers(self, symbols=None, params=None) -> dict:
        tickers = self._account_snapshot().get("tickers") or {}
        if symbols:
            return {symbol: tickers[symbol] for symbol in symbols if symbol in tickers}
        return tickers

    def fetch_ticker(self, symbol: str) -> dict:
        ticker = self.fetch_tickers([symbol]).get(symbol)
        if ticker is None:
            raise PB8ExchangeError(f"Ticker unavailable for {symbol}")
        return ticker

    def fetch_ohlcv(self, symbol: str, timeframe="1m", since=None, limit=None, params=None) -> list:
        return self._call("ohlcv", symbol=symbol, timeframe=timeframe, since=since, limit=limit or 100).get("ohlcv") or []

    @staticmethod
    def _fee_cost(value) -> float:
        if isinstance(value, dict):
            return float(value.get("cost") or 0.0)
        if isinstance(value, list):
            return sum(PB8ExchangeInstance._fee_cost(item) for item in value)
        return 0.0

    def fetch_fills(self, since=None, until=None) -> list[dict]:
        if since is None or until is None:
            result = self._call("fills", since=since, until=until)
            if result.get("too_many"):
                raise PB8ExchangeError("PB8 fill range is too large to page without explicit bounds")
            return result.get("fills") or []

        start = int(since)
        end = int(until)
        if end < start:
            return []
        events = []
        windows = []
        cursor = start
        while cursor <= end:
            window_end = min(end, cursor + self._FILL_WINDOW_MS - 1)
            windows.append((cursor, window_end))
            cursor = window_end + 1
        while windows:
            window_start, window_end = windows.pop(0)
            result = self._call("fills", since=window_start, until=window_end)
            if result.get("too_many"):
                if window_start >= window_end:
                    raise PB8ExchangeError("PB8 fill volume exceeds the safe per-millisecond page size")
                midpoint = (window_start + window_end) // 2
                windows[0:0] = [(window_start, midpoint), (midpoint + 1, window_end)]
                continue
            events.extend(result.get("fills") or [])
        deduplicated = {}
        for event in events:
            key = str(event.get("id") or "") if isinstance(event, dict) else ""
            if key:
                deduplicated[key] = event
        return sorted(
            deduplicated.values(),
            key=lambda event: (int(event.get("timestamp") or 0), str(event.get("id") or "")),
        )

    def fetch_income(self, since=None) -> list[dict]:
        result = []
        for event in self.fetch_fills(since=since, until=self.milliseconds()):
            symbol = str(event.get("symbol") or "")
            compact = symbol.split(":", 1)[0].replace("/", "").replace("-", "")
            result.append({
                "symbol": compact,
                "timestamp": int(event["timestamp"]),
                "income": float(event["pnl"]) - self._fee_cost(event.get("fees")),
                "uniqueid": f"{self.id}:fill:{event['id']}",
            })
        return result

    def fetch_executions(self, since=None) -> list[dict]:
        result = []
        for event in self.fetch_fills(since=since, until=self.milliseconds()):
            result.append({
                "symbol": event["symbol"],
                "timestamp": int(event["timestamp"]),
                "side": event["side"],
                "price": float(event["price"]),
                "qty": float(event["qty"]),
                "fee": self._fee_cost(event.get("fees")),
                "realized_pnl": float(event["pnl"]),
                "order_id": str(event["order_id"]),
                "trade_id": str(event["id"]),
                "raw_json": json.dumps(event.get("raw") or [], separators=(",", ":"), allow_nan=False),
            })
        return result

    def milliseconds(self) -> int:
        return int(time.time() * 1000)

    def close(self) -> None:
        with self._lock:
            self._snapshot = None
            self._snapshot_deadline = 0.0
