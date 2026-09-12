"""Throttled display-only Hyperliquid balances, isolated from transfer preflights."""

from __future__ import annotations

import threading
import time
import hashlib

import requests

from logging_helpers import human_log as _log
from profit_sweep_exchanges import (
    _account_mode, _destination_account_balance, _source_account_balance,
    _spot_usdc_balance, _state_balances, _validated_address,
    _vault_destination_balances, hyperliquid_readonly_info,
)

SERVICE = "ProfitSweep"


class BalanceReadError(RuntimeError):
    """Carry only a safe category, never a URL or exchange response body."""

    def __init__(self, code):
        """Store an allowlisted diagnostic category."""
        super().__init__(code)
        self.code = code


class BalanceReader:
    """Pace all overview requests and interrupt retry waits on API shutdown."""

    def __init__(self):
        """Own request spacing, shared rate-limit cooldown and cancellation state."""
        self.stopping = threading.Event()
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.cooldown = 0.0

    def stop(self):
        """Wake every pacing or retry wait before joining the owning workers."""
        self.stopping.set()

    def _wait(self, seconds):
        """Wait without preventing deterministic worker shutdown."""
        if self.stopping.wait(max(0, seconds)):
            raise BalanceReadError("read_cancelled")

    def _pace(self):
        """Limit overview traffic to at most 2.5 requests per second across accounts."""
        while not self.stopping.is_set():
            with self.lock:
                now = time.monotonic()
                delay = max(self.next_request, self.cooldown) - now
                if delay <= 0:
                    self.next_request = now + 0.4
                    return
            self._wait(delay)
        raise BalanceReadError("read_cancelled")

    def read(self, request_type, **kwargs):
        """Retry only transient failures, with at most three attempts per fixed read."""
        for attempt in range(3):
            self._pace()
            delay = 2 ** attempt
            try:
                return hyperliquid_readonly_info(request_type, timeout_s=10.0, **kwargs)
            except requests.Timeout:
                code, transient = "read_timeout", True
            except requests.exceptions.SSLError:
                code, transient = "read_failed", False
            except requests.ConnectionError:
                code, transient = "connection_error", True
            except requests.HTTPError as exc:
                response = exc.response
                status = response.status_code if response is not None else 0
                code = "rate_limited" if status == 429 else "exchange_unavailable" if status in (500, 502, 503, 504) else "read_failed"
                transient = status in (429, 500, 502, 503, 504)
                if status == 429:
                    try:
                        delay = max(delay, min(60, float(response.headers.get("Retry-After", 2))))
                    except (TypeError, ValueError):
                        delay = max(delay, 2)
                    with self.lock:
                        self.cooldown = max(self.cooldown, time.monotonic() + delay)
            except (ValueError, requests.RequestException):
                code, transient = "invalid_response", False
            if not transient or attempt == 2:
                _log(SERVICE, f"Overview balance read: {request_type}: {code}", level="WARNING")
                raise BalanceReadError(code) from None
            self._wait(delay)
        raise BalanceReadError("read_failed")


def collect_hyperliquid_balances(user, policy, reader, target_cache):
    """Read only balances and mode; never request history, orders, or transfer capability."""
    errors = []
    address = _validated_address(user.wallet_address, "wallet_address")
    snapshot = {"account_kind": "vault" if user.is_vault else "normal", "errors": errors,
                "collected_at_ms": int(time.time() * 1000)}

    def read(kind, *, shared=False, expected=dict, **kwargs):
        """Normalize one narrow read and optionally coalesce shared leader reads."""
        try:
            fetch = lambda: reader.read(kind, **kwargs)
            value = target_cache.read((kind, kwargs["user"].lower()), fetch) if shared else fetch()
            if not isinstance(value, expected):
                raise BalanceReadError("read_failed" if value is None else "invalid_response")
            return value
        except BalanceReadError as exc:
            errors.append({"code": exc.code, "source": kind})
            return None

    if not user.is_vault:
        abstraction = read("userAbstraction", expected=(dict, str), user=address)
        state = read("clearinghouseState", user=address)
        spot_state = read("spotClearinghouseState", user=address)
        mode = _account_mode(abstraction)
        if abstraction is not None and mode != "standard_manual":
            errors.append({"code": "unsupported_account_mode", "source": "userAbstraction"})
        account = _state_balances(state, errors, "clearinghouseState") if state is not None else {}
        spot = _spot_usdc_balance(spot_state) if spot_state is not None else None
        if spot_state is not None and spot is None:
            errors.append({"code": "invalid_response", "source": "spotClearinghouseState"})
        snapshot["account_balances"] = {
            "source": _source_account_balance("Perps", account.get("balance"), account.get("withdrawable"), "USDC"),
            "destination": _destination_account_balance("Spot", (spot or {}).get("total"), "USDC"),
            "max_transferable": account.get("withdrawable"),
        }
    else:
        initial = read("vaultDetails", vault_address=address)
        try:
            leader = _validated_address((initial or {}).get("leader"), "leader")
        except ValueError:
            if initial is not None:
                errors.append({"code": "invalid_response", "source": "vaultDetails"})
            snapshot["account_balances"] = {}
            return snapshot
        details = read("vaultDetails", vault_address=address, user=leader)
        if details is not None and str(details.get("leader") or "").lower() != leader:
            errors.append({"code": "invalid_response", "source": "vaultDetails"})
            details = None
        abstraction = read("userAbstraction", shared=True, expected=(dict, str), user=leader)
        leader_state = read("clearinghouseState", shared=True, user=leader)
        leader_spot = read("spotClearinghouseState", shared=True, user=leader)
        mode = _account_mode(abstraction)
        follower = (details or {}).get("followerState") or {}
        spot = _spot_usdc_balance(leader_spot) if leader_spot is not None else None
        if leader_spot is not None and spot is None:
            errors.append({"code": "invalid_response", "source": "spotClearinghouseState"})
        destination = _vault_destination_balances(mode, (leader_state or {}).get("marginSummary") or {}, spot,
                                                  (leader_state or {}).get("withdrawable")) if abstraction is not None else {}
        snapshot["account_balances"] = {
            "source": _source_account_balance("Vault", follower.get("vaultEquity"), None, "USDC"),
            "destination": destination,
            "max_transferable": (details or {}).get("maxWithdrawable"),
        }
    if mode == "unknown":
        return snapshot
    destinations = snapshot["account_balances"]["destination"]
    for wallet, destination in (destinations.items() if user.is_vault else [("main_spot", destinations)]):
        pool = "unified" if mode in ("unified", "portfolio_margin") else "spot" if wallet == "main_spot" else "perps"
        owner = leader if user.is_vault else address
        destination["overview_key"] = hashlib.sha256(("hyperliquid:" + owner.lower() + ":" + pool).encode()).hexdigest()
    return snapshot
