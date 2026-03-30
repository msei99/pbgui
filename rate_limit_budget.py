"""Token-bucket rate limiter for exchange API weight budgets.

Each exchange has an IP-based weight limit (e.g. Hyperliquid: 1200/min).
The budget tracks consumed weight across ALL request types from all pollers
and automatically paces requests to stay within limits.

Math: for a sliding-window limit of W weight per 60 s, we set:
    refill_rate = (W − burst_capacity) / 60
So burst_capacity + refill_rate × 60 == W — even after a full burst the
60 s sliding-window budget is never exceeded.

Usage::

    budget = RateLimitBudget(weight_per_minute=1200, burst_capacity=120)
    if await budget.acquire(weight=20):
        # make the API call
        ...
"""

import asyncio
import time


class RateLimitBudget:
    """Token-bucket rate limiter for a per-IP weight-per-minute budget.

    Parameters
    ----------
    weight_per_minute : int
        Maximum total weight allowed in any 60 s sliding window.
    burst_capacity : int
        Maximum tokens that can accumulate (= max instant burst).
        Must be < weight_per_minute.  The refill rate is derived as
        ``(weight_per_minute - burst_capacity) / 60``.
    """

    __slots__ = (
        'weight_per_minute', 'burst_capacity', 'refill_per_second',
        'tokens', '_last_refill', '_lock',
        'total_consumed', 'total_waited_ms', 'waits_count', 'requests_count',
        '_per_op',
    )

    def __init__(self, weight_per_minute: int = 1200, burst_capacity: int = 120):
        self.weight_per_minute = weight_per_minute
        self.burst_capacity = burst_capacity
        self.refill_per_second = max(0.1, (weight_per_minute - burst_capacity) / 60.0)
        self.tokens = float(burst_capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        # Cumulative metrics (since creation)
        self.total_consumed = 0
        self.total_waited_ms = 0
        self.waits_count = 0
        self.requests_count = 0
        # Per-operation breakdown: op -> {consumed, requests, waits, wait_ms}
        self._per_op: dict = {}

    # ── internal ────────────────────────────────────────────

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self.tokens = min(
                self.burst_capacity,
                self.tokens + elapsed * self.refill_per_second,
            )
            self._last_refill = now

    # ── public API ──────────────────────────────────────────

    async def acquire(self, weight: int = 1, timeout: float = 60.0, tag: str = '') -> bool:
        """Wait until *weight* tokens are available, then consume them.

        Returns ``True`` if tokens were consumed, ``False`` on timeout.
        *tag* is an optional operation name used for per-op breakdown metrics.
        """
        deadline = time.monotonic() + timeout
        local_waits = 0
        local_wait_ms = 0
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= weight:
                    self.tokens -= weight
                    self.total_consumed += weight
                    self.requests_count += 1
                    if tag:
                        op = self._per_op.setdefault(tag, {'consumed': 0, 'requests': 0, 'waits': 0, 'wait_ms': 0})
                        op['consumed'] += weight
                        op['requests'] += 1
                        op['waits'] += local_waits
                        op['wait_ms'] += local_wait_ms
                    return True
                # How long until enough tokens refill?
                deficit = weight - self.tokens
                wait = deficit / self.refill_per_second

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            sleep_time = min(wait + 0.02, remaining)
            t0 = time.monotonic()
            await asyncio.sleep(sleep_time)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            self.total_waited_ms += elapsed_ms
            self.waits_count += 1
            local_waits += 1
            local_wait_ms += elapsed_ms

    def peek(self) -> dict:
        """Return current budget state for metrics/GUI (read-only estimate)."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        estimated = min(
            self.burst_capacity,
            self.tokens + elapsed * self.refill_per_second,
        )
        return {
            'tokens': round(estimated, 1),
            'capacity': self.burst_capacity,
            'weight_per_minute': self.weight_per_minute,
            'refill_per_second': round(self.refill_per_second, 1),
            'total_consumed': self.total_consumed,
            'total_waited_ms': self.total_waited_ms,
            'waits_count': self.waits_count,
            'requests_count': self.requests_count,
            'per_operation': dict(self._per_op),
        }


# ── Per-exchange rate limit configurations ───────────────────
#
# Sources:
# - Hyperliquid: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
#
# burst_capacity is chosen so that capacity + refill_rate × 60 == weight_per_minute.
# Example: HL → 120 + 18×60 = 1200 ✓

EXCHANGE_RATE_LIMITS: dict[str, dict] = {
    'hyperliquid': {
        'weight_per_minute': 1200,
        'burst_capacity': 120,
    },
    # Future exchanges can be added here:
    # 'bybit': {'weight_per_minute': 600, 'burst_capacity': 60},
}


# ── Per-operation weight estimates ───────────────────────────
#
# Hyperliquid weight classes (from docs):
#   Weight 2:  l2Book, allMids, clearinghouseState, orderStatus,
#              spotClearinghouseState, exchangeStatus
#   Weight 20: all other documented info requests
#   Variable:  +1 per 20 items (userFills, userFunding, …)
#              +1 per 60 items (candleSnapshot)

OPERATION_WEIGHTS: dict[tuple[str, str], int] = {
    # Hyperliquid
    ('hyperliquid', 'fetch_balance'):       2,      # clearinghouseState
    ('hyperliquid', 'fetch_positions'):     2,      # clearinghouseState / userState
    ('hyperliquid', 'fetch_open_orders'):   20,     # openOrders (per-symbol!)
    ('hyperliquid', 'fetch_history'):       40,     # userFunding + fetch_my_trades (1 page each in normal incremental polling)
    ('hyperliquid', 'fetch_executions'):    20,     # userFillsByTime (1 page in normal incremental polling)
    ('hyperliquid', 'candle_snapshot'):     44,     # one candleSnapshot call: 20 base + ceil(1440/60)=24 for a full day of 1m candles
    ('hyperliquid', 'all_mids'):            2,      # allMids
    ('hyperliquid', 'meta'):                20,     # meta
}

DEFAULT_WEIGHT = 20


def get_weight(exchange: str, operation: str) -> int:
    """Return the estimated rate-limit weight for an operation on an exchange."""
    return OPERATION_WEIGHTS.get((exchange, operation), DEFAULT_WEIGHT)
