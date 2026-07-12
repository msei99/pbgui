"""Tests for exchange rate-limit budget configuration."""

import asyncio

from rate_limit_budget import EXCHANGE_RATE_LIMITS, RateLimitBudget, get_weight


def test_hyperliquid_burst_capacity_covers_four_day_candle_snapshot() -> None:
    cfg = EXCHANGE_RATE_LIMITS["hyperliquid"]
    required_weight = get_weight("hyperliquid", "candle_snapshot") * 4

    assert int(cfg["burst_capacity"]) >= required_weight


def test_hyperliquid_budget_can_acquire_four_day_candle_snapshot_immediately() -> None:
    cfg = EXCHANGE_RATE_LIMITS["hyperliquid"]
    required_weight = get_weight("hyperliquid", "candle_snapshot") * 4
    budget = RateLimitBudget(**cfg)

    acquired = asyncio.run(budget.acquire(weight=required_weight, timeout=0.01, tag="candle_snapshot_4d"))

    assert acquired is True