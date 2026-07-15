"""Contract tests for Strategy Explorer parity with the PB7 backtester."""

import json

import pandas as pd
import pytest

from api import strategy_explorer_core as core


@pytest.mark.parametrize(
    ("maker_override", "taker_override", "expected"),
    [
        (0.0, None, (0.0, 0.00055)),
        (None, 0.0, (0.0002, 0.0)),
        (-0.0001, None, (-0.0001, 0.00055)),
    ],
)
def test_fee_overrides_preserve_zero_and_fall_back_only_for_null(
    maker_override: float | None,
    taker_override: float | None,
    expected: tuple[float, float],
) -> None:
    """Zero is a valid PB7 fee override while null selects exchange fees."""
    config = {
        "backtest": {
            "maker_fee_override": maker_override,
            "taker_fee_override": taker_override,
        }
    }

    assert core._resolve_backtest_fee_overrides(config, 0.0002, 0.00055) == pytest.approx(expected)


def test_pnl_lookback_tracker_matches_pb7_window_expiry() -> None:
    """Rolling peak and current PnL use only fills inside the active bar window."""
    tracker = core._PnlLookbackTracker(1.0 / 1440.0)
    tracker.record(0, 10.0)
    tracker.record(1, -4.0)

    assert tracker.effective(1) == pytest.approx((10.0, 6.0))
    assert tracker.effective(2) == pytest.approx((0.0, -4.0))


def test_pnl_lookback_tracker_includes_fees_and_supports_all_history() -> None:
    """The canonical all value retains PB7's full-history net-PnL semantics."""
    tracker = core._PnlLookbackTracker("all")
    tracker.record(0, -1.0)
    tracker.record(1, 6.0)
    tracker.record(2, -2.0)

    assert tracker.effective(100_000) == pytest.approx((5.0, 3.0))


def test_orchestrator_risk_state_uses_raw_balance_and_effective_pnl() -> None:
    """PBGui sends the complete PB7 realized-loss gate state."""
    tracker = core._PnlLookbackTracker(-1.0)
    tracker.record(0, 12.0)
    tracker.record(1, -5.0)

    assert core._orchestrator_risk_state(975.0, 0.05, tracker, 1) == pytest.approx(
        {
            "balance_raw": 975.0,
            "max_realized_loss_pct": 0.05,
            "realized_pnl_cumsum_max": 12.0,
            "realized_pnl_cumsum_last": 7.0,
        }
    )


def test_pair_simulation_sends_complete_realized_loss_state_to_orchestrator() -> None:
    """The shared simulation core includes every PB7 loss-gate payload field."""
    captured: list[dict] = []

    class FakeRust:
        """Capture orchestrator input while providing the one required cost helper."""

        @staticmethod
        def qty_to_cost(qty: float, price: float, c_mult: float) -> float:
            """Return linear-contract cost for the simulation setup."""
            return abs(qty) * price * c_mult

        @staticmethod
        def compute_ideal_orders_json(payload: str) -> str:
            """Capture the decoded request and return no orders."""
            captured.append(json.loads(payload))
            return json.dumps({"orders": []})

    candles = pd.DataFrame(
        {
            "open": [100.0, 100.0],
            "high": [101.0, 101.0],
            "low": [99.0, 99.0],
            "close": [100.0, 100.0],
            "volume": [1.0, 1.0],
        },
        index=pd.date_range("2026-01-01", periods=2, freq="min", tz="UTC"),
    )

    core._simulate_backtest_over_historical_candles_pair(
        pbr=FakeRust(),
        pb7_src="",
        candles=candles,
        exchange_params=core.ExchangeParams(min_qty=0.001, min_cost=1.0, qty_step=0.001, price_step=0.1),
        bot_params_long=core.BotParams(),
        bot_params_short=core.BotParams(),
        starting_position_long=core.Position(),
        starting_position_short=core.Position(),
        balance=1_000.0,
        max_realized_loss_pct=0.05,
        max_candles=2,
    )

    assert len(captured) == 1
    assert captured[0]["balance_raw"] == pytest.approx(1_000.0)
    global_state = captured[0]["global"]
    assert {
        key: global_state[key]
        for key in (
            "max_realized_loss_pct",
            "realized_pnl_cumsum_max",
            "realized_pnl_cumsum_last",
        )
    } == pytest.approx(
        {
            "max_realized_loss_pct": 0.05,
            "realized_pnl_cumsum_max": 0.0,
            "realized_pnl_cumsum_last": 0.0,
        }
    )


def test_pb7_engine_receives_edge_parity_backtest_parameters(monkeypatch) -> None:
    """Mode C forwards the same fee and risk settings used by PB7 itself."""
    captured: dict = {}

    class FakeRust:
        """Capture the PB7 backtest bundle parameters without running Rust."""

        @staticmethod
        def HlcvsBundle(*args):
            """Return an opaque bundle placeholder."""
            return args

        @staticmethod
        def run_backtest_bundle(bundle, bot_params, exchange_params, backtest_params):
            """Capture parameters and return an empty fills array."""
            captured["bot_params"] = bot_params
            captured.update(backtest_params)
            return ([], None, None)

    candles = pd.DataFrame(
        {
            "open": [100.0] * 6,
            "high": [101.0] * 6,
            "low": [99.0] * 6,
            "close": [100.0] * 6,
            "volume": [1.0] * 6,
        },
        index=pd.date_range("2026-01-01", periods=6, freq="min", tz="UTC"),
    )
    config = {
        "backtest": {
            "maker_fee_override": 0.0,
            "taker_fee_override": -0.0001,
        },
        "live": {
            "max_realized_loss_pct": 0.05,
            "pnls_max_lookback_days": "all",
        },
    }
    monkeypatch.setattr(core, "load_historical_ohlcv_v7", lambda *args, **kwargs: pd.DataFrame())

    core._run_pb7_engine_backtest_for_visualizer(
        pbr=FakeRust(),
        exchange="bybit",
        coin="ETHUSDT",
        analysis_time=candles.index[0],
        hist_df=candles,
        exchange_params=core.ExchangeParams(min_qty=0.01, min_cost=1.0, qty_step=0.01, price_step=0.01),
        bot_params_long=core.BotParams(),
        bot_params_short=core.BotParams(),
        starting_balance=1_000.0,
        max_candles_forward=6,
        config=config,
        warmup_minutes_override=0,
    )

    assert captured["maker_fee"] == pytest.approx(0.0)
    assert captured["taker_fee"] == pytest.approx(-0.0001)
    assert captured["max_realized_loss_pct"] == pytest.approx(0.05)
    assert captured["pnls_max_lookback_days"] == pytest.approx(-1.0)
    assert captured["bot_params"][0]["long"]["hsl_tier_ratios"] == {
        "yellow": pytest.approx(0.5),
        "orange": pytest.approx(0.75),
    }
