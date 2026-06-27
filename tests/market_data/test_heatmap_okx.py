"""Regression tests for OKX market-data heatmap views."""

from __future__ import annotations

from typing import Any

from api import heatmap


def test_okx_1m_overview_uses_source_index(monkeypatch) -> None:
    """OKX 1m overview should render from source-index day counts."""

    calls: list[dict[str, Any]] = []

    def fake_counts(**kwargs: Any) -> dict[str, dict[str, int]]:
        """Return a tiny OKX source-index coverage sample."""

        calls.append(dict(kwargs))
        return {
            "20260601": {"api": 1440},
            "20260602": {"api": 720, "other_exchange": 0},
        }

    monkeypatch.setattr(
        "market_data_sources.get_daily_source_counts_for_range",
        fake_counts,
    )

    payload = heatmap.get_heatmap_overview(exchange="okx", dataset="1m", coin="BTC")

    assert payload["error"] is None
    assert payload["figure"]
    assert "api" in payload["legend_html"]
    assert calls == [
        {
            "exchange": "okx",
            "coin": "BTC",
            "start_day": None,
            "end_day": None,
            "lag_minutes": 30,
            "cutoff_ts_ms": None,
        }
    ]
