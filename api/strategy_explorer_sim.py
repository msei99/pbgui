"""Simulation labels for the FastAPI Strategy Explorer page."""

from __future__ import annotations


def simulation_modes() -> list[dict[str, str]]:
    """Return the public simulation labels used by the FastAPI UI."""
    return [
        {"key": "local_simulation", "label": "PBGui Simulation"},
        {"key": "pb7_engine", "label": "PB7 Backtest Engine"},
    ]
