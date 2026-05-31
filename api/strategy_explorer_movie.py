"""Movie Builder helpers for the FastAPI Strategy Explorer page."""

from __future__ import annotations


def movie_builder_status() -> dict[str, object]:
    """Return the Movie Builder page status."""
    return {
        "available": True,
        "message": "Build replay frames from the selected candle window, then export from the browser.",
    }
