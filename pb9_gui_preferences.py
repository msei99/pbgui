"""Bounded non-secret editor preferences shared by native PB9 workflows."""
from __future__ import annotations

import math

SERVICE = "PB9Config"


def filter_preferences(raw: dict) -> dict:
    """Validate the same filter values for Run, Backtest and their drafts."""
    result = {}
    for key in ("market_cap", "vol_mcap"):
        if key not in raw:
            continue
        value = raw[key]
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 10**12:
            raise ValueError("Invalid PB9 editor filter value")
        result[key] = value
    for key in ("only_cpt", "notices_ignore"):
        if key in raw:
            if type(raw[key]) is not bool:
                raise ValueError("Invalid PB9 editor filter flag")
            result[key] = raw[key]
    if "tags" in raw:
        tags = raw["tags"]
        if (not isinstance(tags, list) or len(tags) > 200
                or any(not isinstance(tag, str) or not tag or len(tag) > 128
                       or any(ord(c) < 32 or ord(c) == 127 for c in tag) for tag in tags)):
            raise ValueError("Invalid PB9 editor filter tags")
        result["tags"] = list(tags)
    return result


def backtest_preferences(document: dict) -> dict:
    """Extract only safe GUI preferences, excluding scheduling and credentials."""
    gui = document.get("pbgui", {})
    if not isinstance(gui, dict):
        raise ValueError("PB9 GUI metadata must be an object")
    result = filter_preferences(gui)
    if "note" in gui:
        note = gui["note"]
        if not isinstance(note, str) or len(note) > 4096 or "\x00" in note:
            raise ValueError("Invalid PB9 editor note")
        result["note"] = note
    return result
