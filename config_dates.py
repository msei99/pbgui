"""Calendar-day handling for prepared PB8 configs and daily cloud history."""

import copy
from datetime import date, datetime

SERVICE = "ConfigDates"


def parse_config_date(value: str) -> date:
    """Validate an ISO date or extended datetime without shifting its day."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        if len(value) > 10 and value[10] in ("T", " "):
            return datetime.fromisoformat(value).date()
        raise


def normalize_backtest_dates(config: dict) -> dict:
    """Return owned output with valid extended base/scenario datetimes normalized.

    Preserve relative, inherited and invalid fields for existing validation.
    Never mutate native output, even when its sanitizer returns the input itself.
    """
    normalized = copy.deepcopy(config)
    if not isinstance(normalized, dict):
        return normalized
    backtest = normalized.get("backtest")
    if not isinstance(backtest, dict):
        return normalized
    scenarios = backtest.get("scenarios")
    sections = [backtest, *(scenarios if isinstance(scenarios, list) else [])]
    for section in sections:
        if not isinstance(section, dict):
            continue
        for field in ("start_date", "end_date"):
            value = section.get(field)
            if not isinstance(value, str) or len(value) <= 10 or value[10] not in ("T", " "):
                continue
            try:
                section[field] = parse_config_date(value).isoformat()
            except ValueError:
                continue
    return normalized
