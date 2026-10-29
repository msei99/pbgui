"""Calendar-date handling for PB8 config persistence and daily cloud exports."""

from datetime import date, datetime

SERVICE = "ConfigDates"


def parse_config_date(value: str) -> date:
    """Accept an ISO date or datetime without shifting its calendar day."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        if len(value) > 10 and value[10] in ("T", " "):
            return datetime.fromisoformat(value).date()
        raise


def normalize_backtest_dates(config: dict) -> dict:
    """Normalize valid base/scenario datetimes in owned prepared config output.

    Keep relative dates, inherited/missing values and invalid input unchanged;
    native config validation remains responsible for rejecting invalid fields.
    """
    backtest = config.get("backtest")
    if not isinstance(backtest, dict):
        return config
    for section in [backtest, *(backtest.get("scenarios") or [])]:
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
    return config
