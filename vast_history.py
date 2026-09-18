"""Bound cloud daily-shard exports using PB8's optimizer warmup requirements."""

from datetime import date, datetime, timedelta, timezone

SERVICE = 'VastHistory'


def history_window(config, contexts):
    """Match the suite preload envelope, including base dates and native warmup."""
    from pb8_config import _call_helper, PB8ConfigurationError

    configs = [config, *(context['config'] for context in contexts)]
    starts, ends = [], []
    for item in configs:
        bt = item['backtest']
        try:
            start = date.fromisoformat(bt['start_date'])
            end = (datetime.now(timezone.utc).date() if bt['end_date'] == 'now'
                   else date.fromisoformat(bt['end_date']))
        except (KeyError, ValueError, TypeError):
            raise ValueError('Cloud export requires valid start_date and end_date') from None
        if start > end:
            raise ValueError('Cloud export start_date must not be after end_date')
        starts.append(start)
        ends.append(end)
    try:
        values = _call_helper('optimizer_warmup', configs=configs)['minutes']
    except (PB8ConfigurationError, KeyError) as exc:
        raise ValueError('Cannot determine PB8 optimizer warmup; input export stopped') from exc
    if (not isinstance(values, list) or len(values) != len(configs)
            or any(type(value) is not int or value < 0 for value in values)):
        raise ValueError('Invalid PB8 optimizer warmup response')
    # The suite preloads a shared continuous envelope before slicing scenarios.
    # Use its largest warmup, rounded outward to whole daily shard boundaries.
    try:
        first = min(starts) - timedelta(days=(max(values) + 1439) // 1440)
    except OverflowError:
        raise ValueError('PB8 optimizer warmup exceeds the supported date range') from None
    return first.isoformat(), max(ends).isoformat()
