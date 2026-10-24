"""Bound cloud daily-shard exports using PB8's optimizer warmup requirements."""

from datetime import date, datetime, timedelta, timezone

SERVICE = 'VastHistory'


def freeze_relative_dates(config):
    """Freeze the worker's relative end dates so queued archives cannot drift."""
    day = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
    bt = config['backtest']
    for item in [bt, *(bt.get('scenarios') or [])]:
        if 'end_date' in item and item['end_date'] in ('now', 'today', '', None):
            item['end_date'] = day


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


def integrity_requirements(config, contexts):
    """Describe minute windows for the pinned suite's shared dataset preloads.

    Keep the daily export envelope above unchanged. Dataset windows, unlike
    export days, exclude base dates when active scenarios supply their own.
    """
    from pb8_config import _call_helper, PB8ConfigurationError
    from vast_scenarios import scenario_plan

    configs = [config, *(context['config'] for context in contexts)]
    try:
        minutes = _call_helper('optimizer_warmup', configs=configs)['minutes']
    except (PB8ConfigurationError, KeyError) as exc:
        raise ValueError('Cannot determine PB8 optimizer warmup for integrity validation') from exc
    if (not isinstance(minutes, list) or len(minutes) != len(configs)
            or any(type(value) is not int or value < 0 for value in minutes)):
        raise ValueError('Invalid PB8 optimizer warmup response')
    bt = config['backtest']
    tolerance = bt.get('gap_tolerance_ohlcvs_minutes')
    import math
    if type(tolerance) not in (int, float) or not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError('Missing or invalid backtest.gap_tolerance_ohlcvs_minutes; prepare a hydrated PB8 config')
    minimum_age = config['live'].get('minimum_coin_age_days')
    if type(minimum_age) not in (int, float) or not math.isfinite(minimum_age) or minimum_age < 0:
        raise ValueError('Missing or invalid live.minimum_coin_age_days; prepare a hydrated PB8 config')
    exchanges = sorted(set(bt['exchanges']) | {ex for c in contexts for ex in c['exchanges']})
    combined = len(exchanges) > 1
    all_exchanges = set(exchanges)
    active = bool(bt.get('suite_enabled') and bt.get('scenarios'))
    groups = {}
    if combined:
        groups['combined'] = [c for c in contexts if set(c['exchanges']) == all_exchanges] or contexts
        for exchange in exchanges:
            consumers = [c for c in contexts if exchange in c['exchanges'] and set(c['exchanges']) != all_exchanges]
            if consumers:
                groups[exchange] = consumers
    else:
        groups[exchanges[0]] = contexts
    result = []
    sources = scenario_plan(config)[1]
    today = datetime.now(timezone.utc).date()

    def date_ms(value):
        """Match pinned format_end_date('now'): the UTC day two days ago."""
        day = today - timedelta(days=2) if value in ('now', 'today', '', None) else date.fromisoformat(value)
        return int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp() * 1000)

    for label, consumers in groups.items():
        selected = consumers if active else [dict(config=config, coins=sorted(set(
            config['live']['approved_coins']['long'] + config['live']['approved_coins']['short'])))]
        start = min(date_ms(c['config']['backtest']['start_date']) for c in selected)
        end = max(date_ms(c['config']['backtest']['end_date']) for c in selected)
        result.append({'label': label, 'exchanges': exchanges if label == 'combined' else [label],
                       'coins': sorted({coin for c in selected for coin in c['coins']}),
                       'start_ts': max(0, start - max(minutes) * 60_000), 'end_ts': end,
                       'warmup_minutes': max(minutes), 'tolerance': tolerance,
                       'minimum_coin_age_days': minimum_age, 'coin_sources': sources,
                       'reference_exchanges': sorted(set(exchanges if label == 'combined' else [label]) | set(sources.values()))})
    return result
