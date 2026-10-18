"""Explicit identities for exact CPU reports, independent of filesystem ordering."""
from __future__ import annotations

from pathlib import Path

SERVICE = 'PB8LoopReports'


def report_contexts(paths, reports, job, read_json):
    """Bind report indexes to native suite labels and requested/effective dates.

    Old observations without a configuration remain explicitly unidentified.
    Never infer a window from an array position or a file modification time.
    """
    config = job.get('config', {})
    backtest = config.get('backtest', {})
    scenarios = backtest.get('scenarios', []) if backtest.get('suite_enabled') else []
    labels = {item['label']: item for item in scenarios if isinstance(item.get('label'), str)}
    if len(labels) != len(scenarios):
        raise ValueError('CPU training windows require unique explicit scenario labels')
    summaries, seen, result = {}, set(), []
    for index, (path, report) in enumerate(zip(paths, reports)):
        path = Path(path)
        summary_path = path.parent.parent / 'suite_summary.json'
        if summary_path not in summaries:
            if summary_path.exists() and not summary_path.is_symlink():
                summary = read_json(summary_path)
                native_windows = (summary.get('suite_metrics') or {}).get('scenarios', {})
                summaries[summary_path] = {
                    label: {**native_windows.get(label, {}), **item}
                    for label, item in summary.get('per_scenario', {}).items()}
            else:
                summaries[summary_path] = {}
        matches = []
        for label, item in summaries[summary_path].items():
            artifact = (item.get('artifacts') or {}).get('analysis.json')
            if not isinstance(artifact, str):
                continue
            resolved = (summary_path.parent / artifact).resolve()
            if not resolved.is_relative_to(summary_path.parent.resolve()):
                raise ValueError('CPU report artifact escapes its suite directory')
            if resolved == path.resolve():
                matches.append(label)
        if not matches and path.parent.name in labels:
            matches = [path.parent.name]
        if scenarios:
            if len(matches) != 1 or matches[0] not in labels:
                raise ValueError('CPU report cannot be attributed to a configured training window')
            label = matches[0]
            if label in seen:
                raise ValueError('Multiple CPU reports belong to the same training window')
            seen.add(label)
            requested = labels[label]
            identity = 'verified_scenario_artifact'
        else:
            label = 'comparison' if backtest else None
            requested = backtest
            identity = 'single_comparison' if backtest else 'unavailable_legacy_context'
        native = summaries[summary_path].get(label, {})
        # Analysis schemas vary; only copy explicit native effective dates.
        effective = native.get('effective_window') or native
        result.append({'report_index': index, 'window_id': requested.get('id', label),
                       'label': label, 'identity': identity,
                       'requested_start_date': requested.get('start_date', backtest.get('start_date')),
                       'requested_end_date': requested.get('end_date', backtest.get('end_date')),
                       'effective_start_date': effective.get('effective_start_date', report.get('effective_start_date')),
                       'effective_end_date': effective.get('effective_end_date', report.get('effective_end_date')),
                       'exchanges': requested.get('exchanges', backtest.get('exchanges', [])),
                       'artifact': path.name if not scenarios else f'{label}/{path.name}'})
    if scenarios and seen != set(labels):
        raise ValueError('CPU reports do not cover every configured training window')
    return result
