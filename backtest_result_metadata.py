"""Pure result fingerprints and cache-row decoding shared by owned readers."""

import json
from pathlib import Path
import stat

_CONFIG_NAMES = ("config.json", "analysis_config.json", "config_used.json", "backtest_config.json")

def _file_signature(path: Path) -> tuple[int, int, int]:
    """Return a cheap identity tuple for one regular file."""
    try:
        stat_result = path.lstat()
        if not stat.S_ISREG(stat_result.st_mode):
            return (0, 0, 0)
        return (int(stat_result.st_ino or 0), int(stat_result.st_size), int(stat_result.st_mtime_ns))
    except OSError:
        return (0, 0, 0)


def result_signature(analysis_path: Path, *, gui_version: str, summary_schema: int, version: str = '') -> str:
    """Fingerprint summary-bearing files without reading their contents."""
    result_dir = analysis_path.parent
    values = [
        ["pbgui_version", gui_version],
        ["summary_schema", summary_schema],
        [analysis_path.name, *_file_signature(analysis_path)],
    ]
    if version == 'v9':
        values.append(['pb9_result_envelope', 1])
    for name in (*_CONFIG_NAMES, "config.original.json", "pbgui.source.json", "balance_and_equity.npz", "result.json"):
        candidate = result_dir / name
        signature = _file_signature(candidate)
        if signature != (0, 0, 0):
            values.append([name, *signature])
    return json.dumps(values, separators=(",", ":"))


def cached_payloads(connection, version, signatures):
    """Decode only matching requested rows from an existing SQLite connection."""
    payloads = {}
    paths = list(signatures)
    for start in range(0, len(paths), 200):
        batch = paths[start:start + 200]
        rows = connection.execute(
            'SELECT result_path, signature, payload FROM result_summaries WHERE version = ? '
            + 'AND result_path IN (' + ','.join('?' for _ in batch) + ')', (version, *batch))
        for path, signature, raw in rows:
            if signature != signatures[path]:
                continue
            try:
                value = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(value, dict):
                payloads[path] = value
    return payloads
