"""Read-only choices for the AI Chat optimization prompt builder."""
import json
import re
from pathlib import Path

from logging_helpers import human_log as _log

SERVICE = 'AILoopTemplates'


def local_markets(root):
    """Read canonical coins from cached mappings without fetching exchange data."""
    result = {}
    root = Path(root)
    if not root.is_dir() or root.is_symlink():
        return result
    for directory in sorted(root.iterdir()):
        if (not directory.is_dir() or directory.is_symlink()
                or not re.fullmatch(r'[a-z0-9_]+', directory.name)):
            continue
        path = directory / 'mapping.json'
        if not path.is_file() or path.is_symlink():
            continue
        try:
            rows = json.loads(path.read_text())
            if not isinstance(rows, list):
                raise ValueError('mapping must be a list')
            coins = sorted({row['coin'] for row in rows if isinstance(row, dict)
                            and isinstance(row.get('coin'), str)
                            and re.fullmatch(r'[A-Za-z0-9._-]{1,40}', row['coin'])
                            and row.get('swap') is True and row.get('linear') is not False
                            and row.get('active') is not False and row.get('quote') == 'USDT'})
            if coins:
                result[directory.name] = coins
        except (OSError, ValueError) as exc:
            _log(SERVICE, f'Cannot read cached market choices for {directory.name}: {type(exc).__name__}', level='WARNING')
    return result
