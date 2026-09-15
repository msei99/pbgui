"""Install a verified PB8 first-timestamp snapshot without exchange access."""

import ast
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile

SERVICE = 'VastWorker'
NAMES = ('first_ohlcv_timestamps_unified.json',
         'first_ohlcv_timestamps_unified_exchange_specific.json',
         'first_ohlcv_timestamps_unified_exchange_specific_symbols.json')


def install(root, raw, checksum, safe_path, source=Path('/opt/passivbot/src/utils.py')):
    """Reject incompatible caches and publish the resolver version last."""
    if hashlib.sha256(raw).hexdigest() != checksum:
        raise ValueError('Inception metadata checksum mismatch')
    data = json.loads(raw)
    version = None
    for node in ast.parse(source.read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'FIRST_OHLCV_TIMESTAMPS_CACHE_VERSION' for t in node.targets):
            version = ast.literal_eval(node.value)
    if type(version) is not int or data.get('version') != version:
        raise ValueError('PB8 first-timestamp cache version mismatch')
    files = data['files']
    if set(files) != set(NAMES) or any(not isinstance(files[name], dict) for name in NAMES):
        raise ValueError('Invalid inception cache files')
    unified, specific, symbols = (files[name] for name in NAMES)
    if not unified or set(unified) != set(specific) or set(unified) != set(symbols):
        raise ValueError('Incomplete inception metadata')
    for coin, first in unified.items():
        values = specific[coin]
        if (not isinstance(values, dict) or not values or set(values) - {'binanceusdm', 'bybit'}
                or not isinstance(symbols[coin], dict) or set(symbols[coin]) != set(values)):
            raise ValueError('Incomplete exchange inception metadata')
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 1262304000000 for v in values.values()):
            raise ValueError('Invalid inception timestamp')
        if first != min(values.values()) or any(not isinstance(v, str) or not v for v in symbols[coin].values()):
            raise ValueError('Invalid inception identity')
    cache = safe_path(root, 'output/caches')
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    for name in (*NAMES, 'first_ohlcv_timestamps_unified.version'):
        target = safe_path(cache, name)
        value = str(version) if name.endswith('.version') else json.dumps(files[name], indent=4)
        fd, temporary = tempfile.mkstemp(dir=cache)
        try:
            with os.fdopen(fd, 'w') as stream:
                stream.write(value)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)


def main():
    """Read only the bounded compressed public metadata stream on pinned SSH."""
    root = Path(sys.argv[1])
    if not re.fullmatch(r'/work/pbgui/jobs/[0-9a-f]{32}', str(root)):
        raise ValueError('Invalid worker job path')
    spec = importlib.util.spec_from_file_location('worker_paths', '/work/pbgui/worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    with gzip.GzipFile(fileobj=sys.stdin.buffer) as stream:
        raw = stream.read(8 * 1024**2 + 1)
    if len(raw) > 8 * 1024**2:
        raise ValueError('Inception payload exceeds limit')
    install(root, raw, sys.argv[2], worker.safe_path)


if __name__ == '__main__':
    main()
