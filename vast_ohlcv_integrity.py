"""Offline, catalog-backed candle integrity checks before paid Vast work."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from logging_helpers import human_log as _log
from market_data_integrity import validate_daily_files_cached
from vast_exchanges import CCXT_EXCHANGES
from vast_provider import VastError

SERVICE = 'Vast'
PREFLIGHT_VERSION = 1


def file_signature(value) -> tuple[int, int, int]:
    """Bind a validated local file to the bytes about to be copied."""
    return value.st_ino, value.st_size, value.st_mtime_ns


def preflight_shards(config: dict, shards: list[tuple[Path, Path]], market_root: Path,
                     mapping_root: Path) -> dict[Path, tuple[int, int, int]]:
    """Check every exported market in the suite/warmup envelope, without network I/O.

    Use the catalog underlying the Market Data integrity API, not its aggregate
    status: aggregate counts alone cannot prove the selected files are unchanged.
    Only uncatalogued or changed requested days require reading candle arrays.
    """
    from vast_history import history_window
    from vast_inception import CACHE_FILES, load_local_inception, required_markets
    from vast_scenarios import scenario_plan

    contexts, _, errors = scenario_plan(config)
    if errors:
        raise VastError('Cannot determine OHLCV integrity window', 422)
    first_day, last_day = history_window(config, contexts)
    markets = required_markets({'files': [{'path': str(Path('ohlcv') / relative)}
                                        for _, relative in shards]}, mapping_root)
    # This cache is already used by the pre-rental metadata gate. It is optional
    # here: without verified listing metadata, require the full requested range.
    try:
        first_candles = load_local_inception(markets)['files'][CACHE_FILES[1]]
    except VastError:
        first_candles = {}
        _log(SERVICE, 'No verified local inception metadata; requiring full requested OHLCV coverage', level='DEBUG')
    selected = {}
    for original, relative in shards:
        selected.setdefault((relative.parts[0], relative.parts[2]), {}).setdefault(relative.stem, []).append(original)
    issues, checked = [], {}
    now = datetime.now(timezone.utc)
    today = now.date()
    last_closed_minute = (int(now.timestamp()) // 60 - 1) * 60_000
    for exchange, coins in sorted(markets.items()):
        for coin, symbol in sorted(coins.items()):
            storage_symbol = symbol.replace('/', '_')
            existing = selected[(exchange, storage_symbol)]
            folder = market_root / CCXT_EXCHANGES[exchange] / '1m' / storage_symbol
            inception_ts = first_candles.get(coin, {}).get(CCXT_EXCHANGES[exchange])
            inception_day = (datetime.fromtimestamp(inception_ts / 1000, timezone.utc).date()
                             if inception_ts is not None else None)
            paths = []
            day = max(date.fromisoformat(first_day), inception_day or date.min)
            end = date.fromisoformat(last_day)
            while day <= end:
                day_s = day.isoformat()
                paths.extend(existing.get(day_s, [folder / (day_s + '.npz')]))
                day += timedelta(days=1)
            for path, validation, stable_stat in validate_daily_files_cached(
                exchange=CCXT_EXCHANGES[exchange], coin=storage_symbol,
                paths=paths, db_path=market_root / 'checksums.sqlite', inception_ts=inception_ts,
            ):
                missing = validation.missing_minutes
                invalid = not validation.valid or (validation.status == 'source_gap' and missing >= 120)
                if validation.first_ts is not None and inception_ts is not None and path.stem == inception_day.isoformat():
                    missing_prefix = max(0, (validation.first_ts - inception_ts) // 60_000)
                    if missing_prefix:
                        invalid = True
                        missing += missing_prefix
                # A current-day suffix is expected; a missing prefix is not.
                if validation.status == 'current' and validation.first_ts is not None:
                    start = int(datetime.combine(date.fromisoformat(path.stem), datetime.min.time(),
                                                 tzinfo=timezone.utc).timestamp() * 1000)
                    if inception_ts is not None:
                        start = max(start, inception_ts)
                    missing = max(0, (validation.first_ts - start) // 60_000)
                    if validation.last_ts is not None:
                        missing += max(0, (last_closed_minute - validation.last_ts) // 60_000)
                    invalid = invalid or missing > 0 or date.fromisoformat(path.stem) > today
                if invalid:
                    detail = f'{missing} missing minutes'
                    if not missing and validation.error:
                        detail += '; ' + validation.error
                    issues.append(f'{exchange}/{coin} {path.stem} ({detail})')
                elif stable_stat is not None:
                    checked[path] = file_signature(stable_stat)
    if issues:
        message = 'OHLCV integrity check failed: ' + ', '.join(issues) + ' — repair data before renting'
        _log(SERVICE, message, level='WARNING')
        raise VastError(message, 422)
    return checked


def _manifest_fingerprint(manifest: dict) -> str:
    """Bind successful validation to the exact frozen config and shard hashes."""
    value = {'config_sha256': manifest.get('config_sha256'), 'files': manifest.get('files')}
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def certify_manifest(manifest: dict) -> None:
    """Persist proof only after validated files were copied unchanged and hashed."""
    manifest['ohlcv_integrity'] = {'version': PREFLIGHT_VERSION, 'fingerprint': _manifest_fingerprint(manifest)}


def require_integrity_preflight(manifest: dict) -> None:
    """Never rent for unchecked legacy snapshots or mismatched immutable inputs."""
    proof = manifest.get('ohlcv_integrity')
    if (not isinstance(proof, dict) or proof.get('version') != PREFLIGHT_VERSION
            or proof.get('fingerprint') != _manifest_fingerprint(manifest)):
        raise VastError('OHLCV integrity preflight is missing or outdated; prepare the job again before renting', 422)
