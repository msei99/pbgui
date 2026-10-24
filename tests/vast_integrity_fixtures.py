"""Synthetic proof envelopes for controller unit tests, without data admission I/O."""

from secure_files import ensure_private_directory
import time
from vast_jobs import REVISION, digest, write_json
from vast_ohlcv_integrity import seal_manifest, signature


def freeze_test_proof(store, identifier):
    """Bind a unit fixture's existing files/archive to the new immutable contract.

    This helper only describes synthetic controller inputs. Actual preparation,
    decoding and worker parity are exercised by test_vast_ohlcv_integrity.
    """
    folder = ensure_private_directory(store.directory(identifier) / 'input')
    manifest = store.read(identifier, 'input/manifest.json') if (folder / 'manifest.json').exists() else {'files': []}
    manifest.update(pb8_revision=REVISION, config_sha256=digest(folder / 'optimize.json'),
        ohlcv_requirements=[{'synthetic_controller_input': True}],
        metadata_snapshot={'markets': {'binance': {'markets': {}, 'fetched_at': time.time()}}})
    manifest['ohlcv_integrity'] = seal_manifest(manifest)
    write_json(folder / 'manifest.json', manifest)
    archive = folder.parent / 'input.tar.gz'
    if not archive.exists():
        archive.write_bytes(b'synthetic archive')
    intent_path = folder.parent / 'intent.json'
    intent = store.read(identifier, 'intent.json') if intent_path.exists() else {}
    intent.update(manifest_sha256=digest(folder / 'manifest.json'), bundle_sha256=digest(archive), bundle_bytes=archive.stat().st_size)
    write_json(intent_path, intent)


def selection_only_boundaries(monkeypatch):
    """Mock admission infrastructure for old scenario selection-only fixtures."""
    monkeypatch.setattr('vast_market_cache._load_local_markets', lambda exchanges: {
        ex: {'markets': {f'{coin}/{quote}:{quote}': {} for coin in ('BTC', 'SOL', 'ETH')
                        for quote in ('USDT', 'USDC')}, 'fetched_at': 1} for ex in exchanges})
    monkeypatch.setattr('vast_inception.ensure_local_inception', lambda markets: {'synthetic': True})
    monkeypatch.setattr('vast_ohlcv_integrity.validate_export', lambda shards, *args: {
        str(relative): {'signature': list(signature(path)), 'sha256': digest(path)} for path, relative in shards})
