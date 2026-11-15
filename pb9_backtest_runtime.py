"""Static source identity and native import checks for independent PB9 jobs."""
from pathlib import Path
import importlib
import json
import os
import sys
import subprocess

from passivbot_runtime import classify_passivbot_source

SERVICE = "BacktestV9"


def source_identity(repo: Path) -> dict:
    """Require a clean source tree so a commit identifies all code used by a job."""
    identity = classify_passivbot_source(str(repo))
    if identity.get("generation") != "pb9" or identity.get("errors") or not identity.get("commit"):
        raise RuntimeError("PB9 source identity is not verifiable")
    env = {key:value for key,value in os.environ.items() if not key.startswith('GIT_')}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1', GIT_NO_REPLACE_OBJECTS='1', GIT_OPTIONAL_LOCKS='0')
    result = subprocess.run(['git','-c','core.fsmonitor=false','-C',str(repo),'status','--porcelain=v1'],
                            env=env,capture_output=True,timeout=10,check=True)
    changes = [line for line in result.stdout.decode('utf-8', errors='replace').splitlines()
               if line != '?? .api-keys.json.pbgui-merge.lock']
    if changes:
        raise RuntimeError("PB9 source tree has local changes; commit provenance is not exact")
    return identity


def prepare_native_import(repo: Path, config: Path) -> None:
    """Select only the configured v9 source and verify Rust without building it."""
    identity = source_identity(repo)
    record_path = config.with_name(config.name.replace('.config.json', '.json'))
    record = json.loads(record_path.read_text())
    if identity['commit'] != record.get('commit') or identity['config_schema'] != record.get('schema'):
        raise RuntimeError("PB9 source changed before native import")
    source = repo.resolve() / "src"
    sys.path.insert(0, str(source))
    from pb9_config_helper import check_inline_config
    document = json.loads(config.read_text(encoding="utf-8"))
    check_inline_config(document, identity["config_schema"], allow_inline_overrides=True)
    rust_utils = importlib.import_module("rust_utils")
    if not Path(rust_utils.__file__).resolve().is_relative_to(source):
        raise RuntimeError("PB9 Rust tools were loaded from another source")
    # The verifier compares the embedded source stamp. It never compiles or prunes.
    rust = importlib.import_module("passivbot_rust")
    if not Path(rust.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()):
        raise RuntimeError("PB9 Rust extension is outside its environment")
    fingerprint = rust_utils.source_fingerprint(repo / "passivbot-rust")
    verified = rust_utils.verify_loaded_runtime_extension(fingerprint=fingerprint)
    if not fingerprint or verified.get("runtime_compiled_source_stamp") != fingerprint:
        raise RuntimeError("PB9 Rust extension is stale")


def launch_environment(interpreter: str) -> dict:
    """Do not inherit credentials or Python injection variables from the API."""
    allowed = {"HOME", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR", "SYSTEMROOT", "WINDIR"}
    env = {key: value for key, value in os.environ.items() if key in allowed}
    env["PATH"] = str(Path(interpreter).parent) + os.pathsep + os.defpath
    env["PYTHONNOUSERSITE"] = "1"
    return env


def publish_result_source(config_path: Path) -> None:
    """Keep the exact authored snapshot and source identity beside every native result."""
    import hashlib
    from secure_files import atomic_write_private_text
    record = json.loads(config_path.with_name(config_path.name.replace('.config.json', '.json')).read_text())
    raw = config_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != record['snapshot_sha256']:
        raise RuntimeError('PB9 snapshot changed while its backtest was running')
    config = json.loads(raw)
    root = Path(config['backtest']['base_dir'])
    approved = config_path.parent.parent / 'bt_v9_results'
    if not root.is_absolute() or not root.resolve().is_relative_to(approved.resolve()):
        raise RuntimeError('PB9 result source publication is outside its managed root')
    for analysis in root.glob('**/analysis.json'):
        directory = analysis.parent
        if any(path.is_symlink() for path in (analysis, directory, *directory.parents)):
            raise RuntimeError('PB9 result source publication must not follow symbolic links')
        atomic_write_private_text(directory / 'pbgui.source.json', raw.decode('utf-8'))
        evidence = {key: record[key] for key in ('runtime', 'schema', 'commit', 'package_version', 'snapshot_sha256', 'filename')}
        if isinstance(record.get('comparison_reference'), dict):
            evidence['comparison_reference'] = record['comparison_reference']
        atomic_write_private_text(directory / 'pbgui.provenance.json', json.dumps(evidence, indent=4) + '\n')



def _public_metadata(root: Path, relative: Path, *, version=False):
    """Validate bounded public metadata through no-follow descriptors."""
    import stat
    descriptors = []
    try:
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(descriptor)
        for part in relative.parts[:-1]:
            descriptor = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            descriptors.append(descriptor)
        descriptor = os.open(relative.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
        descriptors.append(descriptor)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return None
        with os.fdopen(os.dup(descriptor), 'rb') as stream:
            raw = stream.read(32 * 1024 * 1024 + 1)
        if len(raw) > 32 * 1024 * 1024:
            return None
        value = int(raw.strip()) if version else json.loads(raw)
        return value if (isinstance(value, int) and value > 0 if version else isinstance(value, dict)) else None
    except (OSError, ValueError, UnicodeError, RecursionError):
        return None
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def prepare_market_metadata(work: Path, config: dict, coindata: Path, repo: Path, metadata_roots=()) -> None:
    """Reference existing public offline inputs without copying candles or metadata.

    Native v9 only reads these inputs in offline mode. Derived caches remain in
    the job directory. Extra configured bot caches are optional data sources;
    no bot code is imported and no PB7/PB8 installation is required.
    """
    import re
    from secure_files import ensure_private_directory

    backtest = config.get('backtest', {})
    if backtest.get('offline') is not True:
        return
    exchanges = backtest.get('exchanges', [])
    if not isinstance(exchanges, list) or not exchanges:
        raise ValueError('Select an exchange before starting an offline PB9 backtest')
    caches = [repo / 'caches', coindata, *[Path(root) / 'caches' for root in metadata_roots if root]]
    targets = []
    for exchange in exchanges:
        if not isinstance(exchange, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,39}', exchange):
            raise ValueError('Invalid offline PB9 exchange')
        target = work / 'caches' / exchange / 'markets.json'
        for parent in (target.parent, target.parent.parent, work):
            if parent.is_symlink():
                raise ValueError('PB9 metadata directories must not be links')
        shared = (coindata, Path(exchange) / 'ccxt_markets.json')
        native = [(root, Path(exchange) / 'markets.json') for root in caches if root != coindata]
        candidates = [shared, *native] if backtest.get('ohlcv_source_dir') else [*native, shared]
        if target.is_file() and not target.is_symlink():
            candidates.insert(0, (work, target.relative_to(work)))
        selected = None
        for root, relative in candidates:
            markets = _public_metadata(root, relative)
            if (markets and all(isinstance(item, dict) and isinstance(item.get('symbol'), str)
                                and item['symbol'] for item in markets.values())):
                selected = root / relative
                break
        if selected is None:
            raise ValueError(f'Offline market metadata missing for {exchange}. Update Coin Data for this exchange or disable offline in Backtest Settings.')
        targets.append((target, selected))
        for root in caches:
            relative = Path(exchange) / 'first_timestamps.json'
            if _public_metadata(root, relative) is not None:
                targets.append((work / 'caches' / relative, root / relative)); break
    # Keep the resolver version and its three documents together; native v9
    # verifies that version and the exact coin/exchange/symbol identities.
    listing = ['first_ohlcv_timestamps_unified.version', 'first_ohlcv_timestamps_unified.json',
               'first_ohlcv_timestamps_unified_exchange_specific.json',
               'first_ohlcv_timestamps_unified_exchange_specific_symbols.json']
    for root in caches:
        if all(_public_metadata(root, Path(name), version=name.endswith('.version')) is not None for name in listing):
            targets.extend((work / 'caches' / name, root / name) for name in listing)
            break
    # Validate every destination before exposing any references. Existing local
    # metadata belongs to this job and is never overwritten.
    for target, selected in targets:
        if any(parent.is_symlink() for parent in (target.parent, target.parent.parent)):
            raise ValueError('PB9 metadata directories must not be links')
        if target.is_symlink() and target.resolve() != selected.resolve():
            raise ValueError('PB9 offline metadata reference changed; enqueue a new job')
    for target, selected in targets:
        if target == selected or target.exists() or target.is_symlink():
            continue
        ensure_private_directory(target.parent)
        target.symlink_to(selected.absolute())
