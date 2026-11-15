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
        atomic_write_private_text(directory / 'pbgui.provenance.json', json.dumps(evidence, indent=4) + '\n')
