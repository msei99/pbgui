"""Build and verify source-bound PB7/PB8 Linux wheels on the controlling master.

Invoked by Ansible, never at API startup. Native builds execute the controller-
selected source as the controlling master. The controller resolves normal PB8
updates from the official master head, or uses the authenticated branch selection;
targets only confirm that commit. Targets install binaries only; no source fallback.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
from urllib.parse import urlsplit
import zipfile

SERVICE = "PBWheels"
BUILD_REQUIREMENTS = ('setuptools==84.2.1', 'wheel==0.48.0',
                      'setuptools-rust==1.13.0', 'maturin==1.16.0',
                      'auditwheel==6.8.2', 'patchelf==0.17.2.4')
PASSIVBOT_UPSTREAM = 'https://github.com/enarjord/passivbot.git'


def _source_url(url: str) -> str:
    """Allow credential-free HTTPS or SSH Git sources chosen by the controller."""
    if not isinstance(url, str) or any(ord(char) < 33 or ord(char) == 127 for char in url):
        raise ValueError('Invalid wheel source URL')
    parsed = urlsplit(url)
    if parsed.query or parsed.fragment:
        raise ValueError('Wheel source URL must not contain query or fragment')
    if parsed.scheme == 'https' and parsed.hostname and not parsed.username and not parsed.password:
        return url
    if parsed.scheme == 'ssh' and parsed.hostname and not parsed.password:
        return url
    if re.fullmatch(r'[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[A-Za-z0-9._~+/-]+', url):
        return url
    raise ValueError('Wheel source must use credential-free HTTPS or SSH')


def _source_env() -> dict:
    """Disable Git credential helpers and non-network transport protocols."""
    clean = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
    return {**clean, 'GIT_TERMINAL_PROMPT': '0', 'GIT_CONFIG_GLOBAL': os.devnull,
            'GIT_CONFIG_NOSYSTEM': '1', 'GIT_LFS_SKIP_SMUDGE': '1',
            'GIT_ALLOW_PROTOCOL': 'https:ssh'}


def resolve_source_head(url: str, branch: str) -> dict:
    """Resolve a branch head on the controller without consulting a target host."""
    url = _source_url(url)
    if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/-]{0,199}', branch)
            or '..' in branch or branch.endswith(('/', '.', '.lock'))):
        raise ValueError('Invalid wheel source branch')
    result = subprocess.run(
        ['git', 'ls-remote', '--exit-code', '--heads', url, f'refs/heads/{branch}'],
        env=_source_env(), text=True, capture_output=True, timeout=120, check=True,
    )
    lines = result.stdout.splitlines()
    if len(lines) != 1:
        raise ValueError('Expected exactly one source branch head')
    fields = lines[0].split('\t')
    if len(fields) != 2 or fields[1] != f'refs/heads/{branch}' or not re.fullmatch(r'[0-9a-f]{40}', fields[0]):
        raise ValueError('Invalid source branch head')
    return {'commit': fields[0]}


def export_trusted_source(commit: str, archive: Path, url: str = PASSIVBOT_UPSTREAM) -> dict:
    """Archive a commit chosen on the controller from its selected Git source."""
    url = _source_url(url)
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('Expected a full upstream Git commit')
    if archive.is_symlink() or archive.exists():
        raise ValueError('Source archive destination must be absent and not a symlink')
    private_directory(archive.parent)
    env = _source_env()
    with tempfile.TemporaryDirectory(prefix='trusted-source-', dir=archive.parent) as temporary:
        repo = Path(temporary)
        run(['git', 'init', '-q', str(repo)], env=env)
        run(['git', '-C', str(repo), '-c', 'credential.helper=', 'fetch', '--no-tags',
             '--depth=1', url, commit], env=env)
        resolved = subprocess.check_output(
            ['git', '-C', str(repo), 'rev-parse', 'FETCH_HEAD'], env=env,
            text=True, stderr=subprocess.PIPE, timeout=30).strip()
        if resolved != commit:
            raise ValueError('Fetched upstream commit differs from requested commit')
        temporary_archive = repo / 'source.tar'
        run(['git', '-C', str(repo), 'archive', '--format=tar',
             f'--output={temporary_archive}', commit], env=env)
        os.replace(temporary_archive, archive)
    return {'commit': commit, 'archive_sha256': digest(archive)}


def builder_identity(target: dict) -> tuple[dict, dict]:
    """Require native master tools; never install a system compiler implicitly."""
    require_target(probe())
    home = Path.home()
    tool_path = os.pathsep.join([str(home / '.cargo/bin'), os.environ.get('PATH', os.defpath)])
    env = {'PATH': tool_path, 'HOME': str(home), 'LANG': 'C.UTF-8',
           'RUSTUP_HOME': os.environ.get('RUSTUP_HOME', str(home / '.rustup')),
           'RUSTUP_TOOLCHAIN': '1.90.0'}
    tools = {}
    for tool in ('cargo', 'rustc', 'cc'):
        executable = shutil.which(tool, path=tool_path)
        if not executable:
            raise ValueError(f'Build master requires {tool}; no target runtime was changed')
        try:
            version = subprocess.check_output([executable, '--version'], env=env, text=True,
                                              stderr=subprocess.PIPE, timeout=30).splitlines()[0]
        except subprocess.CalledProcessError as exc:
            raise ValueError('Build master requires an installed Rust 1.90.0 toolchain and C compiler') from exc
        if tool in {'cargo', 'rustc'} and not version.startswith(tool + ' 1.90.0 '):
            raise ValueError('Build master requires Rust/Cargo 1.90.0')
        tools[tool] = {'path': executable, 'version': version}
    baseline = min(int(probe()['libc'][1].split('.')[1]), int(target['libc'][1].split('.')[1]))
    identity = {'mode': 'native-v2', 'host': probe(), 'tools': tools,
                'platform': f'manylinux_2_{baseline}_x86_64',
                'requirements': list(BUILD_REQUIREMENTS), 'python': sys.version}
    return identity, env


def compile_wheels(source: Path, artifacts: Path, identity: dict, base_env: dict) -> None:
    """Build in private directories on this master, then audit actual target ABI."""
    root = source.parent
    environment = root / 'buildenv'
    home = private_directory(root / 'home')
    cargo_home = private_directory(root / 'cargo')
    temporary = private_directory(root / 'tmp')
    raw = private_directory(root / 'raw')
    env = {**base_env, 'HOME': str(home), 'CARGO_HOME': str(cargo_home),
           'TMPDIR': str(temporary), 'PIP_CONFIG_FILE': os.devnull,
           'PIP_NO_CACHE_DIR': '1', 'CARGO_BUILD_JOBS': '2', 'CARGO_INCREMENTAL': '0',
           'CARGO_PROFILE_RELEASE_DEBUG': '0', 'SETUPTOOLS_RUST_CARGO_PROFILE': 'release',
           'RUSTFLAGS': '-C target-cpu=x86-64'}
    run([sys.executable, '-m', 'venv', str(environment)], env=env)
    python = str(environment / 'bin/python')
    env['PATH'] = str(environment / 'bin') + os.pathsep + env['PATH']
    run([python, '-m', 'pip', 'install', '--disable-pip-version-check', *BUILD_REQUIREMENTS], env=env)
    run(['cargo', 'generate-lockfile', '--manifest-path', 'passivbot-rust/Cargo.toml'], cwd=source, env=env)
    fingerprint = subprocess.check_output(
        [python, '-c', "import sys; sys.path.insert(0,'src'); from rust_utils import source_fingerprint; print(source_fingerprint())"],
        cwd=source, env=env, text=True, timeout=30).strip()
    if not re.fullmatch('[0-9a-f]{64}', fingerprint):
        raise ValueError('Invalid source fingerprint from build checkout')
    env['PASSIVBOT_RUST_SOURCE_FINGERPRINT'] = fingerprint
    run([python, '-m', 'pip', 'wheel', '--no-deps', '--no-build-isolation', '--wheel-dir', str(raw), '.'], cwd=source, env=env)
    run([python, '-m', 'maturin', 'build', '--release', '--manifest-path', 'passivbot-rust/Cargo.toml',
         '--interpreter', python, '--compatibility', 'linux', '--out', str(raw)], cwd=source, env=env)
    for wheel in sorted(raw.glob('*.whl')):
        run([python, '-m', 'auditwheel', 'repair', '--plat', identity['platform'],
             '--wheel-dir', str(artifacts), str(wheel)], cwd=source, env=env)
    shutil.copyfile(source / 'passivbot-rust/Cargo.lock', artifacts / 'Cargo.lock')
    (artifacts / 'source-fingerprint').write_text(fingerprint)


def digest(path: Path) -> str:
    """Hash an artifact without loading large wheels into memory."""
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def private_directory(path: Path) -> Path:
    """Create private staging/cache paths and reject symlink boundaries."""
    if any(character in str(path) for character in ('\x00', '\n', '\r', ',')):
        raise ValueError('Invalid staging/cache path')
    if path.is_symlink() or any(p.is_symlink() for p in path.parents):
        raise ValueError('Symlink staging/cache path is not permitted')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path.resolve()


@contextmanager
def locked(path: Path):
    """Serialize builders/readers and protect concurrent cache publication."""
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def run(argv: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Keep child output out of JSON results and enforce a bounded execution time."""
    child = subprocess.Popen(argv, text=True, stdout=sys.stderr, stderr=sys.stderr,
                             start_new_session=True, **kwargs)
    try:
        result = child.wait(timeout=2400)
        if result:
            raise subprocess.CalledProcessError(result, argv)
        return subprocess.CompletedProcess(argv, result)
    finally:
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait()


def probe() -> dict:
    """Describe the interpreter/platform without importing PBGui or bot runtime."""
    return {'system': platform.system(), 'machine': platform.machine(),
            'python': list(sys.version_info[:2]), 'libc': list(platform.libc_ver())}


def require_target(target: dict) -> None:
    """Refuse unsupported live hosts before changing their active runtime."""
    libc = target.get('libc', [])
    try:
        version = tuple(int(x) for x in libc[1].split('.')[:2])
    except (IndexError, ValueError, AttributeError):
        version = ()
    if (target.get('system') != 'Linux' or target.get('machine') != 'x86_64'
            or target.get('python') != [3, 12] or not libc or libc[0] != 'glibc'
            or version < (2, 28)):
        raise ValueError('Central Passivbot wheels require Linux x86_64, Python 3.12 and glibc >= 2.28; no remote Rust build was started')


def unpack_source(archive: Path, destination: Path) -> None:
    """Extract a bounded Git source archive, rejecting links and traversal."""
    with tarfile.open(archive) as source:
        members = source.getmembers()
        if len(members) > 100_000 or sum(m.size for m in members) > 512 * 1024**2:
            raise ValueError('PB8 source archive exceeds limits')
        for member in members:
            path = PurePosixPath(member.name)
            if (path.is_absolute() or '..' in path.parts or '.git' in path.parts
                    or path.name in {'.env', 'pbgui.ini', 'api-keys.json', '.api-keys.json', 'id_rsa', 'id_ed25519'}
                    or not (member.isfile() or member.isdir())):
                raise ValueError('Unsafe PB8 source archive member')
        source.extractall(destination, members=members, filter='data')
    for required in ('setup.py', 'src/rust_utils.py', 'passivbot-rust/Cargo.toml'):
        if not (destination / required).is_file():
            raise ValueError(f'PB8 source archive lacks {required}')


def wheel_extension_hash(wheel: Path) -> str:
    """Identify the canonical Rust extension packaged by Maturin."""
    with zipfile.ZipFile(wheel) as archive:
        candidates = [x for x in archive.infolist()
                      if x.filename.startswith('passivbot_rust/') and x.filename.endswith('.so')]
        if len(candidates) != 1 or candidates[0].file_size > 128 * 1024**2:
            raise ValueError('Rust wheel must contain exactly one bounded extension')
        return hashlib.sha256(archive.read(candidates[0])).hexdigest()


def verify_bundle(bundle: Path, commit: str | None = None, target: dict | None = None,
                  product: str = 'pb8') -> dict:
    """Verify immutable artifact hashes, names and platform before installation."""
    if bundle.is_symlink():
        raise ValueError('Symlink wheel bundle')
    manifest_path = bundle / 'manifest.json'
    if manifest_path.is_symlink():
        raise ValueError('Symlink wheel manifest')
    manifest = json.loads(manifest_path.read_text())
    if not isinstance(manifest, dict):
        raise ValueError('Expected a wheel manifest object')
    if (product not in {'pb7', 'pb8'} or manifest.get('product', 'pb8') != product
            or manifest.get('format') != 1
            or not re.fullmatch(r'[0-9a-f]{40}', manifest.get('commit', ''))):
        raise ValueError('Invalid PB8 wheel manifest')
    if commit is not None and manifest['commit'] != commit:
        raise ValueError('Wheel source commit mismatch')
    if not re.fullmatch(r'[0-9a-f]{64}', manifest.get('source_fingerprint', '')):
        raise ValueError('Invalid Rust source fingerprint')
    artifacts = manifest.get('artifacts', {})
    if not isinstance(artifacts, dict) or len(artifacts) != 3 or 'Cargo.lock' not in artifacts:
        raise ValueError('Expected PB8 wheel, canonical Rust wheel and Cargo.lock')
    wheels = []
    for name, expected in artifacts.items():
        if not isinstance(name, str) or Path(name).name != name or name in {'.', '..'}:
            raise ValueError('Unsafe artifact filename')
        path = bundle / name
        if path.is_symlink() or not path.is_file() or digest(path) != expected:
            raise ValueError(f'Artifact checksum mismatch: {name}')
        if name != 'Cargo.lock':
            match = re.fullmatch(r'(?:passivbot|passivbot_rust)-[A-Za-z0-9_.+]+-cp312-(?:cp312|abi3)-((?:manylinux_2_\d+_x86_64)(?:\.manylinux_2_\d+_x86_64)*)\.whl', name)
            if not match:
                raise ValueError('Unsupported wheel platform or filename')
            baselines = [int(x) for x in re.findall(r'manylinux_2_(\d+)_x86_64', match[1])]
            if min(baselines) < 28:
                raise ValueError('Unsupported wheel libc baseline')
            if target is not None:
                require_target(target)
                if min(baselines) > int(target['libc'][1].split('.')[1]):
                    raise ValueError('Wheel requires newer glibc than target; no target runtime was changed')
            wheels.append(name)
    rust = [name for name in wheels if name.startswith('passivbot_rust-')]
    python = [name for name in wheels if name.startswith('passivbot-')]
    if len(rust) != 1 or len(python) != 1 or wheel_extension_hash(bundle / rust[0]) != manifest.get('extension_sha256'):
        raise ValueError('Canonical Rust extension checksum mismatch')
    return {**manifest, 'python_wheel': python[0], 'rust_wheel': rust[0]}


def build(archive: Path, commit: str, target: dict, cache: Path, output: Path,
          product: str = 'pb8') -> dict:
    """Reuse local/imported artifacts first; otherwise build natively on this master."""
    require_target(target)
    if product not in {'pb7', 'pb8'}:
        raise ValueError('Unsupported Passivbot runtime')
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('Expected a full Git commit')
    cache = private_directory(cache)
    output = private_directory(output)
    if any(output.iterdir()):
        raise ValueError('Per-run wheel staging must be empty')
    if archive.is_symlink() or not archive.is_file():
        raise ValueError('Expected regular PB8 source archive')
    archive_hash = digest(archive)
    # Imported bundles are usable without Docker, Cargo, a compiler or build venv.
    with locked(cache / 'cleanup.lock'):
        for candidate in sorted(cache.iterdir(), key=lambda p: p.name):
            if not re.fullmatch('[0-9a-f]{64}', candidate.name):
                continue
            try:
                manifest = verify_bundle(candidate, commit, target, product)
            except (ValueError, OSError, json.JSONDecodeError, zipfile.BadZipFile):
                continue
            if manifest.get('source_archive_sha256') == archive_hash:
                return copy_bundle(candidate, output, commit, target, product)
    identity, env = builder_identity(target)
    key = hashlib.sha256(f'{product}:{commit}:{archive_hash}:{json.dumps(identity, sort_keys=True)}:v2'.encode()).hexdigest()
    entry = cache / key
    with locked(cache / f'build-{int(key[:2], 16) % 4}.lock'):
        if entry.is_symlink():
            raise ValueError('Symlink wheel cache entry')
        if entry.exists():
            try:
                existing = verify_bundle(entry, commit, target, product)
                if existing.get('source_archive_sha256') != archive_hash:
                    raise ValueError('Cached source archive mismatch')
            except (ValueError, OSError, json.JSONDecodeError, zipfile.BadZipFile):
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
        if not entry.exists():
            with tempfile.TemporaryDirectory(prefix='build-', dir=cache) as temporary:
                root = Path(temporary)
                source = private_directory(root / 'source')
                artifacts = private_directory(root / 'artifacts')
                unpack_source(archive, source)
                compile_wheels(source, artifacts, identity, env)
                wheels = sorted(artifacts.glob('*.whl'))
                rust = [p for p in wheels if p.name.startswith('passivbot_rust-')]
                if len(wheels) != 2 or len(rust) != 1:
                    raise ValueError('Builder did not produce both PB8 wheels')
                manifest = {'format': 1, 'product': product, 'commit': commit, 'builder': identity,
                            'source_archive_sha256': archive_hash,
                            'source_fingerprint': (artifacts / 'source-fingerprint').read_text().strip(),
                            'extension_sha256': wheel_extension_hash(rust[0]),
                            'artifacts': {p.name: digest(p) for p in [*wheels, artifacts / 'Cargo.lock']}}
                (artifacts / 'source-fingerprint').unlink()
                (artifacts / 'manifest.json').write_text(json.dumps(manifest, indent=4) + '\n')
                verify_bundle(artifacts, commit, target, product)
                os.replace(artifacts, entry)
        manifest = copy_bundle(entry, output, commit, target, product)
    # Copies above pin each run independently of the bounded reusable cache.
    with locked(cache / 'cleanup.lock'):
        entries = sorted((p for p in cache.iterdir() if re.fullmatch('[0-9a-f]{64}', p.name)), key=lambda p: p.stat().st_mtime, reverse=True)
        for old in entries[4:]:
            with locked(cache / f'build-{int(old.name[:2], 16) % 4}.lock'):
                if old != entry and old.exists():
                    shutil.rmtree(old)
    return {**manifest, 'bundle': str(output)}


def copy_bundle(source: Path, output: Path, commit: str, target: dict,
                product: str = 'pb8') -> dict:
    """Materialize a checked run/cache copy while its caller holds the entry lock."""
    manifest = verify_bundle(source, commit, target, product)
    for name in [*manifest['artifacts'], 'manifest.json']:
        shutil.copyfile(source / name, output / name)
        (output / name).chmod(0o600)
    os.utime(source, None)
    return {**manifest, 'bundle': str(output)}


def import_bundle(bundle: Path, cache: Path, product: str = 'pb8') -> dict:
    """Seed an updated master with reusable artifacts, requiring no build tools."""
    manifest = verify_bundle(bundle, target=probe(), product=product)
    archive_hash = manifest.get('source_archive_sha256', '')
    if not re.fullmatch('[0-9a-f]{64}', archive_hash):
        raise ValueError('Bundle lacks source archive provenance')
    cache = private_directory(cache)
    key = hashlib.sha256(json.dumps([product, manifest['commit'], archive_hash,
                                    manifest['artifacts'], 'import-v2'], sort_keys=True).encode()).hexdigest()
    with locked(cache / 'cleanup.lock'):
        for candidate in cache.iterdir():
            if not re.fullmatch('[0-9a-f]{64}', candidate.name):
                continue
            try:
                existing = verify_bundle(candidate, manifest['commit'], probe(), product)
            except (ValueError, OSError, json.JSONDecodeError, zipfile.BadZipFile):
                continue
            if (existing.get('source_archive_sha256') == archive_hash
                    and existing['artifacts'] == manifest['artifacts']):
                os.utime(candidate, None)
                return {'cached': True, 'commit': manifest['commit']}
        with tempfile.TemporaryDirectory(prefix='import-', dir=cache) as temporary:
            staged = private_directory(Path(temporary) / 'bundle')
            copy_bundle(bundle, staged, manifest['commit'], probe(), product)
            if not (cache / key).exists():
                os.replace(staged, cache / key)
            else:
                verify_bundle(cache / key, manifest['commit'], probe(), product)
                os.utime(cache / key, None)
        entries = sorted((p for p in cache.iterdir() if re.fullmatch('[0-9a-f]{64}', p.name)),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        for old in entries[4:]:
            with locked(cache / f'build-{int(old.name[:2], 16) % 4}.lock'):
                shutil.rmtree(old)
    return {'cached': True, 'commit': manifest['commit']}



def require_install_capacity(bundle: Path, destination: Path, product: str = 'pb8') -> dict:
    """Reserve unpacked binary dependencies before changing an active checkout."""
    verify_bundle(bundle, product=product)
    unpacked = 0
    for wheel in bundle.glob('*.whl'):
        if wheel.is_symlink():
            raise ValueError('Symlink dependency wheel')
        with zipfile.ZipFile(wheel) as archive:
            unpacked += sum(member.file_size for member in archive.infolist())
    required = unpacked + 128 * 1024**2
    free = shutil.disk_usage(destination).free
    if free < required:
        raise ValueError(f'Binary {product.upper()} install needs {required // 1048576} MiB free; only {free // 1048576} MiB available. Active runtime was not changed.')
    return {'required_bytes': required, 'free_bytes': free}


def check_receipt_location(receipt: Path) -> dict:
    """Reject unsafe receipt paths before a target runtime is stopped."""
    if not receipt.is_absolute():
        raise ValueError('Wheel receipt path must be absolute')
    for part in (receipt, *receipt.parents):
        if part.is_symlink():
            raise ValueError(f'Symlink wheel receipt path: {part}')
    if receipt.exists() and not receipt.is_file():
        raise ValueError('Wheel receipt is not a regular file')
    parent = receipt.parent
    existing = next((part for part in (parent, *parent.parents) if part.exists()), None)
    if existing is None or not existing.is_dir():
        raise ValueError('Wheel receipt parent is not a directory')
    info = existing.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o022:
        raise ValueError('Wheel receipt directory must be owner-controlled')
    return {'receipt': str(receipt), 'ready': True}


def validate_runtime(bundle: Path, repo: Path, commit: str, product: str = 'pb8',
                     receipt: Path | None = None) -> dict:
    """Trust only a matching installed binary before writing its source stamp."""
    require_target(probe())
    manifest = verify_bundle(bundle, commit, probe(), product)
    actual = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True, timeout=15).strip()
    if actual != commit or digest(repo / 'passivbot-rust/Cargo.lock') != manifest['artifacts']['Cargo.lock']:
        raise ValueError(f'Installed {product.upper()} checkout/lockfile differs from built sources')
    os.chdir(repo)
    spec = importlib.util.spec_from_file_location('pbgui_wheel_rust_utils', repo / 'src/rust_utils.py')
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    if helpers.source_fingerprint() != manifest['source_fingerprint']:
        raise ValueError('Installed Rust sources differ from wheel fingerprint')
    helpers.prune_shadowing_local_extensions()
    import passivbot_rust  # Installed package only; no source/build fallback.
    module_path = helpers._compiled_path_from_loaded_module()
    if not module_path or digest(module_path) != manifest['extension_sha256']:
        raise ValueError('Loaded Rust binary differs from verified wheel')
    helpers.write_source_stamp(module_path, manifest['source_fingerprint'])
    result = helpers.verify_loaded_runtime_extension(fingerprint=manifest['source_fingerprint'])
    if receipt is not None:
        check_receipt_location(receipt)
        receipt.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        record = {'format': 1, 'product': product, 'commit': commit,
                  'extension_sha256': manifest['extension_sha256']}
        descriptor, temporary = tempfile.mkstemp(prefix='.wheel-', dir=receipt.parent)
        try:
            with os.fdopen(descriptor, 'w') as handle:
                json.dump(record, handle, indent=4)
                handle.write('\n')
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, receipt)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    return result


def main() -> None:
    """Serve isolated Ansible preparation, transfer and verification commands."""
    def interrupt(_signum, _frame):
        """Let child-process and staging cleanup run when the helper is stopped."""
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupt)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['probe', 'resolve', 'source', 'build', 'verify', 'runtime', 'receipt', 'capacity', 'cache', 'extract'])
    parser.add_argument('--archive', type=Path)
    parser.add_argument('--commit')
    parser.add_argument('--target')
    parser.add_argument('--cache', type=Path)
    parser.add_argument('--bundle', type=Path)
    parser.add_argument('--repo', type=Path)
    parser.add_argument('--product', choices=['pb7', 'pb8'], default='pb8')
    parser.add_argument('--receipt', type=Path)
    parser.add_argument('--url', default=PASSIVBOT_UPSTREAM)
    parser.add_argument('--branch')
    args = parser.parse_args()
    if args.operation == 'probe':
        result = probe()
        require_target(result)
    elif args.operation == 'resolve':
        result = resolve_source_head(args.url, args.branch)
    elif args.operation == 'source':
        result = export_trusted_source(args.commit, args.archive, args.url)
    elif args.operation == 'build':
        result = build(args.archive, args.commit, json.loads(args.target), args.cache, args.bundle, args.product)
    elif args.operation == 'receipt':
        result = check_receipt_location(args.receipt)
    elif args.operation == 'capacity':
        result = require_install_capacity(args.bundle, args.repo, args.product)
    elif args.operation == 'cache':
        result = import_bundle(args.bundle, args.cache, args.product)
    elif args.operation == 'extract':
        destination = private_directory(args.repo)
        if any(destination.iterdir()):
            raise ValueError('Source staging must be empty')
        unpack_source(args.archive, destination)
        result = {'source': str(destination)}
    elif args.operation == 'runtime':
        result = validate_runtime(args.bundle, args.repo, args.commit, args.product, args.receipt)
    else:
        result = verify_bundle(args.bundle, args.commit, probe(), args.product)
    print(json.dumps(result))  # Script protocol; not a GUI module/logger.


if __name__ == '__main__':
    main()
