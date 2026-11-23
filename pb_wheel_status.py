"""Verify PB7/PB8 wheel receipts and retire unused VPS Rust build tools."""
from __future__ import annotations

import configparser
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import zipfile
from email.parser import BytesParser
from pathlib import PurePosixPath

SERVICE = "PBWheelStatus"


def matching_wheel_installation(pbgdir: Path, product: str, repo: Path, python: Path,
                                bundle: Path, commit: str, extras: str = '') -> dict:
    """Compare verified wheel contents without importing bot code or trusting a missing receipt."""
    result = {'wheels_match': False, 'runtime_current': False}
    if product not in {'pb7', 'pb8', 'pb9'} or not prepared_bundle_verified(bundle, product, commit):
        raise ValueError('Cannot compare an unverified wheel bundle')
    if not re.fullmatch(r'[a-z0-9,-]*', extras):
        raise ValueError('Invalid wheel dependency extras')
    if not python.is_file():
        return result
    # Only standard library / pip metadata is loaded in the target interpreter.
    # Neither the checkout nor installed Passivbot modules are imported.
    probe = r'''
import importlib.metadata as md, json, re, sys, sysconfig
print(json.dumps({'site': sysconfig.get_path('purelib'), 'version': list(sys.version_info[:2]),
                  'distributions': {re.sub(r'[-_.]+', '-', d.metadata['Name']).lower(): d.version
                                    for d in md.distributions() if d.metadata['Name']}}))
'''
    try:
        info = json.loads(subprocess.check_output(
            [str(python), '-I', '-B', '-c', probe], text=True, timeout=30,
            stderr=subprocess.DEVNULL))
        site = Path(info['site'])
        venv = python.parent.parent.resolve()
        if info['version'] != [3, 12] or not site.resolve().is_relative_to(venv):
            return result
        manifest = json.loads((bundle / 'manifest.json').read_text(encoding='utf-8'))
        wheels = [name for name in manifest['artifacts'] if name.endswith('.whl')]
        if len(wheels) != (1 if product == 'pb9' else 2):
            raise ValueError('Unexpected wheel bundle layout')
        requirements = []
        binary_count = 0
        expected_binaries = set()
        for name in wheels:
            with zipfile.ZipFile(bundle / name) as archive:
                entries = archive.infolist()
                if len(entries) > 20000 or sum(entry.file_size for entry in entries) > 1024**3:
                    raise ValueError('Oversized wheel contents')
                for entry in entries:
                    path = PurePosixPath(entry.filename)
                    if path.is_absolute() or '..' in path.parts or '\\' in entry.filename:
                        raise ValueError('Unsafe wheel member')
                    if entry.is_dir() or path.name == 'RECORD':
                        continue  # Pip rewrites RECORD on installation.
                    if (product in {'pb7', 'pb8'} and len(path.parts) == 1
                            and path.name.startswith('passivbot_rust') and path.name.endswith('.so')):
                        # Older wheels ship a second, flat extension. Some
                        # installations retain it, others prune/synchronize it.
                        flat = site / str(path)
                        expected_binaries.add(flat)
                        if flat.exists() or flat.is_symlink():
                            if flat.is_symlink() or not flat.is_file():
                                return result
                            with archive.open(entry) as source, flat.open('rb') as target:
                                value = hashlib.file_digest(target, 'sha256').hexdigest()
                                if value not in {hashlib.file_digest(source, 'sha256').hexdigest(),
                                                 manifest.get('extension_sha256')}:
                                    return result
                        continue
                    parts = path.parts
                    if parts and parts[0].endswith('.data'):
                        if len(parts) < 3 or parts[1] not in {'purelib', 'platlib'}:
                            return result  # Unsupported install schemes must not be guessed.
                        path = PurePosixPath(*parts[2:])
                    installed = site.joinpath(*path.parts)
                    if (installed.is_symlink() or not installed.is_file()
                            or not installed.resolve().is_relative_to(site.resolve())):
                        return result
                    with archive.open(entry) as source, installed.open('rb') as target:
                        if hashlib.file_digest(source, 'sha256').digest() != hashlib.file_digest(target, 'sha256').digest():
                            return result
                    if path.name.endswith('.so') and 'passivbot_rust' in str(path):
                        binary_count += 1
                        expected_binaries.add(installed)
                    if path.name == 'METADATA' and '.dist-info' in str(path):
                        requirements.extend(BytesParser().parsebytes(archive.read(entry)).get_all('Requires-Dist', []))
        if binary_count != 1:
            return result
        binaries = list((site / 'passivbot_rust').glob('passivbot_rust*.so')) + list(site.glob('passivbot_rust*.so'))
        canonical = (binaries if product == 'pb9' else
                     [binary for binary in binaries if binary.parent.name == 'passivbot_rust'])
        if len(canonical) != 1 or any(binary not in expected_binaries for binary in binaries):
            return result
        result['wheels_match'] = True
        # The base/live and full research profiles can share the same wheel,
        # but a missing optional dependency still needs a dependency-only install.
        try:
            from packaging.markers import default_environment
            from packaging.requirements import Requirement
        except ImportError:
            from pip._vendor.packaging.markers import default_environment
            from pip._vendor.packaging.requirements import Requirement
        environments = [dict(default_environment(), extra=extra) for extra in ['', *filter(None, extras.split(','))]]
        for raw in requirements:
            requirement = Requirement(raw)
            if requirement.marker and not any(requirement.marker.evaluate(env) for env in environments):
                continue
            version = info['distributions'].get(re.sub(r'[-_.]+', '-', requirement.name).lower())
            if version is None or not requirement.specifier.contains(version, prereleases=True):
                return result
        env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull)
        actual = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'],
                                         text=True, timeout=10, stderr=subprocess.DEVNULL, env=env).strip()
        changes = subprocess.check_output(['git', '-C', str(repo), 'status', '--porcelain', '--untracked-files=no'],
                                          text=True, timeout=10, stderr=subprocess.DEVNULL, env=env).strip()
        config = configparser.ConfigParser()
        config.read(pbgdir / 'pbgui.ini')
        marker = pbgdir / f'data/locks/{product}-runtime-invalid'
        cli = python.parent / 'passivbot'
        stamp = canonical[0].with_name(canonical[0].name + '.rust-src-sha256')
        result['runtime_current'] = bool(
            actual == commit and not changes and not marker.exists() and not marker.is_symlink()
            and cli.is_file() and os.access(cli, os.X_OK)
            and stamp.is_file() and not stamp.is_symlink()
            and stamp.read_text(encoding='utf-8').strip() == manifest.get('source_fingerprint')
            and (repo / 'passivbot-rust/Cargo.lock').read_bytes() == (bundle / 'Cargo.lock').read_bytes()
            and config.get('main', product + 'dir', fallback='') == str(repo)
            and config.get('main', product + 'venv', fallback='') == str(python))
        return result
    except (OSError, ValueError, TypeError, KeyError, configparser.Error,
            zipfile.BadZipFile, subprocess.SubprocessError):
        return result


def wheel_installed(pbgdir: Path, product: str, repo: Path, python: Path) -> bool:
    """Accept only a receipt tied to this checkout and installed Rust extension."""
    if product not in {"pb7", "pb8", "pb9"} or not repo.is_dir() or not python.is_file():
        return False
    receipt = pbgdir / "data/state" / f"{product}-wheel.json"
    try:
        if receipt.is_symlink() or receipt.stat().st_size > 4096:
            return False
        data = json.loads(receipt.read_text(encoding="utf-8"))
        if (not isinstance(data, dict) or data.get("format") != 1 or data.get("product") != product
                or not re.fullmatch("[0-9a-f]{40}", data.get("commit", ""))
                or not re.fullmatch("[0-9a-f]{64}", data.get("extension_sha256", ""))):
            return False
        commit = subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            text=True, stderr=subprocess.DEVNULL, timeout=10).strip()
        if commit != data["commit"]:
            return False
        site = python.parent.parent / "lib/python3.12/site-packages"
        binaries = list((site/'passivbot_rust').glob("passivbot_rust*.so")) + list(site.glob("passivbot_rust*.so"))
        if not binaries or any(binary.is_symlink() for binary in binaries):
            return False
        # Older installs can retain both flat and package-layout extensions.
        # Every importable copy must match the same source-bound receipt.
        for binary in binaries:
            with binary.open("rb") as handle:
                if hashlib.file_digest(handle, "sha256").hexdigest() != data["extension_sha256"]:
                    return False
        return True
    except (OSError, ValueError, TypeError, KeyError, RecursionError, subprocess.SubprocessError):
        return False


def pb8_live_current(pbgdir: Path, repo: Path, python: Path, expected_commit: str, product: str = 'pb8') -> bool:
    """Skip a live reinstall only when the selected checkout and runtime are intact."""
    if product not in {'pb8', 'pb9'} or not re.fullmatch(r"[0-9a-f]{40}", expected_commit) or not wheel_installed(pbgdir, product, repo, python):
        return False
    config = configparser.ConfigParser()
    try:
        config.read(pbgdir / "pbgui.ini")
    except (OSError, configparser.Error):
        return False
    if (config.get("main", product+"dir", fallback="") != str(repo)
            or config.get("main", product+"venv", fallback="") != str(python)):
        return False
    marker = pbgdir / f"data/locks/{product}-runtime-invalid"
    venv = python.parent.parent
    site = venv / "lib/python3.12/site-packages"
    source_requirements = repo / "requirements-live.txt"
    installed_requirements = venv / "lib/python3.12/requirements-live.txt"
    cli = python.parent / "passivbot"
    rust_binaries = list((site / "passivbot_rust").glob("passivbot_rust*.so")) + list(site.glob("passivbot_rust*.so"))
    try:
        if (marker.exists() or marker.is_symlink()
                or not cli.is_file() or not os.access(cli, os.X_OK)
                or not (site / "procedures.py").is_file()
                or len(rust_binaries) != 1
                or not rust_binaries[0].with_name(f"{rust_binaries[0].name}.rust-src-sha256").is_file()
                or source_requirements.is_symlink() or installed_requirements.is_symlink()
                or source_requirements.read_bytes() != installed_requirements.read_bytes()):
            return False
        actual = subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            text=True, stderr=subprocess.DEVNULL, timeout=10,
        ).strip()
        changes = subprocess.check_output(
            ["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=no"],
            text=True, stderr=subprocess.DEVNULL, timeout=10,
        ).strip()
        return actual == expected_commit and not changes
    except (OSError, subprocess.SubprocessError):
        return False


def prepared_bundle_verified(bundle: Path, product: str, commit: str) -> bool:
    """Check the transferred manifest without relying on target PBGui imports."""
    manifest_path = bundle / "manifest.json"
    try:
        if (bundle.is_symlink() or manifest_path.is_symlink()
                or manifest_path.stat().st_size > 65536):
            return False
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        artifacts = manifest.get("artifacts")
        if (manifest.get("format") != 1 or manifest.get("product", "pb8") != product
                or not re.fullmatch(r"[0-9a-f]{40}", commit)
                or manifest.get("commit") != commit
                or not isinstance(artifacts, dict) or len(artifacts) != (2 if product == 'pb9' else 3)):
            return False
        for name, expected in artifacts.items():
            if (not isinstance(name, str) or Path(name).name != name
                    or name in {".", ".."} or not isinstance(expected, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", expected)):
                return False
            path = bundle / name
            if path.is_symlink() or not path.is_file():
                return False
            with path.open("rb") as handle:
                if hashlib.file_digest(handle, "sha256").hexdigest() != expected:
                    return False
        return True
    except (OSError, ValueError, TypeError, AttributeError, json.JSONDecodeError):
        return False


def cleanup_vps_rust(
    pbgdir: Path, *, prepared_product: str | None = None,
    prepared_bundle: Path | None = None, prepared_commit: str | None = None,
) -> dict:
    """Retire Rust after wheel installs or before a verified wheel conversion."""
    pbgdir = pbgdir.resolve(strict=True)
    home = Path.home().resolve()
    if pbgdir.name != "pbgui" or not pbgdir.is_relative_to(home):
        return {"cleaned": False, "reason": "PBGui is outside the VPS account"}
    config = configparser.ConfigParser()
    config.read(pbgdir / "pbgui.ini")
    role = config.get("main", "role", fallback="").strip().lower()
    if role not in {"vps", "slave"}:
        return {"cleaned": False, "reason": "Rust cleanup is restricted to VPS runners"}
    if prepared_product is not None:
        if prepared_product not in {"pb7", "pb8", "pb9"} or prepared_bundle is None or prepared_commit is None:
            return {"cleaned": False, "reason": "Incomplete prepared wheel selection"}
        if not prepared_bundle_verified(prepared_bundle, prepared_product, prepared_commit):
            return {"cleaned": False, "reason": "Prepared wheel verification failed"}
    installed = []
    for product in ("pb7", "pb8", "pb9"):
        repo_text = config.get("main", f"{product}dir", fallback="").strip()
        python_text = config.get("main", f"{product}venv", fallback="").strip()
        repo = Path(repo_text) if repo_text else pbgdir.parent / product
        executable = Path(python_text) if python_text else pbgdir.parent / f"venv_{product}/bin/python"
        if not repo.exists() and not repo_text:
            continue
        if not repo.is_dir():
            return {"cleaned": False, "reason": f"Configured {product} checkout is missing"}
        if repo.is_symlink() or not repo.resolve().is_relative_to(home):
            return {"cleaned": False, "reason": f"Unsafe {product} checkout path"}
        if product != prepared_product and not wheel_installed(pbgdir, product, repo, executable):
            return {"cleaned": False, "reason": f"{product} has no verified wheel install"}
        installed.append((product, repo))
    if prepared_product is not None and prepared_product not in {product for product, _ in installed}:
        return {"cleaned": False, "reason": "Prepared product checkout is missing"}
    if not installed:
        return {"cleaned": False, "reason": "No PB7/PB8 runtime is installed"}
    try:
        for _, repo in installed:
            rust_source = repo / "passivbot-rust"
            if rust_source.is_symlink() or not rust_source.resolve().is_relative_to(repo.resolve()):
                return {"cleaned": False, "reason": "Unsafe Rust source path"}
            target = rust_source / "target"
            if target.is_symlink():
                target.unlink()
            elif target.is_dir():
                shutil.rmtree(target)
    except OSError as exc:
        return {"cleaned": False, "reason": f"Rust build cleanup failed: {exc.strerror or type(exc).__name__}"}
    rustup = home / ".rustup"
    cargo = home / ".cargo"
    if rustup.is_symlink() or cargo.is_symlink():
        return {"cleaned": False, "reason": "Rust home is a symlink; build targets removed"}
    toolchains = rustup / "toolchains"
    try:
        if toolchains.exists() and any(not p.name.startswith("1.90.0-") for p in toolchains.iterdir()):
            return {"cleaned": False, "reason": "Another Rust toolchain exists; build targets removed"}
    except OSError:
        return {"cleaned": False, "reason": "Cannot inspect Rust toolchains; build targets removed"}
    allowed_bins = {"cargo", "cargo-clippy", "cargo-fmt", "clippy-driver", "rustc",
                    "rustdoc", "rustfmt", "rustup", "rust-gdb", "rust-gdbgui", "rust-lldb"}
    bin_dir = cargo / "bin"
    try:
        if bin_dir.exists() and any(p.name not in allowed_bins for p in bin_dir.iterdir()):
            return {"cleaned": False, "reason": "Another Cargo tool exists; build targets removed"}
    except OSError:
        return {"cleaned": False, "reason": "Cannot inspect Cargo tools; build targets removed"}
    try:
        for directory in (rustup, cargo):
            if directory.is_dir():
                shutil.rmtree(directory)
    except OSError as exc:
        return {"cleaned": False, "reason": f"Rust tool cleanup failed: {exc.strerror or type(exc).__name__}"}
    return {"cleaned": True, "products": [product for product, _ in installed]}


def main() -> None:
    """Inspect a wheel install or run guarded VPS Rust cleanup."""
    parser = argparse.ArgumentParser()
    parser.add_argument("pbgdir", type=Path)
    parser.add_argument("--check-product", choices=["pb7", "pb8", "pb9"])
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--python", type=Path)
    parser.add_argument("--prepare-product", choices=["pb7", "pb8", "pb9"])
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--commit")
    parser.add_argument("--current-commit")
    parser.add_argument("--matches-bundle", choices=["pb7", "pb8", "pb9"])
    parser.add_argument("--extras", default='')
    args = parser.parse_args()
    if args.matches_bundle:
        if (args.check_product or args.prepare_product or args.current_commit
                or args.repo is None or args.python is None or args.bundle is None or args.commit is None):
            parser.error('--matches-bundle requires --repo, --python, --bundle and --commit')
        result = matching_wheel_installation(args.pbgdir, args.matches_bundle, args.repo,
                                             args.python, args.bundle, args.commit, args.extras)
    elif args.check_product:
        if args.prepare_product or args.bundle or args.commit:
            parser.error("--check-product cannot be combined with prepared wheel cleanup")
        if args.repo is None or args.python is None:
            parser.error("--check-product requires --repo and --python")
        if args.current_commit and args.check_product not in {"pb8", "pb9"}:
            parser.error("--current-commit requires --check-product pb8 or pb9")
        result = {"installed": (
            pb8_live_current(args.pbgdir, args.repo, args.python, args.current_commit, product=args.check_product)
            if args.current_commit else wheel_installed(args.pbgdir, args.check_product, args.repo, args.python)
        )}
    else:
        if args.current_commit:
            parser.error("--current-commit requires --check-product pb8")
        if args.prepare_product and (args.bundle is None or args.commit is None):
            parser.error("--prepare-product requires --bundle and --commit")
        result = cleanup_vps_rust(
            args.pbgdir, prepared_product=args.prepare_product,
            prepared_bundle=args.bundle, prepared_commit=args.commit,
        )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
