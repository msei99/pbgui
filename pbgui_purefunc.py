import json
import hjson
import pprint
import configparser
from pathlib import Path
import subprocess

def save_ini(section : str, parameter : str, value : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if not pb_config.has_section(section):
        pb_config.add_section(section)
    pb_config.set(section, parameter, value)
    with open('pbgui.ini', 'w') as pbgui_configfile:
        pb_config.write(pbgui_configfile)

def load_ini(section : str, parameter : str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option(section, parameter):
        return pb_config.get(section, parameter)
    else:
        return ""

def pbdir(): return load_ini("main", "pbdir")

def pbvenv(): return load_ini("main", "pbvenv")

def is_pb_installed():
    if Path(f"{pbdir()}/passivbot.py").exists():
        return True
    return False

def pb7dir(): return load_ini("main", "pb7dir")

def pb7venv(): return load_ini("main", "pb7venv")

def is_pb7_installed():
    if Path(f"{pb7dir()}/src/passivbot.py").exists():
        return True
    return False


def pb7srcdir() -> str:
    d = pb7dir()
    return f"{d}/src" if d else ""


def import_passivbot_rust():
    """Import PB7's compiled Rust extension (passivbot_rust).

    This uses `pb7dir` from `pbgui.ini` and prepends `<pb7dir>/src` to `sys.path`.
    """
    import sys

    src_dir = pb7srcdir()
    if src_dir and src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    import passivbot_rust as pbr  # type: ignore

    return pbr

PBGDIR = Path.cwd()

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except (ValueError,TypeError) as err:
        return False
    return True

def validateHJSON(hjsonData):
    try:
        hjson.loads(hjsonData)
    except (ValueError) as err:
        return False
    return True

def config_pretty_str(config: dict):
    try:
        return json.dumps(config, indent=4)
    except TypeError:
        pretty_str = pprint.pformat(config)
        for r in [("'", '"'), ("True", "true"), ("False", "false"), ("None", "null")]:
            pretty_str = pretty_str.replace(*r)
        return pretty_str

def load_symbols_from_ini(exchange: str, market_type: str):
    pb_config = configparser.ConfigParser()
    pb_config.read('pbgui.ini')
    if pb_config.has_option("exchanges", f'{exchange}.{market_type}'):
        return eval(pb_config.get("exchanges", f'{exchange}.{market_type}'))
    else:
        return []


def list_remote_git_branches(remote_url: str, timeout_sec: int = 20) -> list[str]:
    """Return branch names from a remote URL (e.g. https://github.com/<user>/passivbot.git).

    Uses `git ls-remote --heads` and parses refs/heads/*.
    """
    remote_url = (remote_url or "").strip()
    if not remote_url:
        return []

    try:
        res = subprocess.run(
            ["git", "ls-remote", "--heads", remote_url],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Timeout fetching remote branches ({timeout_sec}s).") from e
    except Exception as e:
        raise RuntimeError("Failed to run git to fetch remote branches.") from e

    if res.returncode != 0:
        msg = (res.stderr or "").strip() or "git ls-remote failed"
        raise RuntimeError(msg)

    branches: list[str] = []
    for line in (res.stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        ref = parts[1]
        if ref.startswith("refs/heads/"):
            branches.append(ref[len("refs/heads/"):])

    return sorted(set(branches))


def list_git_remotes(repo_dir: str, timeout_sec: int = 10) -> list[str]:
    """List git remote names for a local repo directory."""
    repo_dir = (repo_dir or "").strip()
    if not repo_dir:
        return []
    try:
        res = subprocess.run(
            ["git", "-C", repo_dir, "remote"],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except Exception:
        return []
    if res.returncode != 0:
        return []
    remotes = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip()]
    return sorted(set(remotes))


def get_git_remote_url(repo_dir: str, remote_name: str, timeout_sec: int = 10) -> str:
    """Get remote URL from a local repo directory. Returns empty string on failure."""
    repo_dir = (repo_dir or "").strip()
    remote_name = (remote_name or "").strip()
    if not repo_dir or not remote_name:
        return ""
    try:
        res = subprocess.run(
            ["git", "-C", repo_dir, "remote", "get-url", remote_name],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
    except Exception:
        return ""
    if res.returncode != 0:
        return ""
    return (res.stdout or "").strip()
