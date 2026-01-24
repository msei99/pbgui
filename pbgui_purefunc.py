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


def pb7_suite_preflight_errors(config: dict) -> list[str]:
    """Return preflight errors for PB7 suite configs.

    PB7 behavior note:
    - If `backtest.suite.enabled` is true and `include_base_scenario` is false,
      PB7 builds the master coin universe ONLY from scenario `coins` and
      `coin_sources`. In that mode, `live.approved_coins` is effectively ignored
      for coin selection.

    This helper is intentionally pure (no Streamlit, no IO) and conservative.
    """

    errors: list[str] = []
    if not isinstance(config, dict):
        return ["Invalid config: expected a JSON object."]

    backtest = config.get("backtest")
    if not isinstance(backtest, dict):
        return ["Invalid config: missing or invalid 'backtest' section."]

    suite = backtest.get("suite") or {}
    if not isinstance(suite, dict) or not suite.get("enabled"):
        return []

    scenarios = suite.get("scenarios") or []
    if not isinstance(scenarios, list) or not scenarios:
        errors.append("Suite is enabled but has no scenarios.")
        return errors

    include_base = bool(suite.get("include_base_scenario", False))
    if include_base:
        return []

    base_coin_sources = backtest.get("coin_sources") or {}
    has_base_coin_sources = isinstance(base_coin_sources, dict) and any(
        v is not None and str(v).strip() for v in base_coin_sources.values()
    )

    has_any_scenario_coins = False
    has_any_scenario_coin_sources = False
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            continue
        coins = scenario.get("coins")
        if isinstance(coins, (list, tuple, set)):
            if any(str(x).strip() for x in coins if x is not None):
                has_any_scenario_coins = True
        elif isinstance(coins, str) and coins.strip():
            has_any_scenario_coins = True

        coin_sources = scenario.get("coin_sources")
        if isinstance(coin_sources, dict) and any(
            v is not None and str(v).strip() for v in coin_sources.values()
        ):
            has_any_scenario_coin_sources = True

    if not (has_base_coin_sources or has_any_scenario_coins or has_any_scenario_coin_sources):
        errors.append(
            "Suite is enabled with include_base_scenario=false, but no coins are defined in any scenario "
            "(and no coin_sources are set). PB7 will build an empty master coin list and fail with "
            "'No coin data found on any exchange for the requested date range.'\n"
            "Fix: enable include_base_scenario, OR set scenario coins, OR set coin_sources."
        )

    return errors
