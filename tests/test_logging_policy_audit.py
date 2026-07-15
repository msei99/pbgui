"""Repository-wide logging policy and embedded remote logger guardrails."""

import ast
import builtins
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_PARTS = {"tests", "pb7", "upstream", "vendor", "generated", ".git", ".venv", "venv"}
DOCUMENTED_SCRIPT_DIRS = {
    "scripts": "One-off maintenance and diagnostic scripts are human-facing CLIs.",
    "tools": "Developer audit and comparison tools intentionally report to stdout.",
}
PRINT_ALLOWLIST = {
    "pb7_guard.py": "PB7 guard is a human-facing install/update safety CLI.",
    "starter.py": "Starter is the human-facing service-control CLI.",
    "reprocess_tradfi_splits.py": "Maintenance CLI prints progress and its final report.",
    "setup/installer/cli.py": "Installer CLI output is its user interface.",
    "setup/installer/web.py": "Installer subprocess output is forwarded to its transcript.",
    "task_worker.py": "Worker stdout is a machine-readable parent-process protocol.",
}
APPEND_ALLOWLIST = {
    "logging_helpers.py": "Central logging fallback and lock implementation.",
    "hyperliquid_best_1m.py": "Pipeline advisory lock file, not a log sink.",
    "market_data_sources.py": "Pipeline advisory lock file, not a log sink.",
    "PBRun.py": "Dedicated bot stderr transcript.",
    "vps_manager_core.py": "User-visible VPS task transcripts.",
    "task_worker.py": "Dedicated worker protocol/task transcript.",
    "api/optimize_v7.py": "Dedicated optimization job transcript.",
    "api/ohlcv_preload_worker.py": "Detached child owns one canonical managed preload descriptor.",
}
LOGGING_CALL_ALLOWLIST = {
    "PBApiServer.py": "Bridges third-party logger records into human_log.",
}
HUMAN_LOG_SERVICE_MODULES = {
    "Database.py", "Exchange.py", "ParetoDataLoader.py", "Status.py", "vps_manager_core.py",
    "PBRun.py", "PBCoinData.py", "PBData.py", "market_data.py", "binance_best_1m.py",
    "hyperliquid_aws.py", "tradfi_sync.py", "api/live.py",
}
TIER_3_SERVICES = {
    "ApiLogging", "ApiKeys", "BalanceCalc", "CoinDataUI", "Dashboard", "Services",
    "V7Instances", "MarketDataAPI", "PB7OhlcvAPI", "PBV7UI",
}
DEDICATED_SERVICES = {
    "OptimizeQueueAPI", "PBRun", "PBData", "PBCoinData", "PBCluster", "PBApiServer",
    "PBMonitorAgent", "VPSMonitor", "MarketData",
}


def _production_trees():
    """Yield parsed production modules while excluding documented script trees."""
    for path in ROOT.rglob("*.py"):
        relative = path.relative_to(ROOT)
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if relative.parts[0] in DOCUMENTED_SCRIPT_DIRS:
            continue
        yield relative.as_posix(), ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))


def _call_name(node):
    """Return a dotted call target for direct name and attribute calls."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _call_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def _append_open_calls(tree):
    """Yield append-mode built-in or pathlib open calls."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node.func).split(".")[-1] != "open":
            continue
        mode = node.args[1] if len(node.args) > 1 else next(
            (keyword.value for keyword in node.keywords if keyword.arg == "mode"), None
        )
        if isinstance(mode, ast.Constant) and isinstance(mode.value, str) and "a" in mode.value:
            yield node


def _embedded_helper_source():
    """Extract only the standalone helper definitions from the remote command string."""
    module = ast.parse((ROOT / "master" / "async_monitor.py").read_text(encoding="utf-8"))
    assignment = next(
        node for node in module.body
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "INSTANCE_COLLECT_SCRIPT" for target in node.targets)
    )
    command = ast.literal_eval(assignment.value)
    assert command.startswith('python3 -u -c "') and command.endswith('"')
    source = command[len('python3 -u -c "'):-1]
    return source[:source.index("TODAY_START =")]


def test_forbidden_logging_calls_do_not_return():
    """Production code must not revive traceback printing or direct stdlib level calls."""
    violations = []
    levels = {"debug", "info", "warning", "error", "exception", "critical"}
    for relative, tree in _production_trees():
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _call_name(node.func)
            if name == "traceback.print_exc":
                violations.append(f"{relative}:{node.lineno}: {name}")
            if name.startswith("logging.") and name.rsplit(".", 1)[-1] in levels and relative not in LOGGING_CALL_ALLOWLIST:
                violations.append(f"{relative}:{node.lineno}: {name}")
    assert not violations, "Unapproved logging calls:\n" + "\n".join(violations)


def test_alternative_loggers_and_direct_appenders_do_not_return():
    """Alternative logger modules and new append sinks require explicit ownership review."""
    assert not (ROOT / "Log.py").exists()
    violations = []
    for relative, tree in _production_trees():
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [alias.name for alias in node.names]
                if getattr(node, "module", None) == "Log" or "Log" in names:
                    violations.append(f"{relative}:{node.lineno}: imports Log.py")
            if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name == "LogHandler":
                violations.append(f"{relative}:{node.lineno}: defines LogHandler")
        if relative not in APPEND_ALLOWLIST:
            violations.extend(f"{relative}:{node.lineno}: append-mode open" for node in _append_open_calls(tree))
    assert not violations, "Unapproved logger or appender:\n" + "\n".join(violations)


def test_pbgui_transcript_writers_use_canonical_log_root():
    """Known PBGui-owned transcript writers must not target legacy state directories."""
    forbidden = {
        "task_worker.py": ("data/market_data/_tasks/logs",),
        "vps_manager_core.py": ("data/vpsmanager/hosts/{host}/*.log", "data/vpsmanager/*.log"),
        "monitor_agent.py": ('DATA_DIR / "live_metrics.ndjson"',),
        "api/pb7_ohlcv_tools.py": ('"ohlcv_preload" / "logs"',),
    }
    violations = []
    for relative, patterns in forbidden.items():
        source = (ROOT / relative).read_text(encoding="utf-8")
        violations.extend(f"{relative}: {pattern}" for pattern in patterns if pattern in source)
    assert not violations, "Noncanonical PBGui transcript writer:\n" + "\n".join(violations)


def test_external_pb7_log_paths_remain_external():
    """PB7 native bot logs remain outside PBGui's managed transcript scopes."""
    import logging_helpers

    assert logging_helpers.resolve_managed_log_scope(ROOT.parent / "pb7" / "logs" / "bot.log") is None
    source = (ROOT / "master" / "async_logs.py").read_text(encoding="utf-8")
    assert 'pb7dir_value, "logs"' in source


def test_runtime_prints_are_explicitly_allowlisted():
    """Only reviewed CLI, installer, maintenance, and worker protocols may print."""
    violations = []
    for relative, tree in _production_trees():
        if relative in PRINT_ALLOWLIST:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_name(node.func) in {"print", "builtins.print"}:
                violations.append(f"{relative}:{node.lineno}")
    assert not violations, "Unapproved runtime print calls:\n" + "\n".join(violations)


def test_human_log_modules_define_service_constants():
    """Requested human_log adopters must expose a stable module SERVICE identity."""
    for relative in HUMAN_LOG_SERVICE_MODULES:
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"), filename=relative)
        assignments = {
            target.id
            for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign))
            for target in ((node.targets if isinstance(node, ast.Assign) else [node.target]))
            if isinstance(target, ast.Name)
        }
        assert "SERVICE" in assignments, f"{relative} uses human_log without SERVICE"


def test_tier_ownership_contract_remains_explicit():
    """Tier-3 services stay grouped while daemon and pipeline logs stay dedicated."""
    import logging_helpers

    assert {service for service in TIER_3_SERVICES if logging_helpers.LOG_GROUPS.get(service) == "PBGui"} == TIER_3_SERVICES
    assert DEDICATED_SERVICES.isdisjoint(logging_helpers.LOG_GROUPS)
    assert {
        "Auth", "LiveSession", "ApiKeyState", "User",
    } <= {service for service, group in logging_helpers.LOG_GROUPS.items() if group == "PBGui"}


def test_logging_monitor_remains_cookie_only():
    """Logging Monitor must not regain token placeholders or authorization injection."""
    backend = (ROOT / "api" / "logging.py").read_text(encoding="utf-8")
    frontend = (ROOT / "frontend" / "logging_monitor.html").read_text(encoding="utf-8")
    for forbidden in ("%%TOKEN%%", "Authorization", "session.token", "pbgui_session"):
        assert forbidden not in backend
        assert forbidden not in frontend


def test_embedded_remote_logger_rotates_and_redacts(tmp_path, monkeypatch):
    """The standalone VPS logger uses PBGDIR, bounded backups, and broad redaction."""
    monkeypatch.setenv("PBGUI_PBGDIR", str(tmp_path))
    namespace = {}
    exec(compile(_embedded_helper_source(), "<vps-monitor-helper>", "exec"), namespace)
    namespace["LOG_MAX_BYTES"] = 256
    secrets = (
        'AuThOrIzAtIoN: Bearer auth-secret {"ToKeN": "json-secret"} '
        "url=https://host/path?api_key=query-secret&ok=1 password='pass-secret' "
        "-----BEGIN PRIVATE KEY-----\npem-secret\n-----END PRIVATE KEY-----"
    )
    for index in range(8):
        namespace["_log"]("VPSMonitor", f"{secrets} record-{index}", level="ERROR")

    log_dir = tmp_path / "data" / "logs"
    files = sorted(log_dir.glob("VPSMonitor.log*"))
    assert (log_dir / "VPSMonitor.log") in files
    assert not (log_dir / "VPSMonitor.log.4").exists()
    content = "".join(path.read_text(encoding="utf-8") for path in files if path.suffix != ".lock")
    for secret in ("auth-secret", "json-secret", "query-secret", "pass-secret", "pem-secret"):
        assert secret not in content
    assert content.count("[REDACTED]") >= 5
    assert "&ok=1" in content


def test_embedded_remote_logger_sanitizes_stderr_fallback(tmp_path, monkeypatch, capsys):
    """Last-resort stderr output must never expose the original exception text secret."""
    monkeypatch.setenv("PBGUI_PBGDIR", str(tmp_path))
    namespace = {}
    exec(compile(_embedded_helper_source(), "<vps-monitor-helper>", "exec"), namespace)
    real_open = builtins.open

    def failing_open(path, *args, **kwargs):
        if str(path).endswith("VPSMonitor.log.lock"):
            raise OSError("password=fallback-exception-secret")
        return real_open(path, *args, **kwargs)

    namespace["open"] = failing_open
    namespace["_log"]("VPSMonitor", "Cookie: fallback-cookie exception token=exception-secret")
    stderr = capsys.readouterr().err
    assert "fallback-cookie" not in stderr
    assert "exception-secret" not in stderr
    assert "fallback-exception-secret" not in stderr
    assert "[REDACTED]" in stderr


def test_embedded_source_is_a_reviewed_protocol_exception():
    """Embedded code remains standalone and avoids importing PBGui logging modules."""
    source = _embedded_helper_source()
    tree = ast.parse(source)
    imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    assert "logging_helpers" not in imports
    assert not any(
        isinstance(node, ast.Call) and _call_name(node.func) in {"print", "builtins.print"}
        for node in ast.walk(tree)
    )
