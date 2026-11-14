"""Native template bridge tests using only isolated, minimal fixture runtimes."""

import ast
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
import venv

import pytest

import pb9_config


@pytest.fixture
def runtime(tmp_path):
    """Create a real temporary venv with fake config modules, never real bot code."""
    if not pb9_config._SUPPORTED_PLATFORM:
        pytest.skip("Native process-group ownership currently requires Linux")
    root = tmp_path / "pb9"
    package = root / "src" / "config"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    schema = package / "schema.py"
    schema.write_text(
        'CONFIG_SCHEMA_VERSION = "v9.0.0"\n'
        'import os, uuid\n'
        'print("native import noise")\n'
        'def get_template_config():\n'
        '    print("native template noise")\n'
        '    return {"config_version": CONFIG_SCHEMA_VERSION,\n'
        '            "bot": {"global": {}, "long": {}, "short": {}},\n'
        '            "optimize": {"bounds": {}}, "nonce": uuid.uuid4().hex,\n'
        '            "leaked_env": os.environ.get("PBGUI_TEST_SECRET"),\n'
        '            "leaked_library": os.environ.get("LD_LIBRARY_PATH"),\n'
        '            "path": os.environ.get("PATH")}\n', encoding="utf-8",
    )
    (root / "src" / "passivbot_version.py").write_text('__version__ = "8.2.0.dev0"\n', encoding="utf-8")
    env_dir = tmp_path / "venv_pb9"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(env_dir)
    scripts = env_dir / ("Scripts" if os.name == "nt" else "bin")
    python = scripts / ("python.exe" if os.name == "nt" else "python")
    cli = scripts / ("passivbot.exe" if os.name == "nt" else "passivbot")
    cli.write_bytes(b"inert CLI fixture")
    cli.chmod(0o700)
    site = env_dir / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    if os.name == "nt":
        site = env_dir / "Lib" / "site-packages"
    rust = site / "passivbot_rust" / "passivbot_rust.abi3.so"
    rust.parent.mkdir(parents=True, exist_ok=True)
    rust.write_bytes(b"inert Rust presence fixture, never loaded")
    return SimpleNamespace(root=root, python=python, schema=schema, env=env_dir)


def test_template_is_loaded_in_own_process_and_preserves_authored_shape(runtime):
    """Native defaults survive unchanged, including sparse sides and bounds."""
    previous = sys.modules.get("config.schema")
    result = pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert result["config"]["config_version"] == "v9.0.0"
    assert result["config"]["bot"]["long"] == {}
    assert result["config"]["bot"]["short"] == {}
    assert result["config"]["optimize"]["bounds"] == {}
    assert result["source"]["config_schema"] == "v9.0.0"
    assert result["source"]["package_version"] == "8.2.0.dev0"
    assert result["source"]["commit_available"] is False
    assert Path(result["source"]["schema_module_file"]) == runtime.schema
    assert sys.modules.get("config.schema") is previous


def test_requests_have_no_shared_module_or_template_cache(runtime):
    """Every call owns a new process instead of retaining native module state."""
    first = pb9_config.load_pb9_template(runtime.root, runtime.python)
    second = pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert first["config"]["nonce"] != second["config"]["nonce"]


def test_foreign_python_path_and_provider_secrets_are_not_inherited(runtime, tmp_path, monkeypatch):
    """An ambient module path cannot replace v9 modules or receive GUI secrets."""
    foreign = tmp_path / "foreign"
    (foreign / "config").mkdir(parents=True)
    marker = tmp_path / "foreign_executed"
    (foreign / "config" / "__init__.py").write_text(
        f'from pathlib import Path\nPath({str(marker)!r}).touch()\n', encoding="utf-8")
    monkeypatch.setenv("PYTHONPATH", str(foreign))
    monkeypatch.setenv("PYTHONHOME", str(foreign))
    monkeypatch.setenv("VIRTUAL_ENV", str(foreign))
    monkeypatch.setenv("PBGUI_TEST_SECRET", "fixture-only-value")
    monkeypatch.setenv("LD_LIBRARY_PATH", str(foreign))
    monkeypatch.setenv("PATH", str(foreign))
    result = pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert result["config"]["leaked_env"] is None
    assert result["config"]["leaked_library"] is None
    assert result["config"]["path"] == os.pathsep.join((str(runtime.python.parent), os.defpath))
    assert not marker.exists()


def test_v8_runtime_is_rejected_without_starting_python(runtime, monkeypatch):
    """A v8 schema cannot be used through the v9 template bridge."""
    runtime.schema.write_text('CONFIG_SCHEMA_VERSION = "v8.6.0"\n', encoding="utf-8")

    def forbidden(*args, **kwargs):
        """Fail if static rejection starts the configured interpreter."""
        pytest.fail("invalid runtime started a helper")

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == ("runtime_not_detected",)


def test_native_schema_mismatch_is_rejected_before_template_execution(runtime, tmp_path):
    """Native import identity must agree with the inspected literal declarations."""
    marker = tmp_path / "template_executed"
    runtime.schema.write_text(
        'CONFIG_SCHEMA_VERSION = "v9.0.0"\n'
        'globals()["CONFIG_SCHEMA_VERSION"] = "v9.1.0"\n'
        f'def get_template_config():\n    open({str(marker)!r}, "w").close()\n', encoding="utf-8")
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == ("runtime_identity_mismatch",)
    assert not marker.exists()


def test_helper_timeout_kills_and_reaps_owned_process(runtime, tmp_path):
    """A sleeping template cannot outlive the bounded request."""
    pid_file = tmp_path / "helper.pid"
    runtime.schema.write_text(
        'CONFIG_SCHEMA_VERSION = "v9.0.0"\nimport os, time\n'
        f'def get_template_config():\n    open({str(pid_file)!r}, "w").write(str(os.getpid()))\n'
        '    time.sleep(60)\n', encoding="utf-8")
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python, timeout=2)
    assert caught.value.error_codes == ("helper_timeout",)
    assert pid_file.exists(), "timeout test must prove native execution started"
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


@pytest.mark.parametrize(("body", "expected"), [
    ('print("not JSON")\n', "invalid_response"),
    ('import os\nwhile True:\n    os.write(1, b"x" * 65536)\n', "helper_output_limit"),
    ('raise SystemExit(3)\n', "helper_failed"),
])
def test_invalid_or_oversized_protocol_output_is_bounded(runtime, tmp_path, monkeypatch, body, expected):
    """Malformed/noisy helpers cannot feed unbounded or untrusted results to callers."""
    helper = tmp_path / "fake_helper.py"
    helper.write_text('import sys\nsys.stdin.buffer.read()\n' + body, encoding="utf-8")
    monkeypatch.setattr(pb9_config, "_HELPER_PATH", helper)
    monkeypatch.setattr(pb9_config, "MAX_RESPONSE_BYTES", 1024)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == (expected,)


def test_protocol_module_outside_selected_source_is_rejected(runtime, monkeypatch):
    """Result metadata must not claim a config module from another installation."""
    response = {"ok": True, "result": {
        "config": {"config_version": "v9.0.0"}, "config_schema": "v9.0.0",
        "package_version": "8.2.0.dev0", "schema_module_file": "/foreign/schema.py",
        "version_module_file": str(runtime.root / "src" / "passivbot_version.py"),
    }}
    monkeypatch.setattr(pb9_config, "_run_helper", lambda *args: json.dumps(response).encode())
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == ("runtime_identity_mismatch",)


def test_helper_exposes_only_template_operation(runtime):
    """The helper has no migration, save, optimizer or bot-start operation."""
    request = {"operation": "start", "pb9_dir": str(runtime.root),
               "config_schema": "v9.0.0", "package_version": "8.2.0.dev0"}
    proc = subprocess.run([str(runtime.python), "-I", str(pb9_config._HELPER_PATH)],
                          input=json.dumps(request), text=True, capture_output=True, timeout=5)
    assert json.loads(proc.stdout) == {"ok": False, "error_code": "invalid_request"}


def test_bridge_has_no_pb8_imports():
    """PB9 native code has no dependency on PB8 clients, helpers or caches."""
    for path in (Path(pb9_config.__file__), pb9_config._HELPER_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(alias.name.startswith("pb8_") for alias in node.names)
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("pb8_")


@pytest.mark.parametrize("timeout", [True, False, None, 0, -1, float("inf"), float("nan"), 121, 10 ** 1000])
def test_invalid_timeouts_are_rejected_before_runtime_inspection(timeout, monkeypatch):
    """Invalid budgets never start a child or inspect configured filesystem paths."""
    def forbidden(*args, **kwargs):
        """Fail if budget validation reaches runtime detection."""
        pytest.fail("invalid timeout inspected runtime")

    monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(None, None, timeout=timeout)
    assert caught.value.error_codes == ("invalid_timeout",)


def test_unsupported_platform_fails_before_start(monkeypatch):
    """Platforms without the lifecycle backend receive a clear error, never a weaker fallback."""
    monkeypatch.setattr(pb9_config, "_SUPPORTED_PLATFORM", False)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(None, None)
    assert caught.value.error_codes == ("unsupported_platform",)


@pytest.mark.parametrize("value", ["(1, 2)", "{1: 'value'}", "float('nan')"])
def test_non_json_template_values_are_not_silently_coerced(runtime, value):
    """The authored template roundtrip promises JSON values only."""
    runtime.schema.write_text(
        'CONFIG_SCHEMA_VERSION = "v9.0.0"\n'
        f'def get_template_config():\n    return {{"config_version": CONFIG_SCHEMA_VERSION, "value": {value}}}\n',
        encoding="utf-8")
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == ("invalid_template",)


def test_pipe_write_failure_reaps_the_child(runtime, monkeypatch):
    """Input errors still pass through the process-group cleanup path."""
    def broken_write(*args):
        """Simulate the parent losing its input pipe."""
        raise BrokenPipeError("fixture pipe failure")

    monkeypatch.setattr(pb9_config.os, "write", broken_write)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == ("helper_failed",)


@pytest.mark.parametrize("leader_waits", [False, True])
def test_grandchild_holding_stdout_cannot_outlive_request(runtime, tmp_path, monkeypatch, leader_waits):
    """Clean up descendants after leader success and after a whole-request timeout."""
    helper = tmp_path / "grandchild_helper.py"
    pid_file = tmp_path / "grandchild.pid"
    child_code = "import time; time.sleep(60)"
    response = {"ok": True, "result": {
        "config": {"config_version": "v9.0.0"}, "config_schema": "v9.0.0",
        "package_version": "8.2.0.dev0", "schema_module_file": str(runtime.schema),
        "version_module_file": str(runtime.root / "src" / "passivbot_version.py"),
    }}
    helper.write_text(
        'import json, subprocess, sys, time\nfrom pathlib import Path\n'
        'sys.stdin.buffer.read()\n'
        f'child = subprocess.Popen([sys.executable, "-c", {child_code!r}])\n'
        'start_time = Path(f"/proc/{child.pid}/stat").read_text().rsplit(")", 1)[1].split()[19]\n'
        f'Path({str(pid_file)!r}).write_text(json.dumps({{"pid": child.pid, "start_time": start_time}}))\n'
        + ('time.sleep(60)\n' if leader_waits else f'print({json.dumps(response)!r}, flush=True)\n'),
        encoding="utf-8")
    monkeypatch.setattr(pb9_config, "_HELPER_PATH", helper)
    started = time.monotonic()
    try:
        if leader_waits:
            with pytest.raises(pb9_config.PB9ConfigError) as caught:
                pb9_config.load_pb9_template(runtime.root, runtime.python, timeout=2)
            assert caught.value.error_codes == ("helper_timeout",)
        else:
            assert pb9_config.load_pb9_template(runtime.root, runtime.python, timeout=2)["config"] == {
                "config_version": "v9.0.0"}
        assert time.monotonic() - started < 4
        assert pid_file.exists(), "the fixture must actually spawn a grandchild"
        record = json.loads(pid_file.read_text())
        pid = record["pid"]
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            stat = Path(f"/proc/{pid}/stat")
            try:
                state = stat.read_text().rsplit(")", 1)[1].split()[0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            time.sleep(0.01)
        else:
            pytest.fail("owned grandchild remained alive")
    finally:
        # If the implementation regresses, contain the fixture's sleeping child.
        if pid_file.exists():
            record = json.loads(pid_file.read_text())
            try:
                fields = Path(f"/proc/{record['pid']}/stat").read_text().rsplit(")", 1)[1].split()
                if fields[0] != "Z" and fields[19] == record["start_time"]:
                    os.kill(record["pid"], signal.SIGKILL)
            except (FileNotFoundError, ProcessLookupError):
                pass
