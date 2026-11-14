"""Native config bridge and document tests using only isolated fixture runtimes."""

import ast
import copy
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
    assert list(runtime.root.rglob("__pycache__")) == []


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


def test_helper_rejects_start_operation(runtime):
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


@pytest.fixture
def validation_runtime(runtime, tmp_path):
    """Record native validation calls in a fake module without real v9/Rust code."""
    runtime.import_marker = tmp_path / "native_load_imported"
    runtime.source_import_marker = tmp_path / "native_config_imported"
    runtime.record = tmp_path / "native_validation_call.json"
    runtime.loader = runtime.root / "src" / "config" / "load.py"
    (runtime.loader.parent / "__init__.py").write_text(
        f'from pathlib import Path\nPath({str(runtime.source_import_marker)!r}).write_text("imported")\n',
        encoding="utf-8")
    runtime.loader.write_text(
        'import json\nfrom pathlib import Path\n'
        f'Path({str(runtime.import_marker)!r}).write_text("imported")\n'
        'def prepare_config(config, **kwargs):\n'
        f'    Path({str(runtime.record)!r}).write_text(json.dumps({{"config": config, "kwargs": kwargs}}))\n'
        '    config.setdefault("bot", {}).setdefault("long", {})["expanded"] = True\n'
        '    config["_raw"] = {"internal": True}\n'
        '    return config\n', encoding="utf-8")
    return runtime


def _assert_no_native_import(runtime):
    """Both package initialization and the native loader must remain untouched."""
    assert not runtime.source_import_marker.exists()
    assert not runtime.import_marker.exists()


def test_native_validation_preserves_caller_source_and_uses_canonical_kwargs(validation_runtime):
    """Native preparation validates only; expanded results never replace authored input."""
    runtime = validation_runtime
    config = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
              "optimize": {"bounds": {}}, "coin_overrides": {}}
    before = copy.deepcopy(config)
    previous = sys.modules.get("config.load")
    result = pb9_config.validate_pb9_config(config, runtime.root, runtime.python)
    assert result["valid"] is True
    assert runtime.source_import_marker.exists()
    assert runtime.import_marker.exists()
    assert "config" not in result
    assert result["source"]["load_module_file"] == str(runtime.loader)
    assert config == before
    assert sys.modules.get("config.load") is previous
    call = json.loads(runtime.record.read_text())
    assert call["config"] == before
    assert call["kwargs"] == {"base_config_path": "", "verbose": False, "target": "canonical", "runtime": None}


@pytest.mark.parametrize("approved", [[], ["BTC", "binance:ETH/USDT:USDT"], None, "all"])
def test_inline_universe_and_no_io_all_sentinel_are_supported(validation_runtime, approved):
    """Audited native list/inheritance/all branches never request coin-list files."""
    config = {"config_version": "v9.0.0", "bot": {"global": {"universe": {
        "approved_coins": approved, "ignored_coins": []}}}}
    assert pb9_config.validate_pb9_config(config, validation_runtime.root, validation_runtime.python)["valid"] is True


_INVALID_INLINE_CASES = [
    ({}, "config_version_mismatch"),
    ({"config_version": "v8.6.0"}, "config_version_mismatch"),
    ({"config_version": "v9.1.0"}, "config_version_mismatch"),
    ({"pbgui": {"note": "GUI-only"}}, "invalid_config"),
    ({"_raw": {"live": {"approved_coins": "/foreign"}}}, "invalid_config"),
    ({"optimize": {"fixed_runtime_overrides": {"bot._coins_sources": []}}}, "invalid_config"),
    ({"live": {"base_config_path": "/foreign/config.json"}}, "external_reference_rejected"),
    ({"coin_overrides": {"BTC": {"strategy": {}}}}, "external_reference_rejected"),
    ({"optimize": {"fixed_runtime_overrides": {"coin_overrides.BTC": {}}}}, "external_reference_rejected"),
    ({"optimize": {"fixed_runtime_overrides": {"coin_overrides.BTC.override_config_path": "/foreign"}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": "BTC"}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"ignored_coins": "all"}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["../coins.txt"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": [" /foreign"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["coin-list.JSON"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["~/.secrets"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["C:\\foreign"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["BTC,/foreign"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["a,.txt"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"ignored_coins": ["sub/x, .json"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": [".txt"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"ignored_coins": ["sub/.json"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["a,sub/.hjson"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["api-keys.json/"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["sub/x.txt/."]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"ignored_coins": ["a,x.hjson//"]}}}}, "external_reference_rejected"),
    ({"bot": {"global": {"universe": {"approved_coins": ["a/../x.json/"]}}}}, "external_reference_rejected"),
    ({"optimize": {"fixed_runtime_overrides": {"bot.long.universe.approved_coins.0": "/foreign"}}}, "external_reference_rejected"),
    ({"backtest": {"scenarios": [{"overrides": {"bot.long.universe.ignored_coins": ["file:///foreign"]}}]}}, "external_reference_rejected"),
    ({"backtest": {"coin_sources": {"BTC": "binance"}}}, "external_reference_rejected"),
    ({"optimize": {"fixed_runtime_overrides": {"backtest.coin_sources.BTC": "binance"}}}, "external_reference_rejected"),
    ({"backtest": {"organillo_csv": "/foreign/input.csv"}}, "external_reference_rejected"),
    ({"bot": {"long": {"value": float("nan")}}}, "invalid_config"),
]


@pytest.mark.parametrize(("patch", "code"), _INVALID_INLINE_CASES)
def test_inline_gate_rejects_before_parent_starts_any_process(validation_runtime, monkeypatch, patch, code):
    """Unsupported input cannot reach a native process or read config references."""
    config = {"config_version": "v9.0.0", **copy.deepcopy(patch)}
    if patch == {}:
        config = {}

    def forbidden(*args, **kwargs):
        """A gate or schema failure must precede the helper launch."""
        pytest.fail("invalid inline config launched a process")

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config(config, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == (code,)
    _assert_no_native_import(validation_runtime)


@pytest.mark.parametrize(("patch", "code"), [case for case in _INVALID_INLINE_CASES
                                         if case[0].get("config_version") != "v9.1.0"])
def test_helper_repeats_inline_gate_before_native_import(validation_runtime, patch, code):
    """Direct protocol callers cannot bypass the same gate with a file reference."""
    config = {"config_version": "v9.0.0", **copy.deepcopy(patch)}
    if patch == {}:
        config = {}
    request = {"operation": "validate", "pb9_dir": str(validation_runtime.root),
               "config_schema": "v9.0.0", "package_version": "8.2.0.dev0", "config": config}
    proc = subprocess.run([str(validation_runtime.python), "-I", str(pb9_config._HELPER_PATH)],
                          input=json.dumps(request), text=True, capture_output=True, timeout=5)
    assert json.loads(proc.stdout) == {"ok": False, "error_code": code}
    _assert_no_native_import(validation_runtime)


def test_validation_rejects_python_tuples_before_json_can_coerce_them(validation_runtime):
    """Tuple inputs are a parent-only case because JSON would turn them into lists."""
    config = {"config_version": "v9.0.0", "bot": {"long": {"value": (1, 2)}}}
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config(config, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == ("invalid_config",)
    _assert_no_native_import(validation_runtime)


@pytest.mark.parametrize(("exception", "code"), [
    ("ValueError", "native_config_rejected"), ("TypeError", "native_config_rejected"),
    ("KeyError", "native_config_rejected"), ("OSError", "native_validation_failed"),
])
def test_native_validation_failures_never_return_or_log_raw_error_text(validation_runtime, exception, code):
    """Native rejection data remains private; only the fixed diagnostic crosses back."""
    import logging_helpers

    secret = "fixture-private-native-error"
    validation_runtime.loader.write_text(
        f'def prepare_config(config, **kwargs):\n    raise {exception}({secret!r})\n', encoding="utf-8")
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config({"config_version": "v9.0.0"}, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == (code,)
    assert secret not in str(caught.value)
    for path in logging_helpers.LOG_ROOT.glob("*.log"):
        assert secret not in path.read_text()


def test_validation_protocol_accepts_more_than_template_input_budget(validation_runtime):
    """A large request exercises partial pipe writes beyond the Linux pipe buffer."""
    config = {"config_version": "v9.0.0", "bot": {"global": {"universe": {"approved_coins": ["BTC"] * 50000}}}}
    assert len(json.dumps(config)) > 300000
    assert pb9_config.validate_pb9_config(config, validation_runtime.root, validation_runtime.python)["valid"] is True
    assert json.loads(validation_runtime.record.read_text())["config"] == config


@pytest.mark.parametrize("exception", ["ValueError", "TypeError", "KeyError"])
def test_loader_import_failure_is_not_an_authored_config_rejection(validation_runtime, exception):
    """Import failures are runtime failures even when they resemble input exceptions."""
    validation_runtime.loader.write_text(f'raise {exception}("fixture-only-import-error")\n', encoding="utf-8")
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config({"config_version": "v9.0.0"}, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == ("native_validation_failed",)


def test_validation_schema_import_failure_uses_validation_error_code(validation_runtime):
    """Unexpected schema-import failure belongs to validation, not template loading."""
    with validation_runtime.schema.open("a", encoding="utf-8") as stream:
        stream.write('\nraise OSError("fixture-only-schema-error")\n')
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config({"config_version": "v9.0.0"}, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == ("native_validation_failed",)


def test_validation_budget_rejects_oversized_requests_without_native_import(validation_runtime):
    """Input larger than the validation protocol cap never reaches config.load."""
    config = {"config_version": "v9.0.0", "note": "x" * (1024 * 1024)}
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config(config, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == ("invalid_request",)
    _assert_no_native_import(validation_runtime)


def test_validation_uses_the_same_timeout_and_reaping_owner(validation_runtime, tmp_path):
    """A validation timeout cannot retain its native process."""
    pid_file = tmp_path / "validation.pid"
    validation_runtime.loader.write_text(
        'import os, time\nfrom pathlib import Path\n'
        f'def prepare_config(config, **kwargs):\n    Path({str(pid_file)!r}).write_text(str(os.getpid()))\n'
        '    time.sleep(60)\n', encoding="utf-8")
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_config({"config_version": "v9.0.0"}, validation_runtime.root, validation_runtime.python, timeout=2)
    assert caught.value.error_codes == ("helper_timeout",)
    assert pid_file.exists()
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


def test_parent_rejects_nonfinite_json_protocol_constants(runtime, monkeypatch):
    """Even unused response properties must be strict JSON."""
    monkeypatch.setattr(pb9_config, "_run_helper", lambda *args: b'{"ok":true,"extra":NaN}')
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python)
    assert caught.value.error_codes == ("invalid_response",)


@pytest.mark.parametrize("metadata", [None, {}, {"name": "Example", "runtime": "pb8",
                                               "source": {"commit": "authored-note"}, "notes": ["keep"]}])
def test_document_keeps_gui_metadata_and_provenance_separate(validation_runtime, metadata):
    """Only native sparse source reaches the helper; GUI notes cannot define provenance."""
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
                "optimize": {"bounds": {}}}
    if metadata is not None:
        document["pbgui"] = copy.deepcopy(metadata)
    before = copy.deepcopy(document)
    result = pb9_config.validate_pb9_document(document, validation_runtime.root, validation_runtime.python)
    expected_config = {key: value for key, value in before.items() if key != "pbgui"}
    assert set(result) == {"config", "pbgui", "source"}
    assert result["config"] == expected_config
    assert result["pbgui"] == metadata
    assert result["source"]["config_schema"] == "v9.0.0"
    assert result["source"]["commit"] != "authored-note"
    assert json.loads(validation_runtime.record.read_text())["config"] == expected_config
    assert document == before
    result["config"]["bot"]["long"]["caller_isolation"] = True
    if metadata:
        result["pbgui"]["notes"].append("result-only")
    assert document == before


@pytest.mark.parametrize("document", [None, [], "not a document", {"pbgui": None}, {"pbgui": []},
    {"pbgui": {"bad": float("nan")}}, {"pbgui": {"bad": (1, 2)}},
    {"pbgui": {1: "non-string key"}}, {"pbgui": {"bad": object()}},
    {"pbgui": {"note": "x" * pb9_config.MAX_DOCUMENT_BYTES}},
])
def test_invalid_documents_fail_before_runtime_inspection(document, monkeypatch):
    """Malformed or oversized GUI documents cannot inspect or start a runtime."""
    def forbidden(*args, **kwargs):
        """The document check must run before either runtime inspection or launch."""
        pytest.fail("invalid document reached runtime")

    monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_document(document, None, None)
    assert caught.value.error_codes == ("invalid_document",)


@pytest.mark.parametrize(("patch", "code"), [
    ({"bot": {"long": {"pbgui": {}}}}, "invalid_config"),
    ({"bot": {"_raw": {}}}, "invalid_config"),
    ({"config_version": "v8.6.0"}, "config_version_mismatch"),
    ({"coin_overrides": {"BTC": {}}}, "external_reference_rejected"),
])
def test_document_retains_native_fail_closed_rules(validation_runtime, patch, code):
    """Removing root GUI metadata never bypasses native schema and reference guards."""
    document = {"config_version": "v9.0.0", "pbgui": {"name": "Example"}, **patch}
    before = copy.deepcopy(document)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_document(document, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == (code,)
    assert document == before
    _assert_no_native_import(validation_runtime)


def test_document_native_rejection_returns_no_partial_document(validation_runtime):
    """An unsuccessful native check raises its existing code instead of exposing a result."""
    validation_runtime.loader.write_text(
        'def prepare_config(config, **kwargs):\n    raise ValueError("fixture-private-native-rejection")\n',
        encoding="utf-8")
    document = {"config_version": "v9.0.0", "pbgui": {"name": "Example"}}
    before = copy.deepcopy(document)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_document(document, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == ("native_config_rejected",)
    assert document == before


def test_document_snapshot_precedes_runtime_work(validation_runtime, monkeypatch):
    """Caller edits during validation cannot change the returned or validated snapshot."""
    document = {"config_version": "v9.0.0", "bot": {"long": {}}, "pbgui": {"notes": ["initial"]}}
    before = copy.deepcopy(document)
    original_inspect = pb9_config.inspect_pb9_runtime

    def inspect_after_caller_edit(*args, **kwargs):
        """Model a caller changing its mutable form while runtime work proceeds."""
        document["bot"]["long"]["later"] = True
        document["pbgui"]["notes"].append("later")
        return original_inspect(*args, **kwargs)

    monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", inspect_after_caller_edit)
    result = pb9_config.validate_pb9_document(document, validation_runtime.root, validation_runtime.python)
    expected_config = {key: value for key, value in before.items() if key != "pbgui"}
    assert result["config"] == expected_config
    assert json.loads(validation_runtime.record.read_text())["config"] == expected_config
    assert result["pbgui"] == before["pbgui"]
    assert document["pbgui"]["notes"] == ["initial", "later"]


def test_document_metadata_type_is_checked_on_captured_snapshot(monkeypatch):
    """A caller edit during capture cannot return null as if metadata were absent."""
    document = {"config_version": "v9.0.0", "pbgui": {}}
    original_check = pb9_config._require_json_value

    def check_before_caller_edit(value):
        """Change to another JSON value after the initial strict JSON check."""
        original_check(value)
        document["pbgui"] = None

    def forbidden(*args, **kwargs):
        """The captured metadata error must be rejected before runtime work."""
        pytest.fail("invalid captured metadata reached runtime")

    monkeypatch.setattr(pb9_config, "_require_json_value", check_before_caller_edit)
    monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_document(document, None, None)
    assert caught.value.error_codes == ("invalid_document",)


def test_document_limit_is_distinct_from_helper_envelope_budget(validation_runtime, monkeypatch):
    """A near-limit document can fail the separate request budget after inspection."""
    document = {"config_version": "v9.0.0", "bot": {"global": {"universe": {"approved_coins": [""]}}}}
    padding = pb9_config.MAX_DOCUMENT_BYTES - len(json.dumps(document).encode("utf-8"))
    document["bot"]["global"]["universe"]["approved_coins"] = ["X" * padding]
    assert len(json.dumps(document).encode("utf-8")) == pb9_config.MAX_DOCUMENT_BYTES
    status = pb9_config.inspect_pb9_runtime(validation_runtime.root, validation_runtime.python)
    inspections = []

    def inspect(*args, **kwargs):
        """Return an already inspected fake runtime, avoiding Git subprocesses."""
        inspections.append(True)
        return status

    def forbidden(*args, **kwargs):
        """An oversized protocol request must never launch its helper."""
        pytest.fail("oversized protocol request launched helper")

    monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", inspect)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_document(document, validation_runtime.root, validation_runtime.python)
    assert caught.value.error_codes == ("invalid_request",)
    assert inspections == [True]
    _assert_no_native_import(validation_runtime)


def test_shared_file_loader_reaches_isolated_native_validator(validation_runtime, tmp_path):
    """Read a real fixture file through the shared entry point and fake native helper."""
    from pb7_config import load_pb7_config
    from pb9_config_storage import PB9FileContext

    root = tmp_path / "library"
    root.mkdir()
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
                "optimize": {"bounds": {}}, "pbgui": {"name": "Example"}}
    path = root / "example.json"
    path.write_text(json.dumps(document, indent=4), encoding="utf-8")
    before = path.read_bytes()
    context = PB9FileContext(root, validation_runtime.root, validation_runtime.python)
    assert load_pb7_config(path.name, pb9_context=context) == document
    native_input = json.loads(validation_runtime.record.read_text())["config"]
    assert native_input == {key: value for key, value in document.items() if key != "pbgui"}
    assert path.read_bytes() == before
