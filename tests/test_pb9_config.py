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


def test_market_catalog_reuses_pb8_algorithm_in_selected_native_process(runtime):
    """Native coin names and imported exact IDs survive the isolated shared resolver."""
    from pb9_market import get_market_identifiers
    (runtime.root/'src/simulation_data.py').write_text(
        'from contextlib import contextmanager\n'
        '@contextmanager\ndef simulation_data_policy(config):\n'
        '    assert config["backtest"]["offline"] is True\n    yield\n')
    (runtime.root/'src/utils.py').write_text('''
import json
from pathlib import Path
class AmbiguousMarketIdentifier(RuntimeError): pass
class MarketIdentifierExchangeMismatch(RuntimeError): pass
class UnknownMarketIdentifier(RuntimeError): pass
async def load_markets(exchange, **kwargs):
    return json.loads(Path('caches',exchange,'markets.json').read_text())
def filter_markets(markets, exchange, **kwargs): return markets, None
def get_quote(exchange, quote=None): return quote or ('USDC' if exchange=='hyperliquid' else 'USDT')
def _approved_all_market_identifiers(rows): return {'BTC','HYPE'}
def to_standard_exchange_name(value): return value
def split_exchange_qualified_market_identifier(value):
    return tuple(value.split('::',1)) if '::' in value else (None,value)
def looks_like_exact_market_identifier(value): return '::' in value or '/' in value
def symbol_to_coin(symbol, **kwargs): return symbol.split('/')[0]
def coin_to_symbol(value, exchange, **kwargs):
    venue, identifier=split_exchange_qualified_market_identifier(value)
    if venue and venue!=exchange: raise MarketIdentifierExchangeMismatch()
    markets=json.loads(Path('caches',exchange,'markets.json').read_text())
    for symbol, market in markets.items():
        if identifier in (symbol,market['id'],symbol.split('/')[0]): return symbol
    raise UnknownMarketIdentifier()
async def reject_cross_exchange_market_identifier_collisions(*args,**kwargs): pass
''')
    originals={}
    for exchange,quote,ids in [('bybit','USDT',['BTCUSDT','HYPEUSDT']),('hyperliquid','USDC',['0','150'])]:
        directory=runtime.root/'caches'/exchange
        directory.mkdir(parents=True)
        rows={f'{coin}/{quote}:{quote}':{'symbol':f'{coin}/{quote}:{quote}','id':identifier}
              for coin,identifier in zip(['BTC','HYPE'],ids)}
        path=directory/'markets.json'
        path.write_text(json.dumps(rows))
        originals[path]=path.read_bytes()
    result=get_market_identifiers({'pb9dir':str(runtime.root),'pb9venv':str(runtime.python)},
        ['bybit','hyperliquid'],['HYPE','hyperliquid::0','all'],None,coindata=runtime.root/'empty-shared')
    assert result['symbols']==['BTC','HYPE']
    assert [(row['config_id'],row['display']) for row in result['catalog']]==[('BTC','BTC'),('HYPE','HYPE')]
    assert result['statuses']['hyperliquid::0']['normalized']=='hyperliquid::0'
    assert result['statuses']['hyperliquid::0']['resolutions']==[{'exchange':'hyperliquid','symbol':'BTC/USDC:USDC'}]
    assert result['statuses']['HYPE']['status']=='valid'
    assert result['statuses']['all']['status']=='valid'
    assert all(path.read_bytes()==before for path,before in originals.items())
    assert not list(runtime.root.rglob('__pycache__'))


@pytest.mark.parametrize('exchanges,identifiers',[
    ([],[]),(['../bybit'],[]),(['BYBIT'],[]),(['fake'],[]),
    (['bybit'],['HYPE\n']),(['bybit'],['HYPE']*1001),(['bybit'],'HYPE'),
])
def test_market_selection_rejected_before_helper_launch(monkeypatch,exchanges,identifiers):
    """Untrusted selections never inspect a runtime or create a helper process."""
    from pb9_market import get_market_identifiers
    monkeypatch.setattr(pb9_config,'_config_operation',lambda *args,**kwargs:pytest.fail('helper launched'))
    with pytest.raises((ValueError,TypeError)):
        get_market_identifiers({},exchanges,identifiers,None)


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


@pytest.fixture
def result_runtime(validation_runtime):
    """Provide a selected-runtime result reader and a strict authored-key validator."""
    runtime = validation_runtime
    runtime.extract_marker = runtime.root / 'extracted.json'
    runtime.records = runtime.root / 'src/config/result_records.py'
    runtime.records.write_text(
        'from copy import deepcopy\nimport json\nfrom pathlib import Path\n'
        'def extract_result_config(source):\n'
        f'    Path({str(runtime.extract_marker)!r}).write_text(json.dumps(source))\n'
        '    result = deepcopy(source)\n'
        '    if "metrics" in result:\n'
        '        result.pop("metrics")\n'
        '        result.pop("disable_plotting", None)\n'
        '        result.get("backtest", {}).pop("coins", None)\n'
        '        result.get("backtest", {}).pop("cache_dir", None)\n'
        '    return result\n', encoding='utf-8')
    runtime.loader.write_text(runtime.loader.read_text() +
        '\n_original_prepare = prepare_config\n'
        'def prepare_config(config, **kwargs):\n'
        '    if any(key in config.get("backtest", {}) for key in ("coins", "unknown_field")):\n'
        '        raise ValueError("unknown authored backtest field")\n'
        '    return _original_prepare(config, **kwargs)\n', encoding='utf-8')
    return runtime


def test_native_result_reader_extracts_authored_config_without_expanding_defaults(result_runtime):
    """Runtime dataset membership is removed natively; sparse policy and GUI values survive."""
    runtime = result_runtime
    document = {'config_version': 'v9.0.0', 'bot': {'long': {}, 'short': {'enabled': False}},
                'backtest': {'coins': {'bybit': ['HYPE']}, 'cache_dir': '/fixture/cache',
                             'start_date': '2025-01-01'},
                'metrics': {'objectives': {'fixture': 0.3}}, 'disable_plotting': True,
                'pbgui': {'note': 'preserved'}}
    before = copy.deepcopy(document)
    result = pb9_config.validate_pb9_document(document, runtime.root, runtime.python,
                                             validation_target='result')
    assert document == before
    assert result['config'] == {'config_version': 'v9.0.0', 'bot': before['bot'],
                               'backtest': {'start_date': '2025-01-01'}}
    assert result['pbgui'] == before['pbgui']
    assert result['source']['result_records_module_file'] == str(runtime.records)
    assert 'pbgui' not in json.loads(runtime.extract_marker.read_text())
    assert json.loads(runtime.record.read_text())['config'] == result['config']


@pytest.mark.parametrize('record', [False, True])
def test_result_reader_preserves_unknown_fields_for_native_rejection(result_runtime, record):
    """Unknown authored fields are never filtered by a GUI-side whitelist."""
    document = {'config_version': 'v9.0.0', 'bot': {},
                'backtest': {'unknown_field': 1} if record else {'coins': {'bybit': ['HYPE']}}}
    if record:
        document['metrics'] = {}
    with pytest.raises(pb9_config.PB9ConfigError) as error:
        pb9_config.validate_pb9_document(document, result_runtime.root, result_runtime.python,
                                        validation_target='result')
    assert error.value.error_codes == ('native_config_rejected',)


@pytest.mark.parametrize('field,value', [('approved_coins', '/fixture/coins.json'),
                                        ('_raw', {'fixture': True})])
def test_result_conversion_keeps_inline_security_gate(result_runtime, field, value):
    """External-file discovery and internal compiler data never reach native preparation."""
    document = {'config_version': 'v9.0.0', 'metrics': {},
                'bot': {'global': {'universe': {field: value}}}}
    with pytest.raises(pb9_config.PB9ConfigError):
        pb9_config.validate_pb9_document(document, result_runtime.root, result_runtime.python,
                                        validation_target='result')
    assert not result_runtime.record.exists()


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
    if exception == 'RuntimeError':
        assert caught.value.diagnostic == {'stage': 'config_prepare', 'kind': 'runtime_error'}
    for path in logging_helpers.LOG_ROOT.glob("*.log"):
        assert secret not in path.read_text()


@pytest.mark.parametrize('diagnostic', [
    {'stage': 'config_prepare', 'kind': 'runtime_error', 'message': 'fixture-secret'},
    {'stage': 'fixture-secret', 'kind': 'runtime_error'},
    {'stage': 'config_prepare', 'kind': ['fixture-secret']},
])
def test_helper_diagnostics_are_allowlisted(diagnostic):
    """Untrusted helper fields cannot widen the safe diagnostic vocabulary."""
    payload = {'ok': False, 'error_code': 'native_validation_failed', 'diagnostic': diagnostic}
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config._checked_response(json.dumps(payload).encode(), {})
    assert 'fixture-secret' not in repr(caught.value.diagnostic)
    if diagnostic.get('stage') == 'config_prepare' and diagnostic.get('kind') == 'runtime_error':
        assert caught.value.diagnostic == {'stage': 'config_prepare', 'kind': 'runtime_error'}
    else:
        assert caught.value.diagnostic == {}


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


def test_shared_save_and_load_roundtrip_uses_native_snapshot(validation_runtime, tmp_path):
    """Create and reload a private sparse source through the shared entry points."""
    from pb7_config import load_pb7_config, save_pb7_config
    from pb9_config_storage import PB9FileContext

    root = tmp_path / "private-library"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    context = PB9FileContext(root, validation_runtime.root, validation_runtime.python)
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
                "optimize": {"bounds": {}}, "pbgui": {"name": "Example", "source": {"note": "keep"}}}
    before = copy.deepcopy(document)
    assert save_pb7_config(document, "example.json", pb9_context=context) is None
    saved = json.loads((root / "example.json").read_bytes())
    assert saved == before
    assert "_raw" not in saved
    assert "_pbgui_param_status" not in saved
    assert "source" not in saved
    assert saved["bot"]["long"] == {}
    assert (root / "example.json").stat().st_mode & 0o777 == 0o600
    native_input = json.loads(validation_runtime.record.read_text())["config"]
    assert native_input == {key: value for key, value in before.items() if key != "pbgui"}
    assert load_pb7_config("example.json", pb9_context=context) == before
    assert document == before


def test_shared_save_rejects_internal_metadata_before_creation(validation_runtime, tmp_path):
    """GUI runtime hints cannot be persisted as native source or trigger publication."""
    from pb7_config import save_pb7_config
    from pb9_config_storage import PB9FileContext

    root = tmp_path / "never-created-library"
    context = PB9FileContext(root, validation_runtime.root, validation_runtime.python)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        save_pb7_config({"config_version": "v9.0.0", "_pbgui_param_status": {}}, "example.json", pb9_context=context)
    assert caught.value.error_codes == ("invalid_config",)
    assert not root.exists()
    _assert_no_native_import(validation_runtime)


def test_revision_update_roundtrip_uses_isolated_native_validation(validation_runtime, tmp_path):
    """Read, edit and replace one native-validated source with a stale-write guard."""
    from pb7_config import load_pb7_config, save_pb7_config
    from pb9_config_storage import PB9FileContext

    root = tmp_path / "revision-library"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    context = PB9FileContext(root, validation_runtime.root, validation_runtime.python)
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
                "optimize": {"bounds": {}}, "pbgui": {"name": "Original"}}
    save_pb7_config(document, "example.json", pb9_context=context)
    loaded = load_pb7_config("example.json", pb9_context=context, pb9_with_revision=True)
    edited = copy.deepcopy(loaded["config"])
    edited["pbgui"]["name"] = "Edited"
    save_pb7_config(edited, "example.json", pb9_context=context, pb9_expected_revision=loaded["revision"])
    assert json.loads(validation_runtime.record.read_text())["config"] == {
        key: value for key, value in edited.items() if key != "pbgui"}
    current = load_pb7_config("example.json", pb9_context=context, pb9_with_revision=True)
    assert current["config"] == edited
    assert current["config"]["bot"]["long"] == {}
    assert current["revision"] != loaded["revision"]
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        save_pb7_config(document, "example.json", pb9_context=context, pb9_expected_revision=loaded["revision"])
    assert caught.value.error_codes == ("config_revision_conflict",)
    assert load_pb7_config("example.json", pb9_context=context) == edited


def test_api_template_uses_server_selected_isolated_runtime(runtime, monkeypatch):
    """The authenticated API returns an actual fixture-helper template without paths from clients."""
    import asyncio
    import configparser
    import httpx
    from fastapi import FastAPI
    from api import auth, pb9

    parser = configparser.ConfigParser()
    parser.read_dict({"main": {"pb9dir": str(runtime.root), "pb9venv": str(runtime.python)}})
    monkeypatch.setattr(pb9, "load_ini_snapshot", lambda: parser)
    monkeypatch.setattr(auth, "validate_token", lambda token: SimpleNamespace(user_id="fixture")
                        if token == "fixture-auth" else None)

    async def exercise():
        app = FastAPI()
        app.add_middleware(auth.BrowserOriginMiddleware)
        app.include_router(pb9.router, prefix="/api/pb9")
        pb9.startup(app)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                response = await client.post("/api/pb9/template", json={},
                                             headers={"Authorization": "Bearer fixture-auth"})
            assert response.status_code == 200
            assert response.headers["cache-control"] == "no-store"
            assert response.json()["config"]["bot"]["long"] == {}
            assert response.json()["source"]["repo_dir"] == str(runtime.root)
            assert app.state.pb9_requests.active == 0
        finally:
            await pb9.shutdown(app)

    asyncio.run(exercise())


def test_api_cancelled_template_drains_and_reaps_native_helper(runtime, tmp_path, monkeypatch):
    """Cancellation retains ownership until the real fixture subprocess times out and is reaped."""
    import asyncio
    import configparser
    import httpx
    from fastapi import FastAPI
    from api import auth, pb9

    pid_file = tmp_path / "api-helper.pid"
    with runtime.schema.open("a", encoding="utf-8") as stream:
        stream.write('import time, os\nfrom pathlib import Path\n'
                     f'def get_template_config():\n    Path({str(pid_file)!r}).write_text(str(os.getpid()))\n'
                     '    time.sleep(60)\n')
    parser = configparser.ConfigParser()
    parser.read_dict({"main": {"pb9dir": str(runtime.root), "pb9venv": str(runtime.python)}})
    monkeypatch.setattr(pb9, "load_ini_snapshot", lambda: parser)
    original_template = pb9.load_pb9_template
    monkeypatch.setattr(pb9, "load_pb9_template", lambda *args, **kwargs: original_template(*args, timeout=3, **kwargs))
    monkeypatch.setattr(auth, "validate_token", lambda token: SimpleNamespace(user_id="fixture")
                        if token == "fixture-auth" else None)

    async def exercise():
        app = FastAPI()
        app.add_middleware(auth.BrowserOriginMiddleware)
        app.include_router(pb9.router, prefix="/api/pb9")
        pb9.startup(app)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            request = asyncio.create_task(client.post("/api/pb9/template", json={},
                                          headers={"Authorization": "Bearer fixture-auth"}))
            try:
                deadline = asyncio.get_running_loop().time() + 5
                while not pid_file.exists():
                    assert not request.done(), "fixture helper failed before starting"
                    assert asyncio.get_running_loop().time() < deadline
                    await asyncio.sleep(0.01)
                request.cancel()
                await asyncio.sleep(0.01)
                assert not request.done()
                assert app.state.pb9_requests.active == 1
                assert pb9.restart_block_reason(app)
            finally:
                request.cancel()
                outcome = await asyncio.gather(request, return_exceptions=True)
                await pb9.shutdown(app)
            assert isinstance(outcome[0], asyncio.CancelledError)
            assert app.state.pb9_requests.active == 0
            with pytest.raises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)

    asyncio.run(exercise())


@pytest.mark.parametrize("during_preflight", [False, True])
def test_cancelled_template_never_launches_a_late_helper(runtime, monkeypatch, during_preflight):
    """A stop request before or during static checks prevents a later native spawn."""
    cancel = pb9_config.PB9CancelSignal()
    status = pb9_config.inspect_pb9_runtime(runtime.root, runtime.python)

    def inspect(*args, **kwargs):
        """Stop while inspection is completing, before the transport can start."""
        cancel.set()
        return status

    def forbidden(*args, **kwargs):
        """The native process must not start after its owner has stopped admission."""
        pytest.fail("cancelled template started a native process")

    if during_preflight:
        monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", inspect)
    else:
        cancel.set()
        monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.load_pb9_template(runtime.root, runtime.python, cancel_event=cancel)
    assert caught.value.error_codes == ("helper_cancelled",)


def test_api_validation_retains_authored_config_and_excludes_gui_metadata(validation_runtime, monkeypatch):
    """The real fixture helper checks canonical native input; no expanded values return."""
    import asyncio
    import configparser
    import httpx
    from fastapi import FastAPI
    from api import auth, pb9

    runtime = validation_runtime
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
                "pbgui": {"name": "Test", "source": {"commit": "authored-note"}}}
    parser = configparser.ConfigParser()
    parser.read_dict({"main": {"pb9dir": str(runtime.root), "pb9venv": str(runtime.python)}})
    monkeypatch.setattr(pb9, "load_ini_snapshot", lambda: parser)
    monkeypatch.setattr(auth, "validate_token", lambda token: SimpleNamespace(user_id="fixture")
                        if token == "fixture-auth" else None)

    async def exercise():
        app = FastAPI()
        app.add_middleware(auth.BrowserOriginMiddleware)
        app.include_router(pb9.router, prefix="/api/pb9")
        pb9.startup(app)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                response = await client.post("/api/pb9/validate", json={"config": document},
                                             headers={"Authorization": "Bearer fixture-auth"})
            assert response.status_code == 200
            result = response.json()
            assert result["valid"] is True
            expected = {key: value for key, value in document.items() if key != "pbgui"}
            assert result["config"] == expected
            assert result["pbgui"] == document["pbgui"]
            assert result["source"]["repo_dir"] == str(runtime.root)
            assert result["source"]["commit"] != "authored-note"
            recorded = json.loads(runtime.record.read_text())
            assert recorded["config"] == expected
            assert recorded["kwargs"] == {"base_config_path": "", "verbose": False,
                                           "target": "canonical", "runtime": None}
            assert app.state.pb9_requests.active == 0
        finally:
            await pb9.shutdown(app)

    asyncio.run(exercise())


@pytest.mark.parametrize("during_preflight", [False, True])
def test_cancelled_document_validation_never_launches_late_helper(validation_runtime, monkeypatch, during_preflight):
    """Document and native-validation wrappers both forward the owner's stop flag."""
    cancel = pb9_config.PB9CancelSignal()
    status = pb9_config.inspect_pb9_runtime(validation_runtime.root, validation_runtime.python)

    def inspect(*args, **kwargs):
        """Request stop before static evidence is returned to the native caller."""
        assert kwargs["cancel_event"] is cancel
        cancel.set()
        return status

    def forbidden(*args, **kwargs):
        """No subprocess may be created after a stop request."""
        pytest.fail("cancelled validation started a native helper")

    if during_preflight:
        monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", inspect)
    else:
        cancel.set()
        monkeypatch.setattr(pb9_config, "inspect_pb9_runtime", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    with pytest.raises(pb9_config.PB9ConfigError) as caught:
        pb9_config.validate_pb9_document({"config_version": "v9.0.0"}, validation_runtime.root,
                                         validation_runtime.python, cancel_event=cancel)
    assert caught.value.error_codes == ("helper_cancelled",)


def test_file_api_native_validation_create_load_and_update(validation_runtime, monkeypatch, tmp_path):
    """The whole file API uses a real isolated helper and exact revision round trips."""
    import asyncio
    import configparser
    import httpx
    from fastapi import FastAPI
    from api import auth, pb9

    runtime = validation_runtime
    application = tmp_path / "application"
    application.mkdir()
    monkeypatch.setattr(pb9, "PBGDIR", application)
    parser = configparser.ConfigParser()
    parser.read_dict({"main": {"pb9dir": str(runtime.root), "pb9venv": str(runtime.python)}})
    monkeypatch.setattr(pb9, "load_ini_snapshot", lambda: parser)
    monkeypatch.setattr(auth, "validate_token", lambda token: SimpleNamespace(user_id="fixture")
                        if token == "fixture-auth" else None)
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}}, "pbgui": {"name": "Original"}}

    async def exercise():
        app = FastAPI()
        app.add_middleware(auth.BrowserOriginMiddleware)
        app.include_router(pb9.router, prefix="/api/pb9")
        pb9.startup(app)
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture",
                                         headers={"Authorization": "Bearer fixture-auth"}) as client:
                created = await client.post("/api/pb9/configs/save", json={"name": "test.json", "config": document, "revision": None})
                assert created.status_code == 200
                loaded = await client.post("/api/pb9/configs/load", json={"name": "test.json"})
                assert loaded.status_code == 200
                assert loaded.json()["config"] == document
                edited = copy.deepcopy(document)
                edited["pbgui"]["name"] = "Edited"
                updated = await client.post("/api/pb9/configs/save", json={"name": "test.json", "config": edited,
                                                                           "revision": loaded.json()["revision"]})
                assert updated.status_code == 200
                reloaded = await client.post("/api/pb9/configs/load", json={"name": "test.json"})
                assert reloaded.json()["config"] == edited
                assert reloaded.json()["revision"] != loaded.json()["revision"]
                native = json.loads(runtime.record.read_text())
                assert native["config"] == {key: value for key, value in edited.items() if key != "pbgui"}
                listing = await client.get("/api/pb9/configs")
                assert listing.json() == {"configs": ["test.json"]}
        finally:
            await pb9.shutdown(app)

    asyncio.run(exercise())


def test_official_migration_uses_isolated_converter_and_staged_overrides(runtime, tmp_path):
    """Only the selected native converter sees disposable inputs; caller data stays intact."""
    tools = runtime.root / 'src/tools'; tools.mkdir()
    (tools / '__init__.py').write_text('')
    record = tmp_path / 'migration-call.json'
    (tools / 'migrate_config_v9.py').write_text(
        'import json\nfrom pathlib import Path\nfrom types import SimpleNamespace\n'
        'def migrate(config, **kwargs):\n'
        "    assert Path(kwargs['base_config_path']).is_file()\n"
        "    assert json.loads(Path('BTC.json').read_text()) == {'bot': {'long': {}}}\n"
        f"    Path({str(record)!r}).write_text(json.dumps(kwargs))\n"
        "    return SimpleNamespace(config={'config_version':'v9.0.0','bot':{'long':{},'short':{}}}, changes=[{'reason':'official proposal'}], warnings=('Review risk policy',))\n")
    bundle = {'config': {'config_version':'v8.6.0','coin_overrides':{'BTC':{'override_config_path':'BTC.json'}}},
              'files': {'BTC.json': {'bot':{'long':{}}}}}
    original = copy.deepcopy(bundle)
    result = pb9_config.migrate_pb8_config_to_pb9(bundle, runtime.root, runtime.python)
    assert result['migration']['status'] == 'ready'
    assert result['migration']['report']['mode'] == 'best_effort'
    assert result['migration']['report']['warnings'] == ['Review risk policy']
    assert bundle == original
    options = json.loads(record.read_text())
    assert options['best_effort'] is True
    assert options['disabled_sides'] == options['bot_layout'] == 'preserve'
    assert not Path(options['base_config_path']).exists()
    assert 'tools.migrate_config_v9' not in sys.modules


@pytest.mark.parametrize('patch,files', [
    ({'coin_overrides':{'BTC':{'override_config_path':'../secret.json'}}}, {}),
    ({'coin_overrides':{'BTC':{'override_config_path':'api-keys.json'}}}, {}),
    ({'live':{'coin_flags':{'BTC':'-lc /tmp/other.json'}}}, {}),
    ({'live':{'approved_coins':['/tmp/coins.json']}}, {}),
    ({'live':{'approved_coins':['coins.txt']}}, {}),
    ({'live':{'password':'never expose'}}, {}),
    ({}, {'unused.json': {}}),
    ({'coin_overrides':{'BTC':{'override_config_path':'loop.json'}}}, {'loop.json':{'override_config_path':'loop.json'}}),
])
def test_migration_rejects_external_or_recursive_inputs_before_execution(runtime, monkeypatch, patch, files):
    """Unsafe source references cannot reach runtime inspection or process creation."""
    def forbidden(*args, **kwargs):
        """No subprocess/runtime work is permitted for invalid inputs."""
        pytest.fail('Unsafe migration reached runtime inspection')
    monkeypatch.setattr(pb9_config, 'inspect_pb9_runtime', forbidden)
    with pytest.raises(pb9_config.PB9ConfigError):
        pb9_config.migrate_pb8_config_to_pb9({'config':{'config_version':'v8.6.0',**patch},'files':files},runtime.root,runtime.python)


def test_backtest_validation_accepts_inline_coin_overrides(validation_runtime):
    """Parent and helper gates agree on the native backtest override contract."""
    config = {'config_version':'v9.0.0', 'coin_overrides':{'BTC':{'bot':{'long':{'enabled':False}}}}}
    assert pb9_config.validate_pb9_backtest_config(config,validation_runtime.root,validation_runtime.python)['valid']


def test_migration_include_graph_validation_is_bounded(monkeypatch):
    """Shared include subgraphs are visited once rather than exponentially expanded."""
    import pb9_config_helper as helper
    files={f'file{index}.json':{'coin_overrides':{str(child):{'override_config_path':f'file{child}.json'}
                  for child in range(index+1,30)}} for index in range(30)}
    bundle={'config':{'config_version':'v8.6.0','coin_overrides':{'BTC':{'override_config_path':'file0.json'}}},'files':files}
    original=helper.migration_file_references;calls=[]
    def counted(config):
        """Count traversals without timing assumptions or native execution."""
        calls.append(1);return original(config)
    monkeypatch.setattr(helper,'migration_file_references',counted)
    helper.check_migration_input(bundle)
    assert len(calls)<100
