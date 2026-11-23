"""One-request native PB9 config protocol, run only in the selected environment."""

from __future__ import annotations

from contextlib import redirect_stdout
from copy import deepcopy
import importlib
import json
import math
import os
import tempfile
from pathlib import Path
import re
import sys


SERVICE = "PB9ConfigHelper"
MAX_REQUEST_BYTES = 16384
MAX_VALIDATION_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
INCOMPATIBLE_PB9_MESSAGE = (
    "This PB9 development config uses the retired pre-PR #2318 risk layout. "
    "It cannot be edited, queued, started or resumed. Delete it and create a new config "
    "from the current PB9 template. No automatic migration is performed."
)


def incompatible_pb9_layout(document) -> bool:
    """Detect retired risk paths in authored configs, bounds and inline patches, without mutation."""
    retired = re.compile(
        r"(?:^|\.)(?:global|long|short|\*)\.(?:hsl|unstuck)(?:\.|$)"
        r"|(?:^|\.)portfolio\.(?:hsl|unstuck|loss_gate)(?:\.|$)"
        r"|(?:^|[._])unstuck[._](?:close_fraction|close_pct)(?:\.|$)"
        r"|(?:^|\.)(?:unstuck|loss_gate)_memory_ema_span_minutes(?:\.|$)"
    )

    def walk(node, prefix=""):
        """Inspect paths rather than string values, preserving result and UI text."""
        if isinstance(node, dict):
            for key, value in node.items():
                path = prefix + "." + key if prefix else key
                if retired.search(path) or walk(value, path):
                    return True
        elif isinstance(node, list):
            return any(walk(value, prefix) for value in node)
        elif isinstance(node, str) and prefix.endswith('.fixed_params'):
            return bool(retired.search(node))
        return False

    return walk(document)


class _HelperError(RuntimeError):
    """Carry a fixed protocol error code without native exception contents."""

    def __init__(self, code, *, stage=None):
        """Keep only a fixed local stage alongside the existing error code."""
        super().__init__(code)
        self.stage = stage


DIAGNOSTIC_STAGES = frozenset({'native_import', 'metadata', 'config_prepare', 'operation'})
DIAGNOSTIC_KINDS = frozenset({'import_error', 'os_error', 'runtime_error', 'unexpected_error'})


def _failure_diagnostic(exc, stage):
    """Classify native failures without returning names, messages, paths or source."""
    if isinstance(exc, _HelperError) and stage not in DIAGNOSTIC_STAGES:
        return None
    cause = exc.__cause__ if isinstance(exc, _HelperError) else exc
    if cause is None:
        return None
    kind = ('import_error' if isinstance(cause, ImportError) else 'os_error' if isinstance(cause, OSError)
            else 'runtime_error' if isinstance(cause, RuntimeError) else 'unexpected_error')
    return {'stage': stage if stage in DIAGNOSTIC_STAGES else 'operation', 'kind': kind}


def _require_json_value(value) -> None:
    """Reject silent JSON coercion of tuples, non-string keys and nonfinite values."""
    if value is None or type(value) in (str, bool, int):
        return
    if isinstance(value, float) and math.isfinite(value):
        return
    if isinstance(value, list):
        for item in value:
            _require_json_value(item)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for item in value.values():
            _require_json_value(item)
        return
    raise _HelperError("invalid_template")


def _module_path(module, src: Path) -> str:
    """Require imported source modules to belong to the selected checkout."""
    path = Path(module.__file__).resolve()
    if not path.is_relative_to(src):
        raise _HelperError("runtime_identity_mismatch")
    return str(path)


def check_inline_config(config: dict, expected_schema: str | None = None, *, allow_inline_overrides: bool = False) -> None:
    """Reject external-data and metadata inputs before any native config import.

    Shared with the client; this helper module has standard-library imports only.
    Base paths were audited at ad9d6ede; the canonical override path was
    inspected with ec20de4 normalization/override modules. Canonical acceptance
    is not launch acceptance: runtime projection owns override key/effective
    checks. No runtime compilation or file-backed overrides are requested.
    """
    try:
        _require_json_value(config)
    except (_HelperError, RecursionError) as exc:
        raise _HelperError("invalid_config") from exc
    if not isinstance(config, dict):
        raise _HelperError("invalid_config")
    version = config.get("config_version")
    if (not isinstance(version, str) or not re.fullmatch(r"v9\.\d+\.\d+", version)
            or expected_schema is not None and version != expected_schema):
        raise _HelperError("config_version_mismatch")

    def walk(node) -> None:
        """Check all contexts, including scenarios and dotted optimizer patches."""
        if isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for key, value in node.items():
                parts = key.split(".")
                leaf = parts[-1]
                if any(part.startswith("_") or part == "pbgui" for part in parts):
                    raise _HelperError("invalid_config")
                if key.endswith("override_config_path"):
                    raise _HelperError("external_reference_rejected")
                if "coin_overrides" in parts and (leaf != "coin_overrides" or value != {}):
                    if not (allow_inline_overrides and node is config and key == "coin_overrides"
                            and isinstance(value, dict) and all(isinstance(patch, dict) for patch in value.values())):
                        raise _HelperError("external_reference_rejected")
                    for coin in value:
                        if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}", coin)
                                or "://" in coin or ".." in coin.split("/")
                                or coin.lower().endswith((".json", ".hjson", ".txt"))):
                            raise _HelperError("external_reference_rejected")
                if any(part in {"base_config_path", "organillo_csv", "fake_scenario_path"} for part in parts) and value not in (None, ""):
                    raise _HelperError("external_reference_rejected")
                if "coin_sources" in parts and (leaf != "coin_sources" or value not in (None, {})):
                    # Native hydration uses symbol_to_coin(), which reads a map.
                    raise _HelperError("external_reference_rejected")
                if any(part in {"approved_coins", "ignored_coins"} for part in parts) and leaf not in {"approved_coins", "ignored_coins"}:
                    raise _HelperError("external_reference_rejected")
                if leaf in {"approved_coins", "ignored_coins"}:
                    if value is None:
                        continue  # Native inheritance never discovers a file.
                    if leaf == "approved_coins" and isinstance(value, str) and value.strip().lower() == "all":
                        if any(ord(char) < 32 for char in value):
                            raise _HelperError("invalid_config")
                        continue  # Explicit native no-I/O sentinel.
                    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                        raise _HelperError("external_reference_rejected")
                    for item in value:
                        # Native file discovery checks the whole list item before
                        # splitting commas; check both representations.
                        for token in [item, *item.split(",")]:
                            token = token.strip()
                            if (any(ord(char) < 32 or ord(char) == 127 for char in item)
                                    or token.startswith(("./", "../", "/", "~"))
                                    or "\\" in token or "://" in token
                                    or ".." in token.split("/")
                                    # pathlib removes trailing slash/dot segments
                                    # before native suffix checks. Match without
                                    # depending on the two Python versions.
                                    or token.rstrip("/.").lower().endswith((".json", ".hjson", ".txt"))):
                                raise _HelperError("external_reference_rejected")
                walk(value)

    try:
        walk(config)
        if incompatible_pb9_layout(config):
            raise _HelperError("incompatible_pb9_config")
    except RecursionError as exc:
        raise _HelperError("invalid_config") from exc


def _exchange_metadata(src: Path) -> dict:
    """Describe native connector/history capabilities, independently of selected defaults."""
    gate = importlib.import_module('live.order_churn_gate')
    utils = importlib.import_module('utils')
    ccxt = importlib.import_module('ccxt.async_support')
    _module_path(gate, src)
    _module_path(utils, src)
    live = sorted(set(gate.ORDER_CHURN_GATE_SUPPORTED_EXCHANGES) - {'fake'})
    historical = [exchange for exchange in live if utils.to_ccxt_client_id(exchange) in ccxt.exchanges]
    return {'live':live, 'backtest':historical, 'optimize':historical, 'suite':historical}


def _editor_metadata(schema, src: Path) -> dict:
    """Describe native fields and strategies without coupling to the PB8 schema."""
    if incompatible_pb9_layout(schema.get_template_config()):
        raise _HelperError('runtime_upgrade_required')
    def arrays(value):
        """Rust tuples serialize as arrays; preserve every other JSON type exactly."""
        if isinstance(value, (tuple, list)):
            return [arrays(item) for item in value]
        if isinstance(value, dict):
            return {key: arrays(item) for key, item in value.items()}
        return value

    registry = importlib.import_module("config.field_registry")
    field_spec = importlib.import_module("config.field_spec")
    strategy = importlib.import_module("config.strategy_spec")
    rust = importlib.import_module("passivbot_rust")
    registry_file = _module_path(registry, src)
    strategy_file = _module_path(strategy, src)
    _module_path(field_spec, src)
    rust_file = Path(rust.__file__).resolve()
    if not rust_file.is_relative_to(Path(sys.prefix).resolve()):
        raise _HelperError("runtime_identity_mismatch")
    fields = []
    for field in registry.get_field_registry():
        item = {
            "path": list(field.path), "types": [kind.value for kind in field.types],
            "nullable": bool(field.nullable), "description": field.description,
            "unit": field.unit, "scope": field.scope, "order": field.order,
        }
        if field.default is not field_spec.NO_DEFAULT:
            default = arrays(field.default)
            _require_json_value(default)
            item["default"] = default
        if field.domain is not None:
            domain = field.domain
            item["domain"] = {key: getattr(domain, key) for key in (
                "minimum", "maximum", "exclusive_minimum", "exclusive_maximum", "finite", "string_pattern")}
            item["domain"]["choices"] = list(domain.choices)
        if field.coin_override is not None:
            item["coin_override"] = field.coin_override.value
        fields.append(item)
    kinds = list(strategy.get_supported_strategy_kinds())
    metadata = {
        "contract_version": 1, "template": schema.get_template_config(),
        "prototype": schema.get_expanded_config_prototype(), "fields": fields,
        "strategies": kinds, "strategy_specs": {kind: strategy.get_strategy_spec(kind) for kind in kinds},
        "strategy_defaults": strategy.get_all_strategy_defaults(),
        "exchange_options": _exchange_metadata(src),
    }
    metadata = arrays(metadata)
    _require_json_value(metadata)
    return {"metadata": metadata, "registry_module_file": registry_file, "strategy_module_file": strategy_file,
            "rust_module_file": str(rust_file)}



def migration_file_references(config):
    """Allow bounded JSON migration inputs without native file discovery outside staging."""
    _require_json_value(config)
    references = set()

    def file_ref(value):
        """Accept only visible JSON basenames owned by the selected PB8 bundle."""
        if (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_. -]{0,119}\.json", value)
                or any(word in value.lower() for word in ("api-key", "api_key", "secret", "credential", "pbgui"))):
            raise _HelperError("external_reference_rejected")
        references.add(value)

    def coins(value):
        """Reject URLs and path-like coin identifiers before upstream path resolution."""
        if value is None:
            return
        if isinstance(value, dict):
            for item in value.values():
                coins(item)
        elif isinstance(value, list):
            for item in value:
                if not isinstance(item, str):
                    raise _HelperError("invalid_config")
                coins(item)
        elif isinstance(value, str):
            if value.endswith('.json'):
                file_ref(value)
            elif value.rstrip('/.').lower().endswith(('.hjson', '.txt')) or any(ord(c) < 32 for c in value) or any(c in value for c in ('/', '\\', '~', ':')):
                raise _HelperError("external_reference_rejected")
        else:
            raise _HelperError("invalid_config")

    def walk(node):
        """Check all root, scenario and optimizer contexts, including dotted keys."""
        if isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, dict):
            for key, value in node.items():
                leaf = key.split('.')[-1]
                if leaf.lower() in {'password', 'secret', 'api_key', 'apikey', 'token', 'private_key', 'credentials'}:
                    raise _HelperError("invalid_config")
                if leaf == 'override_config_path' and value:
                    file_ref(value)
                elif leaf in {'approved_coins', 'ignored_coins'}:
                    coins(value)
                elif (leaf in {'coin_flags', 'coin_sources'} and value
                      or leaf.endswith('_path') and value
                      or leaf == 'organillo_csv' and value):
                    raise _HelperError("external_reference_rejected")
                walk(value)
    walk(config)
    return references


def check_migration_input(envelope):
    """Validate the complete bounded migration bundle before starting native code."""
    if not isinstance(envelope, dict) or set(envelope) != {'config', 'files'}:
        raise _HelperError('invalid_request')
    config, files = envelope['config'], envelope['files']
    if not isinstance(config, dict) or config.get('config_version') not in {'v8.0.0', 'v8.1.0', 'v8.6.0'}:
        raise _HelperError('config_version_mismatch')
    if not isinstance(files, dict) or len(files) > 32:
        raise _HelperError('invalid_request')
    required = migration_file_references(config)
    for name, document in files.items():
        # Check filenames even when a malicious caller supplies an unreferenced file.
        migration_file_references({'override_config_path': name})
        required.update(migration_file_references(document))
    if required != set(files):
        raise _HelperError('external_reference_rejected')
    done = set()
    def visit(name, ancestors):
        """Reject recursive include graphs before upstream resolution."""
        if name in ancestors:
            raise _HelperError('external_reference_rejected')
        if name in done:
            return
        for child in migration_file_references(files[name]):
            visit(child, ancestors | {name})
        done.add(name)
    for name in migration_file_references(config):
        visit(name, set())
    if len(json.dumps(envelope, allow_nan=False).encode()) > MAX_VALIDATION_BYTES - 4096:
        raise _HelperError('invalid_request')


def _migrate_v8(envelope, src, schema_version):
    """Run the official converter once in a private, disposable file context."""
    check_migration_input(envelope)
    converter = importlib.import_module('tools.migrate_config_v9')
    module_file = _module_path(converter, src)
    with tempfile.TemporaryDirectory(prefix='pbgui-v8-v9-') as directory:
        root = Path(directory)
        for name, document in envelope['files'].items():
            target = root / name
            target.write_text(json.dumps(document, allow_nan=False), encoding='utf-8')
            target.chmod(0o600)
        # Upstream resolves override paths relative to the source directory.
        input_path = root / '__pbgui_migration_source__.json'
        input_path.write_text(json.dumps(envelope['config']), encoding='utf-8')
        input_path.chmod(0o600)
        previous = Path.cwd()
        try:
            os.chdir(root)
            try:
                result = converter.migrate(envelope['config'], base_config_path=str(input_path),
                                           best_effort=True, disabled_sides='preserve', bot_layout='preserve')
                check_inline_config(result.config, schema_version, allow_inline_overrides=True)
            except (ValueError, TypeError, KeyError, _HelperError) as exc:
                message = (INCOMPATIBLE_PB9_MESSAGE if str(exc) == 'incompatible_pb9_config' else
                           ''.join(c if ord(c) >= 32 else ' ' for c in str(exc)))
                return {'status': 'blocked', 'message': message.replace(str(root), '[migration]')[:2000]}, module_file
            report = {'source_version': envelope['config']['config_version'], 'target_version': schema_version,
                      'mode': 'best_effort', 'changes': result.changes, 'warnings': list(result.warnings)}
            _require_json_value(report)
            return {'status': 'ready', 'config': result.config, 'report': report}, module_file
        finally:
            os.chdir(previous)


def _optimize_metadata(schema, src, metadata):
    """Project native PB9 definitions into the shared Optimize editor contract."""
    import copy
    from config.metrics import KNOWN_METRICS
    from config.scoring import OBJECTIVE_GOALS, DEFAULT_OBJECTIVE_GOALS
    from config.reducers import SUPPORTED_REDUCERS
    from config.optimize_bounds import get_optimize_bounds_defaults
    from config.field_registry import get_field_registry
    from optimization.backends import BACKEND_RUNNERS
    from optimizer_overrides import KNOWN_OPTIMIZER_OVERRIDES
    from importlib import util
    import ast

    # Read the native parser declarations without importing the executable CLI.
    declared_flags = set()
    for node in ast.walk(ast.parse((src / 'optimize.py').read_text(encoding='utf-8-sig'))):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'add_argument':
            declared_flags.update(arg.value for arg in node.args if isinstance(arg, ast.Constant) and isinstance(arg.value, str))
    cli_options = [key for key in ('scenarios', 'filter_starting_configs', 'compress_starting_configs', 'preparation_manifest')
                   if '--' + key.replace('_', '-') in declared_flags]

    bounds = get_optimize_bounds_defaults()
    template = schema.get_starter_config()
    optimize = copy.deepcopy(template['optimize'])
    strategies = metadata['strategies']
    bounds['global'] = copy.deepcopy(bounds['long'])
    bounds['portfolio'] = copy.deepcopy(template['optimize']['bounds'].get('portfolio', {}))
    active = {}
    for kind in strategies:
        selected = copy.deepcopy(bounds)
        for side in ('global', 'long', 'short'):
            values = selected.setdefault(side, {})
            values['strategy'] = {kind: copy.deepcopy(values.get('strategy', {}).get(kind, {}))}
        active[kind] = selected
    # Share only the existing hardware probe. It imports no PB8 bot modules.
    spec = util.spec_from_file_location('_pbgui_gpu_probe', Path(__file__).with_name('optimizer_gpu_runtime.py'))
    probe = util.module_from_spec(spec);spec.loader.exec_module(probe)
    runtime = probe.gpu_runtime_contract(list(BACKEND_RUNNERS))
    runtime['reason'] = str(runtime.get('reason', '')).replace('PB8', 'PB9')
    metrics = sorted(KNOWN_METRICS)
    backend_items = {'pymoo':{'recognized':True, 'available':True, 'metric_set':'cpu', 'reason':''},
        'gpu':{'recognized':True, 'available':runtime['device_available'], 'metric_set':'gpu_proxy',
            'metric_eligibility_known':True, 'reason':runtime.get('reason', ''), 'runtime':runtime,
            'effective_defaults':copy.deepcopy(optimize.get('gpu', {})), 'exact_only_metrics':[]}}
    bound_catalog = {
        '.'.join(field.path[1:]): list(field.recommended_bounds) if field.recommended_bounds else None
        for field in get_field_registry()
        if field.path[0] == 'bot' and field.optimizable and '*' not in field.path
    }
    return {**metadata, 'template': template, 'bounds': optimize.get('bounds', {}), 'all_bounds': bounds,
        'bound_catalog': bound_catalog,
        'active_bounds': active, 'optimize_defaults': optimize,
        'bot_parameter_paths':['.'.join(field['path']) for field in metadata['fields'] if field['path'][0]=='bot'
            and '*' not in field['path'] and any(kind in ('number','integer') for kind in field['types'])],
        'backends': list(BACKEND_RUNNERS), 'backend_default': optimize.get('backend', 'pymoo'),
        'backend_contract': {'contract_version':1, 'runtime':runtime, 'items':backend_items, 'metric_sets':{'cpu':metrics, 'gpu_proxy':metrics},
            'backends':{'pymoo':{'available':True,'metric_set':'cpu'}, 'gpu':{'available':runtime['device_available'],
                'metric_set':'gpu_proxy', 'reason':runtime.get('reason', ''), 'supported_metrics':metrics}}},
        'pymoo':{'algorithms':['auto', *optimize.get('pymoo', {}).get('algorithms', {})],
            'ref_dir_methods':['das_dennis', 'energy'], 'defaults':optimize.get('pymoo', {})},
        'scoring':{'metrics':metrics, 'goals':list(OBJECTIVE_GOALS), 'default_goals':dict(DEFAULT_OBJECTIVE_GOALS),
            'basis_field':'reducer', 'defaults':optimize.get('scoring', [])},
        'limits':{'metrics':metrics, 'statistics':sorted(SUPPORTED_REDUCERS), 'basis_field':'reducer',
            'scoring_basis_field':'reducer', 'operators':['greater_than','greater_than_or_equal','less_than',
                'less_than_or_equal','equal_to','not_equal','outside_range','inside_range','auto'],
            'defaults':optimize.get('limits', [])},
        'optimizer_overrides':sorted(KNOWN_OPTIMIZER_OVERRIDES), 'runtime_overrides':[], 'hsl_signal_modes':['coin','position_side','portfolio'],
        'native_cli_options':cli_options,
        'runtime_options':{'mode':{'choices':['fresh','pareto_seed','checkpoint_resume'],'default':'fresh'},
            'fine_tune_params':{'type':'array','default':[]}, 'polish_percentage':{'type':'number_or_null','default':None},
            'polish_bounds_mode':{'choices':['clamp','override-tunable','override-all'],'default':'clamp'}}}


def _optimizer_effective_config(prepared,schema):
    """Resolve resume policy with native defaults, excluding derived runtime metadata."""
    import copy
    from config.bot_inheritance import get_bot_policy
    prototype=schema.get_expanded_config_prototype()
    result={key:copy.deepcopy(prepared.get(key,{})) for key in
        ('config_version','bot','backtest','live','coin_overrides','optimize')}
    policy=get_bot_policy(prepared)
    if policy is not None:result['bot']=policy.effective_view()
    result['backtest']={key:value for key,value in result['backtest'].items()
        if key in prototype['backtest'] and key!='base_dir'}
    for key in ('iters','n_cpus'):result['optimize'].pop(key,None)
    gpu=result['optimize'].get('gpu')
    if isinstance(gpu,dict):
        for key in ('tuning_mode','batch_size','max_dispatch_candidate_bars','checkpoint_interval_seconds'):
            gpu.pop(key,None)
    def public(value):
        """Native source/transform annotations do not change simulation policy."""
        if isinstance(value,dict):return {key:public(item) for key,item in value.items() if not key.startswith('_')}
        if isinstance(value,(list,tuple)):return [public(item) for item in value]
        return value
    return public(result)


def _cloud_export_plan(prepared):
    """Derive export-only data policy from native effective universe and bounds.

    The projection is consumed only by PBGui's shard planner; optimize.json is
    always the original native PB9 document. No PB8 bot module is imported.
    """
    import copy
    from config.universe import get_side_universe_sources
    from config.age_policy import minimum_coin_age_days
    from optimization.warmup import compute_optimizer_backtest_warmup_minutes
    from warmup_utils import compute_backtest_warmup_minutes
    from suite_runner import build_scenarios, apply_scenario_overrides

    def approved_sources(config):
        """Reject unresolved external selectors before native source rebinding."""
        approved = get_side_universe_sources(config, 'approved_coins', {})
        if any(not isinstance(approved.get(side), list) for side in ('long', 'short')):
            raise _HelperError('external_reference_rejected')
        return approved

    approved_sources(prepared)
    configs = [prepared]
    if prepared['backtest'].get('suite_enabled'):
        from optimization.warmup import refresh_optimizer_source_bindings
        scenarios,_ = build_scenarios(prepared['backtest'], base_exchanges=prepared['backtest']['exchanges'])
        for scenario in scenarios:
            config=copy.deepcopy(prepared)
            patches={}
            for field,value in (('approved_coins',scenario.coins),('ignored_coins',scenario.ignored_coins)):
                if value is not None:
                    patches.update({f'bot.{side}.universe.{field}':value for side in ('long','short')})
            patches.update(scenario.overrides or {})
            apply_scenario_overrides(config,patches)
            for key in ('start_date','end_date','exchanges'):
                value=getattr(scenario,key,None)
                if value: config['backtest'][key]=value
            approved_sources(config)
            # Bind the fully patched disposable config, including its dataset
            # window, before native optimizer boundaries consume its sources.
            refresh_optimizer_source_bindings(config)
            configs.append(config)
    plans=[]
    for config in configs:
        approved = approved_sources(config)
        plans.append({'backtest':copy.deepcopy(config['backtest']),
            'live':{'approved_coins':copy.deepcopy(approved),
                'minimum_coin_age_days':max(minimum_coin_age_days(config,side) for side in ('long','short'))},
            'warmup_minutes':max(compute_backtest_warmup_minutes(config),
                compute_optimizer_backtest_warmup_minutes(config))})
    return {'data_plans':plans}


def handle(request: dict) -> dict:
    """Return templates or validate in-memory native configs, never save user configs."""
    if not isinstance(request, dict) or not isinstance(request.get("operation"), str):
        raise _HelperError("invalid_request")
    required = {"operation", "pb9_dir", "config_schema", "package_version"}
    if isinstance(request, dict) and request.get("operation") in {"validate", "validate_live", "validate_backtest", "validate_optimize", "optimize_cloud_plan", "optimize_effective", "result_config", "migrate_v8", "ohlcv_preflight", "ohlcv_start_dates"}:
        required.add("config")
    if request.get("operation") in {"ohlcv_preflight", "ohlcv_start_dates", "market_identifiers"}:
        required.update({"work_dir", "metadata_roots"})
    if request.get('operation') == 'market_identifiers':
        required.add('market_request')
    if not isinstance(request, dict) or set(request) != required:
        raise _HelperError("invalid_request")
    if request.get("operation") in {"ohlcv_preflight", "ohlcv_start_dates", "market_identifiers"}:
        work = request["work_dir"]
        roots = request["metadata_roots"]
        if (not isinstance(work, str) or not Path(work).is_absolute() or Path(work).is_symlink()
                or not Path(work).is_dir() or Path(work).resolve() == Path(tempfile.gettempdir()).resolve()
                or not Path(work).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve())
                or not isinstance(roots, list) or len(roots) > 2
                or any(not isinstance(root, str) or not Path(root).is_absolute()
                       or len(root) > 4096 or any(ord(char) < 32 for char in root) for root in roots)):
            raise _HelperError("invalid_request")
    if not isinstance(request["operation"], str) or request["operation"] not in {"template", "metadata", "optimize_metadata", "validate", "validate_live", "validate_backtest", "validate_optimize", "optimize_cloud_plan", "optimize_effective", "result_config", "migrate_v8", "ohlcv_preflight", "ohlcv_start_dates", "market_identifiers"} or any(
        not isinstance(request[key], str) or not request[key]
        for key in ("pb9_dir", "config_schema", "package_version")
    ):
        raise _HelperError("invalid_request")
    if request["operation"] == "result_config":
        _require_json_value(request["config"])
        if not isinstance(request["config"], dict):
            raise _HelperError("invalid_config")
    elif request["operation"] in {"validate", "validate_live", "validate_backtest", "validate_optimize", "optimize_cloud_plan", "optimize_effective", "ohlcv_preflight", "ohlcv_start_dates"}:
        check_inline_config(request["config"], request["config_schema"],
                            allow_inline_overrides=request["operation"] in {"validate_live", "validate_backtest", "validate_optimize", "optimize_cloud_plan", "optimize_effective", "ohlcv_preflight", "ohlcv_start_dates"})
    if request["operation"] == "migrate_v8":
        check_migration_input(request["config"])
    root = Path(request["pb9_dir"])
    if not root.is_absolute():
        raise _HelperError("invalid_request")
    root = root.resolve()
    if request["operation"] in {"ohlcv_preflight", "ohlcv_start_dates", "market_identifiers"}:
        work = Path(request["work_dir"]).resolve()
        if work.is_relative_to(root) or root.is_relative_to(work):
            raise _HelperError("invalid_request")
    src = (root / "src").resolve()
    if not src.is_relative_to(root) or not src.is_dir():
        raise _HelperError("invalid_request")
    sys.path.insert(0, str(src))
    with redirect_stdout(sys.stderr):
        schema = importlib.import_module("config.schema")
        version = importlib.import_module("passivbot_version")
        schema_file = _module_path(schema, src)
        version_file = _module_path(version, src)
        schema_version = schema.CONFIG_SCHEMA_VERSION
        package_version = version.__version__
        if (not isinstance(schema_version, str) or not re.fullmatch(r"v9\.\d+\.\d+", schema_version)
                or schema_version != request["config_schema"]
                or package_version != request["package_version"]):
            raise _HelperError("runtime_identity_mismatch")
        if request['operation'] == 'market_identifiers':
            try:
                from importlib import util
                spec = util.spec_from_file_location('_pbgui_native_market', Path(__file__).with_name('pb9_market.py'))
                market = util.module_from_spec(spec)
                spec.loader.exec_module(market)
                if not isinstance(request['market_request'],dict):
                    raise _HelperError('invalid_request')
                catalog = market.resolve(request['market_request'], root, Path(request['work_dir']), request['metadata_roots'])
                resolver_file = _module_path(importlib.import_module('utils'), src)
            except _HelperError:
                raise
            except Exception:
                raise _HelperError('native_metadata_failed') from None
            return {'market_catalog':catalog,'resolver_module_file':resolver_file,
                    'config_schema':schema_version,'package_version':package_version,
                    'schema_module_file':schema_file,'version_module_file':version_file}
        if request["operation"] in {"ohlcv_preflight", "ohlcv_start_dates"}:
            try:
                from importlib import util
                spec = util.spec_from_file_location("_pbgui_native_ohlcv_probe", Path(__file__).with_name("pb9_ohlcv_probe.py"))
                probe = util.module_from_spec(spec)
                spec.loader.exec_module(probe)
                result = probe.build(request["config"], root, work=Path(request["work_dir"]),
                                     metadata_roots=request["metadata_roots"], start_dates=request["operation"] == "ohlcv_start_dates")
            except Exception as exc:
                code = getattr(exc, "error_code", None)
                if code not in {"ohlcv_metadata_missing", "ohlcv_coins_required", "ohlcv_pair_limit", "ohlcv_dataset_override"}:
                    code = "native_ohlcv_failed"
                raise _HelperError(code) from exc
            return {**result, "config_schema": schema_version, "package_version": package_version,
                    "schema_module_file": schema_file, "version_module_file": version_file}
        if request["operation"] == "migrate_v8":
            migration, converter_file = _migrate_v8(request["config"], src, schema_version)
            return {"migration": migration, "converter_module_file": converter_file,
                    "config_schema": schema_version, "package_version": package_version,
                    "schema_module_file": schema_file, "version_module_file": version_file}
        if request["operation"] in {"metadata", "optimize_metadata"}:
            try:
                result = _editor_metadata(schema, src)
                if request["operation"] == "optimize_metadata":
                    result["metadata"] = _optimize_metadata(schema, src, result["metadata"])
            except _HelperError:
                raise
            except Exception as exc:
                raise _HelperError("native_metadata_failed", stage='metadata') from exc
            return {**result, "config_schema": schema_version, "package_version": package_version,
                    "schema_module_file": schema_file, "version_module_file": version_file}
        if request["operation"] in {"validate", "validate_live", "validate_backtest", "validate_optimize", "optimize_cloud_plan", "optimize_effective", "result_config"}:
            try:
                loader = importlib.import_module("config.load")
                load_file = _module_path(loader, src)
            except _HelperError:
                raise
            except Exception as exc:
                raise _HelperError("native_validation_failed", stage='native_import') from exc
            try:
                authored = request["config"]
                records_file = None
                if request["operation"] == "result_config":
                    records = importlib.import_module("config.result_records")
                    records_file = _module_path(records, src)
                    authored = records.extract_result_config(authored)
                    check_inline_config(authored, schema_version, allow_inline_overrides=True)
                options = {"live_only": True} if request["operation"] == "validate_live" else {}
                prepared = loader.prepare_config(
                    deepcopy(authored), base_config_path="", verbose=False,
                    target="canonical", runtime=None, **options,
                )
                if request["operation"] in {"validate_optimize", "optimize_cloud_plan", "optimize_effective"}:
                    from optimization.config_adapter import extract_bounds_tuple_list_from_config
                    extract_bounds_tuple_list_from_config(prepared)
                if (request["operation"] in {"validate_optimize", "optimize_cloud_plan"}
                        and prepared.get('optimize', {}).get('backend') == 'gpu'):
                    from simulation_preparation import validate_optimizer_preparation
                    from suite_runner import extract_suite_config
                    try:
                        validate_optimizer_preparation(prepared, extract_suite_config(prepared, None))
                    except ValueError as exc:
                        raise _HelperError('gpu_policy_unsupported') from exc
                if not isinstance(prepared, dict) or prepared.get("config_version") != schema_version:
                    raise _HelperError("runtime_identity_mismatch")
            except _HelperError:
                raise
            except (ValueError, TypeError, KeyError) as exc:
                raise _HelperError("native_config_rejected") from exc
            except Exception as exc:
                raise _HelperError("native_validation_failed", stage='config_prepare') from exc
            return {**({**_cloud_export_plan(prepared),'config':request['config']} if request["operation"] == "optimize_cloud_plan" else
                       {'effective':_optimizer_effective_config(prepared,schema)} if request['operation']=='optimize_effective' else
                       {'config':authored, 'result_records_module_file':records_file} if request['operation']=='result_config' else {}),
                    "valid": True, "config_schema": schema_version, "package_version": package_version,
                    "schema_module_file": schema_file, "version_module_file": version_file,
                    "load_module_file": load_file}
        config = schema.get_template_config()
    if not isinstance(config, dict) or config.get("config_version") != schema_version:
        raise _HelperError("runtime_identity_mismatch")
    try:
        _require_json_value(config)
    except RecursionError as exc:
        raise _HelperError("invalid_template") from exc
    if incompatible_pb9_layout(config):
        raise _HelperError('runtime_upgrade_required')
    return {"config": config, "config_schema": schema_version, "package_version": package_version,
            "schema_module_file": schema_file, "version_module_file": version_file}


def main() -> int:
    """Emit one bounded JSON response; native diagnostics stay off protocol stdout."""
    request = None
    try:
        source = sys.stdin.buffer.read(MAX_VALIDATION_BYTES + 1)
        if len(source) > MAX_VALIDATION_BYTES:
            raise _HelperError("invalid_request")
        request = json.loads(source)
        if isinstance(request, dict) and isinstance(request.get("operation"), str) and request.get("operation") in {"template", "metadata"} and len(source) > MAX_REQUEST_BYTES:
            raise _HelperError("invalid_request")
        response = {"ok": True, "result": handle(request)}
    except _HelperError as exc:
        response = {"ok": False, "error_code": str(exc)}
        diagnostic = _failure_diagnostic(exc, exc.stage)
        if diagnostic is not None:
            response['diagnostic'] = diagnostic
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        response = {"ok": False, "error_code": "invalid_request"}
    except Exception as exc:
        # The PBGui owner logs this fixed error code, never native exception data.
        operation = request.get("operation") if isinstance(request, dict) else None
        code = "native_validation_failed" if isinstance(operation, str) and operation in {"validate", "validate_live", "validate_backtest", "validate_optimize", "optimize_cloud_plan", "optimize_effective", "result_config"} else "native_template_failed"
        response = {"ok": False, "error_code": code}
        response['diagnostic'] = _failure_diagnostic(exc, 'operation')
    try:
        encoded = json.dumps(response, allow_nan=False).encode("utf-8")
        if len(encoded) > MAX_RESPONSE_BYTES:
            encoded = b'{"ok":false,"error_code":"helper_output_limit"}'
    except (TypeError, ValueError, RecursionError):
        encoded = b'{"ok":false,"error_code":"invalid_template"}'
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
