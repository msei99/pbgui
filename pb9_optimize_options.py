"""Portable PB9 CLI options shared by local runners and the cloud worker.

Preparation reports are imported content, never server paths. The native
optimizer remains responsible for comparing their prepared-input identity.
"""
import hashlib
import json
import re
from pathlib import Path

SERVICE = 'OptimizeV9'
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_OPTIONS_BYTES = 32 * 1024 * 1024
OPTIONS_FILENAME = '.pbgui_native_launch.json'


def _unique_object(pairs):
    """Reject ambiguous reports instead of silently accepting duplicate keys."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Preparation manifest contains duplicate keys')
        result[key] = value
    return result


def _reject_constant(value):
    """Preparation reports must contain finite JSON values only."""
    raise ValueError('Preparation manifest contains a nonfinite number')


def validate_options(raw, config=None):
    """Validate native additions; absent options preserve old job behaviour."""
    if not isinstance(raw, dict):
        raise ValueError('Native launch options must be an object')
    selected = raw.get('scenarios', [])
    if not isinstance(selected, list) or any(
        not isinstance(label, str) or not label.strip() or ',' in label
        or any(ord(char) < 32 for char in label) for label in selected
    ):
        raise ValueError('Scenarios must be a list of nonempty labels without commas')
    selected = list(dict.fromkeys(selected))
    filtering = raw.get('filter_starting_configs', False)
    if type(filtering) is not bool:
        raise ValueError('filter_starting_configs must be a boolean')
    maximum = raw.get('compress_starting_configs')
    if maximum is not None and (type(maximum) is not int or maximum <= 0):
        raise ValueError('compress_starting_configs must be a positive integer')
    if (filtering or maximum is not None) and raw.get('mode', 'fresh') != 'pareto_seed':
        raise ValueError('Starting-config filtering and compression require a Pareto seed, not fresh or checkpoint resume')
    if config is not None and selected:
        backtest = config.get('backtest') or {}
        labels = [item.get('label') for item in backtest.get('scenarios', []) if isinstance(item, dict)]
        if any(label not in labels for label in selected):
            raise ValueError('Selected scenario is missing from the frozen config')
    manifest = raw.get('preparation_manifest')
    if manifest is not None:
        if isinstance(manifest, dict) and 'text' not in manifest:
            digest = manifest.get('sha256')
            name = manifest.get('name')
            if (not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)
                    or manifest.get('artifact') != digest or not isinstance(name, str)
                    or name in ('.', '..') or len(name) > 255
                    or any(ord(c) < 32 or ord(c) == 127 for c in name) or '/' in name or '\\' in name):
                raise ValueError('Invalid managed preparation manifest reference')
            return {'scenarios': selected, 'filter_starting_configs': filtering,
                    'compress_starting_configs': maximum,
                    'preparation_manifest': {'name':name, 'sha256':digest, 'artifact':digest}}
        if not isinstance(manifest, dict) or not isinstance(manifest.get('text'), str):
            raise ValueError('Import a completed optimizer preparation manifest')
        content = manifest['text'].encode('utf-8')
        if len(content) > MAX_MANIFEST_BYTES:
            raise ValueError('Preparation manifest exceeds 4 MiB')
        try:
            report = json.loads(content, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
            # Reject finite-looking JSON numbers that overflow Python float.
            json.dumps(report, allow_nan=False)
        except (ValueError, TypeError, RecursionError) as exc:
            raise ValueError('Invalid preparation manifest JSON') from exc
        if not isinstance(report, dict) or report.get('kind') != 'prepared-inputs' or report.get('consumer') != 'optimize':
            raise ValueError('Import the completed download --for-optimize report, not a preparation plan')
        if (type(report.get('schema_version')) is not int or not isinstance(report.get('suite'), dict)
                or not isinstance(report.get('cases'), list) or not report['cases']
                or not isinstance(report.get('datasets'), list) or not report['datasets']):
            raise ValueError('Preparation manifest is incomplete')
        if any(not isinstance(item, dict) or item.get('manifest_present') is not True
               or not isinstance(item.get('manifest_sha256'), str)
               or not re.fullmatch(r'[0-9a-f]{64}', item['manifest_sha256']) for item in report['datasets']):
            raise ValueError('Preparation manifest requires verified dataset hashes')
        digest = hashlib.sha256(content).hexdigest()
        if manifest.get('sha256', digest) != digest:
            raise ValueError('Preparation manifest content changed')
        name = manifest.get('name', 'preparation.json')
        if not isinstance(name, str) or name in ('.', '..') or len(name) > 255 or any(ord(c) < 32 or ord(c) == 127 for c in name) or '/' in name or '\\' in name:
            raise ValueError('Invalid preparation manifest filename')
        manifest = {'name': name, 'text': manifest['text'], 'sha256': digest}
    return {'scenarios': selected, 'filter_starting_configs': filtering,
            'compress_starting_configs': maximum, 'preparation_manifest': manifest}


def cli_arguments(options, manifest_path=None):
    """Build only native argv elements; no shell or host-path interpretation."""
    native = validate_options(options)
    result = []
    if native['scenarios']:
        result.extend(['--scenarios', ','.join(native['scenarios'])])
    if native['filter_starting_configs']:
        result.append('--filter-starting-configs')
    if native['compress_starting_configs'] is not None:
        result.extend(['--compress-starting-configs', str(native['compress_starting_configs'])])
    if native['preparation_manifest'] is not None:
        if manifest_path is None:
            raise ValueError('Preparation manifest has not been staged for this launch')
        path = Path(manifest_path)
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise ValueError('Missing managed preparation manifest')
        if hashlib.sha256(path.read_bytes()).hexdigest() != native['preparation_manifest']['sha256']:
            raise ValueError('Managed preparation manifest changed')
        result.extend(['--preparation-manifest', str(path)])
    return result


def stage_manifest(options, path, writer):
    """Materialize imported bytes through the caller's private atomic writer."""
    manifest = validate_options(options)['preparation_manifest']
    if manifest is None:
        return None
    if 'text' not in manifest:
        raise ValueError('Managed preparation manifest must be resolved before launch')
    writer(Path(path), manifest['text'])
    return str(Path(path).absolute())
