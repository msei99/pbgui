"""Private immutable loop evidence, compact findings and bounded read-only pages."""
from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import time
import uuid
import threading
import codecs

from ai_token_budget import _proxy_encoding
from file_lock import advisory_file_lock
from secure_files import (atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow,
                          read_regular_file_prefix_nofollow)
from logging_helpers import human_log as _log
from pb8_loop_selection import PROJECTION_VERSION as SELECTION_PROJECTION_VERSION

SERVICE = 'PB8LoopEvidence'
MIN_CONTEXT = 256_000
SCHEMA = 1
SAFE_METRICS = {'backtest_completion_ratio', 'drawdown_worst_strategy_eq', 'drawdown_worst',
                'liquidated', 'liquidated_usd', 'liquidated_btc', 'liquidation_count',
                'n_liquidations', 'n_fills'}
TOOL_SCHEMA = {'type': 'object', 'properties': {'path': {'type': 'string', 'maxLength': 8192},
               'offset': {'type': 'integer', 'minimum': 0}}, 'required': ['path', 'offset'],
               'additionalProperties': False}
TOOL_DESCRIPTION = 'Read exact immutable evidence, bounded /selection and /evaluation queries and installed PB8 manuals. Research source inputs: /data/optimizer_sources/{operation}. The mandatory package is already supplied initially; use /required only to recover missing pages after compaction. Query /selection/rank/{metric}/{reducer}/{min|max}, /history/index or a manual evidence_path; follow next_offset.'


class LoopAIPause(Exception):
    """A recoverable controller condition; never an optimizer-job failure."""

    def __init__(self, reason, message, **details):
        self.reason = reason
        self.details = details
        super().__init__(message)


class BootstrapWithoutAdvice(Exception):
    """An optional historical source did not fit; continue the existing fresh policy."""

    def __init__(self, preview):
        self.preview = preview
        super().__init__('Continue fresh without historical advice')


def encode(value):
    """Serialize finite values without whitespace or precision loss."""
    return json.dumps(value, allow_nan=False, ensure_ascii=False, separators=(',', ':'))


def sha(value):
    """Hash canonical finite JSON independently of wire whitespace."""
    return hashlib.sha256(json.dumps(value, allow_nan=False, sort_keys=True).encode()).hexdigest()


def tokens(value):
    """Count a local proxy tokenizer, not claimed provider usage."""
    return len(_proxy_encoding().encode_ordinary(encode(value)))


def pointer(value, path):
    """Resolve RFC 6901 data pointers; never interpret paths as filenames."""
    if not isinstance(path, str) or any(ord(char) < 32 for char in path):
        raise ValueError('Invalid evidence pointer')
    if path == '':
        return value
    if not path.startswith('/'):
        raise ValueError('Evidence pointers must start with /')
    for part in path[1:].split('/'):
        index = 0
        while index < len(part):
            if part[index] == '~':
                if index + 1 >= len(part) or part[index + 1] not in '01':
                    raise ValueError('Invalid pointer escape')
                index += 1
            index += 1
        part = part.replace('~1', '/').replace('~0', '~')
        if isinstance(value, list):
            if not part.isascii() or not part.isdigit() or (len(part) > 1 and part[0] == '0'):
                raise ValueError('Invalid array index')
            value = value[int(part)]
        elif isinstance(value, dict):
            value = value[part]
        else:
            raise ValueError('Pointer traverses a scalar')
    return value


def delta(before, after, path=()):
    """Represent exact experiment changes with path segments and removals."""
    changes, removed = [], []
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(set(before) | set(after)):
            child = path + (key,)
            if key not in after:
                removed.append(list(child))
            elif key not in before:
                changes.append({'path': list(child), 'value': copy.deepcopy(after[key])})
            else:
                added, deleted = delta(before[key], after[key], child)
                changes.extend(added)
                removed.extend(deleted)
    elif encode(before) != encode(after):
        changes.append({'path': list(path), 'value': copy.deepcopy(after)})
    return {'set': changes, 'remove': removed} if not path else (changes, removed)


def compact_finding(entry):
    """Project legacy evidence deterministically; never call a model or truncate text."""
    legacy = entry.get('status')
    kind = {'execution_guidance': 'execution', 'technical_repair': 'technical',
            'historical_optimizer_guidance': 'historical'}.get(legacy, 'finding')
    status = 'hypothesis'
    if legacy == 'rejected_exact':
        status = 'observed_not_qualified'
    elif legacy == 'observed_exact':
        status = 'observed' if entry.get('_has_exact') or any(item.get('exact') for item in entry.get('observations', [])) else 'hypothesis'
    elif legacy in {'confirmed', 'independently_confirmed'} and entry.get('controller_confirmation') is True:
        status = 'confirmed'
    basis = status
    if entry.get('superseded_by'):
        status = 'superseded'
    result = {key: copy.deepcopy(entry.get(key)) for key in
              ('id', 'loop', 'created_at', 'coins', 'fingerprint', 'uncertainty', 'contradicts', 'superseded_by')}
    result.update(status=status, basis_status=basis, kind=kind, legacy_status=legacy,
                  ai_text=True, archive_path='/history/' + str(entry['id']).replace('~', '~0').replace('/', '~1'))
    for key, limit in (('knowledge', 1000), ('hypothesis', 600)):
        if entry.get(key + '_reference'):
            result[key + '_reference'] = copy.deepcopy(entry[key + '_reference'])
        text = entry.get(key)
        if isinstance(text, str) and len(text) <= limit:
            result[key] = text
        elif text:
            result[key + '_reference'] = {'path': result['archive_path'] + '/' + key,
                                         'characters': len(text) if isinstance(text, str) else None}
    # A persisted improved flag has always meant an incumbent replacement.
    if type(entry.get('improved')) is bool:
        result['incumbent_replaced'] = entry['improved']
    if entry.get('controller_facts'):
        result['controller_facts'] = copy.deepcopy(entry['controller_facts'])
    return result


def select_findings(entries, record, budget=6000):
    """Choose whole findings by scope, loop, open contradictions, age and ID."""
    from ai_loop_windows import protected_holdout_plan
    protected = protected_holdout_plan(record) is not None
    eligible = [item for item in entries if not item.get('holdout') and not item.get('observer_only')
                and (not protected or item.get('loop') == record['id'])
                and item.get('kind') not in {'observer_holdout', 'observer_full_range'}
                and (item.get('fingerprint') or {}).get('pb8') == record['fingerprint'].get('pb8')
                and item.get('coins', (item.get('goals') or {}).get('coins', [])) == record['settings']['goals'].get('coins', [])]
    def order(item):
        """Use explicit stable ties; unknown timestamps are oldest."""
        stamp = item.get('created_at')
        stamp = stamp if type(stamp) in (int, float) and math.isfinite(stamp) else -1
        opened = bool(item.get('contradicts')) and not item.get('contradiction_resolution')
        return (item.get('loop') != record['id'], not opened, -stamp, item['id'])
    selected = []
    for item in sorted(eligible, key=order):
        candidate = compact_finding(item)
        if tokens(selected + [candidate]) <= budget:
            selected.append(candidate)
    return {'findings': selected, 'eligible': len(eligible), 'included': len(selected),
            'omitted': len(eligible) - len(selected), 'excluded': len(entries) - len(eligible),
            'selection_version': SCHEMA, 'index_path': '/history/index'}, eligible


def selection_snapshot_stale(snapshot):
    """Old immutable selection snapshots must be rebuilt before their next dispatch."""
    return (snapshot.get('data', {}).get('stage') == 'select'
            and snapshot.get('required', {}).get('selection_projection_version') != SELECTION_PROJECTION_VERSION)


def decision_snapshot_stale(snapshot):
    """Reproject obsolete stage views without rewriting their immutable originals."""
    from pb8_loop_evaluation_evidence import PROJECTION_VERSION
    data = snapshot.get('data') or {}
    if 'optimizer_research' in data.get('evidence', {}):
        from pb8_loop_rng import VISIBILITY_VERSION
        if snapshot.get('required', {}).get('rng_visibility_version') != VISIBILITY_VERSION:
            return True
    if (data.get('stage') in {'bootstrap', 'evaluate'} and 'optimizer_research' in data.get('evidence', {})
            and snapshot.get('required', {}).get('optimizer_source_version') != 2):
        return True
    return selection_snapshot_stale(snapshot) or (
        data.get('stage') == 'evaluate' and 'optimizer_research' in data.get('evidence', {})
        and snapshot.get('required', {}).get('evaluation_projection_version') != PROJECTION_VERSION)


def required_content(content):
    """Keep exact evaluation evidence; select through a bounded population overview."""
    selection = None
    research_evaluation = (content.get('stage') == 'evaluate'
                           and 'optimizer_research' in content.get('evidence', {}))
    if content.get('stage') == 'select' or research_evaluation:
        # Do not copy the population just to build a bounded overview. The
        # original immutable snapshot remains the source for exact queries.
        if content.get('stage') == 'select':
            selection = content['evidence'].get('candidates') or []
        inline = {'stage', 'input_digest', 'goals', 'loop_id', 'round_number', 'completed_rounds',
                  'max_rounds', 'required_goal_ids', 'scenario_enabled', 'strategy_enabled',
                  'execution', 'validation_budget', 'rubric', 'search_policy', 'hsl_mode',
                  'run_limit', 'metric_dependency_policy', 'optimizer_metric_policy', 'passivbot_manual',
                  'variant_config_schema', 'optimizer_patch_contract'}
        raw = {key: copy.deepcopy(value) for key, value in content.items() if key in inline}
        raw['context_references'] = {key: {'path': '/data/' + key.replace('~', '~0').replace('/', '~1')}
                                     for key in content if key not in inline and key != 'evidence'}
        raw['evidence'] = {}
        for key, value in content['evidence'].items():
            if key == 'candidates':
                continue
            if key in {'loop_id', 'round_number', 'max_candidates', 'validation_budget', 'repair_error',
                       'remaining_runs', 'completed_rounds', 'completed_rounds_after_evaluation',
                       'max_rounds', 'remaining_rounds', 'optimizer_attempts', 'cpu_validation_completed',
                       'cpu_validation_pending', 'free_slots', 'required_rounds', 'required_rounds_remaining',
                       'max_variants'} or (
                    key == 'jev' and small_tree(value, 1500) and len(encode(value)) <= 1500):
                raw['evidence'][key] = copy.deepcopy(value)
            else:
                raw['evidence'][key + '_reference'] = {
                    'path': '/data/evidence/' + key.replace('~', '~0').replace('/', '~1')}
    else:
        raw = copy.deepcopy(content)
    relevant = SAFE_METRICS | {metric for rule in raw.get('rubric', []) for metric in rule['metrics']}
    if raw.get('optimizer_patch_contract') and 'variant_config_schema' in raw:
        raw['variant_config_schema'] = {'evidence_path': '/data/variant_config_schema',
            'instruction': 'Complete schema for full-config variants; compact optimizer patches are preferred.'}
    initial = raw.get('initial_config') or {}
    stage = raw['stage']
    def walk(value, key=''):
        """Project observations without rounding, aliases or duplicate raw configs."""
        if isinstance(value, list):
            return [walk(item) for item in value]
        if not isinstance(value, dict):
            return value
        if key in {'metrics', 'per_report', 'metric_statistics'}:
            return {name: copy.deepcopy(item) for name, item in value.items() if name in relevant}
        if key == 'metric_basis':
            return {name: ({metric: reducer for metric, reducer in item.items() if metric in relevant}
                           if name in {'reducers', 'unavailable'} and isinstance(item, dict) else copy.deepcopy(item))
                    for name, item in value.items()}
        result = {}
        for name, item in value.items():
            if name == 'native_semantics' or (stage == 'select' and name == 'overrides'):
                continue
            if name == 'config' and isinstance(item, dict):
                if stage != 'select':
                    result['config_from_initial'] = delta(initial, item)
            elif name == 'executed_config':
                continue
            elif name == 'per_report' and isinstance(item, list):
                result[name] = [walk(report, 'per_report') for report in item]
            else:
                result[name] = walk(item, name)
        return result
    result = {key: copy.deepcopy(value) for key, value in raw.items() if key not in {'evidence', 'passivbot_manual'}}
    if 'passivbot_manual' in raw:
        from ai_loop_manual import manual_summary
        result['passivbot_manual'] = manual_summary(raw['passivbot_manual'])
    evidence = walk(raw['evidence'])
    # Qualification references existing complete observations instead of duplicating them.
    locations = {}
    for index, observation in enumerate(evidence.get('observations') or []):
        if observation.get('operation'):
            locations[observation['operation']] = '/evidence/observations/' + str(index)
    for name in ('baseline', 'best'):
        observation = evidence.get(name)
        if observation and observation.get('operation'):
            locations[observation['operation']] = '/evidence/' + name
    for operation, option in list((evidence.get('winner_options') or {}).items()):
        if operation in locations:
            evidence['winner_options'][operation] = {'candidate_id': option.get('candidate_id'),
                                                    'observation_path': locations[operation]}
    # Store metric names once, while keeping missing distinguishable from null.
    for name in ('candidates', 'existing_results'):
        candidates = evidence.get(name)
        if not candidates:
            continue
        if name == 'candidates':
            originals = raw['evidence'].get(name) or []
            groups = {}
            unique = []
            for projected, original in zip(candidates, originals):
                signature = sha([original.get('config'), original.get('overrides'), original.get('metrics'),
                                 original.get('metric_statistics'), original.get('metric_basis'),
                                 raw.get('comparison'), original.get('comparison_fingerprint')])
                alias = {key: copy.deepcopy(original.get(key)) for key in ('id', 'optimizer_job', 'source')}
                if signature in groups:
                    groups[signature].setdefault('aliases', []).append(alias)
                else:
                    groups[signature] = projected
                    projected['aliases'] = [alias]
                    unique.append(projected)
            candidates = evidence[name] = unique
            for candidate in candidates:
                if len(candidate['aliases']) == 1:
                    # The representative already carries the exact ID and
                    # provenance. Aliases are needed only for actual duplicates.
                    candidate.pop('aliases')
        columns = sorted({key for row in candidates for key in (row.get('metrics') or {})})
        rows, missing = [], {}
        for index, candidate in enumerate(candidates):
            metrics = candidate.pop('metrics', {})
            rows.append([metrics.get(key) for key in columns])
            absent = [column for column, key in enumerate(columns) if key not in metrics]
            if absent:
                missing[str(index)] = absent
            candidate['metrics_row'] = index
        evidence[name + '_metrics'] = {'columns': columns, 'rows': rows, 'missing': missing}
    if selection is not None:
        from pb8_loop_selection import overview
        evidence['selection'] = overview(selection, relevant)
        result['selection_projection_version'] = SELECTION_PROJECTION_VERSION
    if research_evaluation:
        from pb8_loop_evaluation_evidence import evaluation_overview, decision_brief, PROJECTION_VERSION
        evidence['evaluation'] = evaluation_overview(content['evidence'], relevant)
        evidence['decision_brief'] = decision_brief(content)
        evidence['evaluation_policy'] = {'mode': (content['evidence'].get('evaluation_policy') or {}).get('mode'),
            'instruction': 'Keep the same explained criteria for every qualified CPU training option. '
                           'Use exact evaluation queries and candidate_assessment_policy for a shared explanation; '
                           'the controller recomputes every option, including unqueried options. '
                           'All original hard limits and winner selection rules remain binding.'}
        result['evaluation_projection_version'] = PROJECTION_VERSION
    result['evidence'] = evidence
    if 'optimizer_research' in content.get('evidence', {}):
        from pb8_loop_rng import model_view, VISIBILITY_VERSION
        result = model_view(result)
        result['rng_visibility_version'] = VISIBILITY_VERSION
        if content.get('stage') in {'bootstrap', 'evaluate'}:
            result['optimizer_source_version'] = 2
            result.setdefault('context_references', {})['optimizer_sources'] = {'path': '/data/optimizer_sources'}

    return result


class EvidenceStore:
    """Write snapshots once and bind shared historical archives by content hash."""

    def __init__(self, store):
        self.store = store

    def findings(self, record):
        """Index each immutable original once, keeping old rounds reachable beyond the UI index."""
        owner = self.store.owner_dir(record['owner'])
        archive_dir = owner / 'evidence'
        if not archive_dir.exists():
            return self.store.knowledge(record['owner'])['entries']
        directory = ensure_private_directory(owner / 'finding_index')
        result = {}
        with advisory_file_lock(owner / 'evidence-bindings'):
            knowledge = self.store.knowledge(record['owner'])
            reset_at = knowledge.get('learning_reset_at', 0)
            for path in sorted(archive_dir.glob('*.json')):
                if path.is_symlink() or len(path.stem) != 64:
                    continue
                cached = directory / path.name
                try:
                    if cached.exists():
                        value = json.loads(read_regular_file_nofollow(cached, self.store.root))
                    else:
                        raw = read_regular_file_nofollow(path, self.store.root)
                        original = json.loads(raw)
                        if str(original.get('id', '')).startswith('native_source:'):
                            atomic_write_private_text(cached, json.dumps({'skip_native_source': True}, indent=4) + '\n')
                            continue
                        value = {key: copy.deepcopy(original.get(key)) for key in (
                            'id', 'loop', 'created_at', 'coins', 'fingerprint', 'status', 'uncertainty',
                            'contradicts', 'superseded_by', 'controller_confirmation', 'improved', 'controller_facts',
                            'holdout', 'observer_only', 'kind')}
                        if not isinstance(value['id'], str) or not isinstance(value['loop'], str):
                            raise ValueError('Archive index identity is invalid')
                        value['_has_exact'] = any(item.get('exact') for item in original.get('observations', []))
                        value['coins'] = original.get('coins', (original.get('goals') or {}).get('coins', []))
                        prefix = '/history/' + value['id'].replace('~', '~0').replace('/', '~1')
                        for key, limit in (('knowledge', 1000), ('hypothesis', 600)):
                            text = original.get(key)
                            if isinstance(text, str) and len(text) <= limit:
                                value[key] = text
                            elif text:
                                value[key + '_reference'] = {'path': prefix + '/' + key,
                                                             'characters': len(text) if isinstance(text, str) else None}
                        value['_archive_binding'] = {'key': path.stem, 'sha256': hashlib.sha256(raw).hexdigest(),
                                                     'bytes': len(raw), 'loop': value['loop'], 'created_at': value['created_at']}
                        atomic_write_private_text(cached, json.dumps(value, indent=4, allow_nan=False) + '\n')
                    if not value.get('skip_native_source') and (not reset_at or value.get('created_at', 0) > reset_at):
                        result[value['id']] = value
                except (OSError, ValueError, KeyError) as exc:
                    raise LoopAIPause('evidence_recovery', 'Historical evidence index could not be verified',
                                      error_type=type(exc).__name__) from exc
            # Supersession lives in the mutable knowledge index, never by rewriting originals.
            for entry in knowledge['entries']:
                if reset_at and entry.get('created_at', 0) <= reset_at:
                    continue
                if entry['id'] in result:
                    for key in ('superseded_by', 'contradiction_resolution'):
                        if key in entry:
                            result[entry['id']][key] = copy.deepcopy(entry[key])
                else:
                    result[entry['id']] = entry
            for value in list(result.values()):
                for target in value.get('contradicts') or []:
                    if target in result:
                        result[target]['superseded_by'] = value['id']
        return list(result.values())

    def load(self, owner, snapshot_id):
        """Recover only a validated immutable snapshot beneath its owning root."""
        from pb8_loop_store import identifier
        try:
            path = self.store.owner_dir(owner) / 'snapshots' / (identifier(snapshot_id) + '.json')
            value = json.loads(read_regular_file_nofollow(path, self.store.root))
            expected = value.pop('sha256')
            if value.get('owner') != owner or sha(value) != expected:
                raise ValueError('Snapshot integrity mismatch')
            value['sha256'] = expected
            return value
        except (ValueError, OSError, KeyError) as exc:
            raise LoopAIPause('evidence_recovery', 'The original decision snapshot is unavailable or changed') from exc

    def create(self, record, required, raw, history):
        """Persist a decision before sending it; copy no historical archive per turn."""
        from pb8_loop_store import digest
        manifest = {}
        try:
            owner = self.store.owner_dir(record['owner'])
            directory = ensure_private_directory(owner / 'snapshots')
            with advisory_file_lock(owner / 'evidence-bindings'):
                archive_dir = ensure_private_directory(owner / 'evidence')
                for item in history:
                    if item.get('_archive_binding'):
                        manifest[item['id']] = copy.deepcopy(item['_archive_binding'])
                        continue
                    key = digest(item['id'])
                    path = archive_dir / (key + '.json')
                    if not path.exists() and not path.is_symlink():
                        atomic_write_private_text(path, json.dumps(item, indent=4, allow_nan=False) + '\n')
                    text = read_regular_file_nofollow(path, self.store.root)
                    value = json.loads(text)
                    if value.get('id') != item['id'] or value.get('loop') != item.get('loop'):
                        raise ValueError('Archive identity mismatch')
                    if (str(item['id']).startswith('native_source:') and ':optimizer:' in item['id']
                            and value != item):
                        raise ValueError('Optimizer input archive content mismatch')
                    manifest[item['id']] = {'key': key, 'sha256': hashlib.sha256(text).hexdigest(),
                                            'bytes': len(text), 'loop': item.get('loop'),
                                            'created_at': item.get('created_at'), 'required': False}
                snapshot = {'schema': SCHEMA, 'id': uuid.uuid4().hex, 'owner': record['owner'],
                            'loop': record['id'], 'round': record['round'], 'phase': raw['stage'],
                            'created_at': time.time(), 'required': required, 'data': raw, 'history': manifest}
                snapshot['sha256'] = sha(snapshot)
                atomic_write_private_text(directory / (snapshot['id'] + '.json'),
                                          json.dumps(snapshot, indent=4, allow_nan=False) + '\n')
            return snapshot
        except (ValueError, OSError) as exc:
            raise LoopAIPause('evidence_storage', 'Evidence snapshot could not be safely stored',
                              error_type=type(exc).__name__) from exc

    def resolve(self, snapshot, path):
        """Read only archive identities frozen in this snapshot, never caller filenames."""
        def resolve_pointer(value, subpath):
            """Protect scalar reads through historical seed-delta value pointers too."""
            result = pointer(value, subpath)
            if 'optimizer_research' in snapshot.get('data', {}).get('evidence', {}):
                from pb8_loop_rng import seed_path
                if subpath and subpath.rsplit('/', 1)[-1] in {'value', 'before', 'after'}:
                    parent = pointer(value, subpath.rsplit('/', 1)[0])
                    if isinstance(parent, dict) and seed_path(parent.get('path')):
                        return None
            return result
        if path == '/history/index':
            return [{'id': key, **{name: value.get(name) for name in ('loop', 'created_at', 'bytes')}}
                    for key, value in sorted(snapshot['history'].items())]
        if path.startswith('/history/'):
            parts = path.split('/')
            evidence_id = parts[2].replace('~1', '/').replace('~0', '~')
            binding = snapshot['history'].get(evidence_id)
            if binding is None:
                raise ValueError('Archive is outside the decision manifest')
            try:
                path_on_disk = self.store.owner_dir(snapshot['owner']) / 'evidence' / (binding['key'] + '.json')
                text = read_regular_file_nofollow(path_on_disk, self.store.root)
                if hashlib.sha256(text).hexdigest() != binding['sha256']:
                    raise ValueError('Archive content hash mismatch')
                return resolve_pointer(json.loads(text), '/' + '/'.join(parts[3:])) if len(parts) > 3 else json.loads(text)
            except (ValueError, OSError) as exc:
                if binding.get('required'):
                    raise LoopAIPause('evidence_recovery', 'Required archive failed integrity verification') from exc
                return {'error': 'evidence_integrity', 'path': path, 'available': False}
        return resolve_pointer({'required': snapshot['required'], 'data': snapshot['data']}, path)

    def _snapshot_header(self, path, owner):
        """Read only identity/age fields preceding the immutable evidence body."""
        raw = read_regular_file_prefix_nofollow(path, self.store.root)
        text = codecs.getincrementaldecoder('utf-8')().decode(raw, final=False)
        decoder, header, position = json.JSONDecoder(), {}, 0
        text = text.lstrip()
        if not text.startswith('{'):
            raise ValueError('Invalid snapshot header')
        position = 1
        needed = {'id', 'owner', 'loop', 'created_at'}
        while not needed.issubset(header):
            while position < len(text) and text[position].isspace():
                position += 1
            key, position = decoder.raw_decode(text, position)
            if not isinstance(key, str) or key in header:
                raise ValueError('Invalid snapshot header field')
            while position < len(text) and text[position].isspace():
                position += 1
            if position >= len(text) or text[position] != ':':
                raise ValueError('Invalid snapshot header separator')
            position += 1
            while position < len(text) and text[position].isspace():
                position += 1
            value, position = decoder.raw_decode(text, position)
            if not isinstance(value, (str, int, float)) or isinstance(value, bool):
                raise ValueError('Snapshot header fields must be scalar')
            header[key] = value
            while position < len(text) and text[position].isspace():
                position += 1
            if position >= len(text) or text[position] != ',':
                raise ValueError('Incomplete snapshot header')
            position += 1
        from pb8_loop_store import identifier
        if (identifier(header['id']) != path.stem or identifier(header['owner']) != owner
                or identifier(header['loop']) != header['loop']
                or type(header['created_at']) not in (int, float)
                or not math.isfinite(header['created_at']) or header['created_at'] < 0):
            raise ValueError('Snapshot header identity mismatch')
        return header

    def cleanup(self, record, protected=()):
        """Expire only snapshots; shared historical originals are never garbage-collected."""
        owner = self.store.owner_dir(record['owner'])
        protected = set(protected)
        current = self.store.read(record['owner'], record['id'], fields={
            'status', 'ai_snapshot_id', 'pending_ai', 'interrupted_ai', 'interrupted_ai_audit',
        })
        if current['status'] not in {'completed', 'stopped', 'failed', 'unconfirmed'}:
            protected.add(current.get('ai_snapshot_id'))
        for key in ('pending_ai', 'interrupted_ai'):
            protected.add((current.get(key) or {}).get('snapshot_id'))
        for item in current.get('interrupted_ai_audit', []):
            protected.add(item.get('snapshot_id'))
        directory = owner / 'snapshots'
        if not directory.exists() or directory.is_symlink():
            return
        with advisory_file_lock(owner / 'evidence-bindings'):
            paths = []
            for path in directory.glob('*.json'):
                try:
                    value = self._snapshot_header(path, record['owner'])
                    if value.get('loop') == record['id']:
                        paths.append((value.get('created_at', 0), path, value))
                except (ValueError, OSError, RuntimeError) as exc:
                    _log(SERVICE, 'Unreadable evidence snapshot retained during cleanup', level='WARNING',
                         meta={'error_type': type(exc).__name__})
                    continue
            for index, (_at, path, value) in enumerate(sorted(paths, reverse=True)):
                if value['id'] in protected:
                    continue
                expired = record['status'] in {'completed', 'stopped', 'failed', 'unconfirmed'} and time.time() - _at > 14 * 86400
                if expired or (record['status'] not in {'completed', 'stopped', 'failed', 'unconfirmed'} and index >= 20):
                    # Full validation is necessary only for an actual deletion.
                    # Malformed/truncated bodies remain available for recovery.
                    try:
                        complete = json.loads(read_regular_file_nofollow(path, self.store.root))
                        if any(complete.get(key) != value[key] for key in ('id', 'owner', 'loop', 'created_at')):
                            raise ValueError('Snapshot changed during cleanup')
                    except (ValueError, OSError, RuntimeError) as exc:
                        _log(SERVICE, 'Unreadable evidence snapshot retained during cleanup', level='WARNING',
                             meta={'error_type': type(exc).__name__})
                        continue
                    path.unlink()
            size = sum(path.stat().st_size for folder in (directory, owner / 'evidence') if folder.exists()
                       for path in folder.glob('*.json') if not path.is_symlink())
            if size > 1024**3:
                _log(SERVICE, 'Evidence storage exceeds the soft 1 GiB owner budget; protected originals retained',
                     level='WARNING', meta={'bytes': size})


def small_tree(value, limit=4000):
    """Bound work before tokenizing a subtree; never encode an entire large collection."""
    stack = [value]
    remaining = limit
    while stack and remaining >= 0:
        item = stack.pop()
        remaining -= 1
        if isinstance(item, str):
            remaining -= len(item)
        elif isinstance(item, (dict, list)):
            remaining -= len(item)
            if remaining < 0:
                return False
            if isinstance(item, dict):
                stack.extend(item.keys())
                stack.extend(item.values())
            else:
                stack.extend(item)
    return remaining >= 0


def records(value, path='', record_tokens=900):
    """Bundle small subtrees, split larger ones in deterministic pointer order."""
    if path and small_tree(value) and tokens(value) <= record_tokens:
        yield {'path': path, 'value': value}
    elif isinstance(value, dict) and value:
        for key, child in value.items():
            yield from records(child, path + '/' + key.replace('~', '~0').replace('/', '~1'), record_tokens)
    elif isinstance(value, list) and value:
        for index, child in enumerate(value):
            yield from records(child, path + '/' + str(index), record_tokens)
    else:
        yield {'path': path, 'value': value}


class EvidenceSession:
    """Own page coverage and quotas across correction turns and compaction generations."""

    def __init__(self, evidence_store, snapshot, page_tokens=5000):
        if type(page_tokens) is not int or not 512 <= page_tokens <= 6144:
            raise ValueError('Invalid evidence page profile')
        self.store, self.snapshot, self.page_tokens = evidence_store, snapshot, page_tokens
        self.private_rng = 'optimizer_research' in snapshot.get('data', {}).get('evidence', {})
        self.lock = threading.RLock()
        self.charge = None
        self.selection_index = None
        self.evaluation_index = None
        self.dialog = -1
        self.total_calls = 0
        self.total_budget_notices = 0
        self.start_dialog()
        self.required_pages = self.pages(snapshot['required'], '/required')

    def start_dialog(self):
        """Start fresh A coverage, never reset the decision-wide call counter."""
        self.dialog += 1
        if self.dialog >= 2:
            raise LoopAIPause('evidence_recovery', 'Evidence dialog limit reached')
        self.generation, self.calls, self.stalled = 0, 0, 0
        self.covered = None  # A is complete in the initial message.
        self.intervals = {}
        self.cache, self.seen = {}, set()

    def compacted(self):
        """Invalidate exact coverage only after a real Codex compaction event."""
        self.generation += 1
        if self.generation > 1:
            raise LoopAIPause('evidence_recovery', 'Evidence compaction allowance exhausted')
        self.covered, self.seen = set(), set()
        self.intervals = {}

    def page_entries(self, value, path=''):
        """Yield bounded exact records and string fragments without collecting the subtree."""
        for entry in records(value, record_tokens=min(900, self.page_tokens - 256)):
            if self.private_rng:
                from pb8_loop_rng import model_view
                entry = {**entry, 'value': model_view(entry['value'], path + entry['path'])}
            if small_tree(entry) and tokens(entry) + 180 <= self.page_tokens:
                yield entry
                continue
            text = entry['value']
            if not isinstance(text, str):
                raise LoopAIPause('context_budget', 'An evidence scalar cannot fit its page profile')
            offset = 0
            while offset < len(text):
                # Bound each probe too: escaped text can use at most a modest
                # number of characters inside a single output page.
                low, high, best = 1, min(len(text) - offset, self.page_tokens * 4), None
                while low <= high:
                    count = (low + high) // 2
                    fragment = {'path': entry['path'], 'value': text[offset:offset + count],
                                'char_offset': offset, 'char_end': offset + count, 'characters': len(text)}
                    if tokens(fragment) + 180 <= self.page_tokens:
                        best, low = fragment, count + 1
                    else:
                        high = count - 1
                if best is None:
                    raise LoopAIPause('context_budget', 'Evidence pointer exceeds page profile')
                yield best
                offset = best['char_end']

    def iter_pages(self, value, path):
        """Construct only requested pages, with one bounded record of lookahead."""
        current, start = [], 0
        for entry in self.page_entries(value, path):
            candidate = self.envelope(path, start, current + [entry], False)
            if current and tokens(candidate) > self.page_tokens:
                yield self.envelope(path, start, current, False)
                start += len(current)
                current = []
            current.append(entry)
            if tokens(self.envelope(path, start, current, False)) > self.page_tokens:
                raise LoopAIPause('context_budget', 'Evidence page metadata exceeds safe profile')
        yield self.envelope(path, start, current, True)

    def pages(self, value, path):
        """Materialize only the bounded mandatory recovery set when coverage needs it."""
        return list(self.iter_pages(value, path))

    def envelope(self, path, offset, entries, complete):
        """Use the same response shape for measurement and actual tool delivery."""
        return {'snapshot': self.snapshot['id'], 'sha256': self.snapshot['sha256'], 'path': path,
                'offset': offset, 'entries': entries, 'next_offset': None if complete else offset + len(entries),
                'complete': complete}

    def read(self, arguments, call_id):
        """Validate arguments and count progress without allowing retries to double-charge."""
        with self.lock:
            return self._read(arguments, call_id)

    def _read(self, arguments, call_id):
        """Serialize parallel native tool requests against the owning session."""
        if not isinstance(call_id, str) or not call_id:
            raise LoopAIPause('evidence_recovery', 'Evidence calls require a nonempty native call ID')
        cache_key = (self.dialog, self.generation, call_id)
        signature = encode(arguments)
        if cache_key in self.cache:
            previous, result = self.cache[cache_key]
            if previous != signature:
                raise ValueError('A tool call ID was reused with different arguments')
            return result
        # Denying an optional read is a normal tool result, not a failed
        # decision. Do this before charging data reads: a parallel batch of
        # denied requests must not consume the reserved mandatory capacity.
        if (isinstance(arguments, dict) and set(arguments) == {'path', 'offset'}
                and isinstance(arguments['path'], str) and len(arguments['path']) <= 8192
                and type(arguments['offset']) is int and arguments['offset'] >= 0):
            path = arguments['path']
            recovery = len(self.required_pages) if self.covered is None else len(self.missing())
            optional = not (path == '/required' or path.startswith('/required/'))
            if optional and (self.calls + 1 + recovery + 4 > 64
                             or self.total_calls + 1 + recovery + 4 > 128):
                self.total_budget_notices += 1
                if self.total_budget_notices > 128:
                    raise LoopAIPause('evidence_recovery', 'Optional evidence budget notices were repeatedly ignored')
                missing = self.missing()
                result = {'snapshot': self.snapshot['id'], 'sha256': self.snapshot['sha256'],
                          'error': 'optional_evidence_budget_reserved', 'available': False,
                          'requested_path': path, 'can_finalize': not missing,
                          'required_reads': [{'path': '/required', 'offset': offset} for offset in missing],
                          'accepted_reads': self.calls, 'accepted_reads_total': self.total_calls,
                          'reserved_recovery_reads': recovery,
                          'instruction': ('Read the listed missing /required pages, then return the complete JSON decision. '
                                          'Do not repeat optional requests.' if missing else
                                          'Return the complete JSON decision using the evidence already supplied/read. '
                                          'Do not repeat optional requests; report any remaining uncertainty. '
                                          'If compaction occurs, recover every missing /required page first.')}
                self.cache[cache_key] = (signature, result)
                return result
        self.calls += 1
        self.total_calls += 1
        if self.charge is not None:
            self.charge()
        if self.calls > 64 or self.total_calls > 128:
            raise LoopAIPause('evidence_recovery', 'Evidence tool allowance exhausted')
        if not isinstance(arguments, dict) or set(arguments) != {'path', 'offset'}:
            raise ValueError('Expected only path and offset')
        path, offset = arguments['path'], arguments['offset']
        if type(offset) is not int or offset < 0 or not isinstance(path, str) or len(path) > 8192:
            raise ValueError('Invalid evidence offset or pointer')
        if signature in self.seen:
            self.stalled += 1
        else:
            self.stalled = 0
        self.seen.add(signature)
        if self.calls > 32 and self.stalled >= 4:
            raise LoopAIPause('evidence_recovery', 'Repeated evidence reads made no progress')
        if path.startswith('/evaluation/'):
            from pb8_loop_evaluation_evidence import query, EvaluationIndex
            if self.evaluation_index is None:
                self.evaluation_index = EvaluationIndex(self.snapshot['data'].get('evidence') or {})
            result = query(self.snapshot, path, offset, self.page_tokens, self.evaluation_index)
            self.cache[cache_key] = (signature, result)
            return result
        if path.startswith('/selection/'):
            from pb8_loop_selection import query, SelectionIndex
            if path.startswith('/selection/rank/') and self.selection_index is None:
                content = self.snapshot['data']
                self.selection_index = SelectionIndex(content.get('evidence', {}).get('candidates', []),
                                                      content.get('rubric') or [])
            result = query(self.snapshot, path, offset, self.page_tokens, self.selection_index)
            self.cache[cache_key] = (signature, result)
            return result
        pages = self.required_pages if path == '/required' else self.iter_pages(self.store.resolve(self.snapshot, path), path)
        result = next((page for page in pages if page['offset'] == offset), None)
        if result is None:
            raise ValueError('Offset must match a returned page boundary')
        if self.covered is not None and (path == '/required' or path.startswith('/required/')):
            for entry in result['entries']:
                self._mark(path + entry['path'], entry)
        self.cache[cache_key] = (signature, result)
        return result

    def close(self):
        """Release session-owned query indexes after success, failure or cancellation."""
        with self.lock:
            try:
                if self.selection_index is not None:
                    try:
                        self.selection_index.close()
                    finally:
                        self.selection_index = None
            finally:
                if self.evaluation_index is not None:
                    try:
                        self.evaluation_index.close()
                    finally:
                        self.evaluation_index = None

    def missing(self):
        """Return whole required pages not re-read since the last compaction."""
        if self.covered is None:
            return []
        return [page['offset'] for page in self.required_pages if not all(
            self._has('/required' + entry['path'], entry) for entry in page['entries'])]

    def _leaves(self, value, path):
        """Describe exact leaf provenance so small direct reads cover canonical pages too."""
        if isinstance(value, dict) and value:
            for key, child in value.items():
                yield from self._leaves(child, path + '/' + key.replace('~', '~0').replace('/', '~1'))
        elif isinstance(value, list) and value:
            for index, child in enumerate(value):
                yield from self._leaves(child, path + '/' + str(index))
        else:
            yield path

    def _mark(self, path, entry):
        """Record complete leaves or merge explicitly bounded scalar character ranges."""
        if 'char_offset' in entry:
            ranges = self.intervals.setdefault(path, [])
            ranges.append((entry['char_offset'], entry['char_end']))
            merged = []
            for start, end in sorted(ranges):
                if merged and start <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
                else:
                    merged.append((start, end))
            self.intervals[path] = merged
        else:
            self.covered.update(self._leaves(entry['value'], path))

    def _has(self, path, entry):
        """Every fragment and nested leaf must have been delivered in this generation."""
        if 'char_offset' in entry:
            return path in self.covered or any(start <= entry['char_offset'] and end >= entry['char_end']
                                               for start, end in self.intervals.get(path, []))
        return all(leaf in self.covered or self._string_complete(leaf) for leaf in self._leaves(entry['value'], path))

    def _string_complete(self, path):
        """A direct string-fragment read covers a parent only after its last character."""
        if path not in self.intervals:
            return False
        value = pointer({'required': self.snapshot['required']}, path)
        return isinstance(value, str) and any(start == 0 and end == len(value) for start, end in self.intervals[path])
