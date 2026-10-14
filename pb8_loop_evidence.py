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

from ai_token_budget import _proxy_encoding
from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from logging_helpers import human_log as _log

SERVICE = 'PB8LoopEvidence'
MIN_CONTEXT = 256_000
SCHEMA = 1
SAFE_METRICS = {'backtest_completion_ratio', 'drawdown_worst_strategy_eq', 'drawdown_worst',
                'liquidated', 'liquidated_usd', 'liquidated_btc', 'liquidation_count',
                'n_liquidations', 'n_fills'}
TOOL_SCHEMA = {'type': 'object', 'properties': {'path': {'type': 'string', 'maxLength': 8192},
               'offset': {'type': 'integer', 'minimum': 0}}, 'required': ['path', 'offset'],
               'additionalProperties': False}
TOOL_DESCRIPTION = 'Read exact immutable evidence for this decision. Start at /required or /history/index; follow next_offset.'


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
    eligible = [item for item in entries if not item.get('holdout') and not item.get('observer_only')
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


def required_content(content):
    """Keep every current goal/safety value; make historical reports optional."""
    raw = copy.deepcopy(content)
    relevant = SAFE_METRICS | {metric for rule in raw.get('rubric', []) for metric in rule['metrics']}
    initial = raw.get('initial_config') or {}
    stage = raw['stage']
    def walk(value, key=''):
        """Project observations without rounding, aliases or duplicate raw configs."""
        if isinstance(value, list):
            return [walk(item) for item in value]
        if not isinstance(value, dict):
            return value
        if key in {'metrics', 'per_report'}:
            return {name: copy.deepcopy(item) for name, item in value.items() if name in relevant}
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
    result = {key: copy.deepcopy(value) for key, value in raw.items() if key != 'evidence'}
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
    result['evidence'] = evidence
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
                    if not value.get('skip_native_source'):
                        result[value['id']] = value
                except (OSError, ValueError, KeyError) as exc:
                    raise LoopAIPause('evidence_recovery', 'Historical evidence index could not be verified',
                                      error_type=type(exc).__name__) from exc
            # Supersession lives in the mutable knowledge index, never by rewriting originals.
            for entry in self.store.knowledge(record['owner'])['entries']:
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
                return pointer(json.loads(text), '/' + '/'.join(parts[3:])) if len(parts) > 3 else json.loads(text)
            except (ValueError, OSError) as exc:
                if binding.get('required'):
                    raise LoopAIPause('evidence_recovery', 'Required archive failed integrity verification') from exc
                return {'error': 'evidence_integrity', 'path': path, 'available': False}
        return pointer({'required': snapshot['required'], 'data': snapshot['data']}, path)

    def cleanup(self, record, protected=()):
        """Expire only snapshots; shared historical originals are never garbage-collected."""
        owner = self.store.owner_dir(record['owner'])
        protected = set(protected)
        current = self.store.read(record['owner'], record['id'])
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
                    value = json.loads(read_regular_file_nofollow(path, self.store.root))
                    if value.get('loop') == record['id']:
                        paths.append((value.get('created_at', 0), path, value))
                except (ValueError, OSError) as exc:
                    _log(SERVICE, 'Unreadable evidence snapshot retained during cleanup', level='WARNING',
                         meta={'error_type': type(exc).__name__})
                    continue
            for index, (_at, path, value) in enumerate(sorted(paths, reverse=True)):
                if value['id'] in protected:
                    continue
                expired = record['status'] in {'completed', 'stopped', 'failed', 'unconfirmed'} and time.time() - _at > 14 * 86400
                if expired or (record['status'] not in {'completed', 'stopped', 'failed', 'unconfirmed'} and index >= 20):
                    path.unlink()
            size = sum(path.stat().st_size for folder in (directory, owner / 'evidence') if folder.exists()
                       for path in folder.glob('*.json') if not path.is_symlink())
            if size > 1024**3:
                _log(SERVICE, 'Evidence storage exceeds the soft 1 GiB owner budget; protected originals retained',
                     level='WARNING', meta={'bytes': size})


def records(value, path=''):
    """Bundle small subtrees, split larger ones in deterministic pointer order."""
    if path and tokens(value) <= 900:
        yield {'path': path, 'value': value}
    elif isinstance(value, dict) and value:
        for key, child in value.items():
            yield from records(child, path + '/' + key.replace('~', '~0').replace('/', '~1'))
    elif isinstance(value, list) and value:
        for index, child in enumerate(value):
            yield from records(child, path + '/' + str(index))
    else:
        yield {'path': path, 'value': value}


class EvidenceSession:
    """Own page coverage and quotas across correction turns and compaction generations."""

    def __init__(self, evidence_store, snapshot, page_tokens=5000):
        if type(page_tokens) is not int or not 512 <= page_tokens <= 5000:
            raise ValueError('Invalid evidence page profile')
        self.store, self.snapshot, self.page_tokens = evidence_store, snapshot, page_tokens
        self.lock = threading.RLock()
        self.charge = None
        self.dialog = -1
        self.total_calls = 0
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

    def pages(self, value, path):
        """Create actual bounded JSON pages, including metadata and scalar chunks."""
        entries = []
        for entry in records(value):
            if tokens(entry) + 180 <= self.page_tokens:
                entries.append(entry)
                continue
            text = entry['value']
            if not isinstance(text, str):
                raise LoopAIPause('context_budget', 'An evidence scalar cannot fit its page profile')
            offset = 0
            while offset < len(text):
                low, high, best = 1, len(text) - offset, None
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
                entries.append(best)
                offset = best['char_end']
        result, current, start = [], [], 0
        for entry in entries:
            candidate = self.envelope(path, start, current + [entry], False)
            if current and tokens(candidate) > self.page_tokens:
                result.append(self.envelope(path, start, current, False))
                start += len(current)
                current = []
            current.append(entry)
            if tokens(self.envelope(path, start, current, False)) > self.page_tokens:
                raise LoopAIPause('context_budget', 'Evidence page metadata exceeds safe profile')
        result.append(self.envelope(path, start, current, True))
        return result

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
        recovery = len(self.required_pages) if self.covered is None else len(self.missing())
        if not (path == '/required' or path.startswith('/required/')) and self.calls + recovery + 4 > 64:
            raise LoopAIPause('evidence_recovery', 'Optional reads would consume mandatory recovery capacity')
        if signature in self.seen:
            self.stalled += 1
        else:
            self.stalled = 0
        self.seen.add(signature)
        if self.calls > 32 and self.stalled >= 4:
            raise LoopAIPause('evidence_recovery', 'Repeated evidence reads made no progress')
        pages = self.required_pages if path == '/required' else self.pages(self.store.resolve(self.snapshot, path), path)
        result = next((page for page in pages if page['offset'] == offset), None)
        if result is None:
            raise ValueError('Offset must match a returned page boundary')
        if self.covered is not None and (path == '/required' or path.startswith('/required/')):
            for entry in result['entries']:
                self._mark(path + entry['path'], entry)
        self.cache[cache_key] = (signature, result)
        return result

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
