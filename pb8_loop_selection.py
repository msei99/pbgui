"""Population-independent AI selection summaries and bounded exact dataset queries."""
from __future__ import annotations

import copy
import math
from pathlib import Path
import sqlite3
import threading
import json
import tempfile

from secure_files import atomic_write_private_bytes

SERVICE = 'PB8LoopSelection'
PROJECTION_VERSION = 2


def numeric(value):
    """Exclude missing, boolean and nonfinite values without rounding integer scores."""
    return type(value) is int or type(value) is float and math.isfinite(value)


def overview(candidates, metrics):
    """Scan the entire population with constant state per required goal/safety metric."""
    summary = {key: {'finite': 0, 'missing': 0, 'null': 0, 'invalid': 0, 'min': None, 'max': None}
               for key in sorted(metrics)}
    count = 0
    for candidate in candidates:
        count += 1
        values = candidate.get('metrics') or {}
        for key, row in summary.items():
            if key not in values:
                row['missing'] += 1
            elif values[key] is None:
                row['null'] += 1
            elif not numeric(values[key]):
                row['invalid'] += 1
            else:
                value = values[key]
                row['finite'] += 1
                row['min'] = value if row['min'] is None else min(row['min'], value)
                row['max'] = value if row['max'] is None else max(row['max'], value)
    return {'candidate_count': count, 'metrics': summary, 'coverage': 'entire_population',
            'rows_path': '/selection/rows', 'metrics_path': '/selection/metrics',
            'sources_path': '/selection/sources',
            'rank_path': '/selection/rank/{metric}/{reducer}/{min|max}',
            'eligible_rank_path': '/selection/rank/{metric}/{reducer}/{min|max}/goals_met',
            'candidate_path': '/selection/id/{candidate_id}',
            'reducers': ['value', 'mean', 'min', 'max', 'std', 'median'],
            'policy': 'No shortlist or prefix truncation. Query globally ranked rows and read chosen candidates. '
                      'Proxy goal screening is not independent CPU qualification. Query coverage is partial; '
                      'report uncertainty instead of claiming to have read every candidate.'}


def candidate_summary(candidate, index, relevant):
    """Deliver exact goal/safety values and reducers; keep bulky originals reachable."""
    result = {key: copy.deepcopy(candidate[key]) for key in ('id', 'optimizer_job') if key in candidate}
    result['details_path'] = '/data/evidence/candidates/' + str(index)
    for field in ('metrics', 'metric_statistics'):
        result[field] = {key: copy.deepcopy(value) for key, value in (candidate.get(field) or {}).items()
                         if key in relevant}
    basis = candidate.get('metric_basis') or {}
    result['metric_basis'] = {key: ({name: value for name, value in item.items() if name in relevant}
                                  if key in {'reducers', 'unavailable'} and isinstance(item, dict) else copy.deepcopy(item))
                              for key, item in basis.items()}
    return result


class SelectionIndex:
    """Own a private on-disk ranking cache for exactly one evidence session."""

    def __init__(self, candidates, rubric):
        self.candidates, self.rubric = candidates, rubric
        self.lock = threading.RLock()
        self.directory = tempfile.TemporaryDirectory(prefix='pbgui-selection-')
        self.connection = None
        self.tables = {}
        try:
            database = Path(self.directory.name) / 'rank.sqlite3'
            atomic_write_private_bytes(database, b'')
            self.connection = sqlite3.connect(database, check_same_thread=False)
            self.connection.create_collation('EXACT_NUMBER', self.compare)
            self.connection.execute('PRAGMA cache_size=-2048')
            self.connection.execute('PRAGMA temp_store=FILE')
        except BaseException:
            self.close()
            raise

    @staticmethod
    def compare(left, right):
        """Compare exact JSON numbers, including integers beyond float precision."""
        a, b = json.loads(left), json.loads(right)
        return (a > b) - (a < b)

    @staticmethod
    def value(candidate, metric, reducer):
        """Extract labeled optimizer scores; CPU evaluation overrides this source."""
        return ((candidate.get('metrics') or {}).get(metric) if reducer == 'value' else
                ((candidate.get('metric_statistics') or {}).get(metric) or {}).get(reducer))

    def ranked(self, metric, reducer, direction, screened, offset, source=None):
        """Build each metric index once; page every direction with stable ordinal ties."""
        from pb8_loop_evidence import encode
        from pb8_loop_store import evaluate
        with self.lock:
            if self.connection is None:
                raise ValueError('Selection index is closed')
            key = (metric, reducer, screened)
            if key not in self.tables:
                table = 'ranking_' + str(len(self.tables))
                self.connection.execute(f'CREATE TABLE {table} (ordinal INTEGER PRIMARY KEY, score TEXT COLLATE EXACT_NUMBER, source TEXT)')
                def scores():
                    """Stream numeric scores without copying full candidate records."""
                    for index, candidate in enumerate(self.candidates):
                        if screened and not evaluate(candidate.get('metrics') or {}, self.rubric)['hard_targets_met']:
                            continue
                        value = self.value(candidate, metric, reducer)
                        if numeric(value):
                            yield index, encode(value), candidate.get('optimizer_job')
                try:
                    self.connection.executemany(f'INSERT INTO {table} VALUES (?, ?, ?)', scores())
                    self.connection.execute(f'CREATE INDEX {table}_asc ON {table}(score ASC, ordinal ASC)')
                    self.connection.execute(f'CREATE INDEX {table}_desc ON {table}(score DESC, ordinal ASC)')
                    self.connection.execute(f'CREATE INDEX {table}_source_asc ON {table}(source, score ASC, ordinal ASC)')
                    self.connection.execute(f'CREATE INDEX {table}_source_desc ON {table}(source, score DESC, ordinal ASC)')
                    count = self.connection.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                    self.connection.commit()
                except BaseException:
                    self.connection.rollback()
                    self.connection.execute(f'DROP TABLE IF EXISTS {table}')
                    raise
                self.tables[key] = (table, count)
            table, count = self.tables[key]
            order = 'DESC' if direction == 'max' else 'ASC'
            where = ' WHERE source=?' if source is not None else ''
            parameters = (source,) if source is not None else ()
            if source is not None:
                count = self.connection.execute(f'SELECT count(*) FROM {table}{where}', parameters).fetchone()[0]
            rows = self.connection.execute(
                f'SELECT ordinal FROM {table}{where} ORDER BY score {order}, ordinal ASC LIMIT 32 OFFSET ?',
                (*parameters, offset))
            return [row[0] for row in rows], count

    def close(self):
        """Close SQLite before deleting temporary indexes; repeated shutdown is harmless."""
        with self.lock:
            try:
                if self.connection is not None:
                    self.connection.close()
            finally:
                self.connection = None
                self.directory.cleanup()


def query(snapshot, path, offset, page_tokens, index=None):
    """Query all immutable candidates; sort on temporary disk, with bounded responses."""
    from pb8_loop_evidence import SAFE_METRICS, encode, tokens
    if type(offset) is not int or offset < 0 or type(page_tokens) is not int or not 512 <= page_tokens <= 6144:
        raise ValueError('Invalid selection query limits')
    content = snapshot['data']
    if content.get('stage') != 'select':
        raise ValueError('Candidate queries require a selection snapshot')
    candidates = content.get('evidence', {}).get('candidates', [])
    relevant = SAFE_METRICS | {metric for rule in content.get('rubric') or [] for metric in rule['metrics']}
    if offset > len(candidates) and path not in {'/selection/metrics', '/selection/sources'}:
        raise ValueError('Candidate query offset exceeds dataset')
    parts = path.split('/')
    parts = [part.replace('~1', '/').replace('~0', '~') for part in parts]
    result = {'snapshot': snapshot['id'], 'sha256': snapshot['sha256'], 'path': path,
              'offset': offset, 'dataset_count': len(candidates), 'entries': [], 'next_offset': None,
              'coverage': 'queried_subset_of_entire_population'}
    if parts == ['', 'selection', 'sources']:
        counts = {}
        for candidate in candidates:
            source = candidate.get('optimizer_job')
            counts[source] = counts.get(source, 0) + 1
        sources = sorted(counts, key=lambda value: '' if value is None else value)
        matches = len(sources)
        positions = iter(range(offset, matches))
        make_entry = lambda index: {'optimizer_job': sources[index], 'candidate_count': counts[sources[index]]}
    elif parts == ['', 'selection', 'metrics']:
        # Metric names describe the schema, not a candidate prefix. Only a bounded
        # portion of this catalogue is sent to the model at a time.
        names = sorted({key for candidate in candidates for field in ('metrics', 'metric_statistics')
                        for key in (candidate.get(field) or {})})
        matches = len(names)
        positions = iter(range(offset, matches))
        make_entry = lambda index: names[index]
    elif parts[:3] == ['', 'selection', 'id'] and len(parts) == 4:
        positions = (i for i, candidate in enumerate(candidates) if candidate.get('id') == parts[3])
        found = next(positions, None)
        matches = int(found is not None)
        positions = iter([found] if found is not None and offset == 0 else [])
        make_entry = lambda index: candidate_summary(candidates[index], index, relevant)
    elif parts == ['', 'selection', 'rows']:
        matches = len(candidates)
        positions = iter(range(offset, matches))
        make_entry = lambda index: candidate_summary(candidates[index], index, relevant)
    elif parts[:3] == ['', 'selection', 'rank'] and len(parts) in {6, 7, 8, 9}:
        metric, reducer, direction = parts[3:6]
        if reducer not in {'value', 'mean', 'min', 'max', 'median', 'std'} or direction not in {'min', 'max'}:
            raise ValueError('Unsupported candidate ranking')
        suffix = parts[6:]
        screened = bool(suffix and suffix[0] == 'goals_met')
        if screened:
            suffix = suffix[1:]
        source = suffix[1] if len(suffix) == 2 and suffix[0] == 'source' else None
        if suffix and source is None:
            raise ValueError('Unsupported candidate filter')
        owned = index is None
        if owned:
            index = SelectionIndex(candidates, content.get('rubric') or [])
        try:
            if index.candidates is not candidates:
                raise ValueError('Selection index belongs to another dataset')
            positions, matches = index.ranked(metric, reducer, direction, screened, offset, source)
        finally:
            if owned:
                index.close()
        positions = iter(positions)
        make_entry = lambda index: candidate_summary(candidates[index], index, relevant | {metric})
    else:
        raise ValueError('Unsupported candidate query path')
    result['matched_count'] = matches
    if offset > matches:
        raise ValueError('Candidate query offset exceeds dataset')
    for index in positions:
        entry = make_entry(index)
        trial = {**result, 'entries': result['entries'] + [entry], 'next_offset': offset + len(result['entries']) + 1}
        if tokens(trial) > page_tokens:
            if result['entries']:
                break
            # A single unusual record may itself require pages. Do not silently
            # drop its measurements: point to the exact scalar-fragment reader.
            if not isinstance(entry, dict):
                raise ValueError('Candidate schema entry exceeds page budget')
            entry = {key: entry[key] for key in ('id', 'details_path') if key in entry}
            entry['requires_detail_read'] = True
        result['entries'].append(entry)
        if len(result['entries']) >= 32:
            break
    end = offset + len(result['entries'])
    result['next_offset'] = end if end < matches else None
    if tokens(result) > page_tokens:
        raise ValueError('Candidate query envelope exceeds page budget')
    return result
