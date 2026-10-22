"""Offline parity, freshness and work-count coverage for rotation lookups."""
import configparser
from contextlib import contextmanager
import random
from pathlib import Path

import pytest

import logging_helpers as h
import api.logging as logging_api
from ini_settings import apply_metadata


@pytest.fixture
def paths(tmp_path, monkeypatch):
    """Keep every config, advisory lock and transcript inside temporary storage."""
    ini, logs = tmp_path / 'pbgui.ini', tmp_path / 'logs'
    logs.mkdir()
    monkeypatch.setattr(h, 'PBGUI_INI', ini)
    monkeypatch.setattr(h, 'LOG_ROOT', logs)
    monkeypatch.setattr(h, 'PBGDIR', tmp_path)
    return ini, logs


def legacy(cfg, scope=None, key=None):
    """Freeze the old stable-file lookup order; parser/bounds policy is shared."""
    default_max = h._parse_positive_int(cfg.get('logging', 'rotate_default_max_bytes', fallback=str(h.DEFAULT_ROTATE_MAX_BYTES)), h.DEFAULT_ROTATE_MAX_BYTES)
    default_count = h._parse_nonnegative_int(cfg.get('logging', 'rotate_default_backup_count', fallback=str(h.DEFAULT_ROTATE_BACKUP_COUNT)), h.DEFAULT_ROTATE_BACKUP_COUNT)
    defaults = h._bounded_rotate_settings(default_max, default_count)
    scoped = defaults
    if scope:
        normalized = h._normalize_rotate_key(scope)
        scoped = h._bounded_rotate_settings(
            h._parse_positive_int(cfg.get('logging', f'managed_{normalized}_max_bytes', fallback=str(defaults[0])), defaults[0]),
            h._parse_nonnegative_int(cfg.get('logging', f'managed_{normalized}_backup_count', fallback=str(defaults[1])), defaults[1]),
        )
    if not key:
        return scoped
    normalized = h._normalize_rotate_key(key)
    return h._bounded_rotate_settings(
        h._parse_positive_int(cfg.get('logging', f'rotate_{normalized}_max_bytes', fallback=str(scoped[0])), scoped[0]),
        h._parse_nonnegative_int(cfg.get('logging', f'rotate_{normalized}_backup_count', fallback=str(scoped[1])), scoped[1]),
    )


def test_randomized_rotation_precedence_matches_old_stable_file(paths):
    """1,000 lookups preserve clamps, invalid values and filename-over-service precedence."""
    ini, logs = paths
    rng = random.Random(186)
    options = ['rotate_default_max_bytes', 'rotate_default_backup_count',
               'managed_jobs_max_bytes', 'managed_jobs_backup_count',
               'rotate_pbrun_max_bytes', 'rotate_pbrun_backup_count',
               'rotate_run_max_bytes', 'rotate_run_backup_count']
    for _ in range(200):
        cfg = configparser.ConfigParser()
        cfg['logging'] = {key: str(rng.choice([-1, 0, 1, 900, 900000000000, 'bad']))
                          for key in options if rng.randrange(3)}
        with ini.open('w') as handle:
            cfg.write(handle)
        assert h.get_rotate_defaults() == legacy(cfg)
        assert h.get_managed_scope_settings('jobs') == legacy(cfg, 'jobs')
        assert h.get_rotate_settings(service='PBRun') == legacy(cfg, key='PBRun')
        # With a logfile, its physical stem overrides the service name.
        assert h.get_rotate_settings(service='PBRun', logfile=logs / 'jobs/run.log') == legacy(cfg, 'jobs', 'run')
        assert h.get_rotate_settings() == legacy(cfg)


@pytest.mark.parametrize('kind', ['defaults', 'managed', 'service', 'file', 'append', 'rotate'])
def test_one_ini_read_per_operation(paths, monkeypatch, kind):
    """Each public operation obtains one current parser, including transcript writes."""
    ini, logs = paths
    ini.write_text('[logging]\nrotate_default_max_bytes=1000\n')
    original = h._read_rotate_ini
    reads = []

    def read():
        """Count actual locked snapshot acquisition without bypassing the file read."""
        reads.append(1)
        return original()

    monkeypatch.setattr(h, '_read_rotate_ini', read)
    if kind == 'defaults':
        h.get_rotate_defaults()
    elif kind == 'managed':
        h.get_managed_scope_settings('jobs')
    elif kind == 'service':
        h.get_rotate_settings(service='PBRun')
    elif kind == 'file':
        h.get_rotate_settings(service='PBRun', logfile=logs / 'jobs/run.log')
    elif kind == 'append':
        h.append_managed_transcript_line(logs / 'monitor-agent/live_metrics.ndjson', 'safe', 'monitor_agent_live')
    else:
        h.rotate_managed_log_before_open(logs / 'jobs/run.log', 'jobs')
    assert len(reads) == 1


def test_single_cohort_does_not_mix_config_generations(paths, monkeypatch):
    """A subsequent lookup sees replacement settings without a cross-call cache."""
    _, logs = paths
    parsers = []
    for value in (1234, 5678):
        cfg = configparser.ConfigParser()
        cfg['logging'] = {'rotate_default_max_bytes': str(value), 'managed_jobs_backup_count': '3'}
        parsers.append(cfg)
    reads = []

    def read():
        """Simulate atomically replacing INI between snapshot acquisitions."""
        result = parsers[len(reads)]
        reads.append(1)
        return result

    monkeypatch.setattr(h, '_read_rotate_ini', read)
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (1234, 3)
    assert len(reads) == 1
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (5678, 3)
    assert len(reads) == 2


def test_real_settings_replacement_is_visible_next_read(paths):
    """Actual persistence remains immediately visible to defaults and managed scopes."""
    _, logs = paths
    h.set_rotate_defaults(1024, 2)
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (1024, 2)
    h.set_managed_scope_settings('jobs', 2048, 4)
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (2048, 4)
    h.set_rotate_settings('run', 4096, 0)
    assert h.get_rotate_settings(service='Ignored', logfile=logs / 'jobs/run.log') == (4096, 0)


def test_failed_optional_read_and_invalid_scope_preserve_policy(paths, monkeypatch):
    """Missing/unreadable optional config falls back; invalid scope still raises first."""
    monkeypatch.setattr(h, '_read_rotate_ini', lambda: None)
    defaults = (h.DEFAULT_ROTATE_MAX_BYTES, h.DEFAULT_ROTATE_BACKUP_COUNT)
    assert h.get_rotate_defaults() == defaults
    assert h.get_managed_scope_settings('jobs') == defaults
    assert h.get_rotate_settings(service='PBRun') == defaults
    with pytest.raises(ValueError, match='Unknown managed log scope'):
        h.get_managed_scope_settings('../bad')


def test_damaged_interpolation_preserves_old_optional_fallbacks(paths):
    """Invalid defaults and overrides preserve their distinct previous fallbacks."""
    ini, logs = paths
    ini.write_text('[logging]\nrotate_default_max_bytes=%(missing)s\nmanaged_jobs_max_bytes=4096\n')
    defaults = (h.DEFAULT_ROTATE_MAX_BYTES, h.DEFAULT_ROTATE_BACKUP_COUNT)
    assert h.get_rotate_defaults() == defaults
    assert h.get_managed_scope_settings('jobs') == (4096, defaults[1])
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (4096, defaults[1])
    ini.write_text('[logging]\nrotate_default_max_bytes=1234\nrotate_default_backup_count=2\nmanaged_jobs_max_bytes=4096\nrotate_run_max_bytes=%(missing)s\n')
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (1234, 2)
    ini.write_text('[logging]\nrotate_default_max_bytes=1234\nmanaged_jobs_max_bytes=%(missing)s\n')
    with pytest.raises(configparser.Error):
        h.get_managed_scope_settings('jobs')
    assert h.get_rotate_settings(logfile=logs / 'jobs/run.log') == (1234, defaults[1])


@pytest.fixture
def rotation_io(paths, monkeypatch):
    """Track real isolated locking/reading and require evaluation after unlock."""
    state = {'locks': 0, 'reads': 0, 'held': False, 'evaluations': 0}
    original_lock = h.advisory_file_lock
    original_read = h.load_ini_snapshot
    original_resolve = h._rotation_settings_from_snapshot

    @contextmanager
    def lock(target):
        """Count one actual INI lock and observe its lifetime, including failures."""
        assert target == paths[0]
        state['locks'] += 1
        with original_lock(target):
            state['held'] = True
            try:
                yield
            finally:
                state['held'] = False

    def read(target):
        """Require that the single snapshot read takes place under the lock."""
        assert target == paths[0]
        assert state['held']
        state['reads'] += 1
        return original_read(target)

    def resolve(cfg, **kwargs):
        """Check that settings evaluation never extends the lock lifetime."""
        assert not state['held']
        state['evaluations'] += 1
        return original_resolve(cfg, **kwargs)

    monkeypatch.setattr(h, 'advisory_file_lock', lock)
    monkeypatch.setattr(h, 'load_ini_snapshot', read)
    monkeypatch.setattr(h, '_rotation_settings_from_snapshot', resolve)
    return state


@pytest.mark.parametrize('file_count', [0, 15, 120])
def test_rotation_endpoint_reads_once_for_all_rows(paths, rotation_io, file_count):
    """The complete authenticated endpoint needs one lock/read at every log count."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.auth import require_auth

    ini, logs = paths
    ini.write_text('[logging]\nrotate_default_max_bytes=2097152\n')
    for index in range(file_count):
        (logs / f'Service{index:03}.log').touch()
    app = FastAPI()
    app.include_router(logging_api.router, prefix='/api/logging')
    app.dependency_overrides[require_auth] = lambda: object()
    with TestClient(app) as client:
        response = client.get('/api/logging/rotation')
    assert response.status_code == 200
    data = response.json()
    assert list(data['per_service']) == [f'Service{i:03}' for i in range(file_count)]
    assert set(data['managed_scopes']) == set(h.MANAGED_LOG_SCOPES)
    assert rotation_io == {
        'locks': 1, 'reads': 1, 'held': False,
        'evaluations': 1 + file_count + len(h.MANAGED_LOG_SCOPES),
    }


def test_rotation_endpoint_snapshot_coherence_and_next_request(paths, monkeypatch):
    """Replacing INI after acquisition affects only the next complete response."""
    ini, logs = paths
    ini.write_text('[logging]\nrotate_default_max_bytes=2097152\nrotate_default_backup_count=2\n')
    (logs / 'PBGui.log').touch()
    original = h._rotation_settings_from_snapshot
    replaced = False

    def replace_after_read(cfg, **kwargs):
        """Publish a new generation while the first response is being evaluated."""
        nonlocal replaced
        if not replaced:
            replacement = ini.with_suffix('.next')
            replacement.write_text('[logging]\nrotate_default_max_bytes=7340032\nrotate_default_backup_count=4\n')
            replacement.replace(ini)
            replaced = True
        return original(cfg, **kwargs)

    monkeypatch.setattr(h, '_rotation_settings_from_snapshot', replace_after_read)
    for size, count in [(2, 2), (7, 4)]:
        result = logging_api.get_rotation(None)
        expected = {'max_mb': size, 'backup_count': count}
        assert result['default'] == expected
        assert result['per_service'] == {'PBGui': expected}
        for scope in result['managed_scopes'].values():
            assert {key: scope[key] for key in expected} == expected


@pytest.mark.parametrize('options', [
    '',
    'rotate_default_max_bytes=3145728\nrotate_default_backup_count=0\n',
    'rotate_default_max_bytes=bad\nrotate_default_backup_count=-1\n',
    'rotate_default_max_bytes=999999999999\nrotate_default_backup_count=999\n',
    'rotate_pbgui_max_bytes=1\nrotate_pbgui_backup_count=0\nmanaged_jobs_max_bytes=5242880\n',
    'rotate_pbgui_max_bytes=bad\nrotate_pbgui_backup_count=-2\n',
    'rotate_default_max_bytes=%(missing)s\nrotate_pbgui_max_bytes=6291456\nmanaged_jobs_backup_count=0\n',
    'rotate_default_backup_count=%(missing)s\nrotate_pbgui_max_bytes=6291456\nmanaged_jobs_backup_count=0\n',
])
def test_rotation_endpoint_matches_single_getters_and_filters(paths, monkeypatch, options):
    """Keep response shape, physical keys, filters, units, clamps and zero copies."""
    ini, logs = paths
    ini.write_text('[logging]\n' + options)
    for stem in ['PBGui', 'PBRun', *logging_api._RETIRED_LOG_STEMS,
                 'PBApiServer.console', 'api-systemd-handoff', 'pareto_dash']:
        (logs / f'{stem}.log').touch()
    monkeypatch.setitem(h.LOG_GROUPS, 'PBGui', 'PBRun')
    defaults = h.get_rotate_defaults()
    files = {stem: h.get_rotate_settings(logfile=logs / f'{stem}.log') for stem in ['PBGui', 'PBRun']}
    scopes = {scope: h.get_managed_scope_settings(scope) for scope in h.MANAGED_LOG_SCOPES}
    original_scope = h.resolve_managed_log_scope
    resolved = []

    def scope_once(path):
        """Record filtering to catch repeated scope resolution in the bulk loop."""
        resolved.append(path)
        return original_scope(path)

    monkeypatch.setattr(h, 'resolve_managed_log_scope', scope_once)
    result = logging_api.get_rotation(None)
    assert len(resolved) == len(list(logs.glob('*.log')))
    assert len(set(resolved)) == len(resolved)
    assert result == {
        'default': {'max_mb': max(1, defaults[0] // (1024 * 1024)), 'backup_count': defaults[1]},
        'per_service': {stem: {'max_mb': max(1, size // (1024 * 1024)), 'backup_count': count}
                        for stem, (size, count) in files.items()},
        'managed_scopes': {
            scope: {'label': h.MANAGED_LOG_SCOPES[scope]['label'],
                    'description': h.MANAGED_LOG_SCOPES[scope]['description'],
                    'max_mb': max(1, size // (1024 * 1024)), 'backup_count': count}
            for scope, (size, count) in scopes.items()
        },
        'apply': apply_metadata('logging_rotation'),
    }
    if '%(missing)s' in options:
        assert result['default'] == {'max_mb': 10, 'backup_count': 1}
        assert result['per_service']['PBGui'] == {'max_mb': 6, 'backup_count': 1}
        assert result['managed_scopes']['jobs']['backup_count'] == 0


@pytest.mark.parametrize('failure', ['missing', 'unreadable', 'malformed'])
def test_rotation_endpoint_optional_ini_failure(paths, rotation_io, monkeypatch, failure):
    """Optional read failures return built-in defaults after releasing the lock."""
    ini, logs = paths
    (logs / 'PBGui.log').touch()
    if failure == 'unreadable':
        ini.write_text('[logging]\n')
        original_read = Path.read_text

        def denied(path, *args, **kwargs):
            """Fail only the isolated INI read without depending on OS user rights."""
            if path == ini:
                raise PermissionError('isolated unreadable INI')
            return original_read(path, *args, **kwargs)

        monkeypatch.setattr(Path, 'read_text', denied)
    elif failure == 'malformed':
        ini.write_text('not an INI section\n')
    result = logging_api.get_rotation(None)
    expected = {'max_mb': h.DEFAULT_ROTATE_MAX_BYTES // (1024 * 1024),
                'backup_count': h.DEFAULT_ROTATE_BACKUP_COUNT}
    assert result['default'] == expected
    assert result['per_service']['PBGui'] == expected
    for scope in result['managed_scopes'].values():
        assert {key: scope[key] for key in expected} == expected
    assert rotation_io['locks'] == rotation_io['reads'] == 1
    assert not rotation_io['held']


@pytest.mark.parametrize('option', ['max_bytes', 'backup_count'])
def test_rotation_endpoint_bad_file_override_falls_back_per_row(paths, rotation_io, option):
    """Interpolation errors affect only that file, preserving valid sibling rows."""
    ini, logs = paths
    ini.write_text('[logging]\nrotate_default_max_bytes=3145728\nrotate_default_backup_count=2\n'
                   f'rotate_bad_{option}=%(missing)s\n'
                   'rotate_good_max_bytes=5242880\nrotate_good_backup_count=0\n')
    for stem in ['Bad', 'Good']:
        (logs / f'{stem}.log').touch()
    result = logging_api.get_rotation(None)
    assert result['per_service']['Bad'] == result['default'] == {'max_mb': 3, 'backup_count': 2}
    assert result['per_service']['Good'] == {'max_mb': 5, 'backup_count': 0}
    assert not rotation_io['held']


@pytest.mark.parametrize('option', ['max_bytes', 'backup_count'])
def test_rotation_endpoint_managed_interpolation_errors_propagate(paths, rotation_io, option):
    """Managed failures still fail the request, with the INI lock already released."""
    ini, _ = paths
    ini.write_text(f'[logging]\nmanaged_jobs_{option}=%(missing)s\n')
    with pytest.raises(configparser.InterpolationError):
        logging_api.get_rotation(None)
    assert rotation_io['locks'] == rotation_io['reads'] == 1
    assert not rotation_io['held']


def test_rotation_endpoint_lock_failure_propagates(paths, monkeypatch):
    """A lock acquisition failure cannot become a silent successful defaults page."""
    @contextmanager
    def failed_lock(target):
        """Reject the isolated INI lock before reading any settings."""
        assert target == paths[0]
        raise TimeoutError('isolated lock failure')
        yield

    def unexpected_read(*args, **kwargs):
        """No read is permitted after a failed lock acquisition."""
        pytest.fail('INI read after failed lock acquisition')

    monkeypatch.setattr(h, 'advisory_file_lock', failed_lock)
    monkeypatch.setattr(h, 'load_ini_snapshot', unexpected_read)
    with pytest.raises(TimeoutError, match='isolated lock failure'):
        logging_api.get_rotation(None)
