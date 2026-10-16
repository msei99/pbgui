"""Offline parity, freshness and work-count coverage for rotation lookups."""
import configparser
import random
from pathlib import Path

import pytest

import logging_helpers as h


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
