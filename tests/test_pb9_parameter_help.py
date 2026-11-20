"""Offline native documentation and inheritance-help contracts."""

from pathlib import Path

import pytest

from pb9_parameter_help import populate_pb9_field_help


@pytest.fixture
def docs(tmp_path):
    """Provide distinct native meanings, never read a real bot installation."""
    directory = tmp_path / 'docs'
    directory.mkdir()
    (directory / 'configuration.md').write_text(
        '## Shared Bot Policy\n\n'
        '- **bot.portfolio.hedge_mode**: Original portfolio hedge policy.\n'
        '- **bot.<side>.enabled**: Original side activation.\n'
        '## Bot Settings\n\n'
        'Local HSL settings inherit from `bot.global.hsl`, with optional side pins.\n\n'
        '## Live Trading Settings\n\n'
        '- **total_exposure_enforcer.enabled**: Original exposure repair toggle.\n'
        '- **total_exposure_enforcer.policy**: Original exposure repair policy.\n'
        '## Monitor\n\n- **enabled**: Unrelated monitoring toggle.\n', encoding='utf-8')
    return tmp_path


def test_native_policy_labels_have_original_text_and_precise_scope(docs):
    """Duplicate leaf names cannot borrow monitoring or another scope's meaning."""
    fields = [{'path': path.split('.'), 'description': ''} for path in (
        'bot.portfolio.hedge_mode', 'bot.long.enabled', 'bot.global.hsl.enabled',
        'bot.short.risk.total_exposure_enforcer.enabled', 'bot.global.risk.total_exposure_enforcer.policy',
        'bot.global.unknown.enabled')]
    populate_pb9_field_help(docs, fields)
    assert 'Original portfolio hedge policy' in fields[0]['description']
    assert 'Original side activation' in fields[1]['description']
    assert 'inherits bot.global.enabled' not in fields[1]['description']
    assert 'Local HSL settings inherit' in fields[2]['description']
    assert 'Shared default for Long and Short' in fields[2]['description']
    assert 'Original exposure repair toggle' in fields[3]['description']
    assert 'Omission inherits bot.global.risk.total_exposure_enforcer.enabled' in fields[3]['description']
    assert 'docs/configuration.md' in fields[4]['description']
    assert 'No parameter description is supplied' in fields[5]['description']
    assert all('Unrelated monitoring' not in field['description'] for field in fields)


def test_missing_documentation_has_an_explicit_context_fallback():
    """Absent docs are identified honestly while the field remains understandable."""
    fields = [{'path': ['bot', 'portfolio', 'hedge_mode']}]
    populate_pb9_field_help(None, fields, catalog={'entries': {}})
    assert 'No parameter description' in fields[0]['description']
    assert 'Config: bot.portfolio.hedge_mode' in fields[0]['description']
    assert 'Portfolio policy' in fields[0]['description']


def test_native_docs_follow_the_shared_cache_signature(docs):
    """Updating the selected native documentation invalidates its original-text cache."""
    field = {'path': ['bot', 'portfolio', 'hedge_mode']}
    populate_pb9_field_help(docs, [field])
    path = docs / 'docs/configuration.md'
    path.write_text(path.read_text().replace('Original portfolio hedge policy', 'Changed native hedge behavior'))
    second = {'path': field['path']}
    populate_pb9_field_help(docs, [second])
    assert 'Changed native hedge behavior' in second['description']


def test_native_docs_reject_an_external_symlink(docs, tmp_path):
    """The common reader keeps native documentation inside its own installation."""
    external = tmp_path.parent / (tmp_path.name + '-external.md')
    external.write_text('outside')
    (docs / 'docs/configuration.md').unlink()
    (docs / 'docs/configuration.md').symlink_to(external)
    with pytest.raises(ValueError, match='leaves the configured installation'):
        populate_pb9_field_help(docs, [{'path': ['bot', 'portfolio', 'hedge_mode']}])
