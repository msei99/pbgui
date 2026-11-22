"""Bounded native editor tags retain meaningful, unique catalog names."""
import pytest

from pb9_gui_preferences import filter_preferences


@pytest.mark.parametrize('tag', ['', '   ', '\t', '\n', 'a\x00b'])
def test_reject_empty_looking_or_control_tags(tag):
    """Reject invisible filter constraints at the common persistence boundary."""
    with pytest.raises(ValueError):
        filter_preferences({'tags': [tag]})


def test_tags_are_trimmed_and_stably_deduplicated():
    """Keep catalog case and order while avoiding redundant or padded values."""
    source = {'tags': [' defi ', 'defi', 'Layer 1', 'DEFI']}
    assert filter_preferences(source) == {'tags': ['defi', 'Layer 1', 'DEFI']}
    assert source['tags'][0] == ' defi '
