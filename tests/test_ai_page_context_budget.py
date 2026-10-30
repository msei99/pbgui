"""Page-context limits apply to UTF-8 JSON bytes across browsers and providers."""
import json

import pytest

from ai_chat import AIChatError, AIChatService


@pytest.mark.parametrize('control_count,accepted', [(150, True), (300, False)])
def test_page_context_counts_wire_bytes_without_ascii_expansion(control_count, accepted):
    """Unicode controls below the wire limit stay usable; genuinely oversized context is rejected."""
    label = '設定' * 40
    context = {'schema_version': 1, 'page_key': 'v8_optimize', 'section': 'configs', 'controls': [
        {'id': f'control_{index}', 'role': 'button', 'name': label, 'label': label, 'context': label,
         'visible_text': label, 'tooltip': label, 'operations': ['activate']} for index in range(control_count)]}
    wire_size = len(json.dumps(context, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
    assert len(json.dumps(context).encode('utf-8')) > 300 * 1024
    if accepted:
        assert wire_size < 256 * 1024
        projected = AIChatService._validate_page_context(context)
        assert len(projected['controls']) == control_count
        assert projected['controls'][0]['label'] == label
    else:
        assert wire_size > 300 * 1024
        with pytest.raises(AIChatError, match='Page context is too large'):
            AIChatService._validate_page_context(context)
