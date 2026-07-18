"""Contract tests for the shared PBGui choice dialog."""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DIALOG_PATH = ROOT / "frontend" / "js" / "pbgui_dialogs.js"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from the shared dialog module."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"
    brace_start = source.find("{", start)
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def test_choose_returns_only_explicit_action_values_and_defaults_to_null() -> None:
    """Choice resolution returns an explicit value while close and Escape resolve to null."""
    source = DIALOG_PATH.read_text(encoding="utf-8")
    close_function = _extract_function(source, "close")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        var OVERLAY_ID = 'pbgui-dialog-ovl';
        var currentMode = 'choose';
        var resolved = 'unset';
        var resolveDialog = function(value) {{ resolved = value; }};
        var returnFocus = null;
        var overlay = {{
          classList: {{ remove: function() {{}} }},
          setAttribute: function() {{}}
        }};
        var input = {{ value: 'ignored' }};
        var document = {{
          getElementById: function(id) {{
            if (id === 'pbgui-dialog-ovl') return overlay;
            if (id === 'pbgui-dialog-input') return input;
            return null;
          }}
        }};
        {close_function}

        close({{ __pbguiDialogChoice: true, value: 'copy' }});
        assert.equal(resolved, 'copy');

        resolved = 'unset';
        resolveDialog = function(value) {{ resolved = value; }};
        close(false);
        assert.equal(resolved, null);
        """
    )
    completed = subprocess.run(
        ["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "if (event.key === 'Escape')" in source
    assert "close(false);" in source


def test_choose_renders_text_safely_and_requires_explicit_closure() -> None:
    """Choice content uses textContent and the backdrop has no click-to-close handler."""
    source = DIALOG_PATH.read_text(encoding="utf-8")
    open_function = _extract_function(source, "open")
    ensure_overlay = _extract_function(source, "ensureOverlay")

    assert "choose: function (options)" in source
    assert "return open('choose', options);" in source
    assert "title.textContent" in open_function
    assert "message.textContent" in open_function
    assert "detail.textContent" in open_function
    assert "button.textContent = String(action.label)" in open_function
    assert "button.addEventListener('click'" in open_function
    assert "close({ __pbguiDialogChoice: true, value: action.value })" in open_function
    assert "overlay.addEventListener('click'" not in ensure_overlay
    assert ".pbgui-dialog-btn.danger" in source
    assert "currentMode !== 'choose'" in ensure_overlay
