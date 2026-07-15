"""Regression tests for browser-local V7 Backtest table sorting."""

from __future__ import annotations

import json
import re
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "v7_backtest.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from the inline page script."""
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


def _extract_var_assignment(source: str, name: str) -> str:
    """Extract one complete JavaScript var assignment."""
    marker = f"var {name} ="
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript variable {name!r}"
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(start + len(marker), len(source)):
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
        elif char in "{[(":
            depth += 1
        elif char in "}])":
            depth -= 1
        elif char == ";" and depth == 0:
            return source[start : index + 1]
    raise AssertionError(f"Could not extract complete JavaScript variable {name!r}")


def test_backtest_sort_state_validates_restores_and_persists() -> None:
    """All four tables retain valid sort state and reject stale columns."""
    source = HTML_PATH.read_text(encoding="utf-8")
    names = [
        "archiveModeFromValue",
        "parseBacktestViewHash",
        "normalizeBacktestSortState",
        "applyBacktestSortState",
        "currentBacktestSortState",
        "loadStoredBacktestViewState",
        "currentBacktestViewHash",
        "persistBacktestViewState",
        "setSort",
        "setResSort",
        "setArchResSort",
        "setLegacyResSort",
    ]
    functions = "\n\n".join(_extract_function(source, name) for name in names)
    constants = "\n".join(
        _extract_var_assignment(source, name)
        for name in ("BACKTEST_SORT_DEFAULTS", "BACKTEST_SORT_COLUMNS")
    )
    config_header_keys = re.findall(r"thSort\('[^']+',\s*'([^']+)'\)", source)
    result_header_keys = re.findall(r"rthFn\('[^']+',\s*'([^']+)'\)", source)
    assert config_header_keys
    assert result_header_keys
    assert source.index("applyBacktestSortState(initialViewState.sorts);") < source.index("loadConfigs();", source.index("var initialViewState"))
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const BACKTEST_VIEW_STATE_KEY = 'pbgui:v7_backtest:view_state';
        {constants}
        let sortCol = 'modified', sortAsc = false;
        let _resSort = 'modified', _resSortAsc = false;
        let _archResSort = 'adg', _archResSortAsc = false;
        let _legacyResSort = 'adg', _legacyResSortAsc = false;
        let currentPanel = 'configs', selectedArchiveName = '', archiveResultsMode = 'backtests';
        let storedValue = JSON.stringify({{
          panel: 'configs',
          sorts: {{
            configs: {{ col: 'name', asc: true }},
            results: {{ col: 'not-a-column', asc: true }},
            archive: {{ col: 'gain', asc: false }},
            legacy: {{ col: 'final_balance', asc: true }}
          }}
        }});
        global.localStorage = {{
          getItem: () => storedValue,
          setItem: (_key, value) => {{ storedValue = value; }}
        }};
        global.window = {{ location: {{ hash: '#legacy', pathname: '/backtest', search: '?x=1' }} }};
        global.history = {{ replaceState: () => {{}} }};
        global.renderConfigs = () => {{}};
        global.renderResults = () => {{}};
        global.renderArchiveResults = () => {{}};
        global.renderLegacyResults = () => {{}};

        {functions}

        const loaded = loadStoredBacktestViewState();
        assert.equal(loaded.panel, 'legacy');
        for (const key of {json.dumps(config_header_keys)}) assert.ok(BACKTEST_SORT_COLUMNS.configs.includes(key));
        for (const key of {json.dumps(result_header_keys)}) {{
          assert.ok(BACKTEST_SORT_COLUMNS.results.includes(key));
          assert.ok(BACKTEST_SORT_COLUMNS.archive.includes(key));
          assert.ok(BACKTEST_SORT_COLUMNS.legacy.includes(key));
        }}
        applyBacktestSortState(loaded.sorts);
        assert.deepEqual(currentBacktestSortState(), {{
          configs: {{ col: 'name', asc: true }},
          results: {{ col: 'modified', asc: false }},
          archive: {{ col: 'gain', asc: false }},
          legacy: {{ col: 'final_balance', asc: true }}
        }});

        currentPanel = 'results';
        _resSort = 'gain';
        _resSortAsc = true;
        persistBacktestViewState();
        const persisted = JSON.parse(storedValue);
        assert.deepEqual(persisted.sorts.results, {{ col: 'gain', asc: true }});
        assert.deepEqual(normalizeBacktestSortState({{ configs: {{ col: 'removed', asc: true }} }}).configs,
          {{ col: 'modified', asc: false }});

        setSort('name');
        assert.deepEqual(JSON.parse(storedValue).sorts.configs, {{ col: 'name', asc: false }});
        setResSort('modified');
        assert.deepEqual(JSON.parse(storedValue).sorts.results, {{ col: 'modified', asc: false }});
        selectedArchiveName = 'archive-a';
        setArchResSort('modified');
        assert.deepEqual(JSON.parse(storedValue).sorts.archive, {{ col: 'modified', asc: false }});
        setLegacyResSort('config_name');
        assert.deepEqual(JSON.parse(storedValue).sorts.legacy, {{ col: 'config_name', asc: false }});

        window.location.hash = '';
        storedValue = JSON.stringify({{ panel: 'results' }});
        const oldState = loadStoredBacktestViewState();
        assert.equal(oldState.panel, 'results');
        assert.deepEqual(oldState.sorts, BACKTEST_SORT_DEFAULTS);
        """
    )
    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout
