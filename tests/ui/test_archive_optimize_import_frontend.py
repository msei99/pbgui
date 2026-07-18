"""Frontend contracts for archive ownership, rebacktest, and Optimize imports."""

from __future__ import annotations

import re
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BACKTEST_PATH = ROOT / "frontend" / "v7_backtest.html"
OPTIMIZE_PATH = ROOT / "frontend" / "v7_optimize.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from an inline page script."""
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


def _assert_no_native_dialog(function_source: str) -> None:
    """Assert a changed flow does not invoke a native confirm or alert."""
    assert not re.search(r"(?<![\w.])(confirm|alert)\s*\(", function_source)


def test_archive_rebacktest_uses_archived_defaults_and_explicit_boolean() -> None:
    """Archive rebacktest derives defaults from its config and always submits the checkbox value."""
    source = BACKTEST_PATH.read_text(encoding="utf-8")
    normalize = _extract_function(source, "normalizeArchiveMarketDataPath")
    comparator = _extract_function(source, "archiveConfigUsesPbguiMarketData")
    rebacktest = _extract_function(source, "rebacktestSelectedArchive")

    assert "apiFetch('/pbgui_data_path')" in rebacktest
    assert "cfg.backtest && cfg.backtest.start_date" in rebacktest
    assert "cfg.backtest && cfg.backtest.end_date" in rebacktest
    assert "cfg.backtest && cfg.backtest.starting_balance" in rebacktest
    assert "cfg.backtest && cfg.backtest.exchanges" in rebacktest
    assert "archiveConfigUsesPbguiMarketData(cfg, pbguiPath)" in rebacktest
    assert "pbguiMarketDataDefaultCheckedAttr" not in rebacktest
    assert "use_pbgui_market_data: usePbguiData" in rebacktest

    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {normalize}
        {comparator}
        assert.equal(archiveConfigUsesPbguiMarketData(
          {{ backtest: {{ ohlcv_source_dir: '/srv/pbgui-data/' }} }}, '/srv/pbgui-data'
        ), true);
        assert.equal(archiveConfigUsesPbguiMarketData(
          {{ backtest: {{ ohlcv_source_dir: '/srv/pb7-cache' }} }}, '/srv/pbgui-data'
        ), false);
        assert.equal(archiveConfigUsesPbguiMarketData(
          {{ backtest: {{ ohlcv_source_dir: '' }} }}, '/srv/pbgui-data'
        ), false);
        """
    )
    completed = subprocess.run(
        ["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_foreign_archives_hide_and_guard_content_mutations() -> None:
    """Foreign archives retain read actions while result and schedule mutations stay unavailable."""
    source = BACKTEST_PATH.read_text(encoding="utf-8")
    visibility = _extract_function(source, "updateArchiveActionVisibility")
    set_mode = _extract_function(source, "setArchiveResultsMode")
    mode_buttons = _extract_function(source, "setArchiveResultsModeButtons")
    schedule_render = _extract_function(source, "renderArchiveRetestSchedules")

    assert "archive-btn-delete" in visibility
    assert "isBacktests && own" in visibility
    for read_button in (
        "archive-btn-rebacktest",
        "archive-btn-add-run",
        "archive-btn-compare",
        "archive-btn-balance-calc",
        "archive-btn-score-preview",
        "archive-btn-opt-view",
        "archive-btn-opt-open",
        "archive-btn-opt-import",
    ):
        assert read_button in visibility
    assert "mode === 'schedules' && !selectedArchiveIsOwn()" in set_mode
    assert "schedBtn.style.display = own ? '' : 'none'" in mode_buttons
    assert "own ? '\\x3Cth>Actions" in schedule_render
    assert "own ? '\\x3Ctd class=\"actions-cell\">'" in schedule_render

    for name in (
        "deleteSelectedArchiveResults",
        "runArchiveRetestSchedule",
        "toggleArchiveRetestSchedule",
        "deleteArchiveRetestSchedule",
    ):
        function_source = _extract_function(source, name)
        assert "if (!selectedArchiveIsOwn())" in function_source
        _assert_no_native_dialog(function_source)


def test_backtest_archive_optimize_import_uses_collision_modes_and_three_choices() -> None:
    """Backtest archive import retains structured conflicts and offers only the required choices."""
    source = BACKTEST_PATH.read_text(encoding="utf-8")
    request = _extract_function(source, "requestArchiveOptimizeImport")
    choose = _extract_function(source, "chooseArchiveOptimizeCollision")
    retry = _extract_function(source, "importArchiveOptimizeWithCollision")
    open_flow = _extract_function(source, "optimizeFromArchiveOptimizeConfig")
    import_flow = _extract_function(source, "importArchiveOptimizeConfig")

    assert "/optimize-configs/import" in request
    assert "method: 'POST'" in request
    assert "collision: collision" in request
    assert "overwrite:" not in request
    assert "err.detail = detail" in request
    assert "detail.code !== 'optimize_config_exists'" in choose
    assert re.findall(r"label: '([^']+)'", choose) == [
        "Overwrite",
        "Import as Copy",
        "Cancel",
    ]
    assert "value: 'overwrite'" in choose
    assert "value: 'copy'" in choose
    assert "value: null" in choose
    assert "requestArchiveOptimizeImport(path, importName, 'error')" in retry
    assert "requestArchiveOptimizeImport(path, importName, collision)" in retry
    assert "openOptimizeEditorForConfig(data.name || importName)" in open_flow
    assert "data.name || importName" in import_flow
    for function_source in (choose, retry, open_flow, import_flow):
        _assert_no_native_dialog(function_source)


def test_optimize_archive_import_persists_then_opens_server_name() -> None:
    """Optimize archive import uses the collision endpoint while paste and file imports remain drafts."""
    source = OPTIMIZE_PATH.read_text(encoding="utf-8")
    request = _extract_function(source, "requestArchiveOptimizeConfigImport")
    choose = _extract_function(source, "chooseOptimizeArchiveImportCollision")
    archive_flow = _extract_function(source, "importArchiveOptimizeConfigDraft")
    dispatch = _extract_function(source, "importOptimizeConfigDraft")

    assert "/optimize-configs/import" in request
    assert "method: 'POST'" in request
    assert "collision: collision" in request
    assert re.findall(r"label: '([^']+)'", choose) == [
        "Overwrite",
        "Import as Copy",
        "Cancel",
    ]
    assert "requestArchiveOptimizeConfigImport(archiveName, item.path, suggestedName, 'error')" in archive_flow
    assert "requestArchiveOptimizeConfigImport(archiveName, item.path, suggestedName, collision)" in archive_flow
    assert "/optimize-configs/config?" not in archive_flow
    assert "/configs/prepare" not in archive_flow
    assert "await openConfigEditor(localName)" in archive_flow
    assert "closeAllModals();" in archive_flow
    assert "'/configs/prepare'" in dispatch
    for function_source in (choose, archive_flow):
        _assert_no_native_dialog(function_source)


def test_dialog_cache_version_is_bumped_for_all_consumers() -> None:
    """Every frontend HTML consumer references the new shared dialog asset version."""
    html_sources = {
        path.name: path.read_text(encoding="utf-8")
        for path in (ROOT / "frontend").glob("*.html")
    }
    assert not any("pbgui_dialogs.js?v=4" in source for source in html_sources.values())
    expected_consumers = {
        "api_keys_editor.html",
        "cluster.html",
        "dashboard_main.html",
        "dashboard_sidebar.html",
        "dashboard_templates.html",
        "logging_monitor.html",
        "market_data_main.html",
        "market_data_status.html",
        "services_monitor.html",
        "v7_backtest.html",
        "v7_edit.html",
        "v7_optimize.html",
        "v7_run.html",
        "welcome.html",
    }
    for filename in expected_consumers:
        assert "pbgui_dialogs.js?v=5" in html_sources[filename]
