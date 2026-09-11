"""Static lifecycle regressions for the Coin Data frontend."""

from pathlib import Path


HTML = Path("frontend/coin_data.html").read_text(encoding="utf-8")


def _function(name: str, next_name: str) -> str:
    """Return one function body bounded by the following declaration."""

    return HTML.split(f"function {name}(", 1)[1].split(f"function {next_name}(", 1)[0]


def test_refresh_overlay_can_be_dismissed_without_stopping_background_polling() -> None:
    """Dismiss and Escape should restore access while the refresh remains observed."""

    assert 'id="btn-busy-dismiss">Dismiss to background</button>' in HTML
    dismiss = _function("dismissBusy", "setActionStatus")
    assert "classList.remove('visible')" in dismiss
    assert "stopBusyPolling" not in dismiss
    assert "addEventListener('click', dismissBusy)" in HTML
    assert "event.key !== 'Escape'" in HTML


def test_sort_and_row_selection_preserve_filter_dom() -> None:
    """Table-only interactions must not reconstruct sidebar filter controls."""

    sort = _function("handleSortClick", "closeSelectedDetails")
    selection = _function("bindRowSelection", "renderPage")
    assert "renderTableRows(tableName);" in sort
    assert "renderPage();" not in sort
    assert "renderTableRows(uiState.selectedTable);" in selection
    assert "renderPage();" not in selection


def test_table_interactions_keep_open_tag_filter_focused() -> None:
    """Sorting or selecting rows should not blur and close the tag dropdown."""

    controls = _function("bindControls", "loadUiState")
    assert "document.activeElement !== input" in controls
    assert "dropdown.classList.contains('open')" in controls
    assert "event.target.closest('th.sortable, tr.data-row')" in controls
    assert "event.preventDefault();" in controls


def test_reset_filters_updates_numeric_inputs_before_reloading_state() -> None:
    """Reset should display defaults immediately instead of waiting for the API."""

    reset = _function("resetFilters", "bindControls")
    assert "marketCapInput.value = '';" in reset
    assert "volMcapInput.value = '10';" in reset
    assert reset.index("marketCapInput.value = '';") < reset.index("loadState();")
    assert reset.index("volMcapInput.value = '10';") < reset.index("loadState();")
