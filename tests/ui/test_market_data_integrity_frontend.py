"""Frontend contracts for OHLCV integrity and checksum sharing."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_hyperliquid_best_1m_iframe_uses_a_valid_query_string() -> None:
    """The focused Hyperliquid actions iframe must route section through a real query parameter."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert "apiUrl('/data-actions/hyperliquid') + '?section='" in page
    assert "apiUrl('/data-actions/hyperliquid') + '&section='" not in page


def test_integrity_panel_exposes_independent_archive_controls() -> None:
    """Market Data offers separate publish and reference archive selections."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert 'id="integrity-publish-enabled"' in page
    assert 'id="integrity-publish-archive"' in page
    assert 'id="integrity-reference-archive"' in page
    assert "publish_archive: String(document.getElementById('integrity-publish-archive').value" in page
    assert "reference_archive: String(document.getElementById('integrity-reference-archive').value" in page


def test_integrity_rows_and_archive_options_use_safe_dom_rendering() -> None:
    """Archive, coin, day, and error values are assigned with textContent."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert "option.textContent = String(archive.name || '')" in page
    assert "td.textContent = String(value == null ? '' : value)" in page
    assert "action.setAttribute('data-coin', String(row.coin || ''))" in page
    assert "ohlcv_integrity_scan,ohlcv_integrity_repair,ohlcv_integrity_repair_all,ohlcv_removed_coin_delete" in page


def test_integrity_panel_groups_and_repairs_by_coin() -> None:
    """Repair rows aggregate reasons and queue one batch for the selected coin."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert "queueIntegrityOperation(" in page
    assert "data-integrity-repair-coin" in page
    assert "group.reasons[reason]" in page
    assert "groupedRows.length + ' coins / ' + rows.length + ' damaged days'" in page
    assert "coin: String(coinButton.getAttribute('data-coin') || '')" in page
    assert "a verified newer inception automatically removes obsolete local pre-inception data" in page
    assert "'Source gaps'" in page


def test_integrity_panel_offers_explicit_gap_details_visualization() -> None:
    """Grouped findings open a minute grid that distinguishes boundary and internal gaps."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert "data-integrity-gap-details" in page
    assert "'/integrity/day-details?exchange='" in page
    assert 'id="integrity-gap-modal"' in page
    assert 'id="btn-integrity-gap-close"' in page
    assert "repeat(60, minmax(4px, 1fr))" in page
    assert "Possible exchange inception; not necessarily damaged." in page
    assert "Real interruption inside available data." in page
    assert 'id="integrity-day-context"' in page
    assert "Seven days before and after the selected day" in page
    assert "data-integrity-context-day" in page
    assert "repeat(24, minmax(5px, 1fr))" in page
    assert "integrity-gap-modal').addEventListener('click'" not in page


def test_integrity_panel_refreshes_automatically_after_jobs_finish() -> None:
    """The panel polls active jobs and has no manual refresh control."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert 'id="btn-integrity-refresh"' not in page
    assert "jobsApiUrl('/jobs/?states=pending,running&limit=100')" in page
    assert "integrityState.hadActiveJob = true" in page
    assert "await loadIntegrityPanel(false)" in page
    assert "window.setTimeout(pollIntegrityJobs, 2000)" in page


def test_integrity_panel_loads_all_issues_and_offers_repair_all() -> None:
    """Damaged-day display is unpaginated and batch repair uses one job."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert 'id="btn-integrity-repair-all"' in page
    assert "'/integrity/repair-all'" in page
    assert "'/integrity/issues?exchange=' + encodeURIComponent(storageExchange) + '&limit=1000000'" in page
    assert "ohlcv_integrity_repair_all" in page


def test_integrity_panel_offers_confirmed_removed_coin_deletion() -> None:
    """Unavailable markets use a previewed confirmation instead of Repair."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert "data-integrity-remove-coin" in page
    assert "'/integrity/removed-coins/preview'" in page
    assert "'/integrity/removed-coins/remove'" in page
    assert "await showConfirmDialog" in page
    assert "PB7 and PB8 runtime caches are not removed." in page
    assert "ohlcv_removed_coin_delete" in page
    assert 'id="integrity-removed-coins"' in page
    assert "'/integrity/removed-coins?exchange='" in page
    assert "<th>From</th><th>To</th>" in page
    assert "row.from_day, row.to_day, row.market_reason" in page
    assert 'id="btn-integrity-remove-selected"' in page
    assert 'id="btn-integrity-remove-all"' in page
    assert "data-integrity-removed-row" in page
    assert "applyRemovedCoinSelectionRange" in page
    assert "event.key !== 'Delete'" in page
    assert "ohlcv_removed_coins_delete" in page
    assert "Unavailable Coin Data" in page
    assert "String(row.market_status || '') !== 'removed'" in page
    assert "groupedRows.length + ' coins / ' + rows.length + ' damaged days'" in page


def test_integrity_panel_follows_selected_exchange_capabilities() -> None:
    """The global exchange selector scopes scans, status, issues, and actions."""
    page = (ROOT / "frontend" / "market_data_main.html").read_text(encoding="utf-8")

    assert "var storageExchange = meta.statusKey" in page
    assert "'/integrity/status?exchange=' + encodeURIComponent(storageExchange)" in page
    assert "queueIntegrityOperation('/integrity/scan', { exchange: exchange }" in page
    assert "var canRepair = true" in page
    assert "document.getElementById('btn-integrity-repair-all').hidden = !canRepair" in page
    assert "fetchJson('/integrity/removed-coins?exchange=' + encodeURIComponent(storageExchange))" in page
    assert "actionCell.textContent = 'Read-only'" in page
    assert "meta.statusKey === 'hyperliquid' ? ' (crypto only)'" in page
    assert "getExchangeMeta(uiState.contextExchange).statusKey !== storageExchange" in page
    assert "integrityState.reloadAfterSave = true" in page
    assert "integrityState.status = null" in page
    assert "integrityState.exchange = meta.statusKey" in page
    assert "requestId !== integrityState.requestId" in page
    assert "if (exchange !== selectedExchange) return" in page
    assert 'id="btn-integrity-normalize-hl"' in page
    assert "meta.statusKey !== 'hyperliquid'" in page
    assert "'/integrity/hyperliquid/normalize-fallback'" in page
    assert "Only candles marked other_exchange" in page
