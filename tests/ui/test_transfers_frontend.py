"""Offline source contracts for the fixed-route Transfers frontend."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "transfers.html"
NAV = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")


def test_transfers_page_uses_shared_shell_and_cookie_auth() -> None:
    """Transfers must remain a local authenticated FastAPI page."""

    source = PAGE.read_text(encoding="utf-8")

    assert "current:'system_transfers'" in source
    assert '/app/css/sidebar.css?v=4' in source
    assert '/app/js/sidebar_resize.js?v=1' in source
    assert '/app/js/pbgui_dialogs.js?v=8' in source
    assert "credentials:'same-origin'" in source
    assert "Authorization" not in source
    assert "document.cookie" not in source
    assert "localStorage" not in source
    assert "pbgui:transfers:pending-top-up:v1" in source
    assert "sessionStorage.setItem(PENDING_KEY" in source
    assert "pendingRequest:loadPendingRequest()" in source
    assert "button.disabled=state.busy" in source
    assert "if(state.busy&&!force)return" in source
    for sensitive in ("password", "private_key", "api_key", "signature", "credential", "token"):
        assert f"pending.{sensitive}" not in source
    assert "https://" not in source
    for sidebar_id in (
        "sidebar", "sidebar-sticky", "sidebar-header", "sidebar-toolbar",
        "sidebar-inner", "sidebar-resize",
    ):
        assert f'id="{sidebar_id}"' in source


def test_transfers_page_has_fixed_route_review_and_reconciliation() -> None:
    """The browser may choose an advertised route and amount, never a free destination."""

    source = PAGE.read_text(encoding="utf-8")

    assert "Main Perps → Vault" in source
    assert "Vault → Main Perps" in source
    assert "Main Perps → Main Spot" in source
    assert "Main Spot → Main Perps" in source
    assert "Spot → Perps" in source
    assert "Funding → Unified" in source
    assert "Funding → USD-M Futures" in source
    assert "Spot → USDT Futures" in source
    assert "Spot → UTA" in source
    assert "el('destination').textContent=text(route&&route.destination)" in source
    assert 'id="route-select" disabled' in source
    assert 'id="amount" type="number" min="0.000001" step="any" inputmode="decimal" value="5"' in source
    assert 'placeholder="5"' not in source
    assert "Review transfer" in source
    transfer_form = source.split('<div class="transfer-form">', 1)[1].split('</div><div id="status"', 1)[0]
    assert transfer_form.index('for="route-select"') < transfer_form.index('for="amount"') < transfer_form.index('id="submit"')
    assert "el('route-note').textContent=routeNote" in source
    assert "window.PBGuiDialogs.confirm" in source
    assert "crypto.randomUUID" in source
    assert "operation_id:pending.operation_id" in source
    assert "route:route.id" in source
    assert "'/transfers/execute/'" in source
    assert "same operation ID will be reused" in source
    assert "max_transferable" in source
    assert "Your Vault Equity" in source
    assert "Vault Account Value" in source
    assert "Your Max Withdrawable" in source
    assert "Open Vault Positions" in source
    assert "renderPositions(value)" in source
    for column in ("Coin", "Side", "Size", "Position value", "Entry", "Unrealized PnL", "Liquidation", "Leverage"):
        assert column in source
    assert "may affect Passivbot wallet-exposure sizing" in source
    assert "el('amount').addEventListener('input',updateSubmit)" in source
    assert "pendingOperationId=''" not in source
    assert "error.status=response.status" in source
    assert "Number(error.status)>=400&&Number(error.status)<500" in source
    assert "(?:[eE][+-]?\\d+)?" in source
    assert "Unknown operations can only be reconciled" in source
    assert "'/reconcile'" in source
    assert "innerHTML" not in source
    assert "textContent" in source
    assert "Profit Sweep Due" in source


def test_transfers_navigation_and_profit_sweep_handoff_are_registered() -> None:
    """System navigation and the contextual Vault shortcut target the same page."""

    profit_sweep = (ROOT / "frontend" / "profit_sweep.html").read_text(encoding="utf-8")

    assert "{ page: 'system_transfers'" in NAV
    assert "'system_transfers':  '/api/profit-sweep/transfers/main_page'" in NAV
    assert "'system_transfers':             '47_transfers'" in NAV
    assert 'id="fund-account" hidden>Fund account</button>' in profit_sweep
    assert "byId('fund-account').addEventListener('click', openFundAccount)" in profit_sweep
    assert "'/transfers/main_page?user=' + encodeURIComponent(state.selectedUser) + '&route=main_perps_to_vault'" in profit_sweep
