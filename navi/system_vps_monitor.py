"""
VPS Monitor — Real-time monitoring of all VPS servers.

Embeds a WebSocket-powered Custom Component that connects directly to
PBMaster's WebSocket server. All data updates are pushed in real-time
(~2s latency for system metrics, ~0.5s for live logs) — zero Streamlit
reruns, zero polling.

The component handles:
    Dashboard  — All VPS at a glance: SSH status, resource gauges
    Instances  — All bot instances across all VPS (filterable, sortable)
    Services   — PBRun/PBRemote/PBCoinData status per VPS with restart
    Live Logs  — Real-time log streaming (tail -f equivalent)
"""

import streamlit as st
import streamlit.components.v1 as components
from pathlib import Path

from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
)
from pbgui_purefunc import load_ini


# ── Component loading ──────────────────────────────────────

_COMPONENT_DIR = Path(__file__).resolve().parent.parent / "components" / "vps_monitor"
_COMPONENT_HTML: str | None = None


def _load_component_html() -> str:
    """Load and cache the VPS Monitor HTML component."""
    global _COMPONENT_HTML
    if _COMPONENT_HTML is None:
        html_path = _COMPONENT_DIR / "index.html"
        _COMPONENT_HTML = html_path.read_text(encoding="utf-8")
    return _COMPONENT_HTML


def _get_ws_port() -> int:
    """Read WS port from config, default 8765."""
    val = load_ini("pbmaster", "ws_port")
    if val and val.isdigit():
        port = int(val)
        if 1024 <= port <= 65535:
            return port
    return 8765


# ── Page ───────────────────────────────────────────────────

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("VPS Monitor")
st.title("VPS Monitor")

# Check if PBMaster daemon is accessible
from PBMaster import PBMaster

if "pbmaster" not in st.session_state:
    st.session_state.pbmaster = PBMaster()

pbmaster = st.session_state.pbmaster

if not pbmaster.is_running():
    st.warning(
        "PBMaster is not running. Start it from **Services → PBMaster** "
        "to enable real-time monitoring.",
        icon="⚠️",
    )
    st.stop()

# Load HTML and inject WS port
ws_port = _get_ws_port()
html = _load_component_html().replace("__WS_PORT__", str(ws_port))

# CSS to make the iframe fill the remaining viewport.
st.markdown("""
<style>
    /* Minimize padding */
    .stMainBlockContainer {
        padding-top: 1rem !important;
        padding-bottom: 0 !important;
    }

    /* Target the iframe only */
    .stMainBlockContainer iframe {
        height: calc(100vh - 200px) !important;
        min-height: 400px           !important;
        border: none                !important;
    }
</style>
""", unsafe_allow_html=True)

# Render the component.
# height=600 is a fallback; the CSS above overrides it to fill the viewport.
components.html(html, height=600, scrolling=False)
