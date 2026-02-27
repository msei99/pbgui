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
    render_header_with_guide,
)
from pbgui_purefunc import load_ini


# ── Component loading ──────────────────────────────────────

_COMPONENT_DIR = Path(__file__).resolve().parent.parent / "components" / "vps_monitor"
_COMPONENT_HTML: str | None = None


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            label = p.name
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "VPS Monitor"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="vps_monitor_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]
    default_index = 0
    try:
        target = str(default_topic or "").strip().lower()
        if target:
            for i, lbl in enumerate(labels):
                if target in str(lbl).lower():
                    default_index = i
                    break
    except Exception:
        default_index = 0

    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="vps_monitor_help_sel",
    )
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)
    try:
        base = str(st.get_option("server.baseUrlPath") or "").strip("/")
        prefix = f"/{base}" if base else ""
        st.markdown(
            f"<a href='{prefix}/help' target='_blank'>Open full Help page in new tab</a>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


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
render_header_with_guide(
    "VPS Monitor",
    guide_callback=lambda: _help_modal(default_topic="VPS Monitor"),
    guide_key="vps_monitor_guide_btn",
)

# Check if PBMaster daemon is accessible
from PBMaster import PBMaster

if "pbmaster" not in st.session_state:
    st.session_state.pbmaster = PBMaster()

pbmaster = st.session_state.pbmaster

# Load HTML and inject WS port
ws_port = _get_ws_port()
html = _load_component_html().replace("__WS_PORT__", str(ws_port))

# CSS to make the iframe fill the remaining viewport.
st.markdown("""
<style>
    /* Minimize padding */
    .stMainBlockContainer {
        padding-top: 2.25rem !important;
        padding-bottom: 0 !important;
    }

    /* Keep page title from looking clipped against top navigation */
    .stMainBlockContainer h1,
    .stMainBlockContainer h2,
    .stMainBlockContainer h3 {
        margin-top: 0.25rem !important;
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
