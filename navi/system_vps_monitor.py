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
_COMPONENT_MTIME: float = 0.0


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
    """Load and cache the VPS Monitor HTML component.
    
    Auto-reloads if the template file was modified on disk.
    """
    global _COMPONENT_HTML, _COMPONENT_MTIME
    html_path = _COMPONENT_DIR / "index.html"
    try:
        mtime = html_path.stat().st_mtime
    except OSError:
        mtime = 0.0
    if _COMPONENT_HTML is None or mtime != _COMPONENT_MTIME:
        _COMPONENT_HTML = html_path.read_text(encoding="utf-8")
        _COMPONENT_MTIME = mtime
    return _COMPONENT_HTML


def _get_api_port() -> int:
    """Read API server port from config, default 8000."""
    import os
    return int(os.getenv("PBGUI_API_PORT", "8000"))


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

# Ensure API server and get auth token
from pbgui_func import _start_fastapi_server_if_needed
from api.auth import generate_token

api_host, api_port_val, api_ok = _start_fastapi_server_if_needed()
if not api_ok:
    st.error(
        f"⚠️ FastAPI server could not be started on {api_host}:{api_port_val}. "
        "Please check **System → Services → API Server** or start manually: "
        "`python api_server.py`"
    )
    st.stop()

if "api_token" not in st.session_state:
    user_id = (
        st.session_state.get("user", {}).get("id")
        or st.session_state.get("user")
        or "anonymous"
    )
    token_obj = generate_token(str(user_id), expires_in_seconds=86400)
    st.session_state["api_token"] = token_obj.token

api_token = st.session_state["api_token"]

# Determine browser-usable host for WebSocket connections.
# api_host from config may be "0.0.0.0" (bind all interfaces) which
# is not a valid WS target for browsers.  Use the Streamlit request
# host header so remote access works too.
_ws_host = "127.0.0.1"
try:
    _req_host = st.context.headers.get("Host", "")
    if _req_host:
        # Strip port from Host header (e.g. "192.168.1.100:8501" → "192.168.1.100")
        _ws_host = _req_host.split(":")[0] or "127.0.0.1"
except Exception:
    pass

# Load HTML and inject API port + token + host
html = _load_component_html()
html = html.replace("__API_PORT__", str(api_port_val))
html = html.replace("__API_TOKEN__", api_token)
html = html.replace("__API_HOST__", _ws_host)

# CSS to make the iframe fill the remaining viewport.
st.markdown("""
<style>
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
