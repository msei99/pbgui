import streamlit as st
import streamlit.components.v1 as components
import json
from pathlib import Path
import logging_helpers

from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
)
from pbgui_purefunc import load_ini


# ── Component helpers ──────────────────────────────────────

_COMPONENT_DIR = Path(__file__).resolve().parent.parent / "components" / "log_viewer"
_COMPONENT_HTML: str | None = None


def _load_log_viewer_html() -> str:
    global _COMPONENT_HTML
    if _COMPONENT_HTML is None:
        _COMPONENT_HTML = (_COMPONENT_DIR / "index.html").read_text(encoding="utf-8")
    return _COMPONENT_HTML


def _get_ws_port() -> int:
    val = load_ini("pbmaster", "ws_port")
    if val and val.isdigit():
        port = int(val)
        if 1024 <= port <= 65535:
            return port
    return 8765


# ============================================================================
# Page setup
# ============================================================================

set_page_config("PBGui Logging")

if is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])

if not is_authenticted():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])

if "logging_view_mode" not in st.session_state:
    st.session_state.logging_view_mode = "viewer"


# ============================================================================
# Settings view
# ============================================================================

if st.session_state.logging_view_mode == "settings":
    if st.button(":material/arrow_back: Back to Log Viewer", key="logging_back_btn"):
        st.session_state.logging_view_mode = "viewer"
        st.rerun()

    st.header("Log Rotation Settings", divider="rainbow")

    logs_dir = Path.cwd() / "data" / "logs"
    services = sorted([p.stem for p in logs_dir.glob("*.log") if p.is_file()]) if logs_dir.exists() else []

    default_max_bytes, default_backup_count = logging_helpers.get_rotate_defaults()
    default_max_mb = max(1, int(default_max_bytes / (1024 * 1024)))

    st.subheader("Default rotation")
    c1, c2 = st.columns([1, 1])
    with c1:
        st.number_input(
            "Default max size (MB)",
            min_value=1, max_value=10240, value=default_max_mb, step=1,
            key="logging_rotate_default_mb",
        )
    with c2:
        st.number_input(
            "Default rotated files",
            min_value=1, max_value=20, value=int(default_backup_count), step=1,
            key="logging_rotate_default_files",
        )
    if st.button("Save default rotation", key="logging_rotate_save_default"):
        mb = int(st.session_state.get("logging_rotate_default_mb", default_max_mb))
        files = int(st.session_state.get("logging_rotate_default_files", default_backup_count))
        logging_helpers.set_rotate_defaults(mb * 1024 * 1024, files)
        st.success("Saved.")

    st.divider()
    st.subheader("Per-log rotation")

    if services:
        st.caption("Each service log can override size and number of rotated files.")
        for service in services:
            max_bytes, backup_count = logging_helpers.get_rotate_settings(service=service)
            max_mb = max(1, int(max_bytes / (1024 * 1024)))
            row1, row2, row3, row4 = st.columns([2, 2, 2, 1])
            with row1:
                st.text_input("Service", value=service, key=f"logging_rotate_name_{service}", disabled=True)
            with row2:
                st.number_input(
                    "Max MB",
                    min_value=1, max_value=10240, value=max_mb, step=1,
                    key=f"logging_rotate_mb_{service}",
                    label_visibility="collapsed",
                )
            with row3:
                st.number_input(
                    "Files",
                    min_value=1, max_value=20, value=int(backup_count), step=1,
                    key=f"logging_rotate_files_{service}",
                    label_visibility="collapsed",
                )
            with row4:
                if st.button("Save", key=f"logging_rotate_save_{service}"):
                    mb = int(st.session_state.get(f"logging_rotate_mb_{service}", max_mb))
                    files = int(st.session_state.get(f"logging_rotate_files_{service}", backup_count))
                    logging_helpers.set_rotate_settings(service, mb * 1024 * 1024, files)
                    st.success(f"Saved {service}")
    else:
        st.info("No logfiles found in data/logs yet.")

    st.stop()


# ============================================================================
# Log Viewer (default — full height)
# ============================================================================

c_title, c_settings = st.columns([0.90, 0.10], vertical_alignment="center")
with c_title:
    st.header("Logging", divider="rainbow")
with c_settings:
    if st.button(":material/settings: Settings", key="logging_settings_btn"):
        st.session_state.logging_view_mode = "settings"
        st.rerun()

st.caption("Select a log file on the left — live stream with level filter and text search.")

st.markdown("""
<style>
.stMainBlockContainer { padding-bottom: 0 !important; }
.stMainBlockContainer iframe {
    height: calc(100vh - 160px) !important;
    min-height: 400px !important;
    border: none !important;
}
</style>
""", unsafe_allow_html=True)

ws_port = _get_ws_port()
logs_dir = Path.cwd() / "data" / "logs"
log_files = sorted(p.name for p in logs_dir.glob("*.log") if p.is_file()) if logs_dir.exists() else []
html = (
    _load_log_viewer_html()
    .replace("__WS_PORT__", str(ws_port))
    .replace("__INITIAL_FILES__", json.dumps(log_files))
)
components.html(html, height=600, scrolling=False)
components.html(html, height=500, scrolling=False)
