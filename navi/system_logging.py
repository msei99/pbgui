import streamlit as st
from pathlib import Path
import logging_helpers

from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
    render_header_with_guide,
    render_log_viewer,
)


# ── Guide helpers ──────────────────────────────────────────

def _docs_index(lang: str) -> list[tuple[str, str]]:
    folder = "help_de" if str(lang).strip().upper() == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            first = p.read_text(encoding="utf-8").splitlines()[0].strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            pass
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "Logging"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="logging_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return
    labels = [d[0] for d in docs]
    default_index = 0
    target = str(default_topic or "").strip().lower()
    if target:
        for i, lbl in enumerate(labels):
            if target in str(lbl).lower():
                default_index = i
                break
    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="logging_help_sel",
    )
    st.markdown(_read_markdown(docs[int(sel)][1]), unsafe_allow_html=True)


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
    with st.sidebar:
        if st.button(":material/arrow_back: Back to Log Viewer", key="logging_back_btn"):
            st.session_state.logging_view_mode = "viewer"
            st.rerun()

    render_header_with_guide(
        "Log Rotation Settings",
        guide_callback=lambda: _help_modal("Logging"),
        guide_key="logging_guide_btn_settings",
    )

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

with st.sidebar:
    if st.button(":material/settings: Settings", key="logging_settings_btn"):
        st.session_state.logging_view_mode = "settings"
        st.rerun()

render_header_with_guide(
    "Logging",
    guide_callback=lambda: _help_modal("Logging"),
    guide_key="logging_guide_btn_viewer",
)

render_log_viewer()
