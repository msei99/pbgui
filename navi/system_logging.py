import streamlit as st
from pathlib import Path
import logging_helpers

from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
)
from logging_view import view_log_filtered

# ============================================================================
# Page setup
# ============================================================================

set_page_config("PBGui Logging")

if is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])

if not is_authenticted():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])

# ============================================================================
# Page content
# ============================================================================

st.header("Logging", divider="rainbow")

st.caption(
    "Central log viewer for all PBGui services. "
    "Select one or more logfiles to view, filter by level, tags, or free text."
)

st.subheader("Rotation Settings")

logs_dir = Path.cwd() / "data" / "logs"
services = sorted([p.stem for p in logs_dir.glob("*.log") if p.is_file()]) if logs_dir.exists() else []

default_max_bytes, default_backup_count = logging_helpers.get_rotate_defaults()
default_max_mb = max(1, int(default_max_bytes / (1024 * 1024)))

with st.expander("Default rotation", expanded=False):
    c1, c2 = st.columns([1, 1])
    with c1:
        st.number_input(
            "Default max size (MB)",
            min_value=1,
            max_value=10240,
            value=default_max_mb,
            step=1,
            key="logging_rotate_default_mb",
        )
    with c2:
        st.number_input(
            "Default rotated files",
            min_value=1,
            max_value=20,
            value=int(default_backup_count),
            step=1,
            key="logging_rotate_default_files",
        )

    if st.button("Save default rotation", key="logging_rotate_save_default"):
        mb = int(st.session_state.get("logging_rotate_default_mb", default_max_mb))
        files = int(st.session_state.get("logging_rotate_default_files", default_backup_count))
        logging_helpers.set_rotate_defaults(mb * 1024 * 1024, files)
        st.success("Saved default rotation settings")

if services:
    with st.expander("Per-log rotation", expanded=True):
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
                    min_value=1,
                    max_value=10240,
                    value=max_mb,
                    step=1,
                    key=f"logging_rotate_mb_{service}",
                    label_visibility="collapsed",
                )
            with row3:
                st.number_input(
                    "Files",
                    min_value=1,
                    max_value=20,
                    value=int(backup_count),
                    step=1,
                    key=f"logging_rotate_files_{service}",
                    label_visibility="collapsed",
                )
            with row4:
                if st.button("Save", key=f"logging_rotate_save_{service}"):
                    mb = int(st.session_state.get(f"logging_rotate_mb_{service}", max_mb))
                    files = int(st.session_state.get(f"logging_rotate_files_{service}", backup_count))
                    logging_helpers.set_rotate_settings(service, mb * 1024 * 1024, files)
                    st.success(f"Saved rotation for {service}")
else:
    st.info("No logfiles found in data/logs yet.")

# The view_log_filtered widget already handles:
# - multi-file selection & merged view
# - level/tag/user/text filters
# - purge, clear, raw mode
# - auto-rotation awareness
view_log_filtered("PBCoinData")
