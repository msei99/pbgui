import streamlit as st
from pathlib import Path

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

# The view_log_filtered widget already handles:
# - multi-file selection & merged view
# - level/tag/user/text filters
# - purge, clear, raw mode
# - auto-rotation awareness
view_log_filtered("PBCoinData")
