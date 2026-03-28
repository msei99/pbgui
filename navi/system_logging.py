import streamlit as st
from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
    redirect_to_fastapi_logging,
)

# This page is normally intercepted in build_navigation() before navi.run(),
# so this code only runs if navigation interception failed for some reason.
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

redirect_to_fastapi_logging()

st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)
