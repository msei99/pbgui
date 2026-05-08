import streamlit as st

from pbgui_func import (
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    redirect_to_fastapi_vps_manager,
    set_page_config,
)


if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

redirect_to_fastapi_vps_manager()

set_page_config("VPS Manager")
st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)