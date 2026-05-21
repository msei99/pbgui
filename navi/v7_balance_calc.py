import streamlit as st

from pbgui_func import (
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    redirect_to_fastapi_balance_calc,
    set_page_config,
)


# Keep a real page file as a safety net for Streamlit page resolution.
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

redirect_to_fastapi_balance_calc()

set_page_config("PBv7 Balance Calculator")
st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)
