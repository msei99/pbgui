import streamlit as st

from pbgui_func import (
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    redirect_to_fastapi_v7_optimize,
    set_page_config,
)


if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

set_page_config("PBv7 Optimize")

_draft_id = st.session_state.pop("_relay_opt_draft_id", "") or st.query_params.get("opt_draft_id", "")
_draft_name = st.session_state.pop("_relay_draft_name", "") or st.query_params.get("draft_name", "")
if _draft_id:
    st.query_params["opt_draft_id"] = _draft_id
    if _draft_name:
        st.query_params["draft_name"] = _draft_name

redirect_to_fastapi_v7_optimize()

st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)
