import streamlit as st
from pathlib import Path
from pbgui_func import (
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    redirect_to_fastapi_pareto_explorer,
    set_page_config,
)


set_page_config("Pareto Explorer")

if is_session_state_not_initialized() or not is_authenticted():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()


def _first_query_value(value: object) -> str:
    if isinstance(value, list):
        return str(value[0] if value else "").strip()
    return str(value or "").strip()


result_path = _first_query_value(st.query_params.get("result_path"))
relay_result_path = st.session_state.pop("_relay_result_path", "")
if not result_path:
    result_path = _first_query_value(relay_result_path)
if not result_path:
    result_path = _first_query_value(st.session_state.pop("pareto_explorer_path", ""))

if result_path:
    try:
        candidate_result_dir = Path(result_path).expanduser().resolve()
        if not candidate_result_dir.exists():
            result_path = ""
    except Exception:
        result_path = ""

redirect_to_fastapi_pareto_explorer(result_path)

st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)
