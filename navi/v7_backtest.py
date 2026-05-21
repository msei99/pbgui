import streamlit as st

from pbgui_func import (
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    _start_fastapi_server_if_needed,
    redirect_to_fastapi_v7_backtest,
    redirect_to_fastapi_v7_backtest_draft,
    set_page_config,
)


def _redirect_instance_to_fastapi_v7_backtest(instance_name: str) -> None:
    from api.auth import generate_token
    import requests

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(
            f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
            "Please check **System → Services → API Server** or start manually: "
            "`python PBApiServer.py`"
        )
        return

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token

    token = st.session_state["api_token"]

    try:
        resp = requests.get(
            f"http://{api_host}:{api_port}/api/v7/instances/{requests.utils.quote(str(instance_name), safe='')}/config",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        st.error(f"Failed to load instance config for backtest: {e}")
        return

    config = payload.get("config") if isinstance(payload, dict) else None
    if not isinstance(config, dict):
        st.error(f"Instance '{instance_name}' did not return a valid config")
        return

    redirect_to_fastapi_v7_backtest_draft(config, str(instance_name))


if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

set_page_config("PBv7 Backtest")

_relay_instance = st.session_state.pop("_relay_config_file", "")
if _relay_instance:
    _redirect_instance_to_fastapi_v7_backtest(str(_relay_instance))

_draft_id = st.session_state.pop("_relay_draft_id", "")
_draft_name = st.session_state.pop("_relay_draft_name", "")
if _draft_id:
    st.query_params["draft_id"] = _draft_id
    if _draft_name:
        st.query_params["draft_name"] = _draft_name

redirect_to_fastapi_v7_backtest()

st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)
