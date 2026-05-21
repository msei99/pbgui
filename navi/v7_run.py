import streamlit as st

from pbgui_func import (
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    _start_fastapi_server_if_needed,
    redirect_to_fastapi_v7_run,
    set_page_config,
)


def _redirect_to_fastapi_v7_edit(name: str = "", *, is_new: bool = False) -> None:
    from api.auth import generate_token

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

    browser_host = "127.0.0.1"
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
            if ":" in req_host:
                st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/v7/edit_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )
    if is_new:
        url += "&new=1"
    elif name:
        url += f"&name={str(name)}"
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

set_page_config("PBv7 Run")

_add = st.query_params.get("add_instance", "") or st.session_state.pop("_relay_add_instance", "")
if _add == "1":
    st.query_params.pop("add_instance", None)
    _redirect_to_fastapi_v7_edit(is_new=True)

_edit_name = st.query_params.get("edit_instance", "") or st.session_state.pop("_relay_edit_instance", "")
if _edit_name:
    st.query_params.pop("edit_instance", None)
    _redirect_to_fastapi_v7_edit(name=str(_edit_name))

redirect_to_fastapi_v7_run()

st.error(
    "⚠️ FastAPI server unavailable. Please start it via **System → Services → API Server**."
)
