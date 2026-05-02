import streamlit as st

from pbgui_func import (
    _start_fastapi_server_if_needed,
    get_navi_paths,
    is_authenticted,
    is_session_state_not_initialized,
    set_page_config,
)


def _wait_for_api_ready(host: str, port: int, timeout: int = 12) -> bool:
    """Poll the FastAPI health endpoint until it responds or timeout expires."""
    import time
    import urllib.request

    url = f"http://{host}:{port}/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status < 500:
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def open_market_data_fastapi() -> None:
    """Redirect the browser to the standalone parallel FastAPI Market Data page."""
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(f"FastAPI server is not available on {api_host}:{api_port}.")
        return

    if not _wait_for_api_ready(api_host, api_port):
        st.error(f"FastAPI server started but is not responding on port {api_port}. Please try again.")
        return

    if "api_token" not in st.session_state:
        user_id = st.session_state.get("user", {}).get("id") or "anonymous"
        st.session_state["api_token"] = generate_token(
            str(user_id), expires_in_seconds=86400
        ).token
    token = st.session_state["api_token"]

    browser_host = "127.0.0.1"
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if ":" in req_host:
            st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/market-data/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )

    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

set_page_config("Market Data (FastAPI)")
open_market_data_fastapi()