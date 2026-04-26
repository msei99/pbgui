"""Help & Tutorials redirect fallback.

The normal navigation path is intercepted in ``build_navigation`` and routed
directly to the FastAPI Help page. This file remains as a fallback if the
interception cannot run.
"""

from urllib.parse import urlencode

import streamlit as st


st.set_page_config(page_title="Help & Tutorials", layout="centered")

host = "127.0.0.1:8501"
try:
    host = st.context.headers.get("Host", host) or host
except Exception:
    pass

query = {}
if st.session_state.get("api_token"):
    query["token"] = st.session_state["api_token"]
query["st_base"] = f"http://{host}"

help_url = f"http://{host}/app/help.html"
if query:
    help_url = f"{help_url}?{urlencode(query)}"

st.info("Help & Tutorials now open in the FastAPI UI shell.")
st.markdown(
    f'<script>window.location.replace({help_url!r});</script>',
    unsafe_allow_html=True,
)
st.markdown(f"If you are not redirected automatically, open [Help & Tutorials]({help_url}).")
