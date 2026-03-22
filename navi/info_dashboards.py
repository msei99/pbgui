import streamlit as st
import time
import json
from pathlib import Path

from pbgui_func import PBGDIR, set_page_config, is_session_state_not_initialized, is_authenticted, get_navi_paths, render_header_with_guide
from Dashboard import Dashboard


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            label = p.name
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _dashboards_help_modal(default_topic: str = "Dashboards"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="dashboards_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return
    labels = [d[0] for d in docs]
    default_index = 0
    try:
        target = str(default_topic or "").strip().lower()
        if target:
            for i, lbl in enumerate(labels):
                if target in str(lbl).lower():
                    default_index = i
                    break
    except Exception:
        default_index = 0
    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="dashboards_help_sel",
    )
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)
    try:
        base = str(st.get_option("server.baseUrlPath") or "").strip("/")
        prefix = f"/{base}" if base else ""
        st.markdown(
            f"<a href='{prefix}/help' target='_blank'>Open full Help page in new tab</a>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass


def _extract_users_from_dashboard_config(cfg: dict) -> list[str]:
    """Best-effort: extract user strings from dashboard JSON config."""
    users: list[str] = []
    if not isinstance(cfg, dict):
        return users
    for k, v in cfg.items():
        if not isinstance(k, str):
            continue
        if "_users_" not in k:
            continue
        if isinstance(v, str):
            if v:
                users.append(v)
        elif isinstance(v, (list, tuple)):
            for item in v:
                if isinstance(item, str) and item:
                    users.append(item)
    return users


def _find_best_dashboard_for_user(dashboard_names: list[str], requested_user: str) -> str | None:
    """Pick the dashboard which most strongly references requested_user."""
    if not requested_user:
        return None

    dashboards_dir = Path(f"{PBGDIR}/data/dashboards")
    best_name: str | None = None
    best_score = -1
    best_specificity = 10_000
    best_mtime = -1.0

    for name in dashboard_names:
        p = dashboards_dir / f"{name}.json"
        if not p.exists():
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            continue

        extracted = _extract_users_from_dashboard_config(cfg)
        if not extracted:
            continue

        # Score: count exact occurrences (case sensitive to match user ids)
        score = sum(1 for u in extracted if u == requested_user)
        if score <= 0:
            continue

        # Prefer dashboards that are more "specific" (fewer total referenced users)
        specificity = len(set(extracted))
        try:
            mtime = p.stat().st_mtime
        except Exception:
            mtime = 0.0

        # Higher score wins; then fewer distinct users; then newest file
        if (score > best_score) or (
            score == best_score and (specificity < best_specificity or (specificity == best_specificity and mtime > best_mtime))
        ):
            best_name = name
            best_score = score
            best_specificity = specificity
            best_mtime = mtime

    return best_name

def _render_sidebar_html(dashboards):
    """Render the FastAPI/Vanilla JS sidebar for dashboard navigation."""
    from pbgui_func import _start_fastapi_server_if_needed
    from api.auth import generate_token
    import json as _json

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        with st.sidebar:
            st.error("FastAPI server not available")
        return

    if "api_token" not in st.session_state:
        user_id = st.session_state.get("user", {}).get("id") or "anonymous"
        st.session_state["api_token"] = generate_token(
            str(user_id), expires_in_seconds=86400
        ).token
    token = st.session_state["api_token"]

    _browser_host = "127.0.0.1"
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            _browser_host = req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass
    api_base = f"http://{_browser_host}:{api_port}/api"

    current_name = ""
    edit_mode = "edit_dashboard" in st.session_state
    has_dashboard = "dashboard" in st.session_state
    if has_dashboard:
        try:
            current_name = st.session_state.dashboard.name or ""
        except Exception:
            pass

    html_path = Path(__file__).parent.parent / "frontend" / "dashboard_sidebar.html"
    html = html_path.read_text(encoding="utf-8")
    html = html.replace('"%%TOKEN%%"', f'"{token}"')
    html = html.replace('"%%API_BASE%%"', f'"{api_base}"')
    html = html.replace('%%DASHBOARDS%%', _json.dumps(dashboards))
    html = html.replace('"%%CURRENT%%"', _json.dumps(current_name))
    html = html.replace('%%EDIT_MODE%%', 'true' if edit_mode else 'false')
    html = html.replace('%%HAS_DASHBOARD%%', 'true' if has_dashboard else 'false')

    with st.sidebar:
        st.html(html, unsafe_allow_javascript=True)


def _wait_for_api_ready(host: str, port: int, timeout: int = 12) -> bool:
    """Poll the FastAPI health endpoint until it responds or timeout expires."""
    import urllib.request
    import time
    url = f"http://{host}:{port}/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status < 500:
                    return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def dashboard():
    """Redirect the browser to the standalone FastAPI dashboard page.

    The Streamlit session is intentionally abandoned here — the dashboard is
    100% FastAPI.  Navigation from the dashboard back to Streamlit uses the
    relay built into system_login.py (token-authenticated, fresh session init).
    """
    from pbgui_func import _start_fastapi_server_if_needed
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(f"FastAPI server is not available on {api_host}:{api_port}.")
        return

    # Wait for uvicorn to actually accept HTTP connections (process start ≠ HTTP ready)
    if not _wait_for_api_ready(api_host, api_port):
        st.error(f"FastAPI server started but is not responding on port {api_port}. Please try again.")
        return

    if "api_token" not in st.session_state:
        user_id = st.session_state.get("user", {}).get("id") or "anonymous"
        st.session_state["api_token"] = generate_token(
            str(user_id), expires_in_seconds=86400
        ).token
    token = st.session_state["api_token"]

    # Derive browser-visible host from the incoming request
    browser_host = "127.0.0.1"
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    # Derive Streamlit port from the same Host header
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if ":" in req_host:
            st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"

    # Pick the best current dashboard
    current = ""
    try:
        if "dashboard" in st.session_state:
            current = st.session_state.dashboard.name or ""
    except Exception:
        pass

    url = (
        f"http://{browser_host}:{api_port}/api/dashboard/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
        f"&current={current}"
    )

    # Redirect the entire browser window to the standalone FastAPI page.
    # st.html injects directly into the Streamlit page DOM (not a sub-iframe),
    # so window.location.replace navigates the whole window immediately.
    # The URL contains no < or > chars, so DOMPurify leaves the script intact.
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("Dashboards")

# Redirect to standalone FastAPI page — must run before any visible st.* calls
# so the Streamlit chrome is never rendered.
dashboard()  # calls st.stop() on success; only falls through on error

# Only reached when FastAPI server is unavailable (error message shown above)
render_header_with_guide(
    "Dashboards",
    guide_callback=lambda: _dashboards_help_modal(),
    guide_key="dashboards_header_help_btn",
)
