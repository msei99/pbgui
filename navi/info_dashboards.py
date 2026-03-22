import streamlit as st
import time
import json
from pathlib import Path

from pbgui_func import PBGDIR, set_page_config, is_session_state_not_initialized, is_authenticted, get_navi_paths, render_header_with_guide, nav_bridge
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


def dashboard():
    # Mark Dashboards page as recently active so other pages can infer context
    # when navigating via menu.
    st.session_state["dashboards_last_active_ts"] = time.time()
    # Init dashboard
    if "dashboards" not in st.session_state:
        st.session_state.dashboards = Dashboard().list_dashboards()
    dashboards = st.session_state.dashboards

    # If another page requested a specific dashboard (by user), pick best matching dashboard.
    requested = st.session_state.pop("dashboards_open_dashboard", None)
    if requested:
        try:
            best = _find_best_dashboard_for_user(dashboards, str(requested))
            if best:
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                if '_dashboard_edit_original_name' in st.session_state:
                    del st.session_state['_dashboard_edit_original_name']
                st.session_state.dashboard = Dashboard(best)
            else:
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                if "dashboard" in st.session_state:
                    del st.session_state.dashboard
                if 'selected_dashboard' in st.session_state:
                    del st.session_state['selected_dashboard']
                if '_dashboard_edit_original_name' in st.session_state:
                    del st.session_state['_dashboard_edit_original_name']
        except Exception:
            pass

    # Always render nav_bridge (handles both page navigation and sidebar actions)
    nav_bridge()

    # Render FastAPI sidebar
    _render_sidebar_html(dashboards)

    if not "dashboard" in st.session_state:
        if len(dashboards) == 0:
            st.info("Please create a new dashboard.")
        elif len(dashboards) == 1:
            st.session_state.dashboard = Dashboard(dashboards[0])
            st.rerun()
        elif len(dashboards) > 1:
            def on_select_dashboard():
                selected_dashboard = st.session_state['selected_dashboard']
                if not selected_dashboard or selected_dashboard == 'Select a dashboard':
                    return
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                st.session_state.dashboard = Dashboard(selected_dashboard)

            st.selectbox(
                "select a dashboard",
                options=['Select a dashboard'] + dashboards,
                key='selected_dashboard',
                on_change=on_select_dashboard,
                label_visibility="hidden"
            )

    if "edit_dashboard" in st.session_state:
        st.session_state.dashboard.create_dashboard()
    elif "dashboard" in st.session_state:
        st.session_state.dashboard.view()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("Dashboards")
render_header_with_guide(
    "Dashboards",
    guide_callback=lambda: _dashboards_help_modal(),
    guide_key="dashboards_header_help_btn",
)

dashboard()
