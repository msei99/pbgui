import streamlit as st
import time
import json
from pathlib import Path

from pbgui_func import PBGDIR, set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_authenticted, get_navi_paths
from Dashboard import Dashboard


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

def dashboard():
    # Mark Dashboards page as recently active so other pages can infer context
    # when navigating via menu.
    st.session_state["dashboards_last_active_ts"] = time.time()
    # Init dashboard
    # if "dashboard" not in st.session_state:
    #     st.session_state.dashboard = Dashboard()
    # dashboard = st.session_state.dashboard
    # Navigation
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
                # If it doesn't exist, show the dashboard selection page (not the last opened dashboard)
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
    
    if not "dashboard" in st.session_state:
        # No Dashboard? Create a new one
        if len(dashboards) == 0:
            st.info("Please create a new dashboard.")
        # If there's only one dashboard, load it directly
        elif len(dashboards) == 1:
            st.session_state.dashboard = Dashboard(dashboards[0])
            st.rerun()
        # Else create Content area buttons (in addition to sidebar buttons)
        elif len(dashboards) > 1:          
             # Define the callback function
            def on_select_dashboard():
                selected_dashboard = st.session_state['selected_dashboard']
                # Ignore placeholder option
                if not selected_dashboard or selected_dashboard == 'Select a dashboard':
                    return
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                st.session_state.dashboard = Dashboard(selected_dashboard)
                
            # Create the selectbox with the callback
            st.selectbox(
                "select a dashboard",
                options=['Select a dashboard'] + dashboards,
                #index=default_index,
                key='selected_dashboard',
                on_change=on_select_dashboard,
                label_visibility="hidden"
            )
    
    with st.sidebar:
        if "edit_dashboard" in st.session_state:
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                if st.button(":material/save:"):
                    if st.session_state.dashboard.name:
                        st.session_state.dashboard.save()
                        st.session_state.dashboards = st.session_state.dashboard.list_dashboards()
                        # Clear edit mode and reload the saved dashboard
                        del st.session_state.edit_dashboard
                        st.session_state.dashboard.load(st.session_state.dashboard.name)
                        # remove stored original name
                        if '_dashboard_edit_original_name' in st.session_state:
                            del st.session_state['_dashboard_edit_original_name']
                        st.rerun()
                    else:
                        error_popup("Name is empty")
            with col2:
                if st.button(":material/cancel:"):
                    # If we were editing an existing saved dashboard, restore it.
                    orig = st.session_state.get('_dashboard_edit_original_name')
                    # Clean up edit flag first
                    if 'edit_dashboard' in st.session_state:
                        del st.session_state.edit_dashboard
                    # If original name exists and is a known dashboard, restore it
                    if orig:
                        try:
                            available = Dashboard().list_dashboards()
                            if orig in available:
                                st.session_state.dashboard = Dashboard(orig)
                                # remove stored original name
                                if '_dashboard_edit_original_name' in st.session_state:
                                    del st.session_state['_dashboard_edit_original_name']
                                st.rerun()
                        except Exception:
                            pass
                    # Otherwise (new unsaved dashboard) remove dashboard object and show selection
                    if 'dashboard' in st.session_state:
                        del st.session_state.dashboard
                    if '_dashboard_edit_original_name' in st.session_state:
                        del st.session_state['_dashboard_edit_original_name']
                    st.rerun()
            with col3:
                if st.button(":material/delete:"):
                    st.session_state.dashboard.delete()
                    st.session_state.dashboards = st.session_state.dashboard.list_dashboards()
                    del st.session_state.edit_dashboard
                    del st.session_state.dashboard
                    info_popup("Dashboard deleted")
        else:
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                if st.button(":material/refresh:"):
                    st.rerun()
            with col2:
                if st.button(":material/add_box:"):
                    if "dashboard" in st.session_state:
                        del st.session_state.dashboard
                    # Create a new unsaved dashboard; remember original (none)
                    st.session_state.dashboard = Dashboard()
                    st.session_state['_dashboard_edit_original_name'] = None
                    st.session_state.edit_dashboard = True
                    st.rerun()
            if "dashboard" in st.session_state:
                with col3:
                    if st.button(":material/edit:"):
                        if "edit_dashboard" not in st.session_state:
                            # Remember original dashboard name so cancel can restore it
                            try:
                                st.session_state['_dashboard_edit_original_name'] = st.session_state.dashboard.name
                            except Exception:
                                st.session_state['_dashboard_edit_original_name'] = None
                            st.session_state.edit_dashboard = True
                            st.rerun()
        for db in dashboards:
            if st.button(db):
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                if '_dashboard_edit_original_name' in st.session_state:
                    del st.session_state['_dashboard_edit_original_name']
                st.session_state.dashboard = Dashboard(db)
                st.rerun()
                
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
st.header("Dashboards", divider="red")

dashboard()
