import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_authenticted, get_navi_paths
from Dashboard import Dashboard

def dashboard():
    # Init dashboard
    # if "dashboard" not in st.session_state:
    #     st.session_state.dashboard = Dashboard()
    # dashboard = st.session_state.dashboard
    # Navigation
    if "dashboards" not in st.session_state:
        st.session_state.dashboards = Dashboard().list_dashboards()
    dashboards = st.session_state.dashboards
    with st.sidebar:
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button(":material/refresh:"):
                st.rerun()
        with col2:
            if st.button(":material/add_box:"):
                if "dashboard" in st.session_state:
                    del st.session_state.dashboard
                st.session_state.dashboard = Dashboard()
                st.session_state.edit_dashboard = True
                st.rerun()
        col1, col2, col3 = st.columns([1, 1, 2])
        if "dashboard" in st.session_state or "edit_dashboard" in st.session_state:
            with col1:
                if st.button(":material/save:"):
                    if "edit_dashboard" in st.session_state:
                        if st.session_state.dashboard.name:
                            st.session_state.dashboard.save()
                            st.session_state.dashboards = st.session_state.dashboard.list_dashboards()
                            del st.session_state.edit_dashboard
                            st.session_state.dashboard.load(st.session_state.dashboard.name)
                            st.rerun()
                        else:
                            error_popup("Name is empty")
                    elif "dashboard" in st.session_state:
                        st.session_state.dashboard.save()
                        st.rerun()
        if "dashboard" in st.session_state:
            with col2:
                if st.button(":material/edit:"):
                    if "edit_dashboard" not in st.session_state:
                        st.session_state.edit_dashboard = True
                        st.rerun()
            with col3:
                if st.button(":material/delete:"):
                    st.session_state.dashboard.delete()
                    st.session_state.dashboards = st.session_state.dashboard.list_dashboards()
                    del st.session_state.dashboard
                    info_popup("Dashboard deleted")
        for db in dashboards:
            if st.button(db):
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                st.session_state.dashboard = Dashboard(db)
                # dashboard = st.session_state.dashboard
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

if not "dashboard" in st.session_state:
    st.info("Please select a dashboard from the sidebar or create a new one.")

dashboard()
