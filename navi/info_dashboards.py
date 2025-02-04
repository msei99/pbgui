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
                        del st.session_state.edit_dashboard
                        st.session_state.dashboard.load(st.session_state.dashboard.name)
                        st.rerun()
                    else:
                        error_popup("Name is empty")
            with col2:
                if st.button(":material/cancel:"):
                    del st.session_state.edit_dashboard
                    del st.session_state.dashboard
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
                    st.session_state.dashboard = Dashboard()
                    st.session_state.edit_dashboard = True
                    st.rerun()
            if "dashboard" in st.session_state:
                with col3:
                    if st.button(":material/edit:"):
                        if "edit_dashboard" not in st.session_state:
                            st.session_state.edit_dashboard = True
                            st.rerun()
        for db in dashboards:
            if st.button(db):
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
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
