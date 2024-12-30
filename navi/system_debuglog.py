import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_authenticted, get_navi_paths, PBGDIR, get_debuglog
import logging
import os
import random
from pathlib import Path
from Log import LogHandler
import time

def log_viewer_page(max_display_lines=1000, refresh_interval_seconds=3):

    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    
    # Retrieve the logger from session state
    debuglog = get_debuglog()  
    debuglog_path = debuglog.get_log_path()

    # Buttons to clear or rotate the log
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        if st.button("Clear Log"):
            debuglog.clear_log()
            st.success("Log file cleared.")

    with col2:
        if st.button("Rotate Log"):
            debuglog.rotate_logs()
            st.success("Logs rotated.")

    with col3:
        if os.path.exists(debuglog_path):
            with open(debuglog_path, "r") as f:
                log_content = f.read()
            st.download_button(
                label="Download Log",
                data=log_content,
                file_name=os.path.basename(debuglog_path),
                mime="text/plain",
            )
    
    with col4:
        if st.button("Generate Test Log Entry"):
            levels = [
                (debuglog.debug, "DEBUG"),
                (debuglog.info, "INFO"),
                (debuglog.warning, "WARNING"),
                (debuglog.error, "ERROR"),
                (debuglog.critical, "CRITICAL"),
            ]
            chosen_method, level_str = random.choice(levels)
            msg = f"Random log message at level {level_str}."
            chosen_method(msg)
    
    with col5:
        if st.button('Toggle Auto Refresh'):
            st.session_state['auto_refresh'] = not st.session_state['auto_refresh']

    # Show the content of the log file
    if os.path.exists(debuglog_path):
        with open(debuglog_path, "r") as f:
            lines = f.readlines()
            if len(lines) > max_display_lines:
                lines = lines[-max_display_lines:]
            log_text = "".join(lines)
    else:
        log_text = "No log file found."

    st.subheader("Live Log Output")
    st.code(log_text, language="python")
    
    
    while st.session_state['auto_refresh']:
        time.sleep(refresh_interval_seconds)  # Refresh every 10 seconds
        st.rerun()


# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("DEBUGLOG")
st.header("DEBUGLOG", divider="red")
st.info("This page is for developers to make debugging more convenient. There's no relevant information for users here.")

log_viewer_page()
