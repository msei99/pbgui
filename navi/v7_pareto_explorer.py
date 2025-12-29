import streamlit as st
from pbgui_func import is_session_state_not_initialized, is_authenticted
from ParetoExplorer import ParetoExplorer

# Authentication check
if is_session_state_not_initialized() or not is_authenticted():
    st.switch_page("navi/system_login.py")

# Check if we have a path to analyze
if "pareto_explorer_path" not in st.session_state:
    st.error("❌ No optimization result selected")
    st.info("Please go to **PBv7 → Optimize → Results** and click the **🎯 Pareto Explorer** button")
    if st.button("← Back to Optimize"):
        st.session_state.view = "optimize"
        st.rerun()
    st.stop()

# Navigation sidebar
with st.sidebar:
    st.title("🎯 Pareto Explorer")
    
    if st.button("← Back to Optimize Results", use_container_width=True):
        if "pareto_explorer_path" in st.session_state:
            del st.session_state.pareto_explorer_path
        st.switch_page("navi/v7_optimize.py")
    
    st.markdown("---")
    st.caption(f"**Analyzing:**")
    st.caption(st.session_state.pareto_explorer_path)

# Run the Pareto Explorer
try:
    explorer = ParetoExplorer(st.session_state.pareto_explorer_path)
    explorer.run()
except Exception as e:
    st.error(f"❌ Error loading Pareto Explorer: {e}")
    st.exception(e)
    if st.button("← Back"):
        if "pareto_explorer_path" in st.session_state:
            del st.session_state.pareto_explorer_path
        st.switch_page("navi/v7_optimize.py")
