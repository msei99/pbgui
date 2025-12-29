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
    
    # Extract directory name and show shortened version
    from pathlib import Path
    result_path = st.session_state.pareto_explorer_path
    result_name = Path(result_path).name
    
    # Extract meaningful parts: Date + Symbol (e.g., "2025-12-24 | DOGE")
    parts = result_name.split('_')
    if len(parts) >= 5:
        # Format: YYYY-MM-DDTHH_MM_SS_exchange1_exchange2_XXXdays_SYMBOL_hash
        date_part = parts[0].replace('T', ' ')  # "2025-12-24 07:04:00"
        date_short = date_part.split()[0]  # Just the date "2025-12-24"
        
        # Find symbol (usually before the last part which is the hash)
        symbol = parts[-2] if len(parts) > 1 else ""
        
        display_name = f"{date_short} | {symbol}"
    else:
        # Fallback to first 35 chars
        display_name = result_name[:35] + ("..." if len(result_name) > 35 else "")
    
    st.caption(display_name, help=f"📂 {result_name}")
    
    if st.button("← Back to Optimize Results", use_container_width=True):
        if "pareto_explorer_path" in st.session_state:
            del st.session_state.pareto_explorer_path
        st.switch_page("navi/v7_optimize.py")

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
