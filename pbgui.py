import streamlit as st
from pbgui_func import build_navigation

st.logo("images/logo.png", size="large")

# Custom CSS to make help tooltips wider and taller
st.markdown("""
<style>
    /* Wider and taller help tooltips - only for actual tooltips, not selectbox popovers */
    [data-testid="stTooltipContent"] {
        max-width: 800px !important;
        min-width: 400px !important;
        max-height: 600px !important;
        overflow-y: auto !important;
    }
    
    [data-testid="stTooltipContent"] pre,
    [data-testid="stTooltipContent"] code {
        white-space: pre-wrap !important;
        word-wrap: break-word !important;
        font-size: 13px !important;
    }
</style>
""", unsafe_allow_html=True)
build_navigation()