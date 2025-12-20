import streamlit as st
from pbgui_func import build_navigation

st.logo("images/logo.png", size="large")

# Custom CSS to make help tooltips wider and taller
st.markdown("""
<style>
    /* Wider and taller help tooltips - multiple selectors for compatibility */
    [data-testid="stTooltipContent"],
    .stTooltipContent,
    div[data-baseweb="tooltip"] > div,
    div[data-baseweb="popover"] > div > div {
        max-width: 800px !important;
        min-width: 400px !important;
        max-height: 600px !important;
        overflow-y: auto !important;
    }
    [data-testid="stTooltipContent"] pre,
    [data-testid="stTooltipContent"] code,
    div[data-baseweb="tooltip"] pre,
    div[data-baseweb="popover"] pre {
        white-space: pre-wrap !important;
        word-wrap: break-word !important;
        font-size: 13px !important;
    }
</style>
""", unsafe_allow_html=True)

build_navigation()