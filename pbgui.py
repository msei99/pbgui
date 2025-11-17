import streamlit as st
from pbgui_func import build_navigation
from disable_autocomplete import set_proper_autocomplete_attributes

st.logo("images/logo.png", size="large")

# Prevent browser password managers from incorrectly flagging text inputs
set_proper_autocomplete_attributes()

build_navigation()