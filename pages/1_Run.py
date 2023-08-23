import streamlit as st
from streamlit_extras.switch_page_button import switch_page

st.set_page_config(
    page_title="Passivbot GUI - Run",
    page_icon=":screwdriver:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
#        'Get Help': 'https://www.extremelycoolapp.com/help',
        'About': "Passivbot GUI"
    }
)

# Init Session State
if 'pbdir' not in st.session_state:
    switch_page("pbgui")

st.header("Run")


