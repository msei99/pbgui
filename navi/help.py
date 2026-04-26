"""Help & Tutorials redirect page.

This is a thin redirect to the FastAPI-hosted Help page.
The actual Help page is now served at /app/help.html via FastAPI.
"""

import streamlit as st

st.set_page_config(page_title="Help & Tutorials", layout="centered")

# Redirect to FastAPI Help page
help_url = "http://localhost:8501/app/help.html"

st.info("📚 Help & Tutorials are now hosted on the FastAPI backend for better performance.")

col1, col2 = st.columns([1, 1], vertical_alignment="center")
with col1:
    if st.button("🔗 Open Help Page", use_container_width=True):
        st.switch_page("page:" + help_url)

with col2:
    st.write("")  # Placeholder for alignment

st.markdown(
    f"""
    <meta http-equiv="refresh" content="3;url={help_url}" />
    If the button doesn't work, you will be redirected automatically in 3 seconds...
    """,
    unsafe_allow_html=True,
)
