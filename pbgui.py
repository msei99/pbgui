import streamlit as st

def build_navigation():
    
    # Single Pages
    pA1 = st.Page("pages/00_login.py", title="Welcome", icon=":material/logout:")
    
    pM1 = st.Page("pages/98_Setup API-Keys.py", title="API-Keys", icon=":material/key:")
    pM2 = st.Page("pages/99_Services.py", title="PBGUI Services", icon=":material/build:")
    pM3 = st.Page("pages/91_VPS Manager.py", title="VPS Manager", icon=":material/computer:")
    
    pSe1 = st.Page("pages/95_Dashboards.py", title="Dashboards", icon=":material/dashboard:")
    pSe2 = st.Page("pages/92_Coin Data.py", title="Coin Data", icon=":material/monetization_on:")

    pS1 = st.Page("pages/10_Single Run.py", title="Run", icon=":material/play_arrow:")
    pS2 = st.Page("pages/11_Single Backtest.py", title="Backtest", icon=":material/history:")
    pS3 = st.Page("pages/12_Single Optimize.py", title="Optimize", icon=":material/tune:")
    pS4 = st.Page("pages/92_Spot View.py", title="Spot View", icon=":material/remove_red_eye:")

    p61 = st.Page("pages/60_Multi Run.py", title="Run", icon=":material/play_arrow:")
    p62 = st.Page("pages/61_Multi Backtest.py", title="Backtest", icon=":material/history:")
    p63 = st.Page("pages/62_Multi Optimize.py", title="Optimize", icon=":material/tune:")

    p71 = st.Page("pages/70_V7 Run.py", title="Run", icon=":material/play_arrow:")
    p72 = st.Page("pages/71_V7 Backtest.py", title="Backtest", icon=":material/history:")
    p73 = st.Page("pages/72_V7 Optimize.py", title="Optimize", icon=":material/tune:")
    p74 = st.Page("pages/73_V7_Grid Visualizer.py", title="Grid Visualizer", icon=":material/remove_red_eye:")
       
    # Page Groups
    SystemPages = [pA1, pM1, pM2, pM3]
    InfotmationPages = [pSe1, pSe2]
    v7Pages = [p71, p72, p73, p74]
    v6Pages = [p61, p62, p63]
    SinglePages = [pS1, pS2, pS3, pS4]

    # Navigation
    pg = st.navigation(
        {
            "System": SystemPages,
            "Information": InfotmationPages,
            "PBv7": v7Pages,
            "PBv6 Multi": v6Pages,
            "PBv6 Single": SinglePages,
        },position="sidebar"
    )
    st.session_state.navigation = pg
    pg.run()
    
build_navigation()