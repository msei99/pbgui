import streamlit as st
from PBRun import PBRun
from PBRemote import PBRemote
from PBData import PBData
from PBCoinData import CoinData
from PBApiServer import PBApiServer

class Services():
    def __init__(self):
        if "pbrun" not in st.session_state:
            st.session_state.pbrun = PBRun()
        if "pbremote" not in st.session_state:
            st.session_state.pbremote = PBRemote()
        if "pbdata" not in st.session_state:
            st.session_state.pbdata = PBData()
        if "pbcoindata" not in st.session_state:
            st.session_state.pbcoindata = CoinData()
        if "api_server" not in st.session_state:
            st.session_state.api_server = PBApiServer()
        self.pbrun = st.session_state.pbrun
        self.pbremote = st.session_state.pbremote
        self.pbdata = st.session_state.pbdata
        self.pbcoindata = st.session_state.pbcoindata
        self.api_server = st.session_state.api_server

    def stop_all_started(self):
        self.pbrun_was_running = False
        self.pbremote_was_running = False
        self.pbdata_was_running = False
        self.pbcoindata_was_running = False
        if self.pbrun.is_running():
            self.pbrun_was_running = True
            self.pbrun.stop()
        if self.pbremote.is_running():
            self.pbremote_was_running = True
            self.pbremote.stop()
        if self.pbdata.is_running():
            self.pbdata_was_running = True
            self.pbdata.stop()
        if self.pbcoindata.is_running():
            self.pbcoindata_was_running = True
            self.pbcoindata.stop()
        if self.api_server.is_running():
            self.api_server_was_running = True
            self.api_server.stop()
    
    def start_all_was_running(self):
        if self.pbrun_was_running:
            self.pbrun.run()
        if self.pbremote_was_running:
            self.pbremote.run()
        if self.pbdata_was_running:
            self.pbdata.run()
        if self.pbcoindata_was_running:
            self.pbcoindata.run()
