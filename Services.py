import streamlit as st
from PBRun import PBRun
from PBStat import PBStat
from PBRemote import PBRemote
from PBMon import PBMon
from PBData import PBData
from PBCoinData import CoinData

class Services():
    def __init__(self):
        if "pbrun" not in st.session_state:
            st.session_state.pbrun = PBRun()
        if "pbremote" not in st.session_state:
            st.session_state.pbremote = PBRemote()
        if "pbmon" not in st.session_state:
            st.session_state.pbmon = PBMon()
        if "pbstat" not in st.session_state:
            st.session_state.pbstat = PBStat()
        if "pbdata" not in st.session_state:
            st.session_state.pbdata = PBData()
        if "pbcoindata" not in st.session_state:
            st.session_state.pbcoindata = CoinData()
        self.pbrun = st.session_state.pbrun
        self.pbremote = st.session_state.pbremote
        self.pbmon = st.session_state.pbmon
        self.pbstat = st.session_state.pbstat
        self.pbdata = st.session_state.pbdata
        self.pbcoindata = st.session_state.pbcoindata

        # Auto-start PBData and PBCoinData on first initialization
        if "services_autostarted" not in st.session_state:
            if not self.pbdata.is_running():
                self.pbdata.run()
            if not self.pbcoindata.is_running():
                self.pbcoindata.run()
            st.session_state.services_autostarted = True

    def stop_all_started(self):
        self.pbrun_was_running = False
        self.pbremote_was_running = False
        self.pbmon_was_running = False
        self.pbstat_was_running = False
        self.pbdata_was_running = False
        self.pbcoindata_was_running = False
        if self.pbrun.is_running():
            self.pbrun_was_running = True
            self.pbrun.stop()
        if self.pbremote.is_running():
            self.pbremote_was_running = True
            self.pbremote.stop()
        if self.pbmon.is_running():
            self.pbmon_was_running = True
            self.pbmon.stop()
        if self.pbstat.is_running():
            self.pbstat_was_running = True
            self.pbstat.stop()
        if self.pbdata.is_running():
            self.pbdata_was_running = True
            self.pbdata.stop()
        if self.pbcoindata.is_running():
            self.pbcoindata_was_running = True
            self.pbcoindata.stop()
    
    def start_all_was_running(self):
        if self.pbrun_was_running:
            self.pbrun.run()
        if self.pbremote_was_running:
            self.pbremote.run()
        if self.pbmon_was_running:
            self.pbmon.run()
        if self.pbstat_was_running:
            self.pbstat.run()
        if self.pbdata_was_running:
            self.pbdata.run()
        if self.pbcoindata_was_running:
            self.pbcoindata.run()
    
def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
