import streamlit as st
from PBRun import PBRun
from PBStat import PBStat
from PBRemote import PBRemote
from PBShare import PBShare
from PBData import PBData

class Services():
    def __init__(self):
        if "pbrun" not in st.session_state:
            st.session_state.pbrun = PBRun()
        if "pbremote" not in st.session_state:
            st.session_state.pbremote = PBRemote()
        if "pbstat" not in st.session_state:
            st.session_state.pbstat = PBStat()
        if "pbshare" not in st.session_state:
            st.session_state.pbshare = PBShare()
        if "pbdata" not in st.session_state:
            st.session_state.pbdata = PBData()
        self.pbrun = st.session_state.pbrun
        self.pbremote = st.session_state.pbremote
        self.pbstat = st.session_state.pbstat
        self.pbshare = st.session_state.pbshare
        self.pbdata = st.session_state.pbdata

    def stop_all_started(self):
        self.pbrun_was_running = False
        self.pbremote_was_running = False
        self.pbstat_was_running = False
        self.pbshare_was_running = False
        self.pbdata_was_running = False
        if self.pbrun.is_running():
            self.pbrun_was_running = True
            self.pbrun.stop()
        if self.pbremote.is_running():
            self.pbremote_was_running = True
            self.pbremote.stop()
        if self.pbstat.is_running():
            self.pbstat_was_running = True
            self.pbstat.stop()
        if self.pbshare.is_running():
            self.pbshare_was_running = True
            self.pbshare.stop()
        if self.pbdata.is_running():
            self.pbdata_was_running = True
            self.pbdata.stop()
    
    def start_all_was_running(self):
        if self.pbrun_was_running:
            self.pbrun.run()
        if self.pbremote_was_running:
            self.pbremote.run()
        if self.pbstat_was_running:
            self.pbstat.run()
        if self.pbshare_was_running:
            self.pbshare.run()
        if self.pbdata_was_running:
            self.pbdata.run()
    
def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
