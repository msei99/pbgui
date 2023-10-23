import streamlit as st
from pathlib import Path
import json
from pbgui_func import validateJSON, config_pretty_str
import pbgui_help

class Config:
    def __init__(self, file_name = None):
        self._config_file = file_name
        self._config = None
        self._long_we = 1.0
        self._short_we = 1.0
        self._long_enabled = True
        self._short_enabled = False

    @property
    def config_file(self): return self._config_file

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file
        
    @property
    def config(self): return self._config

    @config.setter
    def config(self, new_config):
        if new_config != "None":
            if validateJSON(new_config):
                if self._config != new_config:
                    if new_config:
                        self._config = new_config
                        self.update_config()
                        st.experimental_rerun()
                if "error" in st.session_state:
                    del st.session_state.error
                    st.experimental_rerun()
            else:
                if not "error" in st.session_state:
                    st.session_state.error = "Config is invalid"
                    st.experimental_rerun() 

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file

    @property
    def long_we(self): return self._long_we

    @long_we.setter
    def long_we(self, new_long_we):
        if self._long_we != new_long_we:
            self._long_we = new_long_we
            t = json.loads(self._config)
            t["long"]["wallet_exposure_limit"] = self._long_we
            self.config = config_pretty_str(t)
            st.experimental_rerun()
    
    @property
    def long_enabled(self): return self._long_enabled

    @long_enabled.setter
    def long_enabled(self, new_long_enabled):
        if self._long_enabled != new_long_enabled:
            self._long_enabled = new_long_enabled
            t = json.loads(self._config)
            t["long"]["enabled"] = self._long_enabled
            self.config = config_pretty_str(t)
            st.experimental_rerun()

    @property
    def short_enabled(self): return self._short_enabled

    @short_enabled.setter
    def short_enabled(self, new_short_enabled):
        if self._short_enabled != new_short_enabled:
            self._short_enabled = new_short_enabled
            t = json.loads(self._config)
            t["short"]["enabled"] = self._short_enabled
            self.config = config_pretty_str(t)
            st.experimental_rerun()

    @property
    def short_we(self): return self._short_we

    @short_we.setter
    def short_we(self, new_short_we):
        if self._short_we != new_short_we:
            self._short_we = new_short_we
            t = json.loads(self._config)
            t["short"]["wallet_exposure_limit"] = self._short_we
            self.config = config_pretty_str(t)
            st.experimental_rerun()

    def update_config(self):
        self._long_we = json.loads(self._config)["long"]["wallet_exposure_limit"]
        self._short_we = json.loads(self._config)["short"]["wallet_exposure_limit"]
        self._long_enabled = json.loads(self._config)["long"]["enabled"]
        self._short_enabled = json.loads(self._config)["short"]["enabled"]

    def load_config(self):
        file =  Path(f'{self._config_file}')
        if file.exists():
            with open(file, "r", encoding='utf-8') as f:
                self._config = f.read()
                self.update_config()

    def save_config(self):
        if self._config != None and self._config_file != None:
            file = Path(f'{self._config_file}')
            with open(file, "w", encoding='utf-8') as f:
                f.write(self._config)

    def edit_config(self):
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            self.long_enabled = st.toggle("Long enabled", value=self.long_enabled, key="config_long_enabled", help=None)
            self.long_we = st.number_input("LONG_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=3.0, value=float(round(self.long_we,2)), step=0.05, format="%.2f", key="config_long_we", help=pbgui_help.exposure)
        with col2:
            self.short_enabled = st.toggle("Short enabled", value=self.short_enabled, key="config_short_enabled", help=None)
            self.short_we = st.number_input("SHORT_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=3.0, value=float(round(self.short_we,2)), step=0.05, format="%.2f", key="config_short_we", help=pbgui_help.exposure)
        self.config = st.text_area("Instance config", self.config, key="config_instance_config", height=600)

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
