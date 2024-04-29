import streamlit as st
from pathlib import Path
import json
from pbgui_func import validateJSON, config_pretty_str
import pbgui_help

class Config:
    def __init__(self, file_name = None, config = None):
        self._config_file = file_name
        self._long_we = 1.0
        self._short_we = 1.0
        self._long_enabled = True
        self._short_enabled = False
        self._type = None
        self._preview_grid = False
        if config:
            self.config = config
        else:
            self._config = None

    @property
    def type(self): return self._type

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
                self._config = new_config
                self.update_config()
                if "error_config" in st.session_state:
                    del st.session_state.error_config
            else:
                st.session_state.error_config = "Config is invalid"

    @config_file.setter
    def config_file(self, new_config_file):
        if self._config_file != new_config_file:
            self._config_file = new_config_file

    @property
    def long_we(self): return self._long_we

    @long_we.setter
    def long_we(self, new_long_we):
        self._long_we = round(new_long_we,2)
        if self._config:
            t = json.loads(self._config)
            t["long"]["wallet_exposure_limit"] = self._long_we
            self._config = config_pretty_str(t)
    
    @property
    def long_enabled(self): return self._long_enabled

    @long_enabled.setter
    def long_enabled(self, new_long_enabled):
        self._long_enabled = new_long_enabled
        if self._config:
            t = json.loads(self._config)
            t["long"]["enabled"] = self._long_enabled
            self._config = config_pretty_str(t)

    @property
    def short_enabled(self): return self._short_enabled

    @short_enabled.setter
    def short_enabled(self, new_short_enabled):
        self._short_enabled = new_short_enabled
        if self._config:
            t = json.loads(self._config)
            t["short"]["enabled"] = self._short_enabled
            self._config = config_pretty_str(t)

    @property
    def short_we(self): return self._short_we

    @short_we.setter
    def short_we(self, new_short_we):
        self._short_we = round(new_short_we,2)
        if self._config:
            t = json.loads(self._config)
            t["short"]["wallet_exposure_limit"] = self._short_we
            self._config = config_pretty_str(t)

    @property
    def preview_grid(self): return self._preview_grid
    @preview_grid.setter
    def preview_grid(self, new_preview_grid):
        self._preview_grid = new_preview_grid

    def update_config(self):
        self.long_we = json.loads(self._config)["long"]["wallet_exposure_limit"]
        self.short_we = json.loads(self._config)["short"]["wallet_exposure_limit"]
        self.long_enabled = json.loads(self._config)["long"]["enabled"]
        self.short_enabled = json.loads(self._config)["short"]["enabled"]
        long = json.loads(self._config)["long"]
        if "ddown_factor" in long:
            self._type = "recursive_grid"
        elif "qty_pct_entry" in long:
            self._type = "clock"
        elif "grid_span" in long:
            self._type = "neat_grid"

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
        # Init session_state for keys
        if "config_long_enabled" in st.session_state:
            if st.session_state.config_long_enabled != self.long_enabled:
                self.long_enabled = st.session_state.config_long_enabled
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_short_enabled" in st.session_state:
            if st.session_state.config_short_enabled != self.short_enabled:
                self.short_enabled = st.session_state.config_short_enabled
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_long_we" in st.session_state:
            if st.session_state.config_long_we != self.long_we:
                self.long_we = st.session_state.config_long_we
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_short_we" in st.session_state:
            if st.session_state.config_short_we != self.short_we:
                self.short_we = st.session_state.config_short_we
                if self.config:
                    st.session_state.config_instance_config = self.config
        if "config_preview_grid" in st.session_state:
            if st.session_state.config_preview_grid != self.preview_grid:
                self.preview_grid = st.session_state.config_preview_grid
        if "config_instance_config" in st.session_state:
            if st.session_state.config_instance_config != self.config:
                self.config = st.session_state.config_instance_config
                st.session_state.config_long_enabled = self.long_enabled
                st.session_state.config_short_enabled = self.short_enabled
                st.session_state.config_long_we = self.long_we
                st.session_state.config_short_we = self.short_we
        # if self.config:
        #     self.config = st.session_state.config_instance_config
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            st.toggle("Long enabled", value=self.long_enabled, key="config_long_enabled", help=None)
            st.number_input("LONG_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=3.0, value=float(round(self.long_we,2)), step=0.05, format="%.2f", key="config_long_we", help=pbgui_help.exposure)
        with col2:
            st.toggle("Short enabled", value=self.short_enabled, key="config_short_enabled", help=None)
            st.number_input("SHORT_WALLET_EXPOSURE_LIMIT", min_value=0.0, max_value=3.0, value=float(round(self.short_we,2)), step=0.05, format="%.2f", key="config_short_we", help=pbgui_help.exposure)
        with col3:
            st.toggle("Preview Grid", value=self.preview_grid, key="config_preview_grid", help=None)
            st.selectbox("Config Type", [self.type], index=0, key="config_type", help=None, disabled=True)
        # Display Error
        if "error_config" in st.session_state:
            st.error(st.session_state.error_config, icon="🚨")
        st.text_area("Instance config", self.config, key="config_instance_config", height=600)

def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
