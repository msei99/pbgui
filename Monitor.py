import streamlit as st
from datetime import datetime
import pandas as pd
from time import sleep
from pbgui_func import error_popup, info_popup
from MonitorConfig import MonitorConfig
from master.async_monitor import load_alert_snapshot
from pbgui_purefunc import PBGUI_VERSION

class Monitor():
    def __init__(self):
        self.server = None
        self.d_v7 = []
        self.d_multi = []
        self.d_single = []
        self.servers = []
        self.logfiles = []
        self.monitor_config = MonitorConfig()

        # self.mem_warning_v7 = 250.0
        # self.mem_error_v7 = 500.0
        # self.cpu_warning_v7 = 10.0
        # self.cpu_error_v7 = 15.0
        # self.error_warning_v7 = 100.0
        # self.error_error_v7 = 250.0
        # self.traceback_warning_v7 = 100.0
        # self.traceback_error_v7 = 250.0
        # self.mem_warning_multi = 250.0
        # self.mem_error_multi = 500.0
        # self.cpu_warning_multi = 5.0
        # self.cpu_error_multi = 10.0
        # self.error_warning_multi = 25.0
        # self.error_error_multi = 50.0
        # self.traceback_warning_multi = 25.0
        # self.traceback_error_multi = 50.0
        # self.mem_warning_single = 50.0
        # self.mem_error_single = 100.0
        # self.cpu_warning_single = 5.0
        # self.cpu_error_single = 10.0
        # self.error_warning_single = 25.0
        # self.error_error_single = 50.0
        # self.traceback_warning_single = 25.0
        # self.traceback_error_single = 50.0
        # self.load_monitor_config()

    def view_server(self):
        server = self.server
        if server.is_online():
            color = "green"
        else: color = "red"
        st.markdown(f'### Remote Server: :{color}[{server.name}] ({server.rtd}s) PBGui: {PBGUI_VERSION}')
        if not server.mem or not server.disk or not server.swap:
            st.warning("Server is not online or not responding", icon="⚠️")
            return
        col_1, col_2 = st.columns([1,1])
        with col_1:
            mem_total = int(server.mem[0] / 1024 / 1024)
            mem_free = int(server.mem[1] / 1024 / 1024)
            mem_used = int(server.mem[3] / 1024 / 1024)
            mem_usage = int(server.mem[2])
            st.progress(mem_usage, text=f'### Memory Free: :green[{mem_free}] MB  |  Used: :red[{mem_used}] MB  |  Total: :blue[{mem_total}] MB')
            disk_total = int(server.disk[0] / 1024 / 1024)
            disk_used = int(server.disk[1] / 1024 / 1024)
            disk_free = int(server.disk[2] / 1024 / 1024)
            disk_usage = int(server.disk[3])
            st.progress(disk_usage, text=f'### Disk Free: :green[{disk_free}] MB  |  Used: :red[{disk_used}] MB  |  Total: :blue[{disk_total}] MB')
        with col_2:
            swap_total = int(server.swap[0] / 1024 / 1024)
            swap_used = int(server.swap[1] / 1024 / 1024)
            swap_free = int(server.swap[2] / 1024 / 1024)
            swap_usage = min(int(server.swap[3]),100)
            st.progress(swap_usage, text=f'### Swap Free: :green[{swap_free}] MB  |  Used: :red[{swap_used}] MB  |  Total: :blue[{swap_total}] MB')
            boot = datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S")
            if server.cpu > self.monitor_config.cpu_error_server:
                cpu_color = "red"
            elif server.cpu < self.monitor_config.cpu_warning_server:
                cpu_color = "green"
            else:
                cpu_color = "orange"
            st.markdown(f"##### CPU utilization: :{cpu_color}[{server.cpu}] %  |  System boot: :blue[{boot}]")

    def view_server_instances(self):
        v7_selected = None
        if f"pbremote_v7_select" in st.session_state:
            v7_selected = st.session_state.pbremote_v7_select
        multi_selected = None
        if f"pbremote_multi_select" in st.session_state:
            multi_selected = st.session_state.pbremote_multi_select
        single_selected = None
        if f"pbremote_single_select" in st.session_state:
            single_selected = st.session_state.pbremote_single_select
        if not self.d_v7 and not self.d_multi and not self.d_single:
            # d_v7 = []
            # d_multi = []
            # d_single = []
            self.logfiles = []
            instances_by_server = (load_alert_snapshot().get('instances') or {})
            for server in self.servers:
                for monitor in instances_by_server.get(server.name) or []:
                        version = server.pb7_version
                        if len(monitor["m"]) == 10:
                            swap_value = monitor["m"][9]
                        else:
                            swap_value = 0
                        info = ({
                            # u = user
                            # p = pb_version
                            # v = version
                            # st = start_time
                            # m = memory
                            # c = cpu
                            # i = info
                            # it = infos_today
                            # iy = infos_yesterday
                            # e = error
                            # et = errors_today
                            # t = traceback
                            # tt = tracebacks_today
                            # pt = pnl_today
                            # ct = pnl_counter_today
                            # pnl_hist_total = pnl_history_total
                            # pnls_hist_total = pnl_history_fills
                            'Server': server.name,
                            'Version': version,
                            'Name': monitor["u"],
                            'PB Version': monitor["p"],
                            'Version': version,
                            'Start Time': datetime.fromtimestamp(monitor["st"]),
                            'Memory': monitor["m"][0]/1024/1024,
                            'Swap': swap_value/1024/1024,
                            'CPU': monitor["c"],
                            'PNLs Today': monitor["ct"],
                            'PNL Today': monitor["pt"],
                            'PNLs Hist': monitor.get("pnls_hist_total", 0),
                            'PNL Hist': monitor.get("pnl_hist_total", 0.0),
                            'Last Info': monitor["i"],
                            'Infos Today': monitor["it"],
                            'Infos Yesterday': monitor["iy"],
                            'Last Error': monitor["e"],
                            'Errors Today': monitor["et"],
                            'Errors 4W': monitor.get("errors_4w", 0),
                            'Last Traceback': monitor["t"],
                            'Tracebacks Today': monitor["tt"],
                            'Tracebacks 4W': monitor.get("tracebacks_4w", 0)
                        })
                        if info["PB Version"] == "7":
                            self.d_v7.append(info)
                            self.logfiles.append(f'run_v7/{info["Name"]}/passivbot.log')
                        elif info["PB Version"] == "6":
                            self.d_multi.append(info)
                            self.logfiles.append(f'multi/{info["Name"]}/passivbot.log')
                        elif info["PB Version"] == "s":
                            self.d_single.append(info)
                            self.logfiles.append(f'instances/{info["Name"]}/passivbot.log')

        column_config = {
            "PB Version": None,
            "Last Info": None,
            "Last Error": None,
            "Last Traceback": None,
            "Memory": st.column_config.NumberColumn(format="%.2f MB"),
            "Swap": st.column_config.NumberColumn(format="%.2f MB"),
            "CPU": st.column_config.NumberColumn(format="%.2f %%"),
        }

        if self.d_v7:
            st.subheader(f"Running V7 Instances ({len(self.d_v7)})")
            df = pd.DataFrame(self.d_v7)
            sdf = df.style.map(lambda x: 'color: green' if x < self.monitor_config.cpu_warning_v7 else 'color: orange' if x < self.monitor_config.cpu_error_v7 else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < self.monitor_config.mem_warning_v7 else 'color: orange' if x < self.monitor_config.mem_error_v7 else 'color: red', subset=['Memory'])
            sdf = sdf.map(lambda x: 'color: green' if x < self.monitor_config.swap_warning_v7 else 'color: orange' if x < self.monitor_config.swap_error_v7 else 'color: red', subset=['Swap'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB", 'Swap': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < self.monitor_config.error_warning_v7 else 'color: orange' if x < self.monitor_config.error_error_v7 else 'color: red', subset=['Errors Today', 'Errors 4W'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < self.monitor_config.traceback_warning_v7 else 'color: orange' if x < self.monitor_config.traceback_error_v7 else 'color: red', subset=['Tracebacks Today', 'Tracebacks 4W'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: LightCoral', subset=['PNL Today', 'PNL Hist'])
            st.dataframe(data=sdf, height=36+(len(self.d_v7))*35, key="pbremote_v7_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if v7_selected:
                if v7_selected["selection"]["rows"]:
                    row = v7_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{self.d_v7[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{self.d_v7[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{self.d_v7[row]['Last Traceback']}]")
                    if st.button("Restart", key=f"restart_{self.d_v7[row]['Name']}"):
                        v7_instances = st.session_state.v7_instances
                        version = v7_instances.fetch_instance_version(self.d_v7[row]['Name']) + 1
                        v7_instances.restart_instance(self.d_v7[row]['Name'])
                        timeout = 180
                        with st.spinner(f'Restarting {self.d_v7[row]["Name"]}...'):
                            with st.empty():
                                while version != v7_instances.fetch_instance_version(self.d_v7[row]['Name']):
                                    st.text(f'{timeout} seconds left')
                                    sleep(1)
                                    timeout -= 1
                                    if timeout == 0:
                                        break
                                st.text(f'{timeout} seconds left')
                            st.text(f'')
                            if timeout == 0:
                                error_popup("Restart failed")
                            else:
                                info_popup(f"{self.d_v7[row]['Name']} restarted")
        if self.d_multi:
            st.subheader(f"Running Multi Instances ({len(self.d_multi)})")
            df = pd.DataFrame(self.d_multi)
            sdf = df.style
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB", 'Swap': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors/tracebacks no longer have dedicated Multi thresholds
            sdf = sdf.map(lambda x: 'color: green' if x == 0 else 'color: orange' if x < 10 else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            sdf = sdf.map(lambda x: 'color: green' if x == 0 else 'color: orange' if x < 5 else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: LightCoral', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, height=36+(len(self.d_multi))*35, key="pbremote_multi_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if multi_selected:
                if multi_selected["selection"]["rows"]:
                    row = multi_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{self.d_multi[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{self.d_multi[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{self.d_multi[row]['Last Traceback']}]")

        if self.d_single:
            st.subheader(f"Running Single Instances ({len(self.d_single)})")
            df = pd.DataFrame(self.d_single)
            sdf = df.style
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB", 'Swap': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors/tracebacks no longer have dedicated Single thresholds
            sdf = sdf.map(lambda x: 'color: green' if x == 0 else 'color: orange' if x < 10 else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            sdf = sdf.map(lambda x: 'color: green' if x == 0 else 'color: orange' if x < 5 else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: LightCoral', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, height=36+(len(self.d_single))*35, key="pbremote_single_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if single_selected:
                if single_selected["selection"]["rows"]:
                    row = single_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{self.d_single[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{self.d_single[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{self.d_single[row]['Last Traceback']}]")

    def edit_monitor_config(self, show_navigation=True):
        field_groups = {
            "server": ["mem_warning", "mem_error", "swap_warning", "swap_error", "disk_warning", "disk_error", "cpu_warning", "cpu_error"],
            "v7": ["mem_warning", "mem_error", "swap_warning", "swap_error", "cpu_warning", "cpu_error", "error_warning", "error_error", "traceback_warning", "traceback_error"],
        }

        for suffix, fields in field_groups.items():
            for field in fields:
                attr = f"{field}_{suffix}"
                key = f"edit_{attr}"
                if key in st.session_state:
                    value = st.session_state[key]
                    if getattr(self.monitor_config, attr) != value:
                        setattr(self.monitor_config, attr, value)
                else:
                    st.session_state[key] = getattr(self.monitor_config, attr)

        if show_navigation:
            with st.sidebar:
                if st.button(":material/home:"):
                    for suffix, fields in field_groups.items():
                        for field in fields:
                            st.session_state.pop(f"edit_{field}_{suffix}", None)
                    self.monitor_config.load_monitor_config()
                    st.session_state.pop("monitor_edit", None)
                    st.session_state.pop("monitor", None)
                    st.rerun()
                if st.button(":material/save:"):
                    self.monitor_config.save_monitor_config()
                    st.session_state.pop("monitor_edit", None)
                    st.rerun()

        st.header("Edit Monitor Settings")

        st.subheader("Server Monitor Settings")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('Memory Warning', step=10.0, format="%.1f", key="edit_mem_warning_server")
            st.number_input('Swap Warning', step=10.0, format="%.1f", key="edit_swap_warning_server")
        with col2:
            st.number_input('Memory Error', step=10.0, format="%.1f", key="edit_mem_error_server")
            st.number_input('Swap Error', step=10.0, format="%.1f", key="edit_swap_error_server")
        with col3:
            st.number_input('Disk Warning', step=10.0, format="%.1f", key="edit_disk_warning_server")
            st.number_input('CPU Warning', step=1.0, format="%.1f", key="edit_cpu_warning_server")
        with col4:
            st.number_input('Disk Error', step=10.0, format="%.1f", key="edit_disk_error_server")
            st.number_input('CPU Error', step=1.0, format="%.1f", key="edit_cpu_error_server")

        st.subheader("V7 Monitor Settings")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('Memory Warning', step=10.0, format="%.1f", key="edit_mem_warning_v7")
            st.number_input('Swap Warning', step=10.0, format="%.1f", key="edit_swap_warning_v7")
            st.number_input('Error Warning', step=1.0, format="%.1f", key="edit_error_warning_v7")
        with col2:
            st.number_input('Memory Error', step=10.0, format="%.1f", key="edit_mem_error_v7")
            st.number_input('Swap Error', step=10.0, format="%.1f", key="edit_swap_error_v7")
            st.number_input('Error Error', step=1.0, format="%.1f", key="edit_error_error_v7")
        with col3:
            st.number_input('CPU Warning', step=1.0, format="%.1f", key="edit_cpu_warning_v7")
            st.number_input('Traceback Warning', step=1.0, format="%.1f", key="edit_traceback_warning_v7")
        with col4:
            st.number_input('CPU Error', step=1.0, format="%.1f", key="edit_cpu_error_v7")
            st.number_input('Traceback Error', step=1.0, format="%.1f", key="edit_traceback_error_v7")
