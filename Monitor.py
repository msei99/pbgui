import streamlit as st
from pbgui_purefunc import load_ini, save_ini
from datetime import datetime
import pandas as pd
from time import sleep
from pbgui_func import error_popup, info_popup

class Monitor():
    def __init__(self):
        if "mem_warning_v7" not in st.session_state:
            self.load_monitor_config()
        self.server = None
        self.servers = []
        self.logfiles = []

    def view_server(self):
        server = self.server
        if server.is_online():
            color = "green"
        else: color = "red"
        st.markdown(f'### Remote Server: :{color}[{server.name}] ({server.rtd}s) PBGui: {server.pbgui_version}')
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
            if server.cpu > 90:
                cpu_color = "red"
            elif server.cpu < 50:
                cpu_color = "green"
            else:
                cpu_color = "yellow"
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
        d_v7 = []
        d_multi = []
        d_single = []
        self.logfiles = []
        for server in self.servers:
            if server.monitor:
                for monitor in server.monitor:
                    if monitor["p"] == "7":
                        version = server.pb7_version
                    else:
                        version = server.pb6_version
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
                        # ey = errors_yesterday
                        # t = traceback
                        # tt = tracebacks_today
                        # ty = tracebacks_yesterday
                        # pt = pnl_today
                        # py = pnl_yesterday
                        # ct = pnl_counter_today
                        # cy = pnl_counter_yesterday
                        'Server': server.name,
                        'Version': version,
                        'Name': monitor["u"],
                        'PB Version': monitor["p"],
                        'Version': version,
                        'Start Time': datetime.fromtimestamp(monitor["st"]),
                        'Memory': monitor["m"][0]/1024/1024,
                        'CPU': monitor["c"],
                        'PNLs Today': monitor["ct"],
                        'PNL Today': monitor["pt"],
                        'PNLs Yesterday': monitor["cy"],
                        'PNL Yesterday': monitor["py"],
                        'Last Info': monitor["i"],
                        'Infos Today': monitor["it"],
                        'Infos Yesterday': monitor["iy"],
                        'Last Error': monitor["e"],
                        'Errors Today': monitor["et"],
                        'Errors Yesterday': monitor["ey"],
                        'Last Traceback': monitor["t"],
                        'Tracebacks Today': monitor["tt"],
                        'Tracebacks Yesterday': monitor["ty"]
                    })
                    if info["PB Version"] == "7":
                        d_v7.append(info)
                        self.logfiles.append(f'run_v7/{info["Name"]}/passivbot.log')
                    elif info["PB Version"] == "6":
                        d_multi.append(info)
                        self.logfiles.append(f'multi/{info["Name"]}/passivbot.log')
                    elif info["PB Version"] == "s":
                        d_single.append(info)
                        self.logfiles.append(f'instances/{info["Name"]}/passivbot.log')
            column_config = {
                "PB Version": None,
                "Last Info": None,
                "Last Error": None,
                "Last Traceback": None,
                "Memory": st.column_config.NumberColumn(format="%.2f MB"),
                "CPU": st.column_config.NumberColumn(format="%.2f %%"),
            }

        if d_v7:
            st.subheader(f"Running V7 Instances ({len(d_v7)})")
            df = pd.DataFrame(d_v7)
            sdf = df.style.map(lambda x: 'color: green' if x < float(st.session_state.cpu_warning_v7) else 'color: orange' if x < float(st.session_state.cpu_error_v7) else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.mem_warning_v7) else 'color: orange' if x < float(st.session_state.mem_error_v7) else 'color: red', subset=['Memory'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.error_warning_v7) else 'color: orange' if x < float(st.session_state.error_error_v7) else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.traceback_warning_v7) else 'color: orange' if x < float(st.session_state.traceback_error_v7) else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: LightCoral', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, use_container_width=True, height=36+(len(d_v7))*35, key="pbremote_v7_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if v7_selected:
                if v7_selected["selection"]["rows"]:
                    row = v7_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{d_v7[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{d_v7[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{d_v7[row]['Last Traceback']}]")
                    if st.button("Restart", key=f"restart_{d_v7[row]['Name']}"):
                        v7_instances = st.session_state.v7_instances
                        version = v7_instances.fetch_instance_version(d_v7[row]['Name']) + 1
                        v7_instances.restart_instance(d_v7[row]['Name'])
                        timeout = 120
                        with st.spinner(f'Restarting {d_v7[row]["Name"]}...'):
                            with st.empty():
                                while version != v7_instances.fetch_instance_version(d_v7[row]['Name']):
                                    st.text(f'{timeout} seconds left')
                                    sleep(1)
                                    print(v7_instances.fetch_instance_version(d_v7[row]['Name']))
                                    timeout -= 1
                                    if timeout == 0:
                                        break
                                st.text(f'{timeout} seconds left')
                            st.text(f'')
                            if timeout == 0:
                                error_popup("Restart failed")
                            else:
                                info_popup(f"{d_v7[row]['Name']} restarted")


        
        if d_multi:
            st.subheader(f"Running Multi Instances ({len(d_multi)})")
            df = pd.DataFrame(d_multi)
            sdf = df.style.map(lambda x: 'color: green' if x < float(st.session_state.cpu_warning_v7) else 'color: orange' if x < float(st.session_state.cpu_error_v7) else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.mem_warning_v7) else 'color: orange' if x < float(st.session_state.mem_error_v7) else 'color: red', subset=['Memory'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.error_warning_v7) else 'color: orange' if x < float(st.session_state.error_error_v7) else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.traceback_warning_v7) else 'color: orange' if x < float(st.session_state.traceback_error_v7) else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: LightCoral', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, use_container_width=True, height=36+(len(d_multi))*35, key="pbremote_multi_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if multi_selected:
                if multi_selected["selection"]["rows"]:
                    row = multi_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{d_multi[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{d_multi[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{d_multi[row]['Last Traceback']}]")

        if d_single:
            st.subheader(f"Running Single Instances ({len(d_single)})")
            df = pd.DataFrame(d_single)
            sdf = df.style.map(lambda x: 'color: green' if x < float(st.session_state.cpu_warning_v7) else 'color: orange' if x < float(st.session_state.cpu_error_v7) else 'color: red', subset=['CPU'])
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.mem_warning_v7) else 'color: orange' if x < float(st.session_state.mem_error_v7) else 'color: red', subset=['Memory'])
            sdf = sdf.format({'CPU': "{:.2f} %", 'Start Time': "{:%Y-%m-%d %H:%M:%S}", 'Memory': "{:.2f} MB"})
            #Infos green if > 0, orange if 0 and red if none
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: red', subset=['Infos Today', 'Infos Yesterday'])
            #Errors green if 0, orange if <10 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.error_warning_v7) else 'color: orange' if x < float(st.session_state.error_error_v7) else 'color: red', subset=['Errors Today', 'Errors Yesterday'])
            #Tracebacks green if 0, orange if <5 else red
            sdf = sdf.map(lambda x: 'color: green' if x < float(st.session_state.traceback_warning_v7) else 'color: orange' if x < float(st.session_state.traceback_error_v7) else 'color: red', subset=['Tracebacks Today', 'Tracebacks Yesterday'])
            #PNL green if > 0, orange if 0 else red
            sdf = sdf.map(lambda x: 'color: green' if x > 0 else 'color: orange' if x == 0 else 'color: LightCoral', subset=['PNL Today', 'PNL Yesterday'])
            st.dataframe(data=sdf, use_container_width=True, height=36+(len(d_single))*35, key="pbremote_single_select" ,selection_mode='single-row', on_select="rerun", column_config=column_config)
            if single_selected:
                if single_selected["selection"]["rows"]:
                    row = single_selected["selection"]["rows"][0]
                    # st.subheader(f"{d_v7[row]['Name']}")
                    st.markdown(f":green[Last Info: ] :blue[{d_single[row]['Last Info']}]")
                    st.markdown(f":orange[Last Error: ] :blue[{d_single[row]['Last Error']}]")
                    st.markdown(f":red[Last Traceback: ] :blue[{d_single[row]['Last Traceback']}]")

    def load_monitor_config(self):
        st.session_state.mem_warning_v7 = load_ini("monitor", "mem_warning_v7")
        if st.session_state.mem_warning_v7 == "":
            st.session_state.mem_warning_v7 = 250
        st.session_state.mem_error_v7 = load_ini("monitor", "mem_error_v7")
        if st.session_state.mem_error_v7 == "":
            st.session_state.mem_error_v7 = 500
        st.session_state.cpu_warning_v7 = load_ini("monitor", "cpu_warning_v7")
        if st.session_state.cpu_warning_v7 == "":
            st.session_state.cpu_warning_v7 = 10
        st.session_state.cpu_error_v7 = load_ini("monitor", "cpu_error_v7")
        if st.session_state.cpu_error_v7 == "":
            st.session_state.cpu_error_v7 = 15
        st.session_state.error_warning_v7 = load_ini("monitor", "error_warning_v7")
        if st.session_state.error_warning_v7 == "":
            st.session_state.error_warning_v7 = 100
        st.session_state.error_error_v7 = load_ini("monitor", "error_error_v7")
        if st.session_state.error_error_v7 == "":
            st.session_state.error_error_v7 = 250
        st.session_state.traceback_warning_v7 = load_ini("monitor", "traceback_warning_v7")
        if st.session_state.traceback_warning_v7 == "":
            st.session_state.traceback_warning_v7 = 100
        st.session_state.traceback_error_v7 = load_ini("monitor", "traceback_error_v7")
        if st.session_state.traceback_error_v7 == "":
            st.session_state.traceback_error_v7 = 250
        # Multi
        st.session_state.mem_warning_multi = load_ini("monitor", "mem_warning_multi")
        if st.session_state.mem_warning_multi == "":
            st.session_state.mem_warning_multi = 250
        st.session_state.mem_error_multi = load_ini("monitor", "mem_error_multi")
        if st.session_state.mem_error_multi == "":
            st.session_state.mem_error_multi = 500
        st.session_state.cpu_warning_multi = load_ini("monitor", "cpu_warning_multi")
        if st.session_state.cpu_warning_multi == "":
            st.session_state.cpu_warning_multi = 5
        st.session_state.cpu_error_multi = load_ini("monitor", "cpu_error_multi")
        if st.session_state.cpu_error_multi == "":
            st.session_state.cpu_error_multi = 10
        st.session_state.error_warning_multi = load_ini("monitor", "error_warning_multi")
        if st.session_state.error_warning_multi == "":
            st.session_state.error_warning_multi = 25
        st.session_state.error_error_multi = load_ini("monitor", "error_error_multi")
        if st.session_state.error_error_multi == "":
            st.session_state.error_error_multi = 50
        st.session_state.traceback_warning_multi = load_ini("monitor", "traceback_warning_multi")
        if st.session_state.traceback_warning_multi == "":
            st.session_state.traceback_warning_multi = 25
        st.session_state.traceback_error_multi = load_ini("monitor", "traceback_error_multi")
        if st.session_state.traceback_error_multi == "":
            st.session_state.traceback_error_multi = 50
        # Single
        st.session_state.mem_warning_single = load_ini("monitor", "mem_warning_single")
        if st.session_state.mem_warning_single == "":
            st.session_state.mem_warning_single = 50
        st.session_state.mem_error_single = load_ini("monitor", "mem_error_single")
        if st.session_state.mem_error_single == "":
            st.session_state.mem_error_single = 100
        st.session_state.cpu_warning_single = load_ini("monitor", "cpu_warning_single")
        if st.session_state.cpu_warning_single == "":
            st.session_state.cpu_warning_single = 5
        st.session_state.cpu_error_single = load_ini("monitor", "cpu_error_single")
        if st.session_state.cpu_error_single == "":
            st.session_state.cpu_error_single = 10
        st.session_state.error_warning_single = load_ini("monitor", "error_warning_single")
        if st.session_state.error_warning_single == "":
            st.session_state.error_warning_single = 25
        st.session_state.error_error_single = load_ini("monitor", "error_error_single")
        if st.session_state.error_error_single == "":
            st.session_state.error_error_single = 50
        st.session_state.traceback_warning_single = load_ini("monitor", "traceback_warning_single")
        if st.session_state.traceback_warning_single == "":
            st.session_state.traceback_warning_single = 25
        st.session_state.traceback_error_single = load_ini("monitor", "traceback_error_single")
        if st.session_state.traceback_error_single == "":
            st.session_state.traceback_error_single = 50

    def edit_monitor_config(self):
        # Load config
        if "edit_mem_warning_v7" in st.session_state:
            if st.session_state.mem_warning_v7 != st.session_state.edit_mem_warning_v7:
                st.session_state.mem_warning_v7 = st.session_state.edit_mem_warning_v7
        if "edit_mem_error_v7" in st.session_state:
            if st.session_state.mem_error_v7 != st.session_state.edit_mem_error_v7:
                st.session_state.mem_error_v7 = st.session_state.edit_mem_error_v7
        if "edit_cpu_warning_v7" in st.session_state:
            if st.session_state.cpu_warning_v7 != st.session_state.edit_cpu_warning_v7:
                st.session_state.cpu_warning_v7 = st.session_state.edit_cpu_warning_v7
        if "edit_cpu_error_v7" in st.session_state:
            if st.session_state.cpu_error_v7 != st.session_state.edit_cpu_error_v7:
                st.session_state.cpu_error_v7 = st.session_state.edit_cpu_error_v7
        if "edit_error_warning_v7" in st.session_state:
            if st.session_state.error_warning_v7 != st.session_state.edit_error_warning_v7:
                st.session_state.error_warning_v7 = st.session_state.edit_error_warning_v7
        if "edit_error_error_v7" in st.session_state:
            if st.session_state.error_error_v7 != st.session_state.edit_error_error_v7:
                st.session_state.error_error_v7 = st.session_state.edit_error_error_v7
        if "edit_traceback_warning_v7" in st.session_state:
            if st.session_state.traceback_warning_v7 != st.session_state.edit_traceback_warning_v7:
                st.session_state.traceback_warning_v7 = st.session_state.edit_traceback_warning_v7
        if "edit_traceback_error_v7" in st.session_state:
            if st.session_state.traceback_error_v7 != st.session_state.edit_traceback_error_v7:
                st.session_state.traceback_error_v7 = st.session_state.edit_traceback_error_v7
        # Multi
        if "edit_mem_warning_multi" in st.session_state:
            if st.session_state.mem_warning_multi != st.session_state.edit_mem_warning_multi:
                st.session_state.mem_warning_multi = st.session_state.edit_mem_warning_multi
        if "edit_mem_error_multi" in st.session_state:
            if st.session_state.mem_error_multi != st.session_state.edit_mem_error_multi:
                st.session_state.mem_error_multi = st.session_state.edit_mem_error_multi
        if "edit_cpu_warning_multi" in st.session_state:
            if st.session_state.cpu_warning_multi != st.session_state.edit_cpu_warning_multi:
                st.session_state.cpu_warning_multi = st.session_state.edit_cpu_warning_multi
        if "edit_cpu_error_multi" in st.session_state:
            if st.session_state.cpu_error_multi != st.session_state.edit_cpu_error_multi:
                st.session_state.cpu_error_multi = st.session_state.edit_cpu_error_multi
        if "edit_error_warning_multi" in st.session_state:
            if st.session_state.error_warning_multi != st.session_state.edit_error_warning_multi:
                st.session_state.error_warning_multi = st.session_state.edit_error_warning_multi
        if "edit_error_error_multi" in st.session_state:
            if st.session_state.error_error_multi != st.session_state.edit_error_error_multi:
                st.session_state.error_error_multi = st.session_state.edit_error_error_multi
        if "edit_traceback_warning_multi" in st.session_state:
            if st.session_state.traceback_warning_multi != st.session_state.edit_traceback_warning_multi:
                st.session_state.traceback_warning_multi = st.session_state.edit_traceback_warning_multi
        if "edit_traceback_error_multi" in st.session_state:
            if st.session_state.traceback_error_multi != st.session_state.edit_traceback_error_multi:
                st.session_state.traceback_error_multi = st.session_state.edit_traceback_error_multi
        # Single
        if "edit_mem_warning_single" in st.session_state:
            if st.session_state.mem_warning_single != st.session_state.edit_mem_warning_single:
                st.session_state.mem_warning_single = st.session_state.edit_mem_warning_single
        if "edit_mem_error_single" in st.session_state:
            if st.session_state.mem_error_single != st.session_state.edit_mem_error_single:
                st.session_state.mem_error_single = st.session_state.edit_mem_error_single
        if "edit_cpu_warning_single" in st.session_state:
            if st.session_state.cpu_warning_single != st.session_state.edit_cpu_warning_single:
                st.session_state.cpu_warning_single = st.session_state.edit_cpu_warning_single
        if "edit_cpu_error_single" in st.session_state:
            if st.session_state.cpu_error_single != st.session_state.edit_cpu_error_single:
                st.session_state.cpu_error_single = st.session_state.edit_cpu_error_single
        if "edit_error_warning_single" in st.session_state:
            if st.session_state.error_warning_single != st.session_state.edit_error_warning_single:
                st.session_state.error_warning_single = st.session_state.edit_error_warning_single
        if "edit_error_error_single" in st.session_state:
            if st.session_state.error_error_single != st.session_state.edit_error_error_single:
                st.session_state.error_error_single = st.session_state.edit_error_error_single
        if "edit_traceback_warning_single" in st.session_state:
            if st.session_state.traceback_warning_single != st.session_state.edit_traceback_warning_single:
                st.session_state.traceback_warning_single = st.session_state.edit_traceback_warning_single
        if "edit_traceback_error_single" in st.session_state:
            if st.session_state.traceback_error_single != st.session_state.edit_traceback_error_single:
                st.session_state.traceback_error_single = st.session_state.edit_traceback_error_single
        # Navigation
        with st.sidebar:
            if st.button(":material/home:"):
                del st.session_state.mem_warning_v7
                del st.session_state.mem_error_v7
                del st.session_state.cpu_warning_v7
                del st.session_state.cpu_error_v7
                del st.session_state.error_warning_v7
                del st.session_state.error_error_v7
                del st.session_state.traceback_warning_v7
                del st.session_state.traceback_error_v7
                del st.session_state.mem_warning_multi
                del st.session_state.mem_error_multi
                del st.session_state.cpu_warning_multi
                del st.session_state.cpu_error_multi
                del st.session_state.error_warning_multi
                del st.session_state.error_error_multi
                del st.session_state.traceback_warning_multi
                del st.session_state.traceback_error_multi
                del st.session_state.mem_warning_single
                del st.session_state.mem_error_single
                del st.session_state.cpu_warning_single
                del st.session_state.cpu_error_single
                del st.session_state.error_warning_single
                del st.session_state.error_error_single
                del st.session_state.traceback_warning_single
                del st.session_state.traceback_error_single
                del st.session_state.monitor_edit
                del st.session_state.monitor
                st.rerun()
            if st.button(":material/save:"):
                save_ini("monitor", "mem_warning_v7", str(st.session_state.mem_warning_v7))
                save_ini("monitor", "mem_error_v7", str(st.session_state.mem_error_v7))
                save_ini("monitor", "cpu_warning_v7", str(st.session_state.cpu_warning_v7))
                save_ini("monitor", "cpu_error_v7", str(st.session_state.cpu_error_v7))
                save_ini("monitor", "error_warning_v7", str(st.session_state.error_warning_v7))
                save_ini("monitor", "error_error_v7", str(st.session_state.error_error_v7))
                save_ini("monitor", "traceback_warning_v7", str(st.session_state.traceback_warning_v7))
                save_ini("monitor", "traceback_error_v7", str(st.session_state.traceback_error_v7))
                save_ini("monitor", "mem_warning_multi", str(st.session_state.mem_warning_multi))
                save_ini("monitor", "mem_error_multi", str(st.session_state.mem_error_multi))
                save_ini("monitor", "cpu_warning_multi", str(st.session_state.cpu_warning_multi))
                save_ini("monitor", "cpu_error_multi", str(st.session_state.cpu_error_multi))
                save_ini("monitor", "error_warning_multi", str(st.session_state.error_warning_multi))
                save_ini("monitor", "error_error_multi", str(st.session_state.error_error_multi))
                save_ini("monitor", "traceback_warning_multi", str(st.session_state.traceback_warning_multi))
                save_ini("monitor", "traceback_error_multi", str(st.session_state.traceback_error_multi))
                save_ini("monitor", "mem_warning_single", str(st.session_state.mem_warning_single))
                save_ini("monitor", "mem_error_single", str(st.session_state.mem_error_single))
                save_ini("monitor", "cpu_warning_single", str(st.session_state.cpu_warning_single))
                save_ini("monitor", "cpu_error_single", str(st.session_state.cpu_error_single))
                save_ini("monitor", "error_warning_single", str(st.session_state.error_warning_single))
                save_ini("monitor", "error_error_single", str(st.session_state.error_error_single))
                save_ini("monitor", "traceback_warning_single", str(st.session_state.traceback_warning_single))
                save_ini("monitor", "traceback_error_single", str(st.session_state.traceback_error_single))
                del st.session_state.monitor_edit
                st.rerun()
        st.header("Edit Monitor Settings")
        st.subheader("V7 Monitor Settings")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('Memory Warning',value=int(st.session_state.mem_warning_v7),step=10, key="edit_mem_warning_v7")
            st.number_input('Error Warning',value=int(st.session_state.error_warning_v7),step=1, key="edit_error_warning_v7")
        with col2:
            st.number_input('Memory Error',value=int(st.session_state.mem_error_v7),step=10, key="edit_mem_error_v7")
            st.number_input('Error Error',value=int(st.session_state.error_error_v7),step=1, key="edit_error_error_v7")
        with col3:
            st.number_input('CPU Warning',value=int(st.session_state.cpu_warning_v7),step=1, key="edit_cpu_warning_v7")
            st.number_input('Traceback Warning',value=int(st.session_state.traceback_warning_v7),step=1, key="edit_traceback_warning_v7")
        with col4:
            st.number_input('CPU Error',value=int(st.session_state.cpu_error_v7),step=1, key="edit_cpu_error_v7")
            st.number_input('Traceback Error',value=int(st.session_state.traceback_error_v7),step=1, key="edit_traceback_error_v7")
        st.subheader("Multi Monitor Settings")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('Memory Warning',value=int(st.session_state.mem_warning_multi),step=10, key="edit_mem_warning_multi")
            st.number_input('Error Warning',value=int(st.session_state.error_warning_multi),step=1, key="edit_error_warning_multi")
        with col2:
            st.number_input('Memory Error',value=int(st.session_state.mem_error_multi),step=10, key="edit_mem_error_multi")
            st.number_input('Error Error',value=int(st.session_state.error_error_multi),step=1, key="edit_error_error_multi")
        with col3:
            st.number_input('CPU Warning',value=int(st.session_state.cpu_warning_multi),step=1, key="edit_cpu_warning_multi")
            st.number_input('Traceback Warning',value=int(st.session_state.traceback_warning_multi),step=1, key="edit_traceback_warning_multi")
        with col4:
            st.number_input('CPU Error',value=int(st.session_state.cpu_error_multi),step=1, key="edit_cpu_error_multi")
            st.number_input('Traceback Error',value=int(st.session_state.traceback_error_multi),step=1, key="edit_traceback_error_multi")
        st.subheader("Single Monitor Settings")
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.number_input('Memory Warning',value=int(st.session_state.mem_warning_single),step=10, key="edit_mem_warning_single")
            st.number_input('Error Warning',value=int(st.session_state.error_warning_single),step=1, key="edit_error_warning_single")
        with col2:
            st.number_input('Memory Error',value=int(st.session_state.mem_error_single),step=10, key="edit_mem_error_single")
            st.number_input('Error Error',value=int(st.session_state.error_error_single),step=1, key="edit_error_error_single")
        with col3:
            st.number_input('CPU Warning',value=int(st.session_state.cpu_warning_single),step=1, key="edit_cpu_warning_single")
            st.number_input('Traceback Warning',value=int(st.session_state.traceback_warning_single),step=1, key="edit_traceback_warning_single")
        with col4:
            st.number_input('CPU Error',value=int(st.session_state.cpu_error_single),step=1, key="edit_cpu_error_single")
            st.number_input('Traceback Error',value=int(st.session_state.traceback_error_single),step=1, key="edit_traceback_error_single")



def main():
    print("Don't Run this Class from CLI")

if __name__ == '__main__':
    main()
