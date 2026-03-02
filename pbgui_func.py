import streamlit as st
import streamlit.components.v1 as _st_components
import json
import hjson
import pprint
import uuid
import requests
import os
from time import sleep
from pathlib import Path
from pbgui_purefunc import load_ini, save_ini, load_symbols_from_ini as _load_symbols_from_mapping
# LogHandler removed: centralized debuglog removed per user request
from PBRemote import PBRemote
from MonitorConfig import MonitorConfig
from typing import Optional, Callable, Literal


def render_header_with_guide(
    title: str,
    *,
    guide_callback: Callable[[], None] | None = None,
    guide_key: str = "guide_btn",
    guide_label: str = "📖 Guide",
    guide_help: str = "Open help and tutorials",
    divider: bool = True,
    divider_color_hex: str = "#ff4b4b",
    divider_thickness_px: int = 2,
    level: Literal["header", "subheader"] = "header",
) -> None:
    """Render a consistent page header with optional Guide button.

    Streamlit's built-in `divider=` only spans the column it is rendered in.
    When a Guide button lives in a right-side column, we draw a full-width
    divider below the row so the header looks consistent.
    """

    t = str(title or "").strip()

    if guide_callback is None:
        if level == "subheader":
            st.subheader(t)
        else:
            if divider:
                st.header(t, divider="red")
            else:
                st.header(t)
        return

    c_title, c_help = st.columns([0.94, 0.06], vertical_alignment="center")
    with c_title:
        if level == "subheader":
            st.subheader(t)
        else:
            st.header(t)

    with c_help:
        if st.button(str(guide_label), key=str(guide_key), help=str(guide_help)):
            guide_callback()

    if divider:
        st.markdown(
            (
                "<hr style='width:100%;margin-top:-0.5rem;margin-bottom:1rem;"
                f"border:0;border-top:{int(divider_thickness_px)}px solid {divider_color_hex};' />"
            ),
            unsafe_allow_html=True,
        )

@st.dialog("Select file")
def change_ini(section, parameter):
    filename = st_file_selector(st, path=st.session_state[parameter], key = f'file_change_{parameter}', label = f'select {parameter}')
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            filename = os.path.abspath(filename)
            st.session_state[parameter] = filename
            save_ini(section, parameter, filename)
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.rerun()

@st.dialog("Select file")
def select_file(parameter):
    filename = st_file_selector(st, path=st.session_state[parameter], key = f'file_change_{parameter}', label = f'select {parameter}')
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[OK]"):
            filename = os.path.abspath(filename)
            st.session_state[parameter] = filename
            st.rerun()
    with col2:
        if st.button(":red[Cancel]"):
            st.rerun()

def pbdir(): return load_ini("main", "pbdir")

def pbvenv(): return load_ini("main", "pbvenv")

def is_pb_installed():
    if Path(f"{pbdir()}/passivbot.py").exists():
        return True
    return False

def pb7dir(): return load_ini("main", "pb7dir")

def pb7venv(): return load_ini("main", "pb7venv")

def is_pb7_installed():
    if Path(f"{pb7dir()}/src/passivbot.py").exists():
        return True
    return False

PBGDIR = Path.cwd()


def is_authenticted():
    if 'password_correct' in st.session_state:
        if st.session_state['password_correct']:
            return True   
    return False

def check_password():
    # if secrets file is missing, crate it with password = "PBGui$Data!"
    secrets_path = Path(".streamlit/secrets.toml")
    if not secrets_path.exists():
        with open(secrets_path, "w") as f:
            f.write('password = "PBGui$Bot!"')

    """Returns `True` if the user had the correct password."""
    if "password" in st.secrets:
        if st.secrets["password"] == "":
            st.session_state["password_correct"] = True
            st.session_state["password_missing"] = True
            return True
    else:
        st.session_state["password_correct"] = True
        st.session_state["password_missing"] = True
        return True
    
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.info("Please enter your password to continue.")
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

def set_page_config(page : str = "Start"):
    st.session_state.page = page
    st.set_page_config(
        page_title=f"PBGUI - {page}",
        page_icon=":screwdriver:",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get help': 'https://github.com/msei99/pbgui/#readme',
            'About': "Passivbot GUI v1.63 [![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/Y8Y216Q3QS)"
        }
    )
    # Global layout CSS — applied on every page
    st.markdown("""
<style>
    .stMainBlockContainer {
        padding-top: 2.25rem !important;
        padding-bottom: 0 !important;
    }
    .stMainBlockContainer h1,
    .stMainBlockContainer h2,
    .stMainBlockContainer h3 {
        margin-top: 0.25rem !important;
    }
</style>""", unsafe_allow_html=True)
    # Check VPS Errors
    if str(page) != "VPS Monitor":
        has_vps_errors()
    
    with st.sidebar:
        st.write(f"### {page} Options")
        
def get_navi_paths():
    NAVI_BASE_DIR = "navi/"
    paths = {
        "HELP":              os.path.join(NAVI_BASE_DIR, "help.py"),
        "SYSTEM_LOGIN":        os.path.join(NAVI_BASE_DIR, "system_login.py"),
        "SYSTEM_API_KEYS":     os.path.join(NAVI_BASE_DIR, "system_api_keys.py"),
        "SYSTEM_SERVICES":     os.path.join(NAVI_BASE_DIR, "system_services.py"),
        "SYSTEM_VPS_MANAGER":  os.path.join(NAVI_BASE_DIR, "system_vps_manager.py"),
        "SYSTEM_VPS_MONITOR":  os.path.join(NAVI_BASE_DIR, "system_vps_monitor.py"),
        "SYSTEM_LOGGING":      os.path.join(NAVI_BASE_DIR, "system_logging.py"),
        "SYSTEM_DEBUGLOG":     os.path.join(NAVI_BASE_DIR, "system_debuglog.py"),

        "INFO_DASHBOARDS":     os.path.join(NAVI_BASE_DIR, "info_dashboards.py"),
        "INFO_COIN_DATA":      os.path.join(NAVI_BASE_DIR, "info_coin_data.py"),
        "INFO_MARKET_DATA":    os.path.join(NAVI_BASE_DIR, "info_market_data.py"),

        "V6_SINGLE_RUN":       os.path.join(NAVI_BASE_DIR, "v6_single_run.py"),
        "V6_SINGLE_BACKTEST":  os.path.join(NAVI_BASE_DIR, "v6_single_backtest.py"),
        "V6_SINGLE_OPTIMIZE":  os.path.join(NAVI_BASE_DIR, "v6_single_optimize.py"),
        "V6_SPOT_VIEW":        os.path.join(NAVI_BASE_DIR, "v6_spot_view.py"),

        "V6_MULTI_RUN":        os.path.join(NAVI_BASE_DIR, "v6_multi_run.py"),
        "V6_MULTI_BACKTEST":   os.path.join(NAVI_BASE_DIR, "v6_multi_backtest.py"),
        "V6_MULTI_OPTIMIZE":   os.path.join(NAVI_BASE_DIR, "v6_multi_optimize.py"),

        "V7_RUN":              os.path.join(NAVI_BASE_DIR, "v7_run.py"),
        "V7_BACKTEST":         os.path.join(NAVI_BASE_DIR, "v7_backtest.py"),
        "V7_OPTIMIZE":         os.path.join(NAVI_BASE_DIR, "v7_optimize.py"),
        "V7_STRATEGY_EXPLORER":  os.path.join(NAVI_BASE_DIR, "v7_strategy_explorer.py"),
        "V7_BALANCE_CALC":     os.path.join(NAVI_BASE_DIR, "v7_balance_calc.py"),
        "V7_PARETO_EXPLORER":  os.path.join(NAVI_BASE_DIR, "v7_pareto_explorer.py"),
        "V7_LIVE_VS_BACKTEST": os.path.join(NAVI_BASE_DIR, "v7_live_vs_backtest.py"),
    }
    return paths

def build_navigation():
    paths = get_navi_paths()

    # Single Pages
    pM1 = st.Page(paths["SYSTEM_LOGIN"], title="Welcome", icon=":material/logout:")
    pM2 = st.Page(paths["SYSTEM_API_KEYS"], title="API-Keys", icon=":material/key:")
    pM3 = st.Page(paths["SYSTEM_SERVICES"], title="PBGUI Services", icon=":material/build:")
    pM4 = st.Page(paths["SYSTEM_VPS_MANAGER"], title="VPS Manager", icon=":material/computer:")
    pM4b = st.Page(paths["SYSTEM_VPS_MONITOR"], title="VPS Monitor", icon=":material/monitor_heart:")
    pM5 = st.Page(paths["SYSTEM_LOGGING"], title="Logging", icon=":material/article:")
    # Debuglog page removed

    pSe1 = st.Page(paths["INFO_DASHBOARDS"], title="Dashboards", icon=":material/dashboard:")
    pSe2 = st.Page(paths["INFO_COIN_DATA"], title="Coin Data", icon=":material/monetization_on:")
    pSe3 = st.Page(paths["INFO_MARKET_DATA"], title="Market Data", icon=":material/storage:")
    pH1 = st.Page(paths["HELP"], title="Help", icon=":material/help:", url_path="help")

    pS1 = st.Page(paths["V6_SINGLE_RUN"], title="Run", icon=":material/play_arrow:")
    pS2 = st.Page(paths["V6_SINGLE_BACKTEST"], title="Backtest", icon=":material/history:")
    pS3 = st.Page(paths["V6_SINGLE_OPTIMIZE"], title="Optimize", icon=":material/tune:")
    pS4 = st.Page(paths["V6_SPOT_VIEW"], title="Spot View", icon=":material/remove_red_eye:")

    p61 = st.Page(paths["V6_MULTI_RUN"], title="Run", icon=":material/play_arrow:")
    p62 = st.Page(paths["V6_MULTI_BACKTEST"], title="Backtest", icon=":material/history:")
    p63 = st.Page(paths["V6_MULTI_OPTIMIZE"], title="Optimize", icon=":material/tune:")

    p71 = st.Page(paths["V7_RUN"], title="Run", icon=":material/play_arrow:")
    p72 = st.Page(paths["V7_BACKTEST"], title="Backtest", icon=":material/history:")
    p73 = st.Page(paths["V7_OPTIMIZE"], title="Optimize", icon=":material/tune:")
    p74 = st.Page(paths["V7_STRATEGY_EXPLORER"], title="Strategy Explorer", icon=":material/remove_red_eye:")
    p75 = st.Page(paths["V7_BALANCE_CALC"], title="Balance Calculator", icon=":material/attach_money:")
    p76 = st.Page(paths["V7_PARETO_EXPLORER"], title="🎯 Pareto Explorer", icon=":material/analytics:")
    p77 = st.Page(paths["V7_LIVE_VS_BACKTEST"], title="Live vs Backtest", icon=":material/swap_horiz:")
       
    # Page Groups
    SystemPages = [pM1, pM2, pM3, pM4, pM4b, pM5]
    
    # Do not include DEBUGLOG page; centralized debuglog removed
                
    InfotmationPages = [pSe1, pSe2, pSe3, pH1]
    v7Pages = [p71, p72, p77, p73, p74, p75, p76]
    v6Pages = [p61, p62, p63]
    SinglePages = [pS1, pS2, pS3, pS4]

    # Navigation
    navi = st.navigation(
        {
            "System": SystemPages,
            "Information": InfotmationPages,
            "PBv7": v7Pages,
            "PBv6 Multi": v6Pages,
            "PBv6 Single": SinglePages,
        },position="top"
    )
    st.session_state.navigation = navi
    navi.run()
    
    
# Centralized debuglog removed per user request. Background tasks should use
# independent logging (or rely on stdout) if they need to write logs.
    
def is_session_state_not_initialized():
    # Init Services
    if (
        'pbdir' not in st.session_state or
        'services' not in st.session_state or
        'pbgui_instances' not in st.session_state or
        'multi_instances' not in st.session_state or
        'users' not in st.session_state or
        'pbcoindata' not in st.session_state
    ):
        return True
    return False

def replace_special_chars(input:str):
    # Replace chars that can cause trouble in filenames
    return input.replace(" ", "_").replace("/", "_").replace("\\", "_").replace(":", "_").replace("*", "_").replace("?", "_").replace("\"", "_").replace("<", "_").replace(">", "_").replace("|", "_")
    
def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except (ValueError,TypeError) as err:
        return False
    return True

def validateHJSON(hjsonData):
    try:
        hjson.loads(hjsonData)
    except (ValueError) as err:
        return False
    return True

def upload_pbconfigdb(config: str, symbol: str, source_name : str):
    if validateJSON(config):
        uniq = str(uuid.uuid4().hex)
        url = 'https://pbconfigdb.scud.dedyn.io/uploads/b1ea37f7cfa0ebf9b67c2f6b30b95b8b1a92e249/'
        headers = {
            'Content-Type': 'application/json',
            'data': json.dumps(json.loads(config)),
            'filename': f'{symbol}-{source_name}-{uniq}.json' # {symbol}-{username}-{unique}.json
        }
        response = requests.put(url, headers=headers)
        st.info(response.text, icon="ℹ️")
    else:
        st.error("Invalid config", icon="🚨")

def load_symbols_from_ini(exchange: str, market_type: str):
    return _load_symbols_from_mapping(exchange, market_type)

def update_dir(key):
    choice = st.session_state[key]
    if os.path.isdir(os.path.join(st.session_state[key+'curr_dir'], choice)):
        st.session_state[key+'curr_dir'] = os.path.normpath(os.path.join(st.session_state[key+'curr_dir'], choice))
        files = sorted(os.listdir(st.session_state[key+'curr_dir']))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files

def st_file_selector(st_placeholder, path='.', label='Select a file/folder', key = 'selected'):
    if key+'curr_dir' not in st.session_state:
        base_path = '.' if path is None or path == '' else path
        base_path = base_path if os.path.isdir(base_path) else os.path.dirname(base_path)
        base_path = '.' if base_path is None or base_path == '' else base_path

        files = sorted(os.listdir(base_path))
        files.insert(0, '..')
        files.insert(0, '.')
        st.session_state[key+'files'] = files
        st.session_state[key+'curr_dir'] = base_path
    else:
        base_path = st.session_state[key+'curr_dir']
    selected_file = st_placeholder.selectbox(label=label, 
                                        options=st.session_state[key+'files'], 
                                        key=key, 
                                        on_change = lambda: update_dir(key))
    selected_path = os.path.normpath(os.path.join(base_path, selected_file))
    st_placeholder.write(os.path.abspath(selected_path))
    return selected_path

def sync_api():
    pbremote = st.session_state.pbremote
    if not pbremote.check_if_api_synced():
        st.warning('API not in sync')
        if st.button("Sync API"):
            pbremote.sync_api_up()
            timeout = 180
            with st.spinner(text=f'syncing...'):
                with st.empty():
                    while not pbremote.check_if_api_synced():
                        st.text(f'{timeout} sec ({pbremote.unsynced_api} server to go)')
                        sleep(1)
                        timeout -= 1
                        if timeout == 0:
                            break
                    st.text(f'{timeout} sec ({pbremote.unsynced_api} server to go)')
                st.text(f'')
                if timeout == 0:
                    error_popup("Syncing API failed")
                else:
                    info_popup("API synced")
    else:
        st.success('API in sync')

@st.dialog("Error", width="large")
def error_popup(message):
    st.error(f'{message}', icon="⚠️")
    if st.button(":green[OK]"):
        st.rerun()

@st.dialog("Info", width="large")
def info_popup(message):
    st.info(f'{message}', icon="✅")
    if st.button(":green[OK]"):
        st.rerun()

def has_vps_errors():
    if "pbremote" not in st.session_state:
        st.session_state.pbremote = PBRemote()
    st.session_state.pbremote.update_remote_servers()
    errors = st.session_state.pbremote.has_error()
    if errors:
        with st.expander("VPS Errors", expanded=True):
            for error in errors:
                if error["name"] == "offline":
                    st.error(f'Server: {error["server"]} is offline')
                elif error["name"] == "system":
                    st.warning(f'Server: {error["server"]} Instance: {error["name"]} Mem: {error["mem"]} Swap: {error["swap"]} CPU: {error["cpu"]} Disk: {error["disk"]}')
                else:
                    st.warning(f'Server: {error["server"]} Instance: {error["name"]} Mem: {error["mem"]} Swap: {error["swap"]} CPU: {error["cpu"]} Error: {error["error"]} Traceback: {error["traceback"]}')


# ── Log Viewer component ─────────────────────────────────────────────────────

_LOG_VIEWER_DIR = Path(__file__).resolve().parent / "components" / "log_viewer"
_LOG_VIEWER_HTML: str | None = None


def _load_log_viewer_html() -> str:
    global _LOG_VIEWER_HTML
    if _LOG_VIEWER_HTML is None:
        _LOG_VIEWER_HTML = (_LOG_VIEWER_DIR / "index.html").read_text(encoding="utf-8")
    return _LOG_VIEWER_HTML


def render_log_viewer(
    preselect: str = "",
    iframe_height_offset: int = 200,
    display_title: str = "",
) -> None:
    """Render the WebSocket log viewer component.

    Centralises all boilerplate: HTML loading, placeholder injection, CSS
    override, and component rendering.

    Args:
        preselect: filename (e.g. ``"MarketData.log"``) to auto-open on load.
        iframe_height_offset: pixels subtracted from ``100vh`` for the iframe
            height.  Increase when there are tabs or extra widgets above the
            viewer (default 200 for a plain page, ~260 when inside tabs).
        display_title: optional human-readable label shown in the viewer
            toolbar alongside the filename (e.g. the backtest name).
    """
    ws_port_val = load_ini("pbmaster", "ws_port")
    ws_port = (
        int(ws_port_val)
        if ws_port_val and ws_port_val.isdigit() and 1024 <= int(ws_port_val) <= 65535
        else 8765
    )
    logs_dir = Path(__file__).resolve().parent / "data" / "logs"
    log_files: list[str] = []
    if logs_dir.exists():
        # Only top-level *.log — job logs (data/logs/jobs/) are intentionally excluded
        # from the sidebar to avoid flooding it. They are loaded on-demand via preselect.
        log_files = sorted(p.name for p in logs_dir.glob("*.log") if p.is_file())
    file_sizes: dict = {}
    rotated_files: dict = {}
    if logs_dir.exists():
        for base in log_files:
            file_sizes[base] = (logs_dir / base).stat().st_size
            variants: list[str] = []
            for i in range(1, 20):
                p = logs_dir / f"{base}.{i}"
                if p.is_file():
                    variants.append(p.name)
                    file_sizes[p.name] = p.stat().st_size
                else:
                    break
            p_old = logs_dir / f"{base}.old"
            if p_old.is_file():
                variants.append(p_old.name)
                file_sizes[p_old.name] = p_old.stat().st_size
            if variants:
                rotated_files[base] = variants

    html = (
        _load_log_viewer_html()
        .replace("__WS_PORT__", str(ws_port))
        .replace("__INITIAL_FILES__", json.dumps(log_files))
        .replace("__FILE_SIZES__", json.dumps(file_sizes))
        .replace("__ROTATED_FILES__", json.dumps(rotated_files))
        .replace("__PRESELECT_FILE__", json.dumps(preselect))
        .replace("__DISPLAY_TITLE__",    json.dumps(display_title))
    )

    st.markdown(f"""
<style>
    /* Target the iframe only */
    .stMainBlockContainer iframe {{
        height: calc(100vh - {iframe_height_offset}px) !important;
        min-height: 400px !important;
        border: none !important;
    }}
</style>""", unsafe_allow_html=True)

    _st_components.html(html, height=600, scrolling=False)
