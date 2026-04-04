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
from pbgui_purefunc import load_ini, save_ini, load_symbols_from_ini as _load_symbols_from_mapping, PBGUI_VERSION, PBGDIR
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
    # One-time INI migration (pbmaster → vps_monitor)
    if "_ini_migrated" not in st.session_state:
        from pbgui_purefunc import migrate_ini_sections
        migrate_ini_sections()
        st.session_state._ini_migrated = True
    st.session_state.page = page
    st.set_page_config(
        page_title=f"PBGUI - {page}",
        page_icon=":screwdriver:",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get help': 'https://github.com/msei99/pbgui/#readme',
            'About': f"Passivbot GUI {PBGUI_VERSION} | API Serial {(PBGDIR / 'api' / 'serial.txt').read_text().strip()} [![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/Y8Y216Q3QS)"
        }
    )
    # Global layout CSS — applied on every page
    st.markdown("""
<style>
    :root {
        --fs-xs: 11px;
        --fs-sm: 13px;
        --fs-base: 14px;
        --fs-md: 15px;
        --fs-lg: 18px;
        --fs-xl: 22px;
    }
    .stMainBlockContainer {
        padding-top: 2.25rem !important;
        padding-bottom: 1rem !important;
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

    # ── FastAPI-only pages: dual interception ───────────────────────────────
    # 1. SERVER-SIDE: if Streamlit somehow runs this page (direct URL / slow
    #    client), intercept before navi.run() so the page script never runs.
    # 2. CLIENT-SIDE: inject a history.pushState patch so that ANY Streamlit
    #    nav-bar click that would navigate to a FastAPI-only page is intercepted
    #    synchronously in the browser — before Streamlit re-renders — giving a
    #    true zero-flash direct navigation to FastAPI.
    # ────────────────────────────────────────────────────────────────────────

    # Resolve API server once; reused by both server-side and client-side paths.
    _fa_ok = False
    _fa_port = 0
    try:
        _, _fa_port, _fa_ok = _start_fastapi_server_if_needed()
    except Exception:
        pass

    # Ensure token exists and is still valid (needed for both paths below).
    if _fa_ok:
        _need_token = "api_token" not in st.session_state
        if not _need_token:
            try:
                from api.auth import validate_token
                if not validate_token(st.session_state["api_token"]):
                    _need_token = True
            except Exception:
                _need_token = True
        if _need_token:
            try:
                from api.auth import generate_token
                _uid = (st.session_state.get("user", {}).get("id")
                        or st.session_state.get("user")
                        or "anonymous")
                st.session_state["api_token"] = generate_token(str(_uid), expires_in_seconds=86400).token
            except Exception:
                pass

    # Derive browser host/port once.
    _bhost, _sport = "127.0.0.1", 8501
    try:
        _rhost = st.context.headers.get("Host", "")
        if _rhost:
            _bhost = _rhost.split(":")[0] or "127.0.0.1"
            if ":" in _rhost:
                _sport = int(_rhost.split(":")[1])
    except Exception:
        pass

    # 1. SERVER-SIDE interception (user is already on the logging page).
    if navi.url_path == "system_logging":
        if not is_authenticted() or is_session_state_not_initialized():
            st.switch_page(paths["SYSTEM_LOGIN"])
            st.stop()
        if _fa_ok and "api_token" in st.session_state:
            _url = (f"http://{_bhost}:{_fa_port}/api/logging/main_page"
                    f"?token={st.session_state['api_token']}"
                    f"&st_base=http://{_bhost}:{_sport}")
            st.html(f'<script>window.location.replace("{_url}");</script>',
                    unsafe_allow_javascript=True)
            st.stop()
        redirect_to_fastapi_logging()   # fallback (also handles error display)

    # 1b. SERVER-SIDE interception for VPS Monitor.
    if navi.url_path == "system_vps_monitor":
        if not is_authenticted() or is_session_state_not_initialized():
            st.switch_page(paths["SYSTEM_LOGIN"])
            st.stop()
        if _fa_ok and "api_token" in st.session_state:
            _url = (f"http://{_bhost}:{_fa_port}/api/vps/main_page"
                    f"?token={st.session_state['api_token']}"
                    f"&st_base=http://{_bhost}:{_sport}")
            st.html(f'<script>window.location.replace("{_url}");</script>',
                    unsafe_allow_javascript=True)
            st.stop()
        redirect_to_fastapi_vps_monitor()

    # 1c. SERVER-SIDE interception for Services.
    if navi.url_path == "system_services":
        if not is_authenticted() or is_session_state_not_initialized():
            st.switch_page(paths["SYSTEM_LOGIN"])
            st.stop()
        if _fa_ok and "api_token" in st.session_state:
            _url = (f"http://{_bhost}:{_fa_port}/api/services/main_page"
                    f"?token={st.session_state['api_token']}"
                    f"&st_base=http://{_bhost}:{_sport}")
            st.html(f'<script>window.location.replace("{_url}");</script>',
                    unsafe_allow_javascript=True)
            st.stop()
        redirect_to_fastapi_services()

    # 1d. PBv7 Run: NO server-side interception here.
    # The page script (navi/v7_run.py) handles its own routing:
    # - edit/add mode → stays in Streamlit
    # - list mode → redirect_to_fastapi_v7_run() at end of script
    # This avoids race conditions between interception and relay session state.

    # 2. CLIENT-SIDE history.pushState patch — injected into every other page so
    #    clicking a nav-bar link for a FastAPI-only page is intercepted
    #    synchronously in the browser — before Streamlit re-renders — giving a
    #    true zero-flash direct navigation to FastAPI.
    #    NOTE: v7_run is NOT included here because Streamlit still handles the
    #    edit/add flows. The server-side interception (1d) handles the normal
    #    nav-bar "Run" click; it's fast enough (just injects a redirect script).
    if _fa_ok and "api_token" in st.session_state:
        _token = st.session_state["api_token"]
        _log_url = (f"http://{_bhost}:{_fa_port}/api/logging/main_page"
                    f"?token={_token}&st_base=http://{_bhost}:{_sport}")
        _vps_url = (f"http://{_bhost}:{_fa_port}/api/vps/main_page"
                    f"?token={_token}&st_base=http://{_bhost}:{_sport}")
        _svc_url = (f"http://{_bhost}:{_fa_port}/api/services/main_page"
                    f"?token={_token}&st_base=http://{_bhost}:{_sport}")
        _fa_pages = (
            f'"system_logging":"{_log_url}",'
            f'"system_vps_monitor":"{_vps_url}",'
            f'"system_services":"{_svc_url}"'
        )
        # NOTE: no < or > inside this script — DOMPurify will not strip it.
        # _pbguiOk guard ensures pushState is only patched once per page load;
        # _pbguiFaPgs is updated every render so the token stays fresh.
        st.html(
            f'<script>'
            f'window._pbguiFaPgs={{{_fa_pages}}};'
            f'if(!history._pbguiOk){{'
            f'history._pbguiOk=true;'
            f'var _pp=history.pushState,_pr=history.replaceState;'
            f'function _pbCheck(u){{var s=String(u||""),p=window._pbguiFaPgs||{{}};'
            f'for(var k in p){{if(s.indexOf(k)!==-1){{window.location.replace(p[k]);return true;}}}}return false;}}'
            f'history.pushState=function(a,b,u){{if(!_pbCheck(u))_pp.apply(this,arguments);}};'
            f'history.replaceState=function(a,b,u){{if(!_pbCheck(u))_pr.apply(this,arguments);}};'
            f'}}'
            f'</script>',
            unsafe_allow_javascript=True,
        )
    # ────────────────────────────────────────────────────────────────────────

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
    # API server connection for WebSocket log streaming
    api_host_cfg, api_port_val, _api_ok = _start_fastapi_server_if_needed()

    # Determine browser-usable hostname (bind address 0.0.0.0 is not valid for WS)
    _ws_host = "127.0.0.1"
    try:
        import streamlit as _st
        _req_host = _st.context.headers.get("Host", "")
        if _req_host:
            _ws_host = _req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    # Generate / reuse API token for this session (refresh if expired)
    from api.auth import generate_token as _gen_token, validate_token as _val_token
    _st_mod = __import__('streamlit')
    _need_tok = "api_token" not in _st_mod.session_state
    if not _need_tok:
        try:
            if not _val_token(_st_mod.session_state["api_token"]):
                _need_tok = True
        except Exception:
            _need_tok = True
    if _need_tok:
        _user_id = (
            _st_mod.session_state.get("user", {}).get("id")
            or _st_mod.session_state.get("user")
            or "anonymous"
        )
        _tok = _gen_token(str(_user_id), expires_in_seconds=86400)
        _st_mod.session_state["api_token"] = _tok.token
    _api_token = _st_mod.session_state["api_token"]
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
        .replace("__API_PORT__", str(api_port_val))
        .replace("__API_TOKEN__", _api_token)
        .replace("__API_HOST__", _ws_host)
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


def _start_fastapi_server_if_needed() -> tuple[str, int, bool]:
    """Start FastAPI server if not already running.

    Delegates to PBApiServer (single source of truth for PID lifecycle).
    Returns:
        Tuple of (host, port, success)
    """
    from PBApiServer import PBApiServer

    # Reuse the session-state instance when available (Streamlit context),
    # otherwise create a transient one (in tests / non-UI callers).
    if "api_server" in st.session_state:
        srv = st.session_state.api_server
    else:
        srv = PBApiServer()

    if not srv.is_running():
        srv.run()

    return (srv.host, srv.port, srv.is_running())


def render_fastapi_job_monitor(height: int = 800, exchange: str = "", job_type: str = "") -> None:
    """Render the FastAPI job monitor component via iframe.
    
    Automatically starts the FastAPI server if not already running.
    Generates an API token for the current user session and embeds the
    job monitor in an iframe with the token for authentication.
    
    Configuration is read from pbgui.ini ([api_server] section).
    
    Args:
        height: iframe height in pixels (default: 800)
        exchange: optional exchange filter (e.g. "binanceusdm", "bybit", "hyperliquid")
        job_type: optional job type filter — comma-separated (e.g. "hl_best_1m" or "hl_aws_l2book_auto")
    """
    from api.auth import generate_token, validate_token as _vt
    
    # Ensure FastAPI server is running and get config
    api_host, api_port, success = _start_fastapi_server_if_needed()
    
    if not success:
        st.error(f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
                 f"Please check **System → Services → API Server** or start manually: `python PBApiServer.py`")
        return
    
    # Get or create token for current session (refresh if expired)
    _need_tok = "api_token" not in st.session_state
    if not _need_tok:
        try:
            if not _vt(st.session_state["api_token"]):
                _need_tok = True
        except Exception:
            _need_tok = True
    if _need_tok:
        user_id = st.session_state.get("user", {}).get("id") or st.session_state.get("user") or "anonymous"
        token_obj = generate_token(str(user_id), expires_in_seconds=86400)
        st.session_state["api_token"] = token_obj.token
    
    token = st.session_state["api_token"]

    # Derive browser-usable hostname (0.0.0.0 is not routable from the browser)
    _browser_host = "127.0.0.1"
    try:
        _req_host = st.context.headers.get("Host", "")
        if _req_host:
            _browser_host = _req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    # Build iframe URL with token and optional filters
    exchange_param = f"&exchange={exchange}" if exchange else ""
    job_type_param = f"&job_type={job_type}" if job_type else ""
    iframe_url = f"http://{_browser_host}:{api_port}/app/jobs_monitor.html?token={token}{exchange_param}{job_type_param}"
    _st_components.iframe(iframe_url, height=height, scrolling=True)


def redirect_to_fastapi_api_keys_editor() -> None:
    """Redirect the browser to the standalone FastAPI API Keys editor.

    Uses window.location.replace() so the Streamlit chrome is never rendered.
    The FastAPI /main_page endpoint injects the token server-side.
    Calls st.stop() on success so nothing else is rendered.
    """
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(
            f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
            "Please check **System → Services → API Server** or start manually: "
            "`python PBApiServer.py`"
        )
        return

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token

    token = st.session_state["api_token"]

    browser_host = "127.0.0.1"
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
            if ":" in req_host:
                st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/api-keys/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


def redirect_to_fastapi_logging() -> None:
    """Redirect the browser to the standalone FastAPI Logging Monitor page.

    Uses window.location.replace() so the Streamlit chrome is never rendered.
    The FastAPI /main_page endpoint injects the token server-side.
    Calls st.stop() on success so nothing else is rendered.
    """
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(
            f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
            "Please check **System → Services → API Server** or start manually: "
            "`python PBApiServer.py`"
        )
        return

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token

    token = st.session_state["api_token"]

    browser_host = "127.0.0.1"
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
            if ":" in req_host:
                st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/logging/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


def redirect_to_fastapi_services() -> None:
    """Redirect the browser to the standalone FastAPI Services page."""
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(
            f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
            "Please check **System → Services → API Server** or start manually: "
            "`python PBApiServer.py`"
        )
        return

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token

    token = st.session_state["api_token"]

    browser_host = "127.0.0.1"
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
            if ":" in req_host:
                st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/services/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


def redirect_to_fastapi_vps_monitor() -> None:
    """Redirect the browser to the standalone FastAPI VPS Monitor page."""
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(
            f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
            "Please check **System → Services → API Server** or start manually: "
            "`python PBApiServer.py`"
        )
        return

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token

    token = st.session_state["api_token"]

    browser_host = "127.0.0.1"
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
            if ":" in req_host:
                st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/vps/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


def redirect_to_fastapi_v7_run() -> None:
    """Redirect the browser to the standalone FastAPI PBv7 Run page."""
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(
            f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
            "Please check **System → Services → API Server** or start manually: "
            "`python PBApiServer.py`"
        )
        return

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token

    token = st.session_state["api_token"]

    browser_host = "127.0.0.1"
    st_port = 8501
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            browser_host = req_host.split(":")[0] or "127.0.0.1"
            if ":" in req_host:
                st_port = int(req_host.split(":")[1])
    except Exception:
        pass

    st_base = f"http://{browser_host}:{st_port}"
    url = (
        f"http://{browser_host}:{api_port}/api/v7/main_page"
        f"?token={token}"
        f"&st_base={st_base}"
    )
    st.html(
        f'<script>window.location.replace("{url}");</script>',
        unsafe_allow_javascript=True,
    )
    st.stop()


def render_fastapi_hl_data_actions() -> None:
    """Render combined HL data actions (Download l2Book + Build OHLCV).

    Uses st.html(unsafe_allow_javascript=True) to inject the HTML directly
    into the Streamlit DOM (NO iframe). This allows natural height flow —
    collapsed sections take zero extra space, expanded sections grow naturally.
    All CSS is scoped under .hlda-root to avoid Streamlit style conflicts.
    Collapse/expand uses vanilla JS + localStorage persistence.
    Job monitors are inline WebSocket-connected divs (no iframe).
    """
    from pathlib import Path
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(f"⚠️ FastAPI server could not be started on {api_host}:{api_port}.")
        return

    if "api_token" not in st.session_state:
        user_id = st.session_state.get("user", {}).get("id") or st.session_state.get("user") or "anonymous"
        token_obj = generate_token(str(user_id), expires_in_seconds=86400)
        st.session_state["api_token"] = token_obj.token

    token = st.session_state["api_token"]

    _browser_host = "127.0.0.1"
    try:
        _req_host = st.context.headers.get("Host", "")
        if _req_host:
            _browser_host = _req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    api_host_str = f"{_browser_host}:{api_port}"
    api_base_str = f"http://{_browser_host}:{api_port}/api"

    # Read HTML template
    html_path = Path(__file__).parent / "frontend" / "hl_data_actions.html"
    html_content = html_path.read_text(encoding="utf-8")

    # Unique IDs so multiple instances don't conflict
    instance_id = "hlda_inst"
    html_content = html_content.replace("__HLDA_ROOT__", instance_id)
    html_content = html_content.replace("__HLDA__", f"{instance_id}_")

    # Inject config via data-* attributes on root element
    html_content = html_content.replace(
        'data-token=""', f'data-token="{token}"'
    ).replace(
        'data-api-base=""', f'data-api-base="{api_base_str}"'
    ).replace(
        'data-api-host=""', f'data-api-host="{api_host_str}"'
    )

    st.html(html_content, unsafe_allow_javascript=True)


def render_fastapi_market_data_status(exchange: str) -> None:
    """
    Render FastAPI-based Market Data Status monitor (vanilla HTML/JS).

    Uses st.html(unsafe_allow_javascript=True) to inject the HTML directly
    into the Streamlit DOM (NO iframe). This allows natural height flow —
    collapsed state takes zero extra space, expanded state grows naturally.
    All CSS is scoped under .mds-root to avoid Streamlit style conflicts.
    Collapse/expand uses vanilla JS + localStorage persistence.

    Args:
        exchange: Exchange name ("binanceusdm", "bybit", "hyperliquid")
    """
    from pathlib import Path
    from api.auth import generate_token

    # Ensure FastAPI server is running and get config
    api_host, api_port, success = _start_fastapi_server_if_needed()

    if not success:
        st.error(f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
                 f"Please check **System → Services → API Server** or start manually: `python PBApiServer.py`")
        return

    # Get or create token for current session
    if "api_token" not in st.session_state:
        user_id = st.session_state.get("user", {}).get("id") or st.session_state.get("user") or "anonymous"
        token_obj = generate_token(str(user_id), expires_in_seconds=86400)
        st.session_state["api_token"] = token_obj.token

    token = st.session_state["api_token"]
    exchange_param = exchange.lower().strip()

    # Derive browser-usable hostname (0.0.0.0 is not routable from the browser)
    _browser_host = "127.0.0.1"
    try:
        _req_host = st.context.headers.get("Host", "")
        if _req_host:
            _browser_host = _req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    api_host_str = f"{_browser_host}:{api_port}"
    api_base_str = f"http://{_browser_host}:{api_port}/api"

    # Read HTML template and make element IDs unique per exchange
    html_path = Path(__file__).parent / "frontend" / "market_data_status.html"
    html_content = html_path.read_text(encoding="utf-8")

    # Unique IDs so multiple instances on the same page don't conflict
    instance_id = f"mds_{exchange_param}"
    html_content = html_content.replace("__MDS_ROOT_ID__", instance_id)
    html_content = html_content.replace("__MDS_ID__", f"{instance_id}_")

    # Inject config via data-* attributes on root element (no separate script needed)
    html_content = html_content.replace(
        'data-token=""', f'data-token="{token}"'
    ).replace(
        'data-exchange=""', f'data-exchange="{exchange_param}"'
    ).replace(
        'data-api-host=""', f'data-api-host="{api_host_str}"'
    ).replace(
        'data-api-base=""', f'data-api-base="{api_base_str}"'
    )

    # st.html with unsafe_allow_javascript: renders directly in DOM (no iframe!)
    # Natural height flow — collapsed = just header, expanded = full content
    st.html(html_content, unsafe_allow_javascript=True)


def render_fastapi_gap_heatmap(exchange: str, dataset: str, coin: str) -> None:
    """
    Render the Gap / Coverage Heatmap via FastAPI + Vanilla JS (Plotly.js).

    Uses st.html(unsafe_allow_javascript=True) to inject the HTML directly
    into the Streamlit DOM (NO iframe, NO run_every fragment).
    The JS frontend loads Plotly.js from the local API server, fetches chart
    data from /api/heatmap/* endpoints, and keeps itself up-to-date via a
    WebSocket (/ws/heatmap-watch) that fires when the underlying data changes.

    Args:
        exchange: Exchange name ("hyperliquid", "binanceusdm", "bybit", ...)
        dataset:  Dataset name ("1m", "candles_1m", "l2Book", "pb7_cache:…", …)
        coin:     Coin symbol (e.g. "BTC", "AAPL")
    """
    from pathlib import Path
    from api.auth import generate_token

    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        st.error(f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
                 f"Please check **System → Services → API Server** or start manually: `python PBApiServer.py`")
        return

    if "api_token" not in st.session_state:
        user_id = (st.session_state.get("user", {}).get("id")
                   or st.session_state.get("user")
                   or "anonymous")
        token_obj = generate_token(str(user_id), expires_in_seconds=86400)
        st.session_state["api_token"] = token_obj.token

    token = st.session_state["api_token"]
    exchange_param = str(exchange).lower().strip()
    dataset_param  = str(dataset).strip()
    coin_param     = str(coin).strip()

    # Derive browser-usable hostname (0.0.0.0 is not routable from the browser)
    _browser_host = "127.0.0.1"
    try:
        _req_host = st.context.headers.get("Host", "")
        if _req_host:
            _browser_host = _req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass

    api_base_str   = f"http://{_browser_host}:{api_port}/api"

    html_path = Path(__file__).parent / "frontend" / "gap_heatmap.html"
    html_content = html_path.read_text(encoding="utf-8")

    # Make element IDs unique per exchange/dataset/coin so multiple instances
    # on the same Streamlit page don't collide.
    safe_ds   = dataset_param.replace(":", "_").replace("/", "_")
    safe_coin = coin_param.replace("/", "_").replace(":", "_")
    instance_id = f"hm_{exchange_param}_{safe_ds}_{safe_coin}"
    html_content = html_content.replace("__HM_ROOT_ID__", instance_id)

    html_content = (
        html_content
        .replace('data-token=""',    f'data-token="{token}"')
        .replace('data-exchange=""', f'data-exchange="{exchange_param}"')
        .replace('data-dataset=""',  f'data-dataset="{dataset_param}"')
        .replace('data-coin=""',     f'data-coin="{coin_param}"')
        .replace('data-api-host=""', f'data-api-host="{_browser_host}:{api_port}"')
        .replace('data-api-base=""', f'data-api-base="{api_base_str}"')
    )

    st.html(html_content, unsafe_allow_javascript=True)


# ── Navigation bridge (WebSocket ↔ Streamlit bidirectional component) ──

_nav_bridge_component = _st_components.declare_component(
    "nav_bridge",
    path=str(Path(__file__).parent / "components" / "nav_bridge"),
)


def nav_bridge() -> None:
    """Embed a zero-height bidirectional component that listens for
    ``nav_request`` events on the ``/ws/dashboard`` WebSocket.

    When a widget iframe POSTs to ``/api/nav/request``, FastAPI broadcasts
    a ``nav_request`` to all dashboard WS clients.  This component catches
    it and sends the payload back to Python, which routes with
    ``st.switch_page()`` — no page reload needed.

    Call once per page that should respond to cross-widget navigation.
    """
    api_host, api_port, success = _start_fastapi_server_if_needed()
    if not success:
        return

    from api.auth import generate_token as _gen_token

    if "api_token" not in st.session_state:
        user_id = (
            st.session_state.get("user", {}).get("id")
            or st.session_state.get("user")
            or "anonymous"
        )
        st.session_state["api_token"] = _gen_token(
            str(user_id), expires_in_seconds=86400
        ).token
    token = st.session_state["api_token"]

    # Build WS URL that the browser can reach
    _browser_host = "127.0.0.1"
    try:
        req_host = st.context.headers.get("Host", "")
        if req_host:
            _browser_host = req_host.split(":")[0] or "127.0.0.1"
    except Exception:
        pass
    ws_url = f"ws://{_browser_host}:{api_port}/ws/dashboard?token={token}"

    result = _nav_bridge_component(ws_url=ws_url, key="__nav_bridge__", default=None)
    if result and isinstance(result, dict):
        # Clear component value to prevent re-processing on rerun
        if "__nav_bridge__" in st.session_state:
            del st.session_state["__nav_bridge__"]
        if result.get("action"):
            _handle_dashboard_action(result["action"], result.get("params", {}))
        elif result.get("page"):
            page = result["page"]
            params = result.get("params", {})
            # Store params in session_state so the target page picks them up
            for k, v in params.items():
                st.session_state[k] = v
            # Look up the page path
            navi_paths = get_navi_paths()
            page_path = navi_paths.get(page, "")
            if page_path:
                st.switch_page(page_path)


_VALID_DASHBOARD_ACTIONS = frozenset({
    'select_dashboard', 'new_dashboard', 'edit_dashboard',
    'save_dashboard', 'cancel_edit', 'delete_dashboard', 'refresh',
})

def _handle_dashboard_action(action: str, params: dict) -> None:
    """Handle dashboard sidebar actions received via nav_bridge WebSocket."""
    if action not in _VALID_DASHBOARD_ACTIONS:
        return
    from Dashboard import Dashboard

    if action == "select_dashboard":
        name = params.get("name", "")
        if name:
            if "edit_dashboard" in st.session_state:
                del st.session_state["edit_dashboard"]
            if "_dashboard_edit_original_name" in st.session_state:
                del st.session_state["_dashboard_edit_original_name"]
            st.session_state.dashboard = Dashboard(name)
            st.rerun()

    elif action == "new_dashboard":
        if "dashboard" in st.session_state:
            del st.session_state.dashboard
        st.session_state.dashboard = Dashboard()
        st.session_state["_dashboard_edit_original_name"] = None
        st.session_state.edit_dashboard = True
        st.rerun()

    elif action == "edit_dashboard":
        if "dashboard" in st.session_state and "edit_dashboard" not in st.session_state:
            try:
                st.session_state["_dashboard_edit_original_name"] = st.session_state.dashboard.name
            except Exception:
                st.session_state["_dashboard_edit_original_name"] = None
            st.session_state.edit_dashboard = True
            st.rerun()

    elif action == "save_dashboard":
        if "dashboard" in st.session_state:
            if st.session_state.dashboard.get_draft_name():
                try:
                    st.session_state.dashboard.save()
                except Exception:
                    return
                st.session_state.dashboards = st.session_state.dashboard.list_dashboards()
                if "edit_dashboard" in st.session_state:
                    del st.session_state.edit_dashboard
                try:
                    st.session_state.dashboard.load(st.session_state.dashboard.name)
                except Exception:
                    pass
                if "_dashboard_edit_original_name" in st.session_state:
                    del st.session_state["_dashboard_edit_original_name"]
                st.rerun()

    elif action == "cancel_edit":
        orig = st.session_state.get("_dashboard_edit_original_name")
        if "edit_dashboard" in st.session_state:
            del st.session_state.edit_dashboard
        if "_dashboard_edit_original_name" in st.session_state:
            del st.session_state["_dashboard_edit_original_name"]
        if orig:
            try:
                available = Dashboard().list_dashboards()
                if orig in available:
                    st.session_state.dashboard = Dashboard(orig)
            except Exception:
                # Restore failed — fall through to clear dashboard
                if "dashboard" in st.session_state:
                    del st.session_state.dashboard
        else:
            # Was a new (unsaved) dashboard — clear it
            if "dashboard" in st.session_state:
                del st.session_state.dashboard
        st.rerun()

    elif action == "delete_dashboard":
        if "dashboard" in st.session_state:
            try:
                st.session_state.dashboard.delete()
            except Exception:
                pass
            st.session_state.dashboards = st.session_state.dashboard.list_dashboards()
            if "edit_dashboard" in st.session_state:
                del st.session_state.edit_dashboard
            del st.session_state.dashboard
            st.rerun()

    elif action == "refresh":
        st.session_state.dashboards = Dashboard().list_dashboards()
        st.rerun()

