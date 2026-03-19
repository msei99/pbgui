import streamlit as st
from PBData import PBData
from pathlib import Path
import json
import pandas as pd
from datetime import datetime, date, timedelta, timezone
from dateutil.relativedelta import relativedelta, MO
import numpy as np
from Exchange import Exchange
from pbgui_func import PBGDIR, pb7dir, get_navi_paths
import plotly.express as px
import plotly.graph_objects as go
from Database import Database
import pbgui_help

import time

class Dashboard():

    # Periods
    @property
    def now(self): return datetime.now()
    @property
    def now_ts(self): return int(self.now.timestamp()) * 1000
    @property
    def today(self): return date.today()
    @property
    def today_ts(self): return int(time.mktime(self.today.timetuple())) * 1000
    @property
    def yesterday(self): return self.today - timedelta(days = 1)
    @property
    def yesterday_ts(self): return int(time.mktime(self.yesterday.timetuple())) * 1000
    @property
    def lastMonday(self): return self.today + relativedelta(weekday=MO(-1))
    @property
    def lastMonday_ts(self): return int(time.mktime(self.lastMonday.timetuple())) * 1000
    @property
    def lastWeekMonday(self): return self.lastMonday - timedelta(days = 7)
    @property
    def lastWeekMonday_ts(self): return int(time.mktime(self.lastWeekMonday.timetuple())) * 1000
    @property
    def thisMonth(self): return self.today + relativedelta(day=1)
    @property
    def thisMonth_ts(self): return int(time.mktime(self.thisMonth.timetuple())) * 1000
    @property
    def lastMonth(self): return self.thisMonth - relativedelta(months=1)
    @property
    def lastMonth_ts(self): return int(time.mktime(self.lastMonth.timetuple())) * 1000
    @property
    def thisYear(self): return self.today + relativedelta(day=1, month=1)
    @property
    def thisYear_ts(self): return int(time.mktime(self.thisYear.timetuple())) * 1000
    @property
    def lastYear(self): return self.thisYear - relativedelta(years=1)
    @property
    def lastYear_ts(self): return int(time.mktime(self.lastYear.timetuple())) * 1000
    @property
    def TODAY(self): return [self.today_ts, self.now_ts]
    @property
    def YESTERDAY(self): return [self.yesterday_ts, self.today_ts]
    @property
    def THIS_WEEK(self): return [self.lastMonday_ts, self.now_ts]
    @property
    def LAST_WEEK(self): return [self.lastWeekMonday_ts, self.lastMonday_ts]
    @property
    def LAST_WEEK_NOW(self): return [self.lastWeekMonday_ts, self.now_ts]
    @property
    def THIS_MONTH(self): return [self.thisMonth_ts, self.now_ts]
    @property
    def LAST_MONTH(self): return [self.lastMonth_ts, self.thisMonth_ts]
    @property
    def LAST_MONTH_NOW(self): return [self.lastMonth_ts, self.now_ts]
    @property
    def THIS_YEAR(self): return [self.thisYear_ts, self.now_ts]
    @property
    def LAST_YEAR(self): return [self.lastYear_ts, self.thisYear_ts]
    @property
    def LAST_YEAR_NOW(self): return [self.lastYear_ts, self.now_ts]
    @property
    def ALL_TIME(self): return [0, self.now_ts]

    PERIOD = ['TODAY', 'YESTERDAY', 'THIS_WEEK', 'LAST_WEEK', 'LAST_WEEK_NOW', 'THIS_MONTH', 'LAST_MONTH', 'LAST_MONTH_NOW', 'THIS_YEAR', 'LAST_YEAR', 'LAST_YEAR_NOW',  'ALL_TIME']
    SUM_PERIOD = ['DAY', 'WEEK', 'MONTH', 'YEAR', 'ALL_TIME']
    DASHBOARD_TYPES = ['NONE', 'PNL', 'ADG', 'TOP', 'POSITIONS', 'ORDERS', 'INCOME', 'BALANCE', 'P+L']

    def __init__(self, name : str = None):
        self.cleanup_dashboard_session_state()
        self.name = name
        self.cols = 1
        self.rows = 1
        self.dashboard_config = {}
        self.user = st.session_state.users.users[0]
        self.db = Database()
        self.view_orders_position = []
        # Ensure last DB backup session state is available across dashboard switches
        try:
            # Initialize last backup strictly from the new path only
            backups_dir = Path(f"{PBGDIR}/data/backup/db")
            if backups_dir.exists():
                backups = [p for p in backups_dir.glob("pbgui-*.db") if p.is_file()]
                if backups:
                    latest = max(backups, key=lambda p: p.stat().st_mtime)
                    current = st.session_state.get('db_last_backup')
                    # If missing, stale, or pointing to the legacy folder, set to latest known backup
                    if (not current) or (not Path(current).exists()) or ("/data/backups" in str(current)):
                        st.session_state['db_last_backup'] = str(latest)
        except Exception:
            pass
        if self.name:
            self.load(name)

    def cleanup_dashboard_session_state(self):
        # Snapshot keys first to avoid Streamlit session_state KeyError when
        # session state is mutated during iteration (concurrent access).
        try:
            keys = list(st.session_state.keys())
        except Exception:
            keys = []
        dashboard_keys = [k for k in keys if
            k.startswith("dashboard_") or
            k.startswith("view_orders_") or
            k.startswith("_dbe_init_")]
        for key in dashboard_keys:
            # Use pop with default to avoid KeyError if another part removed it
            st.session_state.pop(key, None)

    def _push_pending_config(self):
        """Push current dashboard_config to FastAPI so the editor can initialise from it."""
        try:
            from pbgui_func import _start_fastapi_server_if_needed
            import urllib.request, urllib.parse, json as _json
            _, api_port, ok = _start_fastapi_server_if_needed()
            if not ok:
                return
            token = st.session_state.get('api_token', '')
            cfg = dict(self.dashboard_config)
            cfg['name'] = self.name
            cfg['rows'] = self.rows
            cfg['cols'] = self.cols
            data = _json.dumps(cfg).encode()
            url = (f'http://127.0.0.1:{api_port}/api/dashboard/pending_full'
                   f'?name={urllib.parse.quote(self.name or "")}&token={token}')
            req = urllib.request.Request(
                url, data=data,
                headers={'Content-Type': 'application/json'},
                method='POST'
            )
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass

    def get_draft_name(self) -> str:
        """Return the working name from the JS editor (FastAPI pending config), or self.name.

        Used by info_dashboards to validate the name before saving, since for new
        (unsaved) dashboards self.name is empty and the name is only in the JS editor.
        """
        try:
            pending = self._pull_pending_config()
            n = str(pending.get('name', '') or '').strip()
            if n:
                return n
        except Exception:
            pass
        return self.name or ''

    def _pull_pending_config(self) -> dict:
        """Pull the full pending config from FastAPI (written by the JS editor)."""
        try:
            from pbgui_func import _start_fastapi_server_if_needed
            import urllib.request, urllib.parse, json as _json
            _, api_port, ok = _start_fastapi_server_if_needed()
            if not ok:
                return {}
            token = st.session_state.get('api_token', '')
            url = (f'http://127.0.0.1:{api_port}/api/dashboard/pending_full'
                   f'?name={urllib.parse.quote(self.name or "")}&token={token}')
            with urllib.request.urlopen(url, timeout=2) as resp:
                data = _json.loads(resp.read())
                if data.get('found'):
                    return data['config']
        except Exception:
            pass
        return {}

    def swap(self, from_row, to_row, from_col, to_col):
        # Helper: read from session_state, fall back to dashboard_config
        def _get(key):
            return st.session_state.get(key, self.dashboard_config.get(key))

        def _del(key):
            st.session_state.pop(key, None)

        dashboard_type_1 = _get(f'dashboard_type_{from_row}_{from_col}') or 'NONE'
        dashboard_type_2 = _get(f'dashboard_type_{to_row}_{to_col}') or 'NONE'
        _del(f'dashboard_type_{from_row}_{from_col}')
        _del(f'dashboard_type_{to_row}_{to_col}')
        st.session_state[f'dashboard_type_{from_row}_{from_col}'] = dashboard_type_2
        st.session_state[f'dashboard_type_{to_row}_{to_col}'] = dashboard_type_1
        move_1 = {}
        move_2 = {}
        if dashboard_type_1 == "PNL":
            move_1 = {
                "pnl_users_1": _get(f'dashboard_pnl_users_{from_row}_{from_col}'),
                "pnl_period_1": _get(f'dashboard_pnl_period_{from_row}_{from_col}'),
                "pnl_mode_1": _get(f'dashboard_pnl_mode_{from_row}_{from_col}'),
            }
            _del(f'dashboard_pnl_users_{from_row}_{from_col}'); _del(f'dashboard_pnl_period_{from_row}_{from_col}'); _del(f'dashboard_pnl_mode_{from_row}_{from_col}')
        if dashboard_type_1 == "ADG":
            move_1 = {
                "adg_users_1": _get(f'dashboard_adg_users_{from_row}_{from_col}'),
                "adg_period_1": _get(f'dashboard_adg_period_{from_row}_{from_col}'),
                "adg_mode_1": _get(f'dashboard_adg_mode_{from_row}_{from_col}'),
            }
            _del(f'dashboard_adg_users_{from_row}_{from_col}'); _del(f'dashboard_adg_period_{from_row}_{from_col}'); _del(f'dashboard_adg_mode_{from_row}_{from_col}')
        if dashboard_type_1 == "INCOME":
            move_1 = {
                "income_users_1": _get(f'dashboard_income_users_{from_row}_{from_col}'),
                "income_period_1": _get(f'dashboard_income_period_{from_row}_{from_col}'),
                "income_last_1": _get(f'dashboard_income_last_{from_row}_{from_col}'),
                "income_filter_1": _get(f'dashboard_income_filter_{from_row}_{from_col}'),
            }
            _del(f'dashboard_income_users_{from_row}_{from_col}'); _del(f'dashboard_income_period_{from_row}_{from_col}')
            _del(f'dashboard_income_last_{from_row}_{from_col}'); _del(f'dashboard_income_filter_{from_row}_{from_col}')
        if dashboard_type_1 == "TOP":
            move_1 = {
                "top_symbols_users_1": _get(f'dashboard_top_symbols_users_{from_row}_{from_col}'),
                "top_symbols_period_1": _get(f'dashboard_top_symbols_period_{from_row}_{from_col}'),
                "top_symbols_top_1": _get(f'dashboard_top_symbols_top_{from_row}_{from_col}'),
            }
            _del(f'dashboard_top_symbols_users_{from_row}_{from_col}'); _del(f'dashboard_top_symbols_period_{from_row}_{from_col}'); _del(f'dashboard_top_symbols_top_{from_row}_{from_col}')
        if dashboard_type_1 == "BALANCE":
            move_1 = {"balance_users_1": _get(f'dashboard_balance_users_{from_row}_{from_col}')}
            _del(f'dashboard_balance_users_{from_row}_{from_col}')
        if dashboard_type_1 == "POSITIONS":
            move_1 = {"positions_users_1": _get(f'dashboard_positions_users_{from_row}_{from_col}')}
            _del(f'dashboard_positions_users_{from_row}_{from_col}')
        if dashboard_type_1 == "ORDERS":
            move_1 = {"orders_1": _get(f'dashboard_orders_{from_row}_{from_col}')}
            _del(f'dashboard_orders_{from_row}_{from_col}')
        if dashboard_type_1 == "P+L":
            move_1 = {
                "ppl_users_1": _get(f'dashboard_ppl_users_{from_row}_{from_col}'),
                "ppl_period_1": _get(f'dashboard_ppl_period_{from_row}_{from_col}'),
                "ppl_sum_period_1": _get(f'dashboard_ppl_sum_period_{from_row}_{from_col}'),
            }
            _del(f'dashboard_ppl_users_{from_row}_{from_col}'); _del(f'dashboard_ppl_period_{from_row}_{from_col}'); _del(f'dashboard_ppl_sum_period_{from_row}_{from_col}')

        if dashboard_type_2 == "PNL":
            move_2 = {
                "pnl_users_2": _get(f'dashboard_pnl_users_{to_row}_{to_col}'),
                "pnl_period_2": _get(f'dashboard_pnl_period_{to_row}_{to_col}'),
                "pnl_mode_2": _get(f'dashboard_pnl_mode_{to_row}_{to_col}'),
            }
            _del(f'dashboard_pnl_users_{to_row}_{to_col}'); _del(f'dashboard_pnl_period_{to_row}_{to_col}'); _del(f'dashboard_pnl_mode_{to_row}_{to_col}')
        if dashboard_type_2 == "ADG":
            move_2 = {
                "adg_users_2": _get(f'dashboard_adg_users_{to_row}_{to_col}'),
                "adg_period_2": _get(f'dashboard_adg_period_{to_row}_{to_col}'),
                "adg_mode_2": _get(f'dashboard_adg_mode_{to_row}_{to_col}'),
            }
            _del(f'dashboard_adg_users_{to_row}_{to_col}'); _del(f'dashboard_adg_period_{to_row}_{to_col}'); _del(f'dashboard_adg_mode_{to_row}_{to_col}')
        if dashboard_type_2 == "INCOME":
            move_2 = {
                "income_users_2": _get(f'dashboard_income_users_{to_row}_{to_col}'),
                "income_period_2": _get(f'dashboard_income_period_{to_row}_{to_col}'),
                "income_last_2": _get(f'dashboard_income_last_{to_row}_{to_col}'),
                "income_filter_2": _get(f'dashboard_income_filter_{to_row}_{to_col}'),
            }
            _del(f'dashboard_income_users_{to_row}_{to_col}'); _del(f'dashboard_income_period_{to_row}_{to_col}')
            _del(f'dashboard_income_last_{to_row}_{to_col}'); _del(f'dashboard_income_filter_{to_row}_{to_col}')
        if dashboard_type_2 == "TOP":
            move_2 = {
                "top_symbols_users_2": _get(f'dashboard_top_symbols_users_{to_row}_{to_col}'),
                "top_symbols_period_2": _get(f'dashboard_top_symbols_period_{to_row}_{to_col}'),
                "top_symbols_top_2": _get(f'dashboard_top_symbols_top_{to_row}_{to_col}'),
            }
            _del(f'dashboard_top_symbols_users_{to_row}_{to_col}'); _del(f'dashboard_top_symbols_period_{to_row}_{to_col}'); _del(f'dashboard_top_symbols_top_{to_row}_{to_col}')
        if dashboard_type_2 == "BALANCE":
            move_2 = {"balance_users_2": _get(f'dashboard_balance_users_{to_row}_{to_col}')}
            _del(f'dashboard_balance_users_{to_row}_{to_col}')
        if dashboard_type_2 == "POSITIONS":
            move_2 = {"positions_users_2": _get(f'dashboard_positions_users_{to_row}_{to_col}')}
            _del(f'dashboard_positions_users_{to_row}_{to_col}')
        if dashboard_type_2 == "ORDERS":
            move_2 = {"orders_2": _get(f'dashboard_orders_{to_row}_{to_col}')}
            _del(f'dashboard_orders_{to_row}_{to_col}')
        if dashboard_type_2 == "P+L":
            move_2 = {
                "ppl_users_2": _get(f'dashboard_ppl_users_{to_row}_{to_col}'),
                "ppl_period_2": _get(f'dashboard_ppl_period_{to_row}_{to_col}'),
                "ppl_sum_period_2": _get(f'dashboard_ppl_sum_period_{to_row}_{to_col}'),
            }
            _del(f'dashboard_ppl_users_{to_row}_{to_col}'); _del(f'dashboard_ppl_period_{to_row}_{to_col}'); _del(f'dashboard_ppl_sum_period_{to_row}_{to_col}')

        for key, val in move_1.items():
            key_new = key.replace(f"_1", f"_{to_row}_{to_col}")
            st.session_state[f'dashboard_{key_new}'] = val
        for key, val in move_2.items():
            key_new = key.replace(f"_2", f"_{from_row}_{from_col}")
            st.session_state[f'dashboard_{key_new}'] = val
        dashboard_orders = {key: val for key, val in st.session_state.items()
            if key.startswith("dashboard_orders_")}
        for key, val in dashboard_orders.items():
            if val == f'view_orders_{from_row}_{from_col}':
                _del(key)
                st.session_state[key] = f'view_orders_{to_row}_{to_col}'
                order = st.session_state.get(f'view_orders_{from_row}_{from_col}')
                _del(f'view_orders_{from_row}_{from_col}')
                st.session_state[f'view_orders_{to_row}_{to_col}'] = order
            if val == f'view_orders_{to_row}_{to_col}':
                _del(key)
                st.session_state[key] = f'view_orders_{from_row}_{from_col}'
                order = st.session_state.get(f'view_orders_{to_row}_{to_col}')
                _del(f'view_orders_{to_row}_{to_col}')
                st.session_state[f'view_orders_{from_row}_{from_col}'] = order

        # Also swap keys in self.dashboard_config so _render_cell_preview() uses
        # the correct data after rerun (it reads from dashboard_config, not session_state).
        suffix_a = f'_{from_row}_{from_col}'
        suffix_b = f'_{to_row}_{to_col}'
        keys_a = {k: v for k, v in self.dashboard_config.items() if k.endswith(suffix_a)}
        keys_b = {k: v for k, v in self.dashboard_config.items() if k.endswith(suffix_b)}
        # Delete originals first, then write at swapped positions
        for k in keys_a:
            del self.dashboard_config[k]
        for k in keys_b:
            del self.dashboard_config[k]
        for k, v in keys_a.items():
            self.dashboard_config[k[:-len(suffix_a)] + suffix_b] = v
        for k, v in keys_b.items():
            self.dashboard_config[k[:-len(suffix_b)] + suffix_a] = v

        st.session_state.swap_rerun = True

    # ---------------------------------------------------------------------- FastAPI helpers

    def _get_pending_grid_config(self) -> dict | None:
        """Read the pending grid config from the JS editor via FastAPI."""
        try:
            from pbgui_func import _start_fastapi_server_if_needed
            from api.auth import generate_token
            import urllib.request
            import json as _j
            _h, _p, _ok = _start_fastapi_server_if_needed()
            if not _ok:
                return None
            _tok = st.session_state.get('api_token', '') or generate_token('save', expires_in_seconds=60).token
            _url = f'http://127.0.0.1:{_p}/api/dashboard/grid/pending'
            req = urllib.request.Request(_url, headers={'Authorization': f'Bearer {_tok}'})
            with urllib.request.urlopen(req, timeout=2) as r:
                data = _j.loads(r.read())
            return data if data.get('found') else None
        except Exception:
            return None

    def _render_grid_editor(self):
        """Embed the compact JS header (name, cols, rows) via st.html.
        Cell type selectors and swap buttons are handled by Streamlit inline."""
        from pathlib import Path as _Path
        from pbgui_func import _start_fastapi_server_if_needed
        from api.auth import generate_token
        import json as _json

        api_host, api_port, success = _start_fastapi_server_if_needed()
        if not success:
            st.error("⚠️ FastAPI server not available. Please check System → Services → API Server.")
            return

        if "api_token" not in st.session_state:
            user_id = (
                st.session_state.get("user", {}).get("id")
                or st.session_state.get("user")
                or "anonymous"
            )
            st.session_state["api_token"] = generate_token(str(user_id), expires_in_seconds=86400).token
        token = st.session_state["api_token"]

        _browser_host = "127.0.0.1"
        try:
            req_host = st.context.headers.get("Host", "")
            if req_host:
                _browser_host = req_host.split(":")[0] or "127.0.0.1"
        except Exception:
            pass

        api_base_str = f"http://{_browser_host}:{api_port}/api"
        name_json = _json.dumps(self.name or "")

        html_path = _Path(__file__).parent / "frontend" / "dashboard_grid_editor.html"
        html = html_path.read_text(encoding="utf-8")
        html = html.replace('"%%TOKEN%%"', f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('%%DASHBOARD_NAME%%', name_json)
        html = html.replace('%%DASHBOARD_ROWS%%', str(self.rows))
        html = html.replace('%%DASHBOARD_COLS%%', str(self.cols))
        st.html(html, unsafe_allow_javascript=True)

    def _render_cell_preview(self, row: int, col: int, dashboard_config: dict):
        """Render a widget preview for one cell in edit mode (based on the loaded config)."""
        pos = f'{row}_{col}'
        # Use live session_state value (selectbox), fall back to saved config
        cell_type = st.session_state.get(f'dashboard_type_{pos}',
                    dashboard_config.get(f'dashboard_type_{pos}', 'NONE'))
        if cell_type == 'NONE':
            return
        if cell_type == "PNL":
            if all(k in dashboard_config for k in [f'dashboard_pnl_users_{pos}', f'dashboard_pnl_period_{pos}', f'dashboard_pnl_mode_{pos}']):
                self.view_pnl_impl(pos, dashboard_config[f'dashboard_pnl_users_{pos}'], dashboard_config[f'dashboard_pnl_period_{pos}'], dashboard_config[f'dashboard_pnl_mode_{pos}'])
            else:
                self.view_pnl_impl(pos)
        elif cell_type == "ADG":
            if all(k in dashboard_config for k in [f'dashboard_adg_users_{pos}', f'dashboard_adg_period_{pos}', f'dashboard_adg_mode_{pos}']):
                self.view_adg_impl(pos, dashboard_config[f'dashboard_adg_users_{pos}'], dashboard_config[f'dashboard_adg_period_{pos}'], dashboard_config[f'dashboard_adg_mode_{pos}'])
            else:
                self.view_adg_impl(pos)
        elif cell_type == "INCOME":
            if f'dashboard_income_last_{pos}' not in dashboard_config:
                dashboard_config[f'dashboard_income_last_{pos}'] = 0
                dashboard_config[f'dashboard_income_filter_{pos}'] = 0.0
            if all(k in dashboard_config for k in [f'dashboard_income_users_{pos}', f'dashboard_income_period_{pos}', f'dashboard_income_last_{pos}', f'dashboard_income_filter_{pos}']):
                self.view_income_impl(pos, dashboard_config[f'dashboard_income_users_{pos}'], dashboard_config[f'dashboard_income_period_{pos}'], dashboard_config[f'dashboard_income_last_{pos}'], dashboard_config[f'dashboard_income_filter_{pos}'])
            else:
                self.view_income_impl(pos)
        elif cell_type == "TOP":
            if all(k in dashboard_config for k in [f'dashboard_top_symbols_users_{pos}', f'dashboard_top_symbols_period_{pos}', f'dashboard_top_symbols_top_{pos}']):
                self.view_top_symbols_impl(pos, dashboard_config[f'dashboard_top_symbols_users_{pos}'], dashboard_config[f'dashboard_top_symbols_period_{pos}'], dashboard_config[f'dashboard_top_symbols_top_{pos}'])
            else:
                self.view_top_symbols_impl(pos)
        elif cell_type == "POSITIONS":
            if f'dashboard_positions_users_{pos}' in dashboard_config:
                self.view_positions_impl(pos, dashboard_config[f'dashboard_positions_users_{pos}'])
            else:
                self.view_positions_impl(pos)
        elif cell_type == "ORDERS":
            self.view_orders_impl(pos, dashboard_config.get(f'dashboard_orders_{pos}'), edit=True)
        elif cell_type == "BALANCE":
            users = dashboard_config.get(f'dashboard_balance_users_{pos}') or None
            self.view_balance_impl(pos, users, edit=True)
        elif cell_type == "P+L":
            if all(k in dashboard_config for k in [f'dashboard_ppl_users_{pos}', f'dashboard_ppl_period_{pos}', f'dashboard_ppl_sum_period_{pos}']):
                self.view_ppl_impl(pos, dashboard_config[f'dashboard_ppl_users_{pos}'], dashboard_config[f'dashboard_ppl_period_{pos}'], dashboard_config[f'dashboard_ppl_sum_period_{pos}'])
            else:
                self.view_ppl_impl(pos)

    def create_dashboard(self):
        """Edit mode: embed the full FastAPI/Vanilla JS dashboard grid editor via st.html.

        The editor HTML is read from frontend/dashboard_editor.html, placeholders
        are substituted in Python, and the result is passed to st.html().
        No iframe — height is dynamic and the preview renders directly below.
        save() pulls the final config back from FastAPI.
        """
        from pbgui_func import _start_fastapi_server_if_needed
        from api.auth import generate_token
        from pathlib import Path as _Path
        import json as _json

        api_host, api_port, success = _start_fastapi_server_if_needed()
        if not success:
            st.error(
                f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
                "Please check **System → Services → API Server**."
            )
            return

        # Ensure session token
        if "api_token" not in st.session_state:
            user_id = (
                st.session_state.get("user", {}).get("id")
                or st.session_state.get("user")
                or "anonymous"
            )
            st.session_state["api_token"] = generate_token(
                str(user_id), expires_in_seconds=86400
            ).token
        token = st.session_state["api_token"]

        # Resolve browser-usable hostname
        _browser_host = "127.0.0.1"
        try:
            req_host = st.context.headers.get("Host", "")
            if req_host:
                _browser_host = req_host.split(":")[0] or "127.0.0.1"
        except Exception:
            pass

        api_base = f"http://{_browser_host}:{api_port}/api"

        # Push current config to FastAPI once when entering edit mode,
        # so the editor initialises from the saved state (not an empty grid).
        _init_key = f'_dbe_init_{self.name or "__new__"}'
        if _init_key not in st.session_state:
            self._push_pending_config()
            st.session_state[_init_key] = True

        # Read template, inject config as JS variables — no iframe needed.
        html_path = _Path(__file__).parent / "frontend" / "dashboard_editor.html"
        html = html_path.read_text(encoding="utf-8")
        html = html.replace('"%%TOKEN%%"',        f'"{token}"')
        html = html.replace('"%%API_BASE%%"',    f'"{api_base}"')
        html = html.replace('%%DASHBOARD_NAME%%', _json.dumps(self.name or ''))
        st.html(html, unsafe_allow_javascript=True)

        # ── Full preview below editor ───────────────────────────────────────
        cfg = self._pull_pending_config() or {}
        if not cfg.get('rows'):
            cfg = self.dashboard_config
        if cfg.get('rows'):
            st.divider()
            prev_rows = int(cfg.get('rows', 1))
            prev_cols = int(cfg.get('cols', 1))
            for _r in range(1, prev_rows + 1):
                if prev_cols == 2:
                    _c1, _c2 = st.columns([1, 1])
                    with _c1:
                        self._render_cell_preview(_r, 1, cfg)
                    with _c2:
                        self._render_cell_preview(_r, 2, cfg)
                else:
                    self._render_cell_preview(_r, 1, cfg)


    def save(self):
        """Save by pulling the full config from the FastAPI JS editor, then writing to JSON."""
        pending = self._pull_pending_config()
        if not pending:
            # Fallback: save whatever is currently in memory (should not normally happen)
            pending = dict(self.dashboard_config)
            pending['name'] = self.name
            pending['rows'] = self.rows
            pending['cols'] = self.cols

        rows = max(1, min(6, int(pending.get('rows', self.rows) or self.rows)))
        cols = max(1, min(2, int(pending.get('cols', self.cols) or self.cols)))
        name = str(pending.get('name', '') or '').strip()
        if name:
            self.name = name
        self.rows = rows
        self.cols = cols

        # Build a clean, normalised dashboard_config from the pending dict.
        # Copy only valid keys: rows/cols/name and per-cell keys ending in _{row}_{col}.
        dashboard_config: dict = {'rows': rows, 'cols': cols}
        for row in range(1, rows + 1):
            for col in range(1, cols + 1):
                pos = f'{row}_{col}'
                cell_type = pending.get(f'dashboard_type_{pos}', 'NONE') or 'NONE'
                dashboard_config[f'dashboard_type_{pos}'] = cell_type
                # Copy every key from pending that ends with _{pos}
                suffix = f'_{pos}'
                for k, v in pending.items():
                    if k != f'dashboard_type_{pos}' and k.endswith(suffix):
                        dashboard_config[k] = v

        self.dashboard_config = dashboard_config
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_path.mkdir(parents=True, exist_ok=True)
        dashboard_file = Path(f'{dashboard_path}/{self.name}.json')
        with dashboard_file.open('w') as f:
            json.dump(dashboard_config, f, indent=4)


    def load(self, name : str):
        self.cleanup_dashboard_session_state()
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_file = Path(f'{dashboard_path}/{name}.json')
        if dashboard_file.exists():
            with dashboard_file.open() as f:
                dashboard_config = json.load(f)
            self.dashboard_config = dashboard_config
            self.rows = dashboard_config["rows"]
            self.cols = dashboard_config["cols"]

    def delete(self):
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_file = Path(f'{dashboard_path}/{self.name}.json')
        if dashboard_file.exists():
            dashboard_file.unlink()

    def list_dashboards(self):
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboards = []
        for file in dashboard_path.glob("*.json"):
            dashboards.append(file.stem)
        dashboards.sort()
        return dashboards

    def view(self):
        # Init
        dashboard_config = self.dashboard_config
        self.rows = dashboard_config["rows"]
        self.cols = dashboard_config["cols"]
        # Titel
        st.subheader(f"Dashboard: {self.name}")

        for row in range(1, self.rows + 1):
            if self.cols == 2:
                db_col1, db_col2 = st.columns([1,1])
                with db_col1:
                    if dashboard_config[f'dashboard_type_{row}_1'] == "PNL":
                        self.view_pnl_impl(f'{row}_1', dashboard_config[f'dashboard_pnl_users_{row}_1'], dashboard_config[f'dashboard_pnl_period_{row}_1'], dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "ADG":
                        self.view_adg_impl(f'{row}_1', dashboard_config[f'dashboard_adg_users_{row}_1'], dashboard_config[f'dashboard_adg_period_{row}_1'], dashboard_config[f'dashboard_adg_mode_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "INCOME":
                        if f'dashboard_income_last_{row}_1' not in dashboard_config:
                            dashboard_config[f'dashboard_income_last_{row}_1'] = 0
                            dashboard_config[f'dashboard_income_filter_{row}_1'] = 0.0
                        self.view_income_impl(f'{row}_1', dashboard_config[f'dashboard_income_users_{row}_1'], dashboard_config[f'dashboard_income_period_{row}_1'], dashboard_config[f'dashboard_income_last_{row}_1'], dashboard_config[f'dashboard_income_filter_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "TOP":
                        self.view_top_symbols_impl(f'{row}_1', dashboard_config[f'dashboard_top_symbols_users_{row}_1'], dashboard_config[f'dashboard_top_symbols_period_{row}_1'], dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "POSITIONS":
                        self.view_positions_impl(f'{row}_1', dashboard_config[f'dashboard_positions_users_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "ORDERS":
                        self.view_orders_impl(f'{row}_1', dashboard_config[f'dashboard_orders_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "BALANCE":
                        self.view_balance_impl(f'{row}_1', dashboard_config[f'dashboard_balance_users_{row}_1'])
                    if dashboard_config[f'dashboard_type_{row}_1'] == "P+L":
                        if f'dashboard_ppl_sum_period_{row}_1' not in dashboard_config:
                            dashboard_config[f'dashboard_ppl_sum_period_{row}_1'] = 'DAY'
                        self.view_ppl_impl(f'{row}_1', dashboard_config[f'dashboard_ppl_users_{row}_1'], dashboard_config[f'dashboard_ppl_period_{row}_1'], dashboard_config[f'dashboard_ppl_sum_period_{row}_1'])
                with db_col2:
                    if dashboard_config[f'dashboard_type_{row}_2'] == "PNL":
                        self.view_pnl_impl(f'{row}_2', dashboard_config[f'dashboard_pnl_users_{row}_2'], dashboard_config[f'dashboard_pnl_period_{row}_2'], dashboard_config[f'dashboard_pnl_mode_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "ADG":
                        self.view_adg_impl(f'{row}_2', dashboard_config[f'dashboard_adg_users_{row}_2'], dashboard_config[f'dashboard_adg_period_{row}_2'], dashboard_config[f'dashboard_adg_mode_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "INCOME":
                        if f'dashboard_income_last_{row}_2' not in dashboard_config:
                            dashboard_config[f'dashboard_income_last_{row}_2'] = 0
                            dashboard_config[f'dashboard_income_filter_{row}_2'] = 0.0
                        self.view_income_impl(f'{row}_2', dashboard_config[f'dashboard_income_users_{row}_2'], dashboard_config[f'dashboard_income_period_{row}_2'], dashboard_config[f'dashboard_income_last_{row}_2'], dashboard_config[f'dashboard_income_filter_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "TOP":
                        self.view_top_symbols_impl(f'{row}_2', dashboard_config[f'dashboard_top_symbols_users_{row}_2'], dashboard_config[f'dashboard_top_symbols_period_{row}_2'], dashboard_config[f'dashboard_top_symbols_top_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "POSITIONS":
                        self.view_positions_impl(f'{row}_2', dashboard_config[f'dashboard_positions_users_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "ORDERS":
                        self.view_orders_impl(f'{row}_2', dashboard_config[f'dashboard_orders_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "BALANCE":
                        self.view_balance_impl(f'{row}_2', dashboard_config[f'dashboard_balance_users_{row}_2'])
                    if dashboard_config[f'dashboard_type_{row}_2'] == "P+L":
                        if f'dashboard_ppl_sum_period_{row}_2' not in dashboard_config:
                            dashboard_config[f'dashboard_ppl_sum_period_{row}_2'] = 'DAY'
                        self.view_ppl_impl(f'{row}_2', dashboard_config[f'dashboard_ppl_users_{row}_2'], dashboard_config[f'dashboard_ppl_period_{row}_2'], dashboard_config[f'dashboard_ppl_sum_period_{row}_2'])
            else:
                if dashboard_config[f'dashboard_type_{row}_1'] == "PNL":
                    self.view_pnl_impl(f'{row}_1', dashboard_config[f'dashboard_pnl_users_{row}_1'], dashboard_config[f'dashboard_pnl_period_{row}_1'], dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "ADG":
                    self.view_adg_impl(f'{row}_1', dashboard_config[f'dashboard_adg_users_{row}_1'], dashboard_config[f'dashboard_adg_period_{row}_1'], dashboard_config[f'dashboard_adg_mode_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "INCOME":
                    if f'dashboard_income_last_{row}_1' not in dashboard_config:
                        dashboard_config[f'dashboard_income_last_{row}_1'] = 0
                        dashboard_config[f'dashboard_income_filter_{row}_1'] = 0.0
                    self.view_income_impl(f'{row}_1', dashboard_config[f'dashboard_income_users_{row}_1'], dashboard_config[f'dashboard_income_period_{row}_1'], dashboard_config[f'dashboard_income_last_{row}_1'], dashboard_config[f'dashboard_income_filter_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "TOP":
                    self.view_top_symbols_impl(f'{row}_1', dashboard_config[f'dashboard_top_symbols_users_{row}_1'], dashboard_config[f'dashboard_top_symbols_period_{row}_1'], dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "POSITIONS":
                    self.view_positions_impl(f'{row}_1', dashboard_config[f'dashboard_positions_users_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "ORDERS":
                    self.view_orders_impl(f'{row}_1', dashboard_config[f'dashboard_orders_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "BALANCE":
                    self.view_balance_impl(f'{row}_1', dashboard_config[f'dashboard_balance_users_{row}_1'])
                if dashboard_config[f'dashboard_type_{row}_1'] == "P+L":
                    if f'dashboard_ppl_sum_period_{row}_1' not in dashboard_config:
                        dashboard_config[f'dashboard_ppl_sum_period_{row}_1'] = 'DAY'
                    self.view_ppl_impl(f'{row}_1', dashboard_config[f'dashboard_ppl_users_{row}_1'], dashboard_config[f'dashboard_ppl_period_{row}_1'], dashboard_config[f'dashboard_ppl_sum_period_{row}_1'])

    def view_pnl_impl(self, position : str, user : str = None, period : str = None, mode : str = "bar"):
        users = st.session_state.users
        if f"dashboard_pnl_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_pnl_users_{position}'] = user
                for user in st.session_state[f'dashboard_pnl_users_{position}']:
                    if user not in users.list() and user != 'ALL':
                        st.session_state[f'dashboard_pnl_users_{position}'].remove(user)
        if f"dashboard_pnl_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_pnl_period_{position}'] = period
        if f"dashboard_pnl_mode_{position}" not in st.session_state:
            if mode:
                st.session_state[f'dashboard_pnl_mode_{position}'] = mode
        st.markdown("#### :blue[Daily PNL]")
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_pnl_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_pnl_period_{position}")
        with col3:
            st.selectbox('Mode', ['bar', 'line'], key=f"dashboard_pnl_mode_{position}")
        if st.session_state[f'dashboard_pnl_users_{position}']:
            if st.session_state[f'dashboard_pnl_period_{position}'] in self.PERIOD:
                period_index = self.PERIOD.index(st.session_state[f'dashboard_pnl_period_{position}'])
                period_range = getattr(self, self.PERIOD[period_index])
                pnl = self.db.select_pnl(st.session_state[f'dashboard_pnl_users_{position}'], period_range[0], period_range[1])
            df = pd.DataFrame(pnl, columns =['Date', 'Income'])
            if st.session_state[f'dashboard_pnl_mode_{position}'] == "line":
                if not pnl:
                    return
                if len(pnl) <= 31:
                    fig = px.line(df, x='Date', y='Income', markers=True, text='Income', hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                else:
                    fig = px.line(df, x='Date', y='Income', markers=True, hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig.update_traces(texttemplate='%{text:.2f}', textposition='top left')
            else:
                fig = px.bar(df, x='Date', y='Income', text='Income', hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig.update_traces(texttemplate='%{text:.2f}', textposition='auto')
            fig.update_traces(marker_color=['red' if val < 0 else 'green' for val in df['Income']])
            st.plotly_chart(fig, key=f"dashboard_pnl_plot_{position}_{st.session_state.get('dashboard_reload_token', 0)}")
    
    def view_adg_impl(self, position : str, user : str = None, period : str = None, mode : str = "bar"):
        users = st.session_state.users
        if f"dashboard_adg_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_adg_users_{position}'] = user
                for user in st.session_state[f'dashboard_adg_users_{position}']:
                    if user not in users.list() and user != 'ALL':
                        st.session_state[f'dashboard_adg_users_{position}'].remove(user)
        if f"dashboard_adg_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_adg_period_{position}'] = period
        if f"dashboard_adg_mode_{position}" not in st.session_state:
            if mode:
                st.session_state[f'dashboard_adg_mode_{position}'] = mode
        st.markdown("#### :blue[ADG]")
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_adg_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_adg_period_{position}")
        with col3:
            st.selectbox('Mode', ['bar', 'line'], key=f"dashboard_adg_mode_{position}")
        if st.session_state[f'dashboard_adg_users_{position}']:
            if st.session_state[f'dashboard_adg_period_{position}'] in self.PERIOD:
                period_index = self.PERIOD.index(st.session_state[f'dashboard_adg_period_{position}'])
                period_range = getattr(self, self.PERIOD[period_index])
                adg = self.db.select_pnl(st.session_state[f'dashboard_adg_users_{position}'], period_range[0], period_range[1])

                # get current balance
                if 'ALL' in st.session_state[f'dashboard_adg_users_{position}']:
                    users_selected = users.list()
                else:
                    users_selected = st.session_state[f'dashboard_adg_users_{position}']
                balances = self.db.fetch_balances(users_selected)
                if not balances:
                    st.warning("No balance found.")
                    return

                # calculate total PNL
                total_pnl = sum(row[1] for row in adg if row[1] is not None)
                
                # calculate starting balance
                starting_balance = balances[0][2] - total_pnl

                current_balance = balances[0][2]
                for i in reversed(range(len(adg))):
                    # adg_last = adg[i-1][1] if i > 0 else 0
                    current_balance -= adg[i][1]
                    adg_value = 100 * (adg[i][1] / current_balance)
                    adg[i] = list(adg[i])
                    adg[i][1] = adg_value
                    adg[i] = tuple(adg[i])
            df = pd.DataFrame(adg, columns =['Date', 'ADG'])
            # fill missing dates with 0 ADG
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date').asfreq('D', fill_value=0).reset_index()
            if st.session_state[f'dashboard_adg_mode_{position}'] == "line":
                if not adg:
                    return
                if len(adg) <= 31:
                    fig = px.line(df, x='Date', y='ADG', markers=True, text='ADG', hover_data={'ADG':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                else:
                    fig = px.line(df, x='Date', y='ADG', markers=True, hover_data={'ADG':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig.update_traces(texttemplate='%{text:.2f}', textposition='top left')
            else:
                fig = px.bar(df, x='Date', y='ADG', text='ADG', hover_data={'ADG':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig.update_traces(texttemplate='%{text:.2f}', textposition='auto')
            fig.update_traces(marker_color=['red' if val < 0 else 'green' for val in df['ADG']])
            # name y axis ADG(%)
            fig.update_layout(
                yaxis_title='ADG(%)')
            fig.update_traces(hovertemplate='Date=%{x}<br>ADG=%{y:.2f}%')
            st.markdown(f"**Calculated Starting Balance:** {starting_balance:.2f} | **Total PNL:** {total_pnl:.2f} | **Current Balance:** {balances[0][2]:.2f}")
            st.plotly_chart(fig, key=f"dashboard_adg_plot_{position}_{st.session_state.get('dashboard_reload_token', 0)}")


    def view_ppl_impl(self, position : str, user : str = None, period : str = None, sum_period : str = None):
        users = st.session_state.users
        if f"dashboard_ppl_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_ppl_users_{position}'] = user
                for user in st.session_state[f'dashboard_ppl_users_{position}']:
                    if user not in users.list() and user != 'ALL':
                        st.session_state[f'dashboard_ppl_users_{position}'].remove(user)
        if f"dashboard_ppl_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_ppl_period_{position}'] = period
        if f"dashboard_ppl_sum_period_{position}" not in st.session_state:
            if sum_period:
                st.session_state[f'dashboard_ppl_sum_period_{position}'] = sum_period
                
        st.markdown("#### :blue[Profits and Losses]")
        col1, col2, col3 = st.columns([2,1,1])
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_ppl_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_ppl_period_{position}")
        with col3:
            st.selectbox('sum_period', self.SUM_PERIOD, key=f"dashboard_ppl_sum_period_{position}")
            
        if st.session_state[f'dashboard_ppl_users_{position}']:
            if st.session_state[f'dashboard_ppl_period_{position}'] in self.PERIOD:
                if st.session_state[f'dashboard_ppl_sum_period_{position}'] in self.SUM_PERIOD:
                    period_index = self.PERIOD.index(st.session_state[f'dashboard_ppl_period_{position}'])
                    period_range = getattr(self, self.PERIOD[period_index])
                    ppl = self.db.select_ppl(st.session_state[f'dashboard_ppl_users_{position}'], period_range[0], period_range[1], st.session_state[f'dashboard_ppl_sum_period_{position}'])
            
            df = pd.DataFrame(ppl, columns =['Date', 'sum_positive', 'sum_negative'])
            
            # Convert 'sum_negative' values to negative
            df['sum_negative'] = -df['sum_negative'].abs()

            # Reshape the DataFrame to long format
            df_long = pd.melt(
                df,
                id_vars=['Date'],
                value_vars=['sum_positive', 'sum_negative'],
                var_name='Type',
                value_name='Sum'
            )
            
            # Convert to Text, remove 0 values
            df_long['Text'] = df_long['Sum'].apply(lambda x: '' if x == 0 else f"{x:.2f}")

            # Map 'Type' values to 'Profits' and 'Losses'
            df_long['Type'] = df_long['Type'].map({
                'sum_positive': 'Profits',
                'sum_negative': 'Losses'
            })

            # Calculate Y-axis limits with 10% padding
            y_min = df_long['Sum'].min()
            y_max = df_long['Sum'].max()
            y_range = y_max - y_min
            padding = y_range * 0.10  # 10% padding

            # Adjust Y-axis limits
            y_axis_min = y_min - padding
            y_axis_max = y_max + padding

            # Create the bar chart
            fig = px.bar(
                df_long,
                x='Date',
                y='Sum',
                color='Type',
                barmode='relative',
                text='Text',  # Change this line
                title=f"From: {df['Date'].min()} To: {df['Date'].max()}",
                color_discrete_map={'Profits': 'green', 'Losses': 'red'}
            )

            # Update the figure layout
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            fig.update_layout(
                xaxis_title='Date',
                yaxis_title='Sum',
                yaxis=dict(
                    automargin=True,
                    range=[y_axis_min, y_axis_max],  # Set the new Y-axis range
                ),
                xaxis=dict(type='category'),  # Add this line to treat x-axis labels as categories
                legend_title_text='Legend'
            )

            # Display the plot
            st.plotly_chart(fig, key=f"dashboard_ppl_plot_{position}_{st.session_state.get('dashboard_reload_token', 0)}")

    def view_income_impl(self, position : str, user : str = None, period : str = None, last : int = 0, filter : float = 0.0):
        users = st.session_state.users
        if f"dashboard_income_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_income_users_{position}'] = user
                for user in st.session_state[f'dashboard_income_users_{position}']:
                    if user not in users.list() and user != 'ALL':
                        st.session_state[f'dashboard_income_users_{position}'].remove(user)
        if f"dashboard_income_period_{position}" not in st.session_state:
            if period:
                st.session_state[f'dashboard_income_period_{position}'] = period
        if f"dashboard_income_last_{position}" not in st.session_state:
            if last:
                st.session_state[f'dashboard_income_last_{position}'] = last
        if f"dashboard_income_filter_{position}" not in st.session_state:
            if filter:
                st.session_state[f'dashboard_income_filter_{position}'] = filter
        st.markdown("#### :blue[Income]")
        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 0.35], vertical_alignment="bottom")
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_income_users_{position}")
        with col2:
            st.selectbox('period', self.PERIOD, key=f"dashboard_income_period_{position}")
        with col3:
            st.number_input('Last N', min_value=0, step=10, key=f"dashboard_income_last_{position}", help=pbgui_help.dashboard_last)
        with col4:
            st.number_input('Filter', min_value=0.0, key=f"dashboard_income_filter_{position}", help=pbgui_help.dashboard_filter)
        with col5:
            users_selected_state = st.session_state.get(f'dashboard_income_users_{position}', [])
            single_user = None
            if users_selected_state and 'ALL' not in users_selected_state and len(users_selected_state) == 1:
                single_user = users_selected_state[0]
            if st.button(
                ":material/swap_horiz:",
                key=f"dashboard_income_open_live_vs_backtest_{position}",
                help="Open PBv7 Live vs Backtest",
                disabled=(single_user is None),
                width='stretch',
            ):
                st.session_state["v7_live_vs_backtest_prefill_user"] = single_user
                st.switch_page(get_navi_paths()["V7_LIVE_VS_BACKTEST"])
        if st.session_state[f'dashboard_income_users_{position}']:
            if st.session_state[f'dashboard_income_period_{position}'] in self.PERIOD:
                period_index = self.PERIOD.index(st.session_state[f'dashboard_income_period_{position}'])
                period_range = getattr(self, self.PERIOD[period_index])

                # include Id for delete operations
                income = self.db.select_income_by_symbol_with_id(
                    st.session_state[f'dashboard_income_users_{position}'], period_range[0], period_range[1]
                )
            df = pd.DataFrame(income, columns=['Id', 'Date', 'Symbol', 'Income', 'User'])
            # Preserve original ms timestamp for accurate cutoff deletion
            df['DateMs'] = df['Date']
            df['Date'] = pd.to_datetime(df['Date'], unit='ms')
            if st.session_state[f'dashboard_income_last_{position}'] > 0:
                # filter out lower than
                filter_value = st.session_state[f'dashboard_income_filter_{position}']
                df = df[(df['Income'] >= filter_value) | (df['Income'] <= -filter_value)]
                last_n = int(st.session_state[f'dashboard_income_last_{position}'])
                df = df.tail(last_n)
                # Sort by Date, oldest first
                df = df.sort_values(by='Date', ascending=False)
                # remove ms from Date
                df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                # Display as dataframe with selection + delete controls
                if len(df) > 25:
                    height = 36 + 25 * 35
                else:
                    height = 36 + (len(df)) * 35
                # Colorize Income: positive = green, negative = red
                def color_income(val):
                    color = 'green' if val >= 0 else 'red'
                    return f'color: {color};'
                styled_df = df[['Id', 'DateMs', 'Date', 'User', 'Symbol', 'Income']].style.map(color_income, subset=['Income']).format({'Income': '{:.2f}'})
                # Keep raw df for selection mapping
                st.session_state[f'dashboard_income_sdf_{position}'] = df
                column_config = {
                    'Id': None,
                    'DateMs': None  # hide helper columns from display but keep in df
                }
                reload_token = st.session_state.get('dashboard_reload_token', 0)
                table_key = f"dashboard_income_{position}_{reload_token}"
                st.dataframe(
                    styled_df,
                    height=height,
                    key=table_key,
                    on_select="rerun",
                    selection_mode='multi-row',
                    hide_index=True,
                    column_config=column_config
                )

                # Determine current selection and only show delete controls when rows selected
                selection = []
                if table_key in st.session_state:
                    selection = st.session_state[table_key].get("selection", {}).get("rows", []) or []

                if selection:
                    # Delete controls using confirmation dialogs (visible only when rows selected)
                    del_col1, del_col2, _ = st.columns([1,1,2])
                    with del_col1:
                        if st.button('Delete selected…', key=f"income_delete_selected_{position}"):
                            ids = [int(st.session_state[f'dashboard_income_sdf_{position}'].iloc[i]['Id']) for i in selection]
                            st.session_state[f'income_delete_selected_open_{position}'] = True
                            st.session_state[f'income_delete_selected_ids_{position}'] = ids
                    with del_col2:
                        if st.button('Delete older than selected…', key=f"income_delete_older_{position}"):
                            df_sel = st.session_state[f'dashboard_income_sdf_{position}'].iloc[selection]
                            cutoff_ms = int(df_sel['DateMs'].min())
                            current_users_sel = st.session_state[f'dashboard_income_users_{position}']
                            if 'ALL' in current_users_sel:
                                target_users = ['ALL']
                            else:
                                selected_row_users = list(df_sel['User'].unique())
                                target_users = [u for u in selected_row_users if u in current_users_sel] or current_users_sel
                            st.session_state[f'income_delete_older_open_{position}'] = True
                            st.session_state[f'income_delete_older_users_{position}'] = target_users
                            st.session_state[f'income_delete_older_cutoff_{position}'] = cutoff_ms

                # Render dialogs if requested via state flags
                if st.session_state.get(f'income_delete_selected_open_{position}', False):
                    ids = st.session_state.get(f'income_delete_selected_ids_{position}', [])
                    self.dialog_confirm_delete_income_selected(position, ids)
                if st.session_state.get(f'income_delete_older_open_{position}', False):
                    users_to_del = st.session_state.get(f'income_delete_older_users_{position}', [])
                    cutoff_ms = st.session_state.get(f'income_delete_older_cutoff_{position}', 0)
                    self.dialog_confirm_delete_income_older(position, users_to_del, cutoff_ms)
                # Offer backup history + restore (only when rows selected)
                if selection:
                    try:
                        backups_dir = Path(f"{PBGDIR}/data/backup/db")
                        backups = []
                        if backups_dir.exists():
                            backups = sorted(
                                [p for p in backups_dir.glob("pbgui-*.db") if p.is_file()],
                                key=lambda p: p.stat().st_mtime,
                                reverse=True
                            )[:10]
                    except Exception:
                        backups = []

                    if backups:
                        with st.container(border=True):
                            st.markdown("##### Restore from Backup")
                            options = []
                            for p in backups:
                                ts = datetime.fromtimestamp(p.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                                options.append(f"{p.name} — {ts}")
                            # Default to the latest
                            key_select = f"income_backup_select_{position}"
                            selected_label = st.selectbox("Select backup", options, key=key_select, index=0)
                            # Map selection back to path
                            label_to_path = {options[i]: str(backups[i]) for i in range(len(options))}
                            selected_path = label_to_path.get(selected_label)
                            if st.button('Restore selected backup', key=f'income_restore_selected_{position}') and selected_path:
                                # Pause PBData before restore
                                was_running = False
                                try:
                                    pb = PBData()
                                    was_running = pb.is_running()
                                    if was_running:
                                        pb.stop()
                                except Exception:
                                    pass
                                ok = self.db.restore_db_from(selected_path)
                                if ok:
                                    st.session_state['db_last_backup'] = selected_path
                                    st.success(f'Restored database from {selected_label}.')
                                    # Restart PBData if it was running
                                    try:
                                        if was_running:
                                            pb.run()
                                    except Exception:
                                        pass
                                    st.rerun()
            else:
                income = df[['Date', 'Symbol', 'Income', 'User']].copy()
                income['Income'] = income['Income'].cumsum()
                fig = px.line(income, x='Date', y='Income', hover_data={'Income':':.2f'}, title=f"From: {df['Date'].min()} To: {df['Date'].max()}")
                fig['data'][0]['showlegend'] = True
                fig['data'][0]['name'] = 'Total Income'
                # Sort df by Symbol
                df = df.sort_values(by=['Symbol', 'Date'])
                for symbol in df['Symbol'].unique():
                    symbol_df = df[df['Symbol'] == symbol].copy()
                    symbol_df['Income'] = symbol_df['Income'].cumsum()
                    fig.add_trace(go.Scatter(x=symbol_df['Date'], y=symbol_df['Income'], name=symbol))
                reload_token = st.session_state.get('dashboard_reload_token', 0)
                st.plotly_chart(fig, key=f"dashboard_income_plot_{position}_{reload_token}")

    @st.dialog("Delete selected income?")
    def dialog_confirm_delete_income_selected(self, position: str, ids: list):
        count = len(ids)
        st.warning(f"Delete {count} selected income row(s)?", icon="⚠️")
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button(":green[Yes]", key=f"income_yes_sel_{position}"):
                # Pause PBData to avoid concurrent writes during backup/delete
                was_running = False
                try:
                    pb = PBData()
                    was_running = pb.is_running()
                    if was_running:
                        pb.stop()
                except Exception:
                    pass
                # Full DB backup then delete
                backup_path = self.db.backup_full_db()
                if backup_path:
                    st.session_state['db_last_backup'] = backup_path
                deleted = self.db.delete_income_by_ids(ids)
                st.success(f"Deleted {deleted} income row(s). Backup created.")
                # Restart PBData if it was running
                try:
                    if was_running:
                        pb.run()
                except Exception:
                    pass
                # close dialog and refresh
                st.session_state[f'income_delete_selected_open_{position}'] = False
                st.session_state.pop(f'income_delete_selected_ids_{position}', None)
                st.session_state['dashboard_reload_token'] = st.session_state.get('dashboard_reload_token', 0) + 1
                st.rerun()
        with col2:
            if st.button(":red[No]", key=f"income_no_sel_{position}"):
                st.session_state[f'income_delete_selected_open_{position}'] = False
                st.session_state.pop(f'income_delete_selected_ids_{position}', None)
                st.rerun()

    @st.dialog("Delete income older than cutoff?")
    def dialog_confirm_delete_income_older(self, position: str, users: list, cutoff_ms: int):
        cutoff_str = pd.to_datetime(cutoff_ms, unit='ms').strftime('%Y-%m-%d %H:%M:%S')
        if 'ALL' in users:
            msg = f"Delete all income entries across ALL users with timestamp <= {cutoff_str}?"
        else:
            msg = f"Delete all income entries for {', '.join(users)} with timestamp <= {cutoff_str}?"
        st.warning(msg, icon="⚠️")
        col1, col2 = st.columns([1,1])
        with col1:
            if st.button(":green[Yes]", key=f"income_yes_older_{position}"):
                # Pause PBData to avoid concurrent writes during backup/delete
                was_running = False
                try:
                    pb = PBData()
                    was_running = pb.is_running()
                    if was_running:
                        pb.stop()
                except Exception:
                    pass
                backup_path = self.db.backup_full_db()
                if backup_path:
                    st.session_state['db_last_backup'] = backup_path
                deleted = self.db.delete_income_older_than(users, cutoff_ms)
                st.success(f"Deleted {deleted} income row(s). Backup created.")
                # Restart PBData if it was running
                try:
                    if was_running:
                        pb.run()
                except Exception:
                    pass
                st.session_state[f'income_delete_older_open_{position}'] = False
                st.session_state.pop(f'income_delete_older_users_{position}', None)
                st.session_state.pop(f'income_delete_older_cutoff_{position}', None)
                st.session_state['dashboard_reload_token'] = st.session_state.get('dashboard_reload_token', 0) + 1
                st.rerun()
        with col2:
            if st.button(":red[No]", key=f"income_no_older_{position}"):
                st.session_state[f'income_delete_older_open_{position}'] = False
                st.session_state.pop(f'income_delete_older_users_{position}', None)
                st.session_state.pop(f'income_delete_older_cutoff_{position}', None)
                st.rerun()

    def view_top_symbols_impl(self, position: str, user=None, period=None, top=None):
        """Render the Top Symbols bar chart via st.html (FastAPI + Vanilla JS / Plotly)."""
        from pathlib import Path as _Path
        from pbgui_func import _start_fastapi_server_if_needed
        from api.auth import generate_token
        import json as _json

        api_host, api_port, success = _start_fastapi_server_if_needed()
        if not success:
            st.error(
                f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
                "Please check **System → Services → API Server**."
            )
            return

        if 'api_token' not in st.session_state:
            user_id = (
                st.session_state.get('user', {}).get('id')
                or st.session_state.get('user')
                or 'anonymous'
            )
            st.session_state['api_token'] = generate_token(
                str(user_id), expires_in_seconds=86400
            ).token
        token = st.session_state['api_token']

        _browser_host = '127.0.0.1'
        try:
            req_host = st.context.headers.get('Host', '')
            if req_host:
                _browser_host = req_host.split(':')[0] or '127.0.0.1'
        except Exception:
            pass
        api_base_str = f'http://{_browser_host}:{api_port}/api'
        api_host_str = f'{_browser_host}:{api_port}'

        # Resolve users list
        if user and isinstance(user, (list, tuple)) and len(user) > 0:
            users_json = _json.dumps(list(user))
        elif user and isinstance(user, str) and user != 'ALL':
            users_json = _json.dumps([user])
        else:
            users_json = '["ALL"]'

        period_str  = str(period) if period else 'THIS_MONTH'
        top_val     = int(top) if top else 10

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_top.html'
        html = html_path.read_text(encoding='utf-8')
        html = html.replace('"%%TOKEN%%"',    f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"', f'"{api_host_str}"')
        html = html.replace('%%USERS%%',      users_json)
        html = html.replace('"%%PERIOD%%"',   f'"{period_str}"')
        html = html.replace('%%TOP%%',        str(top_val))
        st.html(html, unsafe_allow_javascript=True)

    def color_we(self, value):
        # bgcolor green < 10, orange 100-200, red > 200
        color = "green" if value < 100 else "orange" if value < 200 else "red"
        return f"color: {color};"

    def color_upnl(self, value):
        color = "red" if value < 0 else "green"
        return f"color: {color};"
    
    def view_balance_impl(self, position: str, user=None, edit=False):
        """Render the Balance view via st.html (FastAPI + Vanilla JS).

        The HTML component connects via WebSocket (/ws/dashboard) and fetches
        fresh data from /api/dashboard/balance. PBData notifies the server
        after each update_balances() write, triggering a fan-out to all
        connected WS clients. No Streamlit reruns needed.
        """
        from pathlib import Path as _Path
        from pbgui_func import _start_fastapi_server_if_needed
        from api.auth import generate_token

        api_host, api_port, success = _start_fastapi_server_if_needed()
        if not success:
            st.error(
                f"⚠️ FastAPI server could not be started on {api_host}:{api_port}. "
                "Please check **System → Services → API Server**."
            )
            return

        # Get or create session token
        if "api_token" not in st.session_state:
            user_id = (
                st.session_state.get("user", {}).get("id")
                or st.session_state.get("user")
                or "anonymous"
            )
            st.session_state["api_token"] = generate_token(
                str(user_id), expires_in_seconds=86400
            ).token
        token = st.session_state["api_token"]

        # Resolve browser-usable hostname (0.0.0.0 is not routable from browser)
        _browser_host = "127.0.0.1"
        try:
            req_host = st.context.headers.get("Host", "")
            if req_host:
                _browser_host = req_host.split(":")[0] or "127.0.0.1"
        except Exception:
            pass

        api_host_str = f"{_browser_host}:{api_port}"
        api_base_str = f"http://{_browser_host}:{api_port}/api"

        # Load HTML template and inject config as JS variables (%%PLACEHOLDER%% pattern)
        html_path = _Path(__file__).parent / "frontend" / "dashboard_balance.html"
        html = html_path.read_text(encoding="utf-8")

        # Build users list for initial selection.
        # In edit mode: prefer the live selection stored in FastAPI (survives Streamlit reruns).
        import json as _json
        resolved_user = user
        # Key for the balance selection API: {dashboard_name}:{position}
        # Using the dashboard name prevents different dashboards at the same position
        # (e.g. 1_1) from sharing each other's user selections.
        _sel_key = f'{self.name}:{position}' if edit else position
        if edit:
            try:
                import urllib.request as _ureq
                _tok = st.session_state.get('api_token', token)
                _url = f'http://127.0.0.1:{api_port}/api/dashboard/balance/selection?position={_sel_key}&token={_tok}'
                with _ureq.urlopen(_url, timeout=2) as _resp:
                    _data = _json.loads(_resp.read())
                    if _data.get('found'):
                        resolved_user = _data['users']
            except Exception:
                pass
        if resolved_user and isinstance(resolved_user, (list, tuple)) and len(resolved_user) > 0:
            users_json = _json.dumps(list(resolved_user))
        else:
            users_json = '["ALL"]'

        # Build full list of available users for the select dropdown
        try:
            all_users_list = ['ALL'] + sorted(st.session_state.users.list(), key=str.lower)
        except Exception:
            all_users_list = ['ALL']
        all_users_json = _json.dumps(all_users_list)

        # Unique instance ID to prevent stale WS connections overwriting DOM
        import uuid as _uuid
        instance_id = _uuid.uuid4().hex[:8]

        html = html.replace('"%%TOKEN%%"',    f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"', f'"{api_host_str}"')
        html = html.replace('%%USERS%%',      users_json)
        html = html.replace('%%ALL_USERS%%',  all_users_json)
        html = html.replace('%%EDIT_MODE%%',  'true' if edit else 'false')
        html = html.replace('"%%INSTANCE_ID%%"', f'"{instance_id}"')
        # In edit mode use namespaced key so each dashboard has its own selection store
        _pos_key = f'{self.name}:{position}' if edit else position
        html = html.replace('"%%POSITION%%"', f'"{_pos_key}"')

        st.html(html, unsafe_allow_javascript=True)

    def bgcolor_positive_or_negative(self, value):
        bgcolor = "red" if value < 0 else "green"
        return f"background-color: {bgcolor};"

    def view_positions_impl(self, position : str, user : str = None):
        users = st.session_state.users
        if f"dashboard_positions_users_{position}" not in st.session_state:
            if user:
                st.session_state[f'dashboard_positions_users_{position}'] = user
                for user in st.session_state[f'dashboard_positions_users_{position}']:
                    if user not in users.list() and user != 'ALL':
                        st.session_state[f'dashboard_positions_users_{position}'].remove(user)
        # Init Orders View (respect tokenized table key)
        reload_token = st.session_state.get('dashboard_reload_token', 0)
        table_key = f"dashboard_positions_{position}_{reload_token}"
        if table_key in st.session_state:
            if st.session_state[table_key]["selection"]["rows"]:
                row = st.session_state[table_key]["selection"]["rows"][0]
                st.session_state[f'view_orders_{position}'] = st.session_state[f'dashboard_positions_sdf_{position}'].iloc[row]
                if not "edit_dashboard" in st.session_state:
                    st.rerun()
        st.markdown("#### :blue[Positions]")
        col1, col2 = st.columns([5,0.2], vertical_alignment="bottom")
        with col1:
            st.multiselect('Users', ['ALL'] + users.list(), key=f"dashboard_positions_users_{position}")
        with col2:
            if st.button(":material/refresh:", key=f"dashboard_positions_rerun_{position}"):
                st.rerun()
        # Init view_orders that it can be selected in edit mode
        if f'view_orders_{position}' not in st.session_state:
            st.session_state[f'view_orders_{position}'] = None
        if st.session_state[f'dashboard_positions_users_{position}']:
            all_positions = []
            users = st.session_state.users
            if 'ALL' in st.session_state[f'dashboard_positions_users_{position}']:
                users_selected = users.list()
            else:
                users_selected = st.session_state[f'dashboard_positions_users_{position}']
            for user in users_selected:
                positions = self.db.fetch_positions(users.find_user(user))
                prices = self.db.fetch_prices(users.find_user(user))
                for pos in positions:
                    symbol = pos[1]
                    user = pos[6]
                    orders = self.db.fetch_orders_by_symbol(user, symbol)
                    dca = 0
                    next_tp = 0
                    next_dca = 0
                    for order in orders:
                        # print(order)
                        if order[5] == "buy":
                            dca += 1
                            if next_dca < order[4]:
                                next_dca = order[4]
                        elif order[5] == "sell":
                            if next_tp == 0 or next_tp > order[4]:
                                next_tp = order[4]
                    # Find price from prices
                    price = 0
                    for p in prices:
                        if p[1] == symbol:
                            price = p[3]
                    # cals pos value
                    pos_value = pos[3] * price
                    all_positions.append(tuple(pos) + (price,) + (dca,) + (next_dca,) + (next_tp, pos_value))
            df = pd.DataFrame(all_positions, columns =['Id', 'Symbol', 'PosId', 'Size', 'uPnl', 'Entry', 'User', 'Side', 'Price', 'DCA', 'Next DCA', 'Next TP', 'Pos Value'])
            # sorty df by User, Symbol
            df = df.sort_values(by=['User', 'Symbol'])
            # Move User to second column
            df = df[['Id', 'User', 'Symbol', 'Side', 'PosId', 'Size', 'uPnl', 'Entry', 'Price', 'DCA', 'Next DCA', 'Next TP', 'Pos Value']]
            sdf = df.style.map(self.color_upnl, subset=['uPnl']).format({'Size': "{:.3f}"}).format({'Pos Value': "{:.2f}"})
            st.session_state[f'dashboard_positions_sdf_{position}'] = df
            column_config = {
                "Id": None,
                "PosId": None
            }
            st.dataframe(sdf, height=36+(len(df))*35, key=f"dashboard_positions_{position}_{st.session_state.get('dashboard_reload_token', 0)}", on_select="rerun", selection_mode='single-row', hide_index=None, column_order=None, column_config=column_config)

    def view_orders_impl(self, pos : str, orders : str = None, tf : str = "4h", edit : bool = False):
        position = None
        view_orders = {key: val for key, val in st.session_state.items()
            if key.startswith("view_orders_")}
        if orders and orders in view_orders:
            position = st.session_state[f'{orders}']
            if f"dashboard_orders_{pos}" not in st.session_state:
                if pos:
                    st.session_state[f'dashboard_orders_{pos}'] = orders
        if view_orders and edit:
            index = 0
            if orders in list(view_orders.keys()):
                index = list(view_orders.keys()).index(orders)
            selected_pos = st.selectbox('From Positions', view_orders, index=index, key=f"dashboard_orders_{pos}")
            position = st.session_state[f'{selected_pos}']
        if f"dashboard_orders_tf_{pos}" not in st.session_state:
            if tf:
                st.session_state[f'dashboard_orders_tf_{pos}'] = tf
        st.markdown("#### :blue[Orders]")
        if position is None:
            return
        # Init Exchange
        users = st.session_state.users
        user = users.find_user(position["User"])
        exchange = Exchange(user.exchange, user)
        market_type = "futures"
        col1, col2, col3 = st.columns([1,1,8], vertical_alignment="bottom")
        with col1:
            st.selectbox('Timeframe',exchange.tf,index=exchange.tf.index(tf), key=f"dashboard_orders_tf_{pos}")
        with col2:
            since = None
            if f'dashboard_orders_leftclick_{pos}' not in st.session_state:
                st.session_state[f'dashboard_orders_leftclick_{pos}'] = 0
            if st.button(":material/arrow_left:", key=f"dashboard_orders_left{pos}"):
                since = st.session_state[f'dashboard_orders_since_{pos}'] - st.session_state[f'dashboard_orders_range_{pos}']
                st.session_state[f'dashboard_orders_leftclick_{pos}'] += 1
        with col3:
            if st.session_state[f'dashboard_orders_leftclick_{pos}'] > 0:
                if st.button(":material/arrow_right:", key=f"dashboard_orders_right{pos}"):
                    since = st.session_state[f'dashboard_orders_since_{pos}'] + st.session_state[f'dashboard_orders_range_{pos}']
                    st.session_state[f'dashboard_orders_leftclick_{pos}'] -= 1
        symbol = position["Symbol"]
        # symbol to ccxt_symbol
        if symbol[-4:] == "USDT":
            symbol_ccxt = f'{symbol[0:-4]}/USDT:USDT'
        elif symbol[-4:] == "USDC":
            symbol_ccxt = f'{symbol[0:-4]}/USDC:USDC'
        ohlcv = exchange.fetch_ohlcv(symbol_ccxt, market_type, timeframe=st.session_state[f'dashboard_orders_tf_{pos}'], limit=100, since=since)
        ohlcv_df = pd.DataFrame(ohlcv, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        st.session_state[f'dashboard_orders_since_{pos}'] = int(ohlcv_df.iloc[0]["timestamp"])
        st.session_state[f'dashboard_orders_range_{pos}'] = int(ohlcv_df.iloc[-1]["timestamp"] - ohlcv_df.iloc[0]["timestamp"])
        ohlcv_df["color"] = np.where(ohlcv_df["close"] > ohlcv_df["open"], "green", "red")
        # w = (ohlcv_df["timestamp"][1] - ohlcv_df["timestamp"][0]) * 0.8
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        col1, col2, col3, col4 = st.columns([1, 1, 1, 0.2])
        with col1:
            st.markdown(f"#### :blue[User:] :green[{user.name}]")
        with col2:
            st.markdown(f"#### :blue[Symbol:] :green[{symbol}]")
        with col3:
            st.markdown(f"#### :blue[Time:] :green[{time}]")
        with col4:
            if st.button(":material/refresh:", key=f"dashboard_orders_rerun_{pos}"):
                st.rerun()
        # layout = go.Layout(title=f'{symbol} | {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC', title_font=dict(size=36), showlegend=True)
        fig = go.Figure(data=[go.Candlestick(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'),
               open=ohlcv_df["open"], high=ohlcv_df["high"],
               low=ohlcv_df["low"], close=ohlcv_df["close"],
               increasing_line_color='green', decreasing_line_color='red')])
        # remove legend from trace 0
        fig.data[0].showlegend = False
        fig.update_layout(yaxis=dict(title='USDT', title_font=dict(size=24)), xaxis_rangeslider_visible=False, height=800, xaxis_type='category')
        fig.update_layout(xaxis_rangeslider_visible=False, xaxis_tickformat='%H:%M')
        fig.update_xaxes(tickangle=-90, tickfont=dict(size=14), dtick='8')
        # fig.update_layout(xaxis_rangeslider_visible=False, width=1280, height=1024)
        # balance = exchange.fetch_balance(market_type)
        # periodic fragment refresh ensures updates
        prices = self.db.fetch_prices(user)
        price = 0
        for p in prices:
            if p[1] == symbol:
                price = p[3]
        orders = self.db.fetch_orders_by_symbol(user.name, symbol)
        color = "red" if price < ohlcv_df["open"].iloc[-1] else "green"
        # add price line to candlestick
        fig.add_trace(go.Scatter(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'), y=[price] * len(ohlcv_df), mode='lines', line=dict(color=color, width=1), name=f'price: {str(round(price,5))}'))
        # position
        color = "red" if price < position["Entry"] else "green"
        size = position["Size"]
        fig.add_trace(go.Scatter(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'),
                                y=[position["Entry"]] * len(ohlcv_df), mode='lines',
                                line=dict(color=color, width=1, dash = 'dash'),
                                name=f'position: {str(round(position["Entry"],5))} size: {str(size)}<br>Pnl: {str(round(position["uPnl"],5))}'))
        amount = 3
        price = 4
        side = 5
        orders = sorted(orders, key=lambda x: x[price], reverse=True)
        for order in orders:
            color = "red" if order[side] == "sell" else "green"
            legend = f'close: {str(order[price])} amount: {str(order[amount])}' if order[side] == "sell" else f'open: {str(order[price])} amount: {str(order[amount])}'
            fig.add_trace(go.Scatter(x=pd.to_datetime(ohlcv_df["timestamp"], unit='ms'),
                                    y=[order[price]] * len(ohlcv_df),
                                    mode='lines',
                                    line=dict(color=color, width=2, dash = 'dot'), name=legend))
        fig.update_layout(legend = dict(font = dict(size = 14)))
        st.plotly_chart(fig, key=f"dashboard_orders_plot_{pos}_{st.session_state.get('dashboard_reload_token', 0)}")
