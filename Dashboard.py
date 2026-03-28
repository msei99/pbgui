import re
import streamlit as st
from pathlib import Path
import json
import os
import tempfile
from Exchange import Exchange
from pbgui_func import PBGDIR, pb7dir
from Database import Database
import pbgui_help

_VALID_DASHBOARD_NAME = re.compile(r'^[^\x00-\x1f/\\]+$')

def _sanitize_dashboard_name(name: str) -> str:
    """Validate and return a safe dashboard name, or raise ValueError."""
    name = str(name or '').strip()
    if not name:
        raise ValueError('Dashboard name must not be empty')
    if not _VALID_DASHBOARD_NAME.match(name):
        raise ValueError(f'Invalid dashboard name: {name!r}')
    return name

class Dashboard():

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
            self.name = _sanitize_dashboard_name(self.name)
            self.load(self.name)

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

        # Also swap keys in self.dashboard_config so the editor uses
        # the correct data after rerun.
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
        html = html.replace('%%VIEW_ONLY%%', '0')
        html = html.replace('%%STANDALONE%%', '0')
        html = html.replace('%%EDIT_ONLY_STYLE%%', '')
        html = html.replace('%%BODY_CLASS%%', '')
        st.html(html, unsafe_allow_javascript=True)

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
            self.name = _sanitize_dashboard_name(name)
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

        self._normalize_config(dashboard_config)
        self.dashboard_config = dashboard_config
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_path.mkdir(parents=True, exist_ok=True)
        dashboard_file = dashboard_path / f'{self.name}.json'
        # Atomic write: temp file + os.replace to prevent corruption
        fd, tmp_path = tempfile.mkstemp(dir=str(dashboard_path), suffix='.tmp')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(dashboard_config, f, indent=4)
            os.replace(tmp_path, str(dashboard_file))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


    def _normalize_config(self, cfg: dict) -> bool:
        """Ensure every INCOME cell has all required keys.

        Returns True if any key was added (caller may want to persist).
        """
        changed = False
        rows = int(cfg.get("rows", 1))
        cols = int(cfg.get("cols", 1))
        defaults = {
            "dashboard_income_users_":  ["ALL"],
            "dashboard_income_period_": "THIS_MONTH",
            "dashboard_income_last_":   0,
            "dashboard_income_filter_": 0.0,
        }
        for r in range(1, rows + 1):
            for c in range(1, cols + 1):
                pos = f"{r}_{c}"
                if cfg.get(f"dashboard_type_{pos}") != "INCOME":
                    continue
                for prefix, default in defaults.items():
                    key = f"{prefix}{pos}"
                    if key not in cfg:
                        cfg[key] = default
                        changed = True
                    # Ensure users is always a list
                    if prefix == "dashboard_income_users_" and isinstance(cfg[key], str):
                        cfg[key] = [cfg[key]] if cfg[key] else ["ALL"]
                        changed = True
        return changed

    def load(self, name : str):
        self.cleanup_dashboard_session_state()
        name = _sanitize_dashboard_name(name)
        self.name = name
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_file = dashboard_path / f'{name}.json'
        if dashboard_file.exists():
            with dashboard_file.open() as f:
                try:
                    dashboard_config = json.load(f)
                except (json.JSONDecodeError, ValueError):
                    return
            if not isinstance(dashboard_config, dict):
                return
            # Normalise incomplete INCOME cells and persist the fix
            if self._normalize_config(dashboard_config):
                try:
                    fd, tmp_path = tempfile.mkstemp(
                        dir=str(dashboard_path), suffix='.tmp'
                    )
                    with os.fdopen(fd, 'w') as f:
                        json.dump(dashboard_config, f, indent=4)
                    os.replace(tmp_path, str(dashboard_file))
                except Exception:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
            self.dashboard_config = dashboard_config
            self.rows = dashboard_config.get("rows", 0)
            self.cols = dashboard_config.get("cols", 1)

    def delete(self):
        if not self.name:
            return
        _sanitize_dashboard_name(self.name)
        dashboard_path = Path(f'{PBGDIR}/data/dashboards')
        dashboard_file = dashboard_path / f'{self.name}.json'
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
        self.rows = dashboard_config.get("rows", 0)
        self.cols = dashboard_config.get("cols", 1)
        # Titel
        st.subheader(f"Dashboard: {self.name}")

        for row in range(1, self.rows + 1):
            if self.cols == 2:
                db_col1, db_col2 = st.columns([1,1])
                with db_col1:
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "PNL":
                        self.view_pnl_impl(f'{row}_1', dashboard_config[f'dashboard_pnl_users_{row}_1'], dashboard_config[f'dashboard_pnl_period_{row}_1'], dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "ADG":
                        self.view_adg_impl(f'{row}_1', dashboard_config[f'dashboard_adg_users_{row}_1'], dashboard_config[f'dashboard_adg_period_{row}_1'], dashboard_config[f'dashboard_adg_mode_{row}_1'])
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "INCOME":
                        dashboard_config.setdefault(f'dashboard_income_users_{row}_1', ['ALL'])
                        dashboard_config.setdefault(f'dashboard_income_period_{row}_1', 'THIS_MONTH')
                        dashboard_config.setdefault(f'dashboard_income_last_{row}_1', 0)
                        dashboard_config.setdefault(f'dashboard_income_filter_{row}_1', 0.0)
                        self.view_income_impl(f'{row}_1', dashboard_config[f'dashboard_income_users_{row}_1'], dashboard_config[f'dashboard_income_period_{row}_1'], dashboard_config[f'dashboard_income_last_{row}_1'], dashboard_config[f'dashboard_income_filter_{row}_1'])
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "TOP":
                        self.view_top_symbols_impl(f'{row}_1', dashboard_config[f'dashboard_top_symbols_users_{row}_1'], dashboard_config[f'dashboard_top_symbols_period_{row}_1'], dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "POSITIONS":
                        self.view_positions_impl(f'{row}_1', dashboard_config[f'dashboard_positions_users_{row}_1'])
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "ORDERS":
                        self.view_orders_impl(f'{row}_1', dashboard_config.get(f'dashboard_orders_{row}_1'))
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "BALANCE":
                        self.view_balance_impl(f'{row}_1', dashboard_config[f'dashboard_balance_users_{row}_1'])
                    if dashboard_config.get(f'dashboard_type_{row}_1') == "P+L":
                        if f'dashboard_ppl_sum_period_{row}_1' not in dashboard_config:
                            dashboard_config[f'dashboard_ppl_sum_period_{row}_1'] = 'DAY'
                        self.view_ppl_impl(f'{row}_1', dashboard_config[f'dashboard_ppl_users_{row}_1'], dashboard_config[f'dashboard_ppl_period_{row}_1'], dashboard_config[f'dashboard_ppl_sum_period_{row}_1'])
                with db_col2:
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "PNL":
                        self.view_pnl_impl(f'{row}_2', dashboard_config[f'dashboard_pnl_users_{row}_2'], dashboard_config[f'dashboard_pnl_period_{row}_2'], dashboard_config[f'dashboard_pnl_mode_{row}_2'])
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "ADG":
                        self.view_adg_impl(f'{row}_2', dashboard_config[f'dashboard_adg_users_{row}_2'], dashboard_config[f'dashboard_adg_period_{row}_2'], dashboard_config[f'dashboard_adg_mode_{row}_2'])
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "INCOME":
                        dashboard_config.setdefault(f'dashboard_income_users_{row}_2', ['ALL'])
                        dashboard_config.setdefault(f'dashboard_income_period_{row}_2', 'THIS_MONTH')
                        dashboard_config.setdefault(f'dashboard_income_last_{row}_2', 0)
                        dashboard_config.setdefault(f'dashboard_income_filter_{row}_2', 0.0)
                        self.view_income_impl(f'{row}_2', dashboard_config[f'dashboard_income_users_{row}_2'], dashboard_config[f'dashboard_income_period_{row}_2'], dashboard_config[f'dashboard_income_last_{row}_2'], dashboard_config[f'dashboard_income_filter_{row}_2'])
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "TOP":
                        self.view_top_symbols_impl(f'{row}_2', dashboard_config[f'dashboard_top_symbols_users_{row}_2'], dashboard_config[f'dashboard_top_symbols_period_{row}_2'], dashboard_config[f'dashboard_top_symbols_top_{row}_2'])
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "POSITIONS":
                        self.view_positions_impl(f'{row}_2', dashboard_config[f'dashboard_positions_users_{row}_2'])
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "ORDERS":
                        self.view_orders_impl(f'{row}_2', dashboard_config.get(f'dashboard_orders_{row}_2'))
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "BALANCE":
                        self.view_balance_impl(f'{row}_2', dashboard_config[f'dashboard_balance_users_{row}_2'])
                    if dashboard_config.get(f'dashboard_type_{row}_2') == "P+L":
                        if f'dashboard_ppl_sum_period_{row}_2' not in dashboard_config:
                            dashboard_config[f'dashboard_ppl_sum_period_{row}_2'] = 'DAY'
                        self.view_ppl_impl(f'{row}_2', dashboard_config[f'dashboard_ppl_users_{row}_2'], dashboard_config[f'dashboard_ppl_period_{row}_2'], dashboard_config[f'dashboard_ppl_sum_period_{row}_2'])
            else:
                if dashboard_config.get(f'dashboard_type_{row}_1') == "PNL":
                    self.view_pnl_impl(f'{row}_1', dashboard_config[f'dashboard_pnl_users_{row}_1'], dashboard_config[f'dashboard_pnl_period_{row}_1'], dashboard_config[f'dashboard_pnl_mode_{row}_1'])
                if dashboard_config.get(f'dashboard_type_{row}_1') == "ADG":
                    self.view_adg_impl(f'{row}_1', dashboard_config[f'dashboard_adg_users_{row}_1'], dashboard_config[f'dashboard_adg_period_{row}_1'], dashboard_config[f'dashboard_adg_mode_{row}_1'])
                if dashboard_config.get(f'dashboard_type_{row}_1') == "INCOME":
                    dashboard_config.setdefault(f'dashboard_income_users_{row}_1', ['ALL'])
                    dashboard_config.setdefault(f'dashboard_income_period_{row}_1', 'THIS_MONTH')
                    dashboard_config.setdefault(f'dashboard_income_last_{row}_1', 0)
                    dashboard_config.setdefault(f'dashboard_income_filter_{row}_1', 0.0)
                    self.view_income_impl(f'{row}_1', dashboard_config[f'dashboard_income_users_{row}_1'], dashboard_config[f'dashboard_income_period_{row}_1'], dashboard_config[f'dashboard_income_last_{row}_1'], dashboard_config[f'dashboard_income_filter_{row}_1'])
                if dashboard_config.get(f'dashboard_type_{row}_1') == "TOP":
                    self.view_top_symbols_impl(f'{row}_1', dashboard_config[f'dashboard_top_symbols_users_{row}_1'], dashboard_config[f'dashboard_top_symbols_period_{row}_1'], dashboard_config[f'dashboard_top_symbols_top_{row}_1'])
                if dashboard_config.get(f'dashboard_type_{row}_1') == "POSITIONS":
                    self.view_positions_impl(f'{row}_1', dashboard_config[f'dashboard_positions_users_{row}_1'])
                if dashboard_config.get(f'dashboard_type_{row}_1') == "ORDERS":
                    self.view_orders_impl(f'{row}_1', dashboard_config.get(f'dashboard_orders_{row}_1'))
                if dashboard_config.get(f'dashboard_type_{row}_1') == "BALANCE":
                    self.view_balance_impl(f'{row}_1', dashboard_config[f'dashboard_balance_users_{row}_1'])
                if dashboard_config.get(f'dashboard_type_{row}_1') == "P+L":
                    if f'dashboard_ppl_sum_period_{row}_1' not in dashboard_config:
                        dashboard_config[f'dashboard_ppl_sum_period_{row}_1'] = 'DAY'
                    self.view_ppl_impl(f'{row}_1', dashboard_config[f'dashboard_ppl_users_{row}_1'], dashboard_config[f'dashboard_ppl_period_{row}_1'], dashboard_config[f'dashboard_ppl_sum_period_{row}_1'])

    def view_pnl_impl(self, position : str, user : str = None, period : str = None, mode : str = "bar"):
        """Render the Daily PNL widget via st.html (FastAPI + Vanilla JS / Plotly)."""
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

        period_str = str(period) if period else 'THIS_MONTH'
        # Map legacy mode values from old editor configs (Cumulative→line, Daily→bar)
        _mode_map = {'Cumulative': 'line', 'Daily': 'bar'}
        mode_str  = _mode_map.get(mode, mode) if mode else 'bar'

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_pnl.html'
        html = html_path.read_text(encoding='utf-8')

        html = html.replace('"%%TOKEN%%"',    f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"', f'"{api_host_str}"')
        html = html.replace('%%USERS%%',      users_json)
        html = html.replace('"%%PERIOD%%"',   f'"{period_str}"')
        html = html.replace('"%%MODE%%"',     f'"{mode_str}"')
        html = html.replace('%%POSITION%%',   position)
        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%',     str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)

    def view_adg_impl(self, position : str, user : str = None, period : str = None, mode : str = "bar"):
        """Render the ADG widget via st.html (FastAPI + Vanilla JS / Plotly)."""
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

        period_str = str(period) if period else 'THIS_MONTH'
        mode_str   = mode if mode else 'bar'

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_adg.html'
        html = html_path.read_text(encoding='utf-8')

        html = html.replace('"%%TOKEN%%"',    f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"', f'"{api_host_str}"')
        html = html.replace('%%USERS%%',      users_json)
        html = html.replace('"%%PERIOD%%"',   f'"{period_str}"')
        html = html.replace('"%%MODE%%"',     f'"{mode_str}"')
        html = html.replace('%%POSITION%%',   position)
        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%',     str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)


    def view_ppl_impl(self, position : str, user : str = None, period : str = None, sum_period : str = None):
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

        period_str     = str(period) if period else 'THIS_MONTH'
        _valid_sum = ('DAY', 'WEEK', 'MONTH')
        sum_period_str = str(sum_period) if sum_period in _valid_sum else 'MONTH'

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_ppl.html'
        html = html_path.read_text(encoding='utf-8')

        html = html.replace('"%%TOKEN%%"',      f'"{token}"')
        html = html.replace('"%%API_BASE%%"',   f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"',   f'"{api_host_str}"')
        html = html.replace('%%USERS%%',        users_json)
        html = html.replace('"%%PERIOD%%"',     f'"{period_str}"')
        html = html.replace('"%%SUM_PERIOD%%"', f'"{sum_period_str}"')
        html = html.replace('%%POSITION%%',     position)
        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%',       str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)

    def view_income_impl(self, position : str, user : str = None, period : str = None, last : int = 0, filter : float = 0.0):
        """Render the Income widget via st.html (FastAPI + Vanilla JS / Plotly)."""
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

        period_str = str(period) if period else 'THIS_MONTH'
        last_val   = int(last)   if last   else 0
        filter_val = float(filter) if filter else 0.0

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_income.html'
        html = html_path.read_text(encoding='utf-8')
        # Determine single user for the Live vs Backtest nav button inside widget
        single_user = None
        if user and isinstance(user, (list, tuple)) and len(user) == 1 and user[0] != 'ALL':
            single_user = user[0]
        elif user and isinstance(user, str) and user != 'ALL':
            single_user = user

        html = html.replace('"%%TOKEN%%"',    f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"', f'"{api_host_str}"')
        html = html.replace('%%USERS%%',      users_json)
        html = html.replace('"%%PERIOD%%"',   f'"{period_str}"')
        html = html.replace('%%LAST_N%%',     str(last_val))
        html = html.replace('%%FILTER%%',     str(filter_val))
        html = html.replace('"%%LVB_USER%%"', f'"{single_user or ""}"')
        html = html.replace('%%POSITION%%',   position)
        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%',     str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)

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
        html = html.replace('%%POSITION%%',   position)
        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%',     str(_chart_h))
        st.html(html, unsafe_allow_javascript=True)

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

        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%', str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)

    def view_positions_impl(self, position : str, user : str = None):
        """Render the Positions widget via st.html (FastAPI + Vanilla JS)."""
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

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_positions.html'
        html = html_path.read_text(encoding='utf-8')

        html = html.replace('"%%TOKEN%%"',    f'"{token}"')
        html = html.replace('"%%API_BASE%%"', f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"', f'"{api_host_str}"')
        html = html.replace('%%USERS%%',      users_json)
        html = html.replace('%%POSITION%%',   position)

        _h = self.dashboard_config.get(f'dashboard_height_{position}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%', str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)

    def view_orders_impl(self, pos : str, orders : str = None, tf : str = "4h", edit : bool = False):
        """Render the Orders widget via st.html (FastAPI + Vanilla JS / Plotly)."""
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

        # Linked positions key (e.g. 'view_orders_1_1')
        linked_pos = orders or ''
        timeframe = tf or '4h'

        html_path = _Path(__file__).parent / 'frontend' / 'dashboard_orders.html'
        html = html_path.read_text(encoding='utf-8')

        html = html.replace('"%%TOKEN%%"',      f'"{token}"')
        html = html.replace('"%%API_BASE%%"',   f'"{api_base_str}"')
        html = html.replace('"%%API_HOST%%"',   f'"{api_host_str}"')
        html = html.replace('%%POSITION%%',     pos)
        html = html.replace('"%%LINKED_POS%%"', f'"{linked_pos}"')
        html = html.replace('"%%TIMEFRAME%%"',  f'"{timeframe}"')

        _h = self.dashboard_config.get(f'dashboard_height_{pos}', 0)
        try:
            _chart_h = max(100, int(_h)) if _h else 0
        except (TypeError, ValueError):
            _chart_h = 0
        html = html.replace('%%HEIGHT%%', str(_chart_h))

        st.html(html, unsafe_allow_javascript=True)
