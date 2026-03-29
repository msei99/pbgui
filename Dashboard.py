import re
import streamlit as st
from pathlib import Path
import json
import os
import tempfile
from pbgui_func import PBGDIR

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

