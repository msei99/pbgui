import streamlit as st
from pathlib import Path
import re
import json
from datetime import datetime
import streamlit_scrollable_textbox as stx
import pbgui_help
import logging_helpers

def view_log_filtered(log_filename: str):
    """A minimal, standalone filtered log viewer.

    Supports selecting one or more logfiles (multi-select) and shows a
    merged view sorted by timestamp. The caller may pass a default
    `log_filename` (single name) which will be preselected.
    """
    pbgdir = Path.cwd()
    logs_dir = Path(f'{pbgdir}/data/logs')
    # discover available .log files (names without suffix)
    candidates = []
    try:
        if logs_dir.exists():
            for p in logs_dir.glob('*.log'):
                if p.is_file():
                    candidates.append(p.stem)
    except Exception:
        candidates = []

    # default selection: the provided filename if present, otherwise first
    default_sel = [log_filename] if log_filename in candidates else ([candidates[0]] if candidates else [])
    sel_logs = st.multiselect('Logfiles', sorted(candidates), default=default_sel, key=f'lv_selected_logs_{log_filename}')
    if not sel_logs:
        # nothing selected: show helper and return
        st.info('No logfile selected.')
        return
    # build a compound key for session state based on selected files
    sel_key = '+'.join(sorted(sel_logs))
    logfile = None

    # Keys for refresh/truncate counters used to bust the cached reader
    refresh_key = f'lv_{sel_key}_refresh'
    trunc_key = f'lv_{sel_key}_truncated'
    if refresh_key not in st.session_state:
        st.session_state[refresh_key] = 0
    if trunc_key not in st.session_state:
        # use integer counter so cache keys remain hashable and simple
        st.session_state[trunc_key] = 0

    # Top header removed; logfile is shown inside the 'Logfile' expander below

    # Read logfile (newest first) using a cached reader. The cached reader
    # is keyed on the logfile path and a refresh counter so we only re-read
    # the file when the user requests it.
    def _read_log(path_str: str):
        p = Path(path_str)
        if not p.exists():
            return []
        try:
            with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                log = f.readlines()
                return list(reversed(log))
        except Exception:
            return []

    # If logfile doesn't exist, show an info message; otherwise use cached reader.
    # Read selected logfiles and merge by timestamp. Limit per-file read to
    # recent 2000 lines for performance.
    lines = []
    for name in sel_logs:
        p = logs_dir / f"{name}.log"
        if not p.exists():
            continue
        try:
            with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                all_lines = f.readlines()
                # newest-first
                rev = list(reversed(all_lines))
                # keep recent portion to avoid massive merges
                lines.extend(rev[:2000])
        except Exception:
            continue

    # Make sure we have some recent lines loaded so the view will show
    # content immediately without requiring a manual refresh. Limit the
    # pre-load to the most recent 2000 lines to avoid reading extremely
    # large files on initial render.
    if not lines:
        # try a full read for selected files if previous step yielded nothing
        for name in sel_logs:
            p = logs_dir / f"{name}.log"
            if not p.exists():
                continue
            try:
                with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                    all_lines = f.readlines()
                    if all_lines:
                        lines.extend(list(reversed(all_lines)))
            except Exception:
                pass

    # When at least one logfile is selected, show the logfile area.
    show_log = True
    if show_log:
        # When the user opens the view, re-read all selected files from disk
        # (uncached) so the up-to-date logfile(s) are visible immediately.
        lines = []
        for name in sel_logs:
            p = logs_dir / f"{name}.log"
            if not p.exists():
                continue
            try:
                with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                    all_lines = f.readlines()
                    if all_lines:
                        lines.extend(list(reversed(all_lines)))
            except Exception:
                pass

        # users
        try:
            user_list = st.session_state.users.list()
        except Exception:
            user_list = []

        # candidate tags extraction
        candidates = set()
        # Only accept tokens that contain at least one letter and are reasonably short.
        # This prevents numeric indices like [0] from becoming tags.
        simple_re = re.compile(r'^(?=.*[A-Za-z])[A-Za-z0-9_.-]{1,40}$')
        window = lines[:2000]

        # Accept ISO timestamps with either 'T' or space, optional fractional seconds and optional trailing Z
        ts_re = re.compile(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?Z?\s*')

        def parse_log_line(line: str):
            """Parse a human_log line into components.
            Returns dict: {timestamp, service, tags, user, message, meta} or None if timestamp missing.
            """
            m = ts_re.match(line)
            if not m:
                return None
            pos = m.end()
            L = len(line)
            service = None
            tags = []
            user = None
            # consume consecutive bracket tokens; first bracket token is the service
            while True:
                while pos < L and line[pos].isspace():
                    pos += 1
                if pos < L and line[pos] == '[':
                    end = line.find(']', pos+1)
                    if end == -1:
                        break
                    content = line[pos+1:end].strip()
                    if service is None:
                        service = content
                    else:
                        if content.lower().startswith('user:'):
                            user = content.split(':', 1)[1].strip()
                        else:
                            tags.append(content)
                    pos = end + 1
                    continue
                break

            rest = line[pos:].strip()
            meta = None
            message = rest
            # Try to extract trailing JSON metadata if present
            idx = rest.rfind('{')
            if idx != -1:
                try:
                    candidate = rest[idx:]
                    meta = json.loads(candidate)
                    message = rest[:idx].strip()
                except Exception:
                    meta = None
                    message = rest

            # Simple level parsing: the first bracket after the service is
            # expected to be the uppercase level token (e.g. [INFO], [DEBUG]).
            # Use that as the severity and remove it from tags. Default to
            # 'info' when missing to keep display consistent.
            # Recognize bracketed level token (e.g. [INFO]) and keep it
            # in its original case; comparisons below will use uppercasing.
            recognized_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            level = 'INFO'
            if tags:
                first = tags[0]
                if isinstance(first, str) and first.upper() in recognized_levels:
                    level = first.upper()
                    tags = tags[1:]

            return {
                'timestamp': m.group(0).strip(),
                'service': service,
                'tags': tags,
                'user': user,
                'message': message,
                'meta': meta,
                'level': level,
                'raw': line,
            }

        def extract_leading_tags(line: str):
            tags = []
            m = ts_re.match(line)
            if not m:
                return tags
            pos = m.end()
            L = len(line)
            # consume sequences of optional spaces and bracketed tokens only
            while True:
                # skip spaces
                while pos < L and line[pos].isspace():
                    pos += 1
                if pos < L and line[pos] == '[':
                    end = line.find(']', pos+1)
                    if end == -1:
                        break
                    content = line[pos+1:end].strip()
                    tags.append(content)
                    pos = end + 1
                    continue
                break
            return tags

        services = set()
        for line in window:
            parsed = parse_log_line(line)
            if not parsed:
                continue
            if parsed.get('service'):
                services.add(parsed['service'])
            for tok in parsed.get('tags', []):
                tok = tok.strip()
                # skip list-style prints or quoted reprs
                if ',' in tok or '"' in tok or "'" in tok:
                    continue
                if not simple_re.match(tok):
                    continue
                if tok in user_list:
                    continue
                candidates.add(tok)

        tags = sorted(candidates)
        services = sorted(services)

        # Arrange filters compactly in two columns to save vertical space
        col_f1, col_f2 = st.columns([1, 1])
        with col_f1:
            sel_users = st.multiselect('Users (filter)', user_list, key=f'lv_{sel_key}_sel_users')
        with col_f2:
            sel_services = st.multiselect('Services (filter)', sorted(services), key=f'lv_{sel_key}_sel_services')

        col_f3, col_f4 = st.columns([1, 1])
        with col_f3:
            sel_tags = st.multiselect('Tags (from [tag])', tags, key=f'lv_{sel_key}_sel_tags')
        with col_f4:
            free_text = st.text_input('Free-text', key=f'lv_{sel_key}_free_text', placeholder='search...')

        # Add a compact levels filter so users can restrict visible severities
        levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        # place levels control under the tags column to keep layout compact
        with col_f3:
            sel_levels = st.multiselect('Levels (filter)', levels, key=f'lv_{sel_key}_sel_levels')

        # Layout: compact right cluster for buttons
        # Use a wide spacer column and three small columns for the buttons so
        # they appear grouped rather than spread across the whole width.
        # Buttons as small emoji-only icons on the left, spacer to the right
        col_clear, col_refresh, col_raw, col_trunc, col_spacer = st.columns([1, 1, 1, 1, 5])

        # session keys for refresh/truncate actions
        refresh_key = f'lv_{sel_key}_refresh'
        trunc_key = f'lv_{sel_key}_truncated'
        # Use an integer refresh counter so we can invalidate the cached reader
        if refresh_key not in st.session_state:
            st.session_state[refresh_key] = 0
        if trunc_key not in st.session_state:
            st.session_state[trunc_key] = False

        def _clear_filters():
            # callback used by the button to safely update widget-backed session keys
            st.session_state[f'lv_{sel_key}_sel_users'] = []
            st.session_state[f'lv_{sel_key}_sel_services'] = []
            st.session_state[f'lv_{sel_key}_sel_tags'] = []
            st.session_state[f'lv_{sel_key}_sel_levels'] = []
            st.session_state[f'lv_{sel_key}_free_text'] = ''
            st.session_state[f'lv_{sel_key}_show_raw'] = False
            # bump refresh counter so the reader reloads and the view updates
            st.session_state[refresh_key] = st.session_state.get(refresh_key, 0) + 1
            # keep the filters visible so user sees the cleared state
            pass

        def _mark_refresh(session_key: str, expander_session_key: str = None):
            # bump the refresh counter to force the cached reader to reload
            st.session_state[session_key] = st.session_state.get(session_key, 0) + 1
            # Ensure the expander remains open after a refresh so the user
            # immediately sees the updated log content without having to
            # re-open the panel manually.
            # no-op for expander since the view is always visible when files are selected
        def _truncate_and_mark(path_str: str, session_key: str):
            # If path_str is empty, operate on all selected files (multi-select)
            paths = []
            if path_str:
                paths = [path_str]
            else:
                for name in sel_logs:
                    p = logs_dir / f"{name}.log"
                    if p.exists():
                        paths.append(str(p))

            if not paths:
                st.error('No logfile selected to purge')
                return

            any_success = False
            for pth in paths:
                success, msg = logging_helpers.purge_log_to_rotated(pth, 10 * 1024 * 1024)
                if success:
                    st.success(f'{Path(pth).name}: {msg}')
                    any_success = True
                else:
                    st.error(f'{Path(pth).name}: {msg}')

            if any_success:
                # bump refresh counter so readers reload immediately
                st.session_state[session_key] = st.session_state.get(session_key, 0) + 1

        # Emoji-only buttons (compact "image-like" appearance). Left-aligned.
        # Remove textual captions to save vertical space; icons should be intuitive.
        with col_clear:
            st.button('✖', key=f'lv_{sel_key}_clear', on_click=_clear_filters)
        with col_refresh:
            st.button('🔄', key=f'lv_{sel_key}_refresh_btn', on_click=_mark_refresh, args=(refresh_key,))
        with col_raw:
            # Small persistent toggle to view the raw logfile (ignores filters)
            st.checkbox('RAW', key=f'lv_{sel_key}_show_raw', help=pbgui_help.show_raw_log)
        with col_trunc:
            st.button('🗑️', key=f'lv_{sel_key}_truncate', on_click=_truncate_and_mark, args=(str(logfile) if logfile else '', trunc_key))

        # Determine whether any filters active
        has_filters = False
        if st.session_state.get(f'lv_{sel_key}_sel_users'):
            has_filters = True
        if st.session_state.get(f'lv_{sel_key}_sel_services'):
            has_filters = True
        if st.session_state.get(f'lv_{sel_key}_sel_tags'):
            has_filters = True
        if st.session_state.get(f'lv_{sel_key}_sel_levels'):
            has_filters = True
        if st.session_state.get(f'lv_{sel_key}_free_text'):
            has_filters = True

        # Handle refresh / truncate actions triggered by callbacks in the filters area.
        # If the refresh flag is set, re-read the logfile into `lines` so the
        # displayed content reflects the latest file state. If the truncate flag is
        # set, treat the logfile as empty.
        refresh_key = f'lv_{sel_key}_refresh'
        trunc_key = f'lv_{sel_key}_truncated'
        if st.session_state.get(trunc_key):
            # logfile was truncated — show empty view and clear the flag
            lines = []
            st.session_state[trunc_key] = False
        if st.session_state.get(refresh_key):
            # re-read logfile from disk and clear flag
            # re-read selected files
            lines = []
            for name in sel_logs:
                p = logs_dir / f"{name}.log"
                if not p.exists():
                    continue
                try:
                    with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                        log = f.readlines()
                        lines.extend(list(reversed(log)))
                except Exception:
                    pass
            st.session_state[refresh_key] = False

        # If multiple files selected, merge them by timestamp
        def parse_iso_to_epoch(ts_str: str):
            try:
                s = ts_str.strip()
                if s.endswith('Z'):
                    s = s[:-1]
                s = s.replace('T', ' ')
                # datetime.fromisoformat handles fractional seconds
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                try:
                    # Fallback to parsing prefix only
                    return datetime.strptime(ts_str.split()[0], '%Y-%m-%d').timestamp()
                except Exception:
                    return 0

        annotated = []
        for ln in lines:
            parsed = parse_log_line(ln)
            if parsed and parsed.get('timestamp'):
                epoch = parse_iso_to_epoch(parsed.get('timestamp'))
            else:
                epoch = 0
            annotated.append((epoch, ln))
        # sort newest-first
        annotated.sort(key=lambda x: x[0], reverse=True)
        display_lines = [ln for _, ln in annotated]
        # If the user requests the raw view, ignore filters and show the
        # cached raw lines (newest-first). The `RAW` checkbox is persistent
        # via session_state key `lv_<logfile>_show_raw`.
        show_raw = st.session_state.get(f'lv_{sel_key}_show_raw', False)
        if show_raw:
            # RAW mode: show unadorned lines, but if any filters are active
            # respect them — users expect RAW to disable formatting/icons,
            # not to disable filtering entirely.
            if has_filters:
                # Reuse the filtered-path but do not add severity markers.
                filter_sig = (tuple(sel_users), tuple(sel_services), tuple(sel_tags), tuple(sel_levels), free_text)
                # Read all selected files into all_lines for filtering
                all_lines = []
                for name in sel_logs:
                    p = logs_dir / f"{name}.log"
                    if not p.exists():
                        continue
                    try:
                        with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                            all_lines.extend(list(reversed(f.readlines())))
                    except Exception:
                        pass
                sel_users = st.session_state.get(f'lv_{sel_key}_sel_users', [])
                sel_services = st.session_state.get(f'lv_{sel_key}_sel_services', [])
                sel_tags = st.session_state.get(f'lv_{sel_key}_sel_tags', [])
                free_text = st.session_state.get(f'lv_{sel_key}_free_text', '')
                sel_levels = st.session_state.get(f'lv_{sel_key}_sel_levels', [])

                def line_has_any_tag(line, taglist):
                    for t in taglist:
                        if f'[{t}]' in line:
                            return True
                    return False

                filtered_raw = []
                for ln in all_lines:
                    ok = True
                    parsed = parse_log_line(ln)
                    if sel_services:
                        if not parsed or parsed.get('service') not in sel_services:
                            ok = False
                    if not ok:
                        continue
                    if sel_users:
                        if not any(u in ln for u in sel_users):
                            ok = False
                    if ok and sel_tags:
                        if not line_has_any_tag(ln, sel_tags):
                            ok = False
                    if ok and free_text:
                        if free_text.lower() not in ln.lower():
                            ok = False
                    if ok:
                        level = parsed.get('level') if parsed else None
                        if sel_levels:
                            if not level or level not in sel_levels:
                                ok = False
                                continue
                        filtered_raw.append(ln)

                display_lines = filtered_raw
            else:
                display_lines = lines
        elif has_filters:
            # When any filter is active, re-read the logfile from disk to
            # ensure we filter against the full, up-to-date content rather
            # than potentially stale cached results. This avoids cases where
            # selecting a user initially shows an incomplete subset until a
            # manual refresh is clicked.
            # Create a filter-signature so the cached reader will produce a
            # fresh read when any of the filter controls change. Using the
            # cached reader keeps centralized caching behaviour while still
            # allowing an automatic cache-bust when filters change.
            filter_sig = (tuple(sel_users), tuple(sel_services), tuple(sel_tags), tuple(sel_levels), free_text)
            try:
                # Always re-read the selected files on filter change to avoid stale results.
                all_lines = []
                for name in sel_logs:
                    p = logs_dir / f"{name}.log"
                    if not p.exists():
                        continue
                    try:
                        with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                            all_lines.extend(list(reversed(f.readlines())))
                    except Exception:
                        pass
            except Exception:
                all_lines = lines
            sel_users = st.session_state.get(f'lv_{sel_key}_sel_users', [])
            sel_services = st.session_state.get(f'lv_{sel_key}_sel_services', [])
            sel_tags = st.session_state.get(f'lv_{sel_key}_sel_tags', [])
            free_text = st.session_state.get(f'lv_{sel_key}_free_text', '')
            sel_levels = st.session_state.get(f'lv_{sel_key}_sel_levels', [])

            def line_has_any_tag(line, taglist):
                for t in taglist:
                    if f'[{t}]' in line:
                        return True
                return False

            filtered = []
            for ln in all_lines:
                ok = True
                parsed = parse_log_line(ln)
                if sel_services:
                    # if we can't parse service, treat as non-matching
                    if not parsed or parsed.get('service') not in sel_services:
                        ok = False
                if not ok:
                    continue
                if sel_users:
                    if not any(u in ln for u in sel_users):
                        ok = False
                if ok and sel_tags:
                    if not line_has_any_tag(ln, sel_tags):
                        ok = False
                if ok and free_text:
                    if free_text.lower() not in ln.lower():
                        ok = False
                if ok:
                    # prefix line with a small severity marker emoji for visibility
                    level = parsed.get('level') if parsed else None
                    # Level is stored as the exact bracket token (e.g. 'INFO')
                    norm_level = level if level else None
                    if sel_levels:
                        if not norm_level or norm_level not in sel_levels:
                            ok = False
                            continue
                    marker = ''
                    level_val = level if level else ''
                    if level_val in ('ERROR', 'CRITICAL'):
                        marker = '⛔ '
                    elif level_val in ('WARNING',):
                        marker = '⚠️ '
                    elif level_val == 'INFO':
                        marker = 'ℹ️ '
                    elif level_val == 'DEBUG':
                        marker = '🔍 '
                    filtered.append(marker + ln)

            display_lines = filtered
        else:
            # Non-RAW, non-filtered view: prefix each line with a small
            # severity marker so icons are visible by default in the GUI.
            prefixed = []
            for ln in display_lines:
                try:
                    parsed = parse_log_line(ln)
                except Exception:
                    parsed = None
                level = parsed.get('level') if parsed else None
                marker = ''
                if level in ('ERROR', 'CRITICAL'):
                    marker = '⛔ '
                elif level in ('WARNING',):
                    marker = '⚠️ '
                elif level == 'INFO':
                    marker = 'ℹ️ '
                elif level == 'DEBUG':
                    marker = '🔍 '
                prefixed.append(marker + ln)
            display_lines = prefixed

        # Render log inside the expander (below the filters)
        stx.scrollableTextbox(''.join(display_lines), height="800", key=f'stx_lv_{sel_key}_inner')

    # Render (log rendering moved into the expander above)
    # Keep a small, read-only fallback if the inner textbox isn't available
    try:
        # If we get here, the inner textbox has already rendered inside the expander
        pass
    except Exception:
        stx.scrollableTextbox(''.join(lines), height="800", key=f'stx_lv_{sel_key}_fallback')
