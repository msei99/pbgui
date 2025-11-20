import streamlit as st
from pathlib import Path
import re
import json
import streamlit_scrollable_textbox as stx

def view_log_filtered(log_filename: str):
    """A minimal, standalone filtered log viewer for a single logfile.
    Designed to be used initially only for PBData.
    """
    pbgdir = Path.cwd()
    logfile = Path(f'{pbgdir}/data/logs/{log_filename}.log')

    # Read logfile (newest first) using a cached reader. The cached reader
    # is keyed on the logfile path and a refresh counter so we only re-read
    # the file when the user requests it.
    @st.cache_data
    def _read_log_cached(path_str: str, refresh_count: int):
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
    if not logfile.exists():
        st.info(f'{log_filename} logfile not found yet.')
        lines = []
    else:
        lines = _read_log_cached(str(logfile), st.session_state.get(f'lv_{log_filename}_refresh', 0))

    # Logfile expander (collapsed by default). Track open state in session_state
    expander_key = f'lv_{log_filename}_expander'
    if expander_key not in st.session_state:
        st.session_state[expander_key] = False
    with st.expander(f'{log_filename} logfile', expanded=st.session_state.get(expander_key, False)):
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

            return {
                'timestamp': m.group(0).strip(),
                'service': service,
                'tags': tags,
                'user': user,
                'message': message,
                'meta': meta,
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
            sel_users = st.multiselect('Users (filter)', user_list, key=f'lv_{log_filename}_sel_users')
        with col_f2:
            sel_services = st.multiselect('Services (filter)', services, key=f'lv_{log_filename}_sel_services')

        col_f3, col_f4 = st.columns([1, 1])
        with col_f3:
            sel_tags = st.multiselect('Tags (from [tag])', tags, key=f'lv_{log_filename}_sel_tags')
        with col_f4:
            free_text = st.text_input('Free-text', key=f'lv_{log_filename}_free_text', placeholder='search...')

        # Layout: compact right cluster for buttons
        # Use a wide spacer column and three small columns for the buttons so
        # they appear grouped rather than spread across the whole width.
        # Buttons as small emoji-only icons on the left, spacer to the right
        col_clear, col_refresh, col_trunc, col_spacer = st.columns([1, 1, 1, 6])

        # session keys for refresh/truncate actions
        refresh_key = f'lv_{log_filename}_refresh'
        trunc_key = f'lv_{log_filename}_truncated'
        # Use an integer refresh counter so we can invalidate the cached reader
        if refresh_key not in st.session_state:
            st.session_state[refresh_key] = 0
        if trunc_key not in st.session_state:
            st.session_state[trunc_key] = False

        def _clear_filters():
            # callback used by the button to safely update widget-backed session keys
            st.session_state[f'lv_{log_filename}_sel_users'] = []
            st.session_state[f'lv_{log_filename}_sel_tags'] = []
            st.session_state[f'lv_{log_filename}_free_text'] = ''
            # Removed reverse checkbox: nothing to clear here anymore

        def _mark_refresh(session_key: str):
            # bump the refresh counter to force the cached reader to reload
            st.session_state[session_key] = st.session_state.get(session_key, 0) + 1

        def _truncate_and_mark(path_str: str, session_key: str):
            try:
                with open(path_str, 'r+') as f:
                    f.truncate()
                # bump refresh counter so readers reload immediately
                st.session_state[session_key] = st.session_state.get(session_key, 0) + 1
                # Keep the Filters expander open after truncating so the UI
                # doesn't unexpectedly collapse for the user.
                st.session_state[expander_key] = True
            except Exception as e:
                st.error(f'Failed to truncate logfile: {e}')

        # Emoji-only buttons (compact "image-like" appearance). Left-aligned.
        # Remove textual captions to save vertical space; icons should be intuitive.
        with col_clear:
            st.button('✖', key=f'lv_{log_filename}_clear', on_click=_clear_filters)
        with col_refresh:
            st.button('🔄', key=f'lv_{log_filename}_refresh_btn', on_click=_mark_refresh, args=(refresh_key,))
        with col_trunc:
            st.button('🗑️', key=f'lv_{log_filename}_truncate', on_click=_truncate_and_mark, args=(str(logfile), trunc_key))
        # Determine whether any filters active
        has_filters = False
        if st.session_state.get(f'lv_{log_filename}_sel_users'):
            has_filters = True
        if st.session_state.get(f'lv_{log_filename}_sel_services'):
            has_filters = True
        if st.session_state.get(f'lv_{log_filename}_sel_tags'):
            has_filters = True
        if st.session_state.get(f'lv_{log_filename}_free_text'):
            has_filters = True

        # Handle refresh / truncate actions triggered by callbacks in the filters area.
        # If the refresh counter changed, re-read the logfile into `lines` so the
        # displayed content reflects the latest file state. If the truncate flag is
        # set, treat the logfile as empty.
        refresh_key = f'lv_{log_filename}_refresh'
        trunc_key = f'lv_{log_filename}_truncated'
        if st.session_state.get(trunc_key):
            # logfile was truncated — show empty view and clear the flag
            lines = []
            st.session_state[trunc_key] = False
        if st.session_state.get(refresh_key):
            # re-read logfile from disk and clear flag
            if logfile.exists():
                try:
                    with open(logfile, 'r', encoding='utf-8', errors='ignore') as f:
                        log = f.readlines()
                        lines = list(reversed(log))
                except Exception:
                    lines = []
            else:
                lines = []
            st.session_state[refresh_key] = False

        display_lines = lines
        if has_filters:
            sel_users = st.session_state.get(f'lv_{log_filename}_sel_users', [])
            sel_services = st.session_state.get(f'lv_{log_filename}_sel_services', [])
            sel_tags = st.session_state.get(f'lv_{log_filename}_sel_tags', [])
            free_text = st.session_state.get(f'lv_{log_filename}_free_text', '')

            def line_has_any_tag(line, taglist):
                for t in taglist:
                    if f'[{t}]' in line:
                        return True
                return False

            filtered = []
            for ln in lines:
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
                    filtered.append(ln)

            display_lines = filtered

        # Render the logfile inside the expander so it's collapsible with the filters
        stx.scrollableTextbox(''.join(display_lines), height="800", key=f'stx_lv_{log_filename}')
