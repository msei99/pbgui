import streamlit as st
from pbgui_func import (
    set_page_config,
    is_session_state_not_initialized,
    is_authenticted,
    get_navi_paths,
    sync_api,
    render_header_with_guide,
)
from User import User, Users
from Exchange import Exchange, Exchanges, Spot, Passphrase
from PBRemote import PBRemote
import json
from pathlib import Path

def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            label = p.name
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "API-Keys"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="api_keys_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]
    default_index = 0
    try:
        target = str(default_topic or "").strip().lower()
        if target:
            for i, lbl in enumerate(labels):
                if target in str(lbl).lower():
                    default_index = i
                    break
    except Exception:
        default_index = 0

    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="api_keys_help_sel",
    )
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)
    try:
        base = str(st.get_option("server.baseUrlPath") or "").strip("/")
        prefix = f"/{base}" if base else ""
        st.markdown(
            f"<a href='{prefix}/help' target='_blank'>Open full Help page in new tab</a>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass

@st.dialog("Delete User?")
def delete_user(user):
    st.warning(f"Delete User {user} ?", icon="⚠️")
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(":green[Yes]"):
            st.session_state.users.remove_user(user)
            st.session_state.ed_user_key += 1
            st.rerun()
    with col2:
        if st.button(":red[No]"):
            st.session_state.ed_user_key += 1
            st.rerun()

def edit_user():
    # Init
    user = st.session_state.edit_user
    users = st.session_state.users
    instances = st.session_state.pbgui_instances
    in_use = instances.is_user_used(user.name)
    balance_futures = None
    balance_spot = None
    # Display Error
    if "error" in st.session_state:
        st.error(st.session_state.error, icon="🚨")
    if "api_keys_error" in st.session_state:
        st.error(st.session_state.api_keys_error, icon="🚨")
    with st.sidebar:
        if st.button(":back:"):
            if "error" in st.session_state:
                del st.session_state.error
            del st.session_state.edit_user
            del st.session_state.users
            with st.spinner('Initializing Users...'):
               st.session_state.users = Users()

            st.rerun()
        if not in_use and not "error" in st.session_state:
            if st.button(":wastebasket:"):
                users.users.remove(user)
                users.save()
                if "error" in st.session_state:
                    del st.session_state.error
                # Return to API-Keys editor list without clearing session state.
                # Clearing `users` would make the session look uninitialized and redirect to Welcome.
                del st.session_state.edit_user
                st.session_state.ed_user_key = int(st.session_state.get("ed_user_key", 0)) + 1
                # cleanup for Remote Server Manager
                if "remote" in st.session_state:
                    del st.session_state.remote
                PBRemote().restart()
                st.rerun()
        if user.name and not "error" in st.session_state and not "api_keys_error" in st.session_state:
            if st.button(":floppy_disk:"):
                if not users.has_user(user):
                    users.users.append(user)
                users.save()
    # Reset editor state when switching between users
    if st.session_state.get("api_editor_user") != user.name:
        st.session_state.api_editor_user = user.name
        for k in [
            "api_wallet_address",
            "api_private_key",
            "api_is_vault",
            "api_passphrase",
            "api_secret",
            "api_exchange",
            "api_key",
            "api_quote",
            "api_options_json",
            "api_extra_json",
        ]:
            if k in st.session_state:
                del st.session_state[k]
        if "api_keys_error" in st.session_state:
            del st.session_state.api_keys_error
    # Init session states for keys
    if "api_wallet_address" in st.session_state:
        if st.session_state.api_wallet_address != user.wallet_address:
            user.wallet_address = st.session_state.api_wallet_address
    if "api_private_key" in st.session_state:
        if st.session_state.api_private_key != user.private_key:
            user.private_key = st.session_state.api_private_key
    if "api_is_vault" in st.session_state:
        if st.session_state.api_is_vault != user.is_vault:
            user.is_vault = st.session_state.api_is_vault
    if "api_passphrase" in st.session_state:
        if st.session_state.api_passphrase != user.passphrase:
            user.passphrase = st.session_state.api_passphrase
    if "api_secret" in st.session_state:
        if st.session_state.api_secret != user.secret:
            user.secret = st.session_state.api_secret
    if "api_exchange" in st.session_state:
        if st.session_state.api_exchange != user.exchange:
            user.exchange = st.session_state.api_exchange
    if "api_key" in st.session_state:
        if st.session_state.api_key != user.key:
            user.key = st.session_state.api_key
    if "api_quote" in st.session_state:
        if st.session_state.api_quote != user.quote:
            user.quote = st.session_state.api_quote
    col_1, col_2, col_3 = st.columns([1,1,1],vertical_alignment="bottom")
    with col_1:
        new_name = st.text_input("Username", value=user.name, max_chars=32, type="default", help=None, disabled=in_use)
        if new_name != user.name:
            user.name = new_name
            if users.has_user(user):
                st.session_state.error = "Username already in use"
            else:
                if "error" in st.session_state:
                    del st.session_state.error
            st.rerun()
    with col_2:
        if user.exchange:
            index_exc = Exchanges.list().index(user.exchange)
        else:
            index_exc = 0
        st.selectbox('Exchange', Exchanges.list(), index=index_exc, key = "api_exchange", disabled=in_use)
    with col_3:
        if st.button("Test"):
            exchange = Exchange(user.exchange, user)
            balance_futures = exchange.fetch_balance('swap')
            if exchange.name in Spot.list():
                balance_spot = exchange.fetch_balance('spot')

    col_1, col_2, col_3 = st.columns([1,1,1],vertical_alignment="bottom")
    with col_1:
        if user.exchange == "hyperliquid":
            st.text_input("Wallet Address", value=user.wallet_address, key="api_wallet_address", help=None)
        else:
            st.text_input("API-Key", value=user.key, type="default", key="api_key", help=None)
    with col_2:
        if user.exchange == "hyperliquid":
            st.text_input("Private Key", value=user.private_key, type="password", key="api_private_key",help=None)
        else:
            st.text_input("API-Secret", value=user.secret, type="password", key="api_secret", help=None)
    with col_3:
        if user.exchange == "hyperliquid":
            st.checkbox("Vault", value=user.is_vault, key="api_is_vault", help=None)
        if user.exchange in Passphrase.list():
            st.text_input("Passphrase / Password", value=user.passphrase, type="password", key="api_passphrase", help=None)

    with st.expander("Advanced (optional)"):
        st.caption("Optional fields used by PB7/CCXT. See the Guide button for examples.")
        st.text_input("quote", value=user.quote or "", key="api_quote", help=None)

        options_default = ""
        if isinstance(user.options, dict) and user.options:
            options_default = json.dumps(user.options, indent=2)
        options_raw = st.text_area("options (JSON object)", value=options_default, key="api_options_json", help=None)
        if options_raw.strip() == "":
            user.options = None
        else:
            try:
                parsed = json.loads(options_raw)
                if not isinstance(parsed, dict):
                    raise ValueError("options must be a JSON object")
                user.options = parsed
                if "api_keys_error" in st.session_state:
                    del st.session_state.api_keys_error
            except Exception as e:
                st.session_state.api_keys_error = f"Invalid JSON in 'options': {e}"

        extra_default = ""
        if isinstance(user.extra, dict) and user.extra:
            extra_default = json.dumps(user.extra, indent=2)
        extra_raw = st.text_area("extra (JSON passthrough)", value=extra_default, key="api_extra_json", help=None)
        if extra_raw.strip() == "":
            user.extra = {}
        else:
            try:
                parsed = json.loads(extra_raw)
                if not isinstance(parsed, dict):
                    raise ValueError("extra must be a JSON object")
                user.extra = parsed
                if "api_keys_error" in st.session_state:
                    del st.session_state.api_keys_error
            except Exception as e:
                st.session_state.api_keys_error = f"Invalid JSON in 'extra': {e}"
    col_1, col_2, col_3 = st.columns([1,1,1],vertical_alignment="bottom")
    with col_1:
        st.markdown(f'### <center>Futures Wallet Balance</center>', unsafe_allow_html=True)
        if type(balance_futures) == float:
            st.markdown(f'# <center>{balance_futures}</center>', unsafe_allow_html=True)
        elif balance_futures:
            st.error(balance_futures, icon="🚨")    
    with col_2:
        if user.exchange in Spot.list():
            st.markdown(f'### <center>Spot Wallet Balance</center>', unsafe_allow_html=True)
            if type(balance_spot) == float:
                st.markdown(f'# <center>{balance_spot}</center>', unsafe_allow_html=True)
            elif balance_spot:
                st.error(balance_spot, icon="🚨")    

def _run_tradfi_test(py: str, dir: str, provider: str, api_key: str = "", api_secret: str = "") -> tuple[bool, str]:
    """Run tradfi connection test in pb7 venv. Returns (success, message)."""
    import subprocess, tempfile, os, re

    def _is_inotify_watch_error(text: str) -> bool:
        t = (text or "").lower()
        return (
            "ran out of inotify watches" in t
            or "inotify watch limit" in t
            or "failed to initialize c-ares channel" in t
        )

    def _zero_candle_reason(provider_name: str, stderr_text: str) -> str:
        e = (stderr_text or "").lower()
        if "rate limit" in e or "429" in e:
            return "rate limit reached"
        if "invalid" in e or "unauthorized" in e or "403" in e:
            return "invalid/unauthorized API credentials or insufficient plan"
        if "timeout" in e:
            return "request timed out"
        if provider_name == "finnhub":
            return "Finnhub free tier does not support 1-minute intraday"
        if provider_name == "alphavantage":
            return "Alpha Vantage free tier is heavily limited"
        if provider_name == "polygon":
            return "no 1-minute data access for this key/plan or no data in requested range"
        if provider_name == "alpaca":
            return "missing market data access/permissions or no data in requested range"
        if provider_name == "yfinance":
            return "market holiday/weekend or temporary data gap"
        return "unknown reason"

    _kwargs = {}
    if api_key:
        _kwargs["api_key"] = repr(api_key)
    if api_secret:
        _kwargs["api_secret"] = repr(api_secret)
    _kw = (", " + ", ".join(f"{k}={v}" for k, v in _kwargs.items())) if _kwargs else ""
    _script = f"""\
import sys, asyncio
sys.path.insert(0, {repr(str(dir) + '/src')})
from tradfi_data import get_provider
from datetime import datetime, timedelta, timezone

async def _test():
    p = get_provider({repr(provider)}{_kw})
    async with p:
        # Use a completed historical window (exclude current day)
        # to avoid false negatives on delayed/EOD-limited plans.
        end = datetime.now(timezone.utc) - timedelta(days=1)
        start = end - timedelta(days=7)
        c = await p.fetch_1m_candles(
            'AAPL',
            int(start.timestamp() * 1000),
            int(end.timestamp() * 1000),
        )
        print(f'OK:{{len(c)}}')
        if {repr(provider)} == 'polygon' and len(c) == 0:
            import json
            import aiohttp
            url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/{{int(start.timestamp() * 1000)}}/{{int(end.timestamp() * 1000)}}"
            params = {{
                'adjusted': 'true',
                'sort': 'asc',
                'limit': 50000,
                'apiKey': {repr(api_key)},
            }}
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params) as r:
                    print(f'PHTTP:{{r.status}}')
                    t = await r.text()
                    try:
                        d = json.loads(t)
                        print(f"PSTATUS:{{d.get('status')}}")
                        print(f"PERROR:{{d.get('error') or d.get('message') or ''}}")
                        results = d.get('results')
                        print(f"PRESULTS:{{len(results) if isinstance(results, list) else -1}}")
                    except Exception:
                        print(f"PRAW:{{t[:240]}}")

asyncio.run(_test())
"""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as tf:
            tf.write(_script)
            _tmp = tf.name
        result = subprocess.run([py, _tmp], capture_output=True, text=True, timeout=30)
        os.unlink(_tmp)
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()
        if result.returncode == 0 and "OK:" in out:
            m = re.search(r"OK:(\d+)", out)
            if not m:
                return False, "Test failed: could not parse candle count from provider response."
            n = int(m.group(1))
            if n > 0:
                return True, f"Connection OK — {n} candles received (AAPL, last 7 days)."
            else:
                if _is_inotify_watch_error(err):
                    return False, "System error: inotify watch limit reached (DNS resolver). Increase fs.inotify.max_user_watches and retry."
                if provider == "polygon":
                    phttp = ""
                    pstatus = ""
                    perror = ""
                    for line in out.splitlines():
                        if line.startswith("PHTTP:"):
                            phttp = line.split("PHTTP:", 1)[1].strip()
                        elif line.startswith("PSTATUS:"):
                            pstatus = line.split("PSTATUS:", 1)[1].strip()
                        elif line.startswith("PERROR:"):
                            perror = line.split("PERROR:", 1)[1].strip()
                    if phttp or pstatus or perror:
                        details = ", ".join(x for x in [f"http={phttp}" if phttp else "", f"status={pstatus}" if pstatus else "", f"error={perror}" if perror else ""] if x)
                        return False, f"0 candles — Polygon diagnostic: {details}."
                reason = _zero_candle_reason(provider, err)
                if err:
                    err_last = err.splitlines()[-1]
                    return False, f"0 candles — {reason}. Details: {err_last}"
                return False, f"0 candles — {reason}."
        else:
            if _is_inotify_watch_error(err):
                return False, "System error: inotify watch limit reached (DNS resolver). Increase fs.inotify.max_user_watches and retry."
            msg = err.splitlines()[-1] if err else out or "Unknown error"
            return False, f"Test failed: {msg}"
    except subprocess.TimeoutExpired:
        return False, "Test timed out (30s)."
    except Exception as e:
        return False, f"Test error: {e}"


def edit_tradfi():
    """TradFi data provider config section for stock perps backtesting."""
    import subprocess
    from pbgui_func import pb7venv, pb7dir

    PROVIDERS = ["alpaca", "polygon", "finnhub", "alphavantage"]
    PROVIDER_NOTES = {
        "alpaca": "Free, 5+ years of 1-minute data. Recommended.",
        "polygon": "Plan-dependent: intraday coverage and history vary by Polygon plan/key; free access may return no 1-minute data.",
        "finnhub": "⚠️ Free tier does NOT support 1-minute intraday data — unusable for backtesting.",
        "alphavantage": "Free tier: 25 calls/day, very limited for backtesting.",
    }
    PROVIDER_LINKS = {
        "alpaca": ("Get free Alpaca API key", "https://app.alpaca.markets/signup"),
        "polygon": ("Get free Polygon API key", "https://polygon.io/dashboard/signup"),
        "finnhub": ("Get free Finnhub API key", "https://finnhub.io/register"),
        "alphavantage": ("Get free Alpha Vantage API key", "https://www.alphavantage.co/support/#api-key"),
    }
    NEEDS_SECRET = {"alpaca"}

    # Always use a fresh Users() instance to avoid stale session state objects
    # that were created before the tradfi property was added to the Users class.
    users = Users()
    st.session_state.users = users
    tradfi = users.tradfi or {}
    provider = tradfi.get("provider", "alpaca")
    api_key = tradfi.get("api_key", "")
    api_secret = tradfi.get("api_secret", "")

    # Sync widget session state to saved config when config changed on disk
    # (e.g. after save/clear, or first load).
    _saved_sig = f"{provider}|{bool(api_key)}|{bool(api_secret)}"
    if st.session_state.get("_tradfi_sig") != _saved_sig:
        st.session_state["_tradfi_sig"] = _saved_sig
        st.session_state["tradfi_provider"] = provider
        st.session_state["tradfi_api_key"] = api_key
        st.session_state["tradfi_api_secret"] = api_secret

    def _on_tradfi_provider_change():
        selected = st.session_state.get("tradfi_provider", "alpaca")
        if selected == provider:
            st.session_state["tradfi_api_key"] = api_key
            st.session_state["tradfi_api_secret"] = api_secret
        else:
            st.session_state["tradfi_api_key"] = ""
            st.session_state["tradfi_api_secret"] = ""

    has_config = bool(api_key)

    _py = pb7venv()
    _dir = pb7dir()

    with st.expander("TradFi Data Provider  (Stock Perps Backtesting)", expanded=has_config):
        st.info(
            "Stock perp backtests use **yfinance** automatically for the last 7 days (free, no key). "
            "For older data (months/years), configure an extended provider like **Alpaca** (free, 5+ years)."
        )

        # ── Section 1: yfinance ────────────────────────────────────────────
        st.markdown("**yfinance** — automatic default, last 7 days, no API key")
        col_yf_status, col_yf_install, col_yf_test = st.columns([2, 1, 1])
        _yf_installed = False
        _yf_version = ""
        if _py:
            r = subprocess.run([_py, "-c", "import yfinance; print(yfinance.__version__)"],
                               capture_output=True, text=True)
            _yf_installed = r.returncode == 0
            _yf_version = r.stdout.strip() if _yf_installed else ""
        with col_yf_status:
            if not _py:
                st.warning("pb7 venv not configured")
            elif _yf_installed:
                st.success(f"yfinance {_yf_version} installed ✓", icon=":material/check_circle:")
            else:
                st.warning("yfinance not installed", icon=":material/warning:")
        with col_yf_install:
            if _yf_installed:
                if st.button("Uninstall yfinance", key="tradfi_yf_install"):
                    with st.spinner("Uninstalling yfinance..."):
                        r = subprocess.run([_py, "-m", "pip", "uninstall", "yfinance", "-y"],
                                           capture_output=True, text=True, timeout=60)
                    if r.returncode == 0:
                        st.success("yfinance uninstalled.")
                        st.rerun()
                    else:
                        st.error(f"Uninstall failed: {r.stderr.splitlines()[-1] if r.stderr else r.stdout}", icon="🚨")
            else:
                if st.button("Install yfinance", key="tradfi_yf_install"):
                    if not _py:
                        st.error("pb7 venv not configured.", icon="🚨")
                    else:
                        with st.spinner("Installing yfinance..."):
                            r = subprocess.run([_py, "-m", "pip", "install", "yfinance"],
                                               capture_output=True, text=True, timeout=120)
                        if r.returncode == 0:
                            st.success("yfinance installed successfully.")
                            st.rerun()
                        else:
                            st.error(f"Install failed: {r.stderr.splitlines()[-1] if r.stderr else r.stdout}", icon="🚨")
        with col_yf_test:
            if _yf_installed:
                if st.button("Test yfinance", key="tradfi_yf_test"):
                    if not _py or not _dir:
                        st.error("pb7 venv/dir not configured.", icon="🚨")
                    else:
                        with st.spinner("Testing yfinance..."):
                            ok, msg = _run_tradfi_test(_py, _dir, "yfinance")
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg, icon="🚨")

        st.divider()

        # ── Section 2: Extended provider (optional) ────────────────────────
        st.markdown("**Extended provider** — optional, for backtests older than 7 days")
        col1, col2, col3 = st.columns([1, 1, 1], vertical_alignment="bottom")
        with col1:
            _cur_provider = st.session_state.get("tradfi_provider", PROVIDERS[0])
            sel_provider = st.selectbox(
                "Provider",
                PROVIDERS,
                index=PROVIDERS.index(provider) if provider in PROVIDERS else 0,
                key="tradfi_provider",
                help=PROVIDER_NOTES.get(_cur_provider if _cur_provider in PROVIDERS else PROVIDERS[0], ""),
                on_change=_on_tradfi_provider_change,
            )
        with col2:
            sel_key = st.text_input("API Key", key="tradfi_api_key", type="password")
        with col3:
            if sel_provider in NEEDS_SECRET:
                sel_secret = st.text_input("API Secret", key="tradfi_api_secret", type="password")
            else:
                st.text_input("API Secret", value="", disabled=True, placeholder="not required", key="tradfi_secret_na")
                sel_secret = ""
        _link = PROVIDER_LINKS.get(sel_provider)
        if _link:
            st.caption(f"🔗 [{_link[0]}]({_link[1]})")

        col_test, col_save, col_clear = st.columns([1, 1, 1])
        with col_test:
            if st.button("Test Connection", key="tradfi_test"):
                if not _py or not _dir:
                    st.error("pb7 venv/dir not configured.", icon="🚨")
                elif not sel_key:
                    st.warning("Enter an API key first.", icon=":material/warning:")
                else:
                    with st.spinner(f"Testing {sel_provider}..."):
                        ok, msg = _run_tradfi_test(_py, _dir, sel_provider, sel_key, sel_secret)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg, icon="🚨")
        with col_save:
            if st.button("Save TradFi Config", key="tradfi_save"):
                if not sel_key:
                    st.warning("Enter an API key first.", icon=":material/warning:")
                else:
                    new_tradfi: dict = {"provider": sel_provider, "api_key": sel_key}
                    if sel_secret:
                        new_tradfi["api_secret"] = sel_secret
                    users.tradfi = new_tradfi
                    users.save()
                    st.success("TradFi config saved.")
        with col_clear:
            if st.button("Clear TradFi Config", key="tradfi_clear"):
                users.tradfi = {}
                users.save()
                st.session_state.pop("tradfi_provider", None)
                st.session_state.pop("tradfi_api_key", None)
                st.session_state.pop("tradfi_api_secret", None)
                st.rerun()


def select_user():
    # Init
    users = st.session_state.users
    instances = st.session_state.pbgui_instances
    multi_instances = st.session_state.multi_instances
    v7_instances = st.session_state.v7_instances
    # Check API is in Sync
    pbremote = st.session_state.pbremote
    if not "ed_user_key" in st.session_state:
        st.session_state.ed_user_key = 0
    with st.sidebar:
        if st.button(":material/refresh:"):
            pbremote.update_remote_servers()
            st.rerun()
        if st.button("Add"):
            st.session_state.edit_user = User()
            st.rerun()
        sync_api()
    if f'editor_{st.session_state.ed_user_key}' in st.session_state:
        ed = st.session_state[f'editor_{st.session_state.ed_user_key}']
        for row in ed["edited_rows"]:
            if "Edit" in ed["edited_rows"][row]:
                st.session_state.edit_user = users.users[row]
                st.rerun()
            if "Delete" in ed["edited_rows"][row]:
                if not instances.is_user_used(users.users[row].name) and not multi_instances.is_user_used(users.users[row].name) and not v7_instances.is_user_used(users.users[row].name):
                    delete_user(users.users[row].name)
    d = []
    for id, user in enumerate(users):
        in_use = False
        if instances.is_user_used(user.name) or multi_instances.is_user_used(user.name) or v7_instances.is_user_used(user.name):
            in_use = None
        d.append({
            'id': id,
            'Edit': False,
            'User': user.name,
            'Exchange': user.exchange,
            'Delete': in_use,
        })
    column_config = {
        "id": None}
    st.data_editor(data=d, height=(len(users.users)+1)*36, key=f'editor_{st.session_state.ed_user_key}', hide_index=None, column_order=None, column_config=column_config, disabled=['id','User','Exchange',])
    st.divider()
    edit_tradfi()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("API-Keys")

render_header_with_guide(
    "API-Keys",
    guide_callback=lambda: _help_modal("API-Keys"),
    guide_key="api_keys_header_help_btn",
)

# Display Setup
if 'edit_user' in st.session_state:
    edit_user()
else:
    select_user()
