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

def edit_tradfi():
    """TradFi data provider config section for stock perps backtesting."""
    PROVIDERS = ["alpaca", "polygon", "yfinance", "finnhub", "alphavantage"]
    PROVIDER_NOTES = {
        "alpaca": "Free, 5+ years of 1-minute data. Recommended.",
        "polygon": "Free tier: ~2 years of data.",
        "yfinance": "No API key required. Limited to last 7 days.",
        "finnhub": "Free tier available.",
        "alphavantage": "Free tier available.",
    }
    NEEDS_SECRET = {"alpaca"}

    users = st.session_state.users
    tradfi = getattr(users, "tradfi", {}) or {}
    provider = tradfi.get("provider", "yfinance")
    api_key = tradfi.get("api_key", "")
    api_secret = tradfi.get("api_secret", "")

    has_config = bool(provider and provider != "yfinance" and api_key)

    with st.expander("TradFi Data Provider  (Stock Perps Backtesting)", expanded=has_config):
        st.info(
            "Required for backtesting stock perpetuals (TSLA, NVDA, etc.) "
            "beyond the last 7 days. **yfinance** works without a key but only covers "
            "the most recent 7 days of data. **Alpaca** is recommended for free 5+ year history."
        )
        col1, col2 = st.columns([1, 2], vertical_alignment="top")
        with col1:
            idx = PROVIDERS.index(provider) if provider in PROVIDERS else PROVIDERS.index("yfinance")
            sel_provider = st.selectbox(
                "Provider",
                PROVIDERS,
                index=idx,
                key="tradfi_provider",
            )
            st.caption(PROVIDER_NOTES.get(sel_provider, ""))
        with col2:
            if sel_provider == "yfinance":
                st.info("No API key required for yfinance.")
                sel_key = ""
                sel_secret = ""
            else:
                sel_key = st.text_input("API Key", value=api_key, key="tradfi_api_key")
                if sel_provider in NEEDS_SECRET:
                    sel_secret = st.text_input(
                        "API Secret", value=api_secret, key="tradfi_api_secret", type="password"
                    )
                else:
                    sel_secret = ""

        col_save, col_clear = st.columns([1, 1])
        with col_save:
            if st.button("Save TradFi Config", key="tradfi_save"):
                new_tradfi: dict = {"provider": sel_provider}
                if sel_key:
                    new_tradfi["api_key"] = sel_key
                if sel_secret:
                    new_tradfi["api_secret"] = sel_secret
                if hasattr(type(users), "tradfi"):
                    users.tradfi = new_tradfi
                elif hasattr(users, "_top_level_extras"):
                    users._top_level_extras["tradfi"] = new_tradfi
                users.save()
                st.success("TradFi config saved.")
        with col_clear:
            if st.button("Clear TradFi Config", key="tradfi_clear"):
                if hasattr(type(users), "tradfi"):
                    users.tradfi = {}
                elif hasattr(users, "_top_level_extras"):
                    users._top_level_extras.pop("tradfi", None)
                users.save()
                st.info("TradFi config cleared.")


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
