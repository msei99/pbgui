import streamlit as st
from pathlib import Path
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb7_installed, is_authenticted, get_navi_paths
from Config import BalanceCalculator


# ── Guide helpers ──────────────────────────────────────────

def _docs_index(lang: str) -> list[tuple[str, str]]:
    folder = "help_de" if str(lang).strip().upper() == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for p in sorted(docs_dir.glob("*.md")):
        label = p.name
        try:
            first = p.read_text(encoding="utf-8").splitlines()[0].strip()
            if first.startswith("#"):
                label = first.lstrip("#").strip() or p.name
        except Exception:
            pass
        out.append((label, str(p)))
    return out


def _read_markdown(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except Exception as e:
        return f"Failed to read docs: {e}"


@st.dialog("Help & Tutorials", width="large")
def _help_modal(default_topic: str = "Balance Calculator"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="bc_help_lang")
    docs = _docs_index(str(lang))
    if not docs:
        st.info("No help docs found.")
        return
    labels = [d[0] for d in docs]
    default_index = 0
    target = str(default_topic or "").strip().lower()
    if target:
        for i, lbl in enumerate(labels):
            if target in str(lbl).lower():
                default_index = i
                break
    sel = st.selectbox(
        "Select Topic",
        options=list(range(len(labels))),
        format_func=lambda i: labels[int(i)],
        index=int(default_index),
        key="bc_help_sel",
    )
    st.markdown(_read_markdown(docs[int(sel)][1]), unsafe_allow_html=True)

def balance_calculator():
    # Init balance calculator
    if "balance_calc" not in st.session_state:
        st.session_state.balance_calc = BalanceCalculator()
    balance_calc = st.session_state.balance_calc
    # View
    balance_calc.view()
    
# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Balance Calculator")
_c_title, _c_help = st.columns([0.94, 0.06], vertical_alignment="center")
with _c_title:
    st.header("PBv7 Balance Calculator")
with _c_help:
    if st.button("📖 Guide", key="bc_guide_btn", help="Open help and tutorials"):
        _help_modal("Balance Calculator")
        st.stop()
st.markdown(
    "<hr style='width:100%;margin-top:-0.5rem;margin-bottom:1rem;border:0;border-top:2px solid #ff4b4b;' />",
    unsafe_allow_html=True,
)

# Check if PB7 is installed
if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

# Check if CoinData is configured
if st.session_state.pbcoindata.api_error:
    st.warning('Coin Data API is not configured / Go to Coin Data and configure your API-Key', icon="⚠️")
    st.stop()

balance_calculator()