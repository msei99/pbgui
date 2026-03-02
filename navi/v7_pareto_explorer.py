import streamlit as st
from pathlib import Path
from pbgui_func import is_session_state_not_initialized, is_authenticted, set_page_config, render_header_with_guide
from ParetoExplorer import ParetoExplorer


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
def _help_modal(default_topic: str = "Pareto Explorer"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="pareto_help_lang")
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
        key="pareto_help_sel",
    )
    st.markdown(_read_markdown(docs[int(sel)][1]), unsafe_allow_html=True)

set_page_config("Pareto Explorer")

# Authentication check
if is_session_state_not_initialized() or not is_authenticted():
    st.switch_page("navi/system_login.py")

# Check if we have a path to analyze
if "pareto_explorer_path" not in st.session_state:
    st.error("❌ No optimization result selected")
    st.info("Please go to **PBv7 → Optimize → Results** and click the **🎯 Pareto Explorer** button")
    if st.button("← Back to Optimize"):
        st.session_state.view = "optimize"
        st.rerun()
    st.stop()

# Navigation sidebar
with st.sidebar:
    st.title("🎯 Pareto Explorer")
    
    # Extract directory name and show shortened version
    result_path = st.session_state.pareto_explorer_path
    result_name = Path(result_path).name
    
    # Extract meaningful parts: Date + Symbol (e.g., "2025-12-24 | DOGE")
    parts = result_name.split('_')
    if len(parts) >= 5:
        # Format: YYYY-MM-DDTHH_MM_SS_exchange1_exchange2_XXXdays_SYMBOL_hash
        date_part = parts[0].replace('T', ' ')  # "2025-12-24 07:04:00"
        date_short = date_part.split()[0]  # Just the date "2025-12-24"
        
        # Find symbol (usually before the last part which is the hash)
        symbol = parts[-2] if len(parts) > 1 else ""
        
        display_name = f"{date_short} | {symbol}"
    else:
        # Fallback to first 35 chars
        display_name = result_name[:35] + ("..." if len(result_name) > 35 else "")
    
    st.caption(display_name, help=f"📂 {result_name}")

    if st.button("← Back to Optimize Results", width='stretch'):
        if "pareto_explorer_path" in st.session_state:
            del st.session_state.pareto_explorer_path
        st.switch_page("navi/v7_optimize.py")

# Page header with Guide button (top-right)
render_header_with_guide(
    "Pareto Explorer",
    guide_callback=lambda: _help_modal("Pareto Explorer"),
    guide_key="pareto_guide_btn",
)

# Stage navigation (same pattern as Backtest / Optimize)
_PARETO_TABS = ["Command Center", "Pareto Playground", "Deep Intelligence"]
if "pareto_main_view" not in st.session_state:
    st.session_state.pareto_main_view = "Command Center"
_active_stage = st.segmented_control(
    "", options=_PARETO_TABS, default="Command Center", key="pareto_main_view"
) or "Command Center"

# Run the Pareto Explorer
try:
    explorer = ParetoExplorer(st.session_state.pareto_explorer_path)
    explorer.run(stage=_active_stage)
except Exception as e:
    st.error(f"❌ Error loading Pareto Explorer: {e}")
    st.exception(e)
    if st.button("← Back"):
        if "pareto_explorer_path" in st.session_state:
            del st.session_state.pareto_explorer_path
        st.switch_page("navi/v7_optimize.py")
