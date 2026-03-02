import streamlit as st
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, info_popup, is_pb7_installed, is_authenticted, get_navi_paths, render_header_with_guide
from OptimizeV7 import OptimizeV7Item, OptimizesV7, OptimizeV7Queue, OptimizeV7Results
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
def _opt_v7_help_modal(default_topic: str = "Optimize"):
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="opt_v7_help_lang")
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
        key="opt_v7_help_sel",
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


# ── Sub-view: edit/create a single optimize config ──────────────────────────

def opt_v7():
    opt_v7 = st.session_state.opt_v7
    with st.sidebar:
        if st.button(":material/home:", help="Back to Config list"):
            del st.session_state.opt_v7
            if "opt_v7_list" in st.session_state:
                del st.session_state.opt_v7_list
            st.rerun()
        if st.button(":material/save:"):
            if opt_v7.name:
                with st.spinner("Saving..."):
                    opt_v7.save()
                    if "opt_v7_list" in st.session_state:
                        del st.session_state.opt_v7_list
            else:
                info_popup("Name is empty")
        if st.button("Add to Queue"):
            if not opt_v7.name:
                info_popup("Name is empty")
            else:
                with st.spinner("Saving and adding to queue"):
                    opt_v7.save()
                    if "opt_v7_list" in st.session_state:
                        del st.session_state.opt_v7_list
                if opt_v7.config.config_file:
                    # save() succeeded (config_file is now set)
                    if opt_v7.save_queue():
                        del st.session_state.opt_v7
                        # Force queue to reload from disk so the new item appears
                        if "opt_v7_queue" in st.session_state:
                            del st.session_state.opt_v7_queue
                        st.session_state["_opt_v7_main_view_next"] = "Queue"
                        st.rerun()
        st.markdown("---")
        st.selectbox('Preset...', opt_v7.find_presets(), key="opt_v7_preset_select")
        col1, col2, col3 = st.columns(3)
        if col1.button(":material/folder: Load", key="opt_v7_preset_load"):
            opt_v7.preset_load(st.session_state.opt_v7_preset_select)
            st.rerun()
        if col2.button(":material/save: Save", key="opt_v7_preset_save"):
            if opt_v7.preset_save():
                st.rerun()
        if col3.button(":material/delete: Del", key="opt_v7_preset_delete"):
            opt_v7.preset_remove(st.session_state.opt_v7_preset_select)
            st.rerun()
    opt_v7.edit()

# ── Sub-view: list of available optimize configs ─────────────────────────────

def opt_v7_list():
    if "opt_v7_list" not in st.session_state:
        st.session_state.opt_v7_list = OptimizesV7()
    opt_v7_list = st.session_state.opt_v7_list
    with st.sidebar:
        if st.button(":material/refresh:"):
            st.session_state.opt_v7_list = OptimizesV7()
            st.rerun()
        if st.button(":material/add:", help="Add new Optimize"):
            st.session_state.opt_v7 = OptimizeV7Item()
            st.rerun()
        if st.button(":material/edit:", help="Edit selected Optimize"):
            opt_v7_list.edit_selected()
        if st.button(":material/delete:", help="Delete selected Optimizes. If none selected all will be deleted"):
            opt_v7_list.remove_selected()
    opt_v7_list.view_optimizes()

# ── Sub-view: Results list ───────────────────────────────────────────────────

def opt_v7_results():
    opt_v7_results = st.session_state.opt_v7_results
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_results.find_results()
            st.rerun()
        if st.button(":material/delete: selected"):
            opt_v7_results.remove_selected_results()
            st.rerun()
        if st.button(":material/delete: all"):
            opt_v7_results.remove_all_results()
            st.rerun()
    opt_v7_results.view_results()

# ── Sub-view: Pareto explorer ────────────────────────────────────────────────

def opt_v7_pareto():
    opt_v7_results = st.session_state.opt_v7_results
    opt_v7_pareto = st.session_state.opt_v7_pareto
    opt_v7_pareto_name = st.session_state.opt_v7_pareto_name
    opt_v7_pareto_directory = st.session_state.opt_v7_pareto_directory
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_results.paretos = []
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
            st.rerun()
        if st.button(":material/arrow_upward_alt:", help="Back to Results list"):
            del st.session_state.opt_v7_pareto
            if "d_paretos" in st.session_state:
                del st.session_state.d_paretos
            opt_v7_results.paretos = []
            st.rerun()
        if st.button("BT selected"):
            opt_v7_results.backtest_selected()
        if st.button("BT all"):
            opt_v7_results.backtest_all()
    st.subheader(f"Name: :blue[{opt_v7_pareto_name}] Directory: :blue[{opt_v7_pareto_directory}]")
    opt_v7_results.view_pareto(opt_v7_pareto)

# ── Sub-view: Queue ──────────────────────────────────────────────────────────

def opt_v7_queue(show_log: bool = False):
    opt_v7_queue = st.session_state.opt_v7_queue
    if "optimize_v7_autostart" in st.session_state:
        if st.session_state.optimize_v7_autostart != opt_v7_queue.autostart:
            opt_v7_queue.autostart = st.session_state.optimize_v7_autostart
    with st.sidebar:
        if st.button(":material/refresh:"):
            opt_v7_queue.items = []
            opt_v7_queue.d = []
            st.rerun()
        st.toggle("Autostart", value=opt_v7_queue.autostart, key="optimize_v7_autostart", help=None)
        if st.button(":material/delete: selected"):
            opt_v7_queue.remove_selected()
            st.rerun()
        if st.button(":material/delete: finished"):
            opt_v7_queue.remove_finish()
            st.rerun()
        if st.button(":material/delete: all"):
            opt_v7_queue.remove_finish(all=True)
            st.rerun()
    if show_log:
        opt_v7_queue.view_log()
    else:
        opt_v7_queue.view()

# ── Redirect to Login ────────────────────────────────────────────────────────

if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# ── Page setup ───────────────────────────────────────────────────────────────

set_page_config("PBv7 Optimize")
render_header_with_guide(
    "PBv7 Optimize",
    guide_callback=lambda: _opt_v7_help_modal(),
    guide_key="opt_v7_header_help_btn",
)

if not is_pb7_installed():
    st.warning('Passivbot Version 7.x is not installed', icon="⚠️")
    st.stop()

if st.session_state.pbcoindata.api_error:
    st.warning('Coin Data API is not configured / Go to Coin Data and configure your API-Key', icon="⚠️")
    st.stop()

# ── Main tab navigation ──────────────────────────────────────────────────────

_MAIN_TABS = ["Config", "Queue", "Log", "Results"]

# Apply pending tab switch (must happen before widget is instantiated)
if "_opt_v7_main_view_next" in st.session_state:
    st.session_state.opt_v7_main_view = st.session_state["_opt_v7_main_view_next"]
    del st.session_state["_opt_v7_main_view_next"]
elif "opt_v7_main_view" not in st.session_state:
    if "opt_v7_queue_log_preselect" in st.session_state:
        st.session_state.opt_v7_main_view = "Log"
    elif "opt_v7_queue" in st.session_state:
        st.session_state.opt_v7_main_view = "Queue"
    elif "opt_v7_results" in st.session_state:
        st.session_state.opt_v7_main_view = "Results"
    elif "opt_v7" in st.session_state:
        st.session_state.opt_v7_main_view = "Config"
    else:
        st.session_state.opt_v7_main_view = "Config"

_active = st.segmented_control(
    "", options=_MAIN_TABS, default="Config", key="opt_v7_main_view"
)

if _active == "Queue":
    if "opt_v7_queue" not in st.session_state:
        st.session_state.opt_v7_queue = OptimizeV7Queue()
    opt_v7_queue()
elif _active == "Log":
    if "opt_v7_queue" not in st.session_state:
        st.session_state.opt_v7_queue = OptimizeV7Queue()
    opt_v7_queue(show_log=True)
elif _active == "Results":
    if "opt_v7_results" not in st.session_state:
        st.session_state.opt_v7_results = OptimizeV7Results()
    if "opt_v7_pareto" in st.session_state:
        opt_v7_pareto()
    else:
        opt_v7_results()
else:  # Config
    if "opt_v7" in st.session_state:
        opt_v7()
    else:
        opt_v7_list()
