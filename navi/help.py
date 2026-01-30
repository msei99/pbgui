from pbgui_func import set_page_config
import streamlit as st
from pathlib import Path


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    root = Path(__file__).resolve().parents[1] / "docs"

    # Central help topics
    folder_help = "help_de" if ln == "DE" else "help"
    dirs: list[tuple[str, Path]] = [("", root / folder_help)]

    # Also include Strategy Explorer docs in the central selector
    folder_se = "strategy_explorer_de" if ln == "DE" else "strategy_explorer"
    dirs.append(("Strategy Explorer: ", root / folder_se))

    out: list[tuple[str, str]] = []
    for prefix, docs_dir in dirs:
        if not docs_dir.is_dir():
            continue
        for p in sorted(docs_dir.glob("*.md")):
            # Movie Builder is part of Strategy Explorer; keep it under Strategy Explorer docs.
            if prefix == "" and p.name == "10_movie_builder.md":
                continue
            label = p.name
            try:
                with open(p, "r", encoding="utf-8") as f:
                    first = f.readline().strip()
                if first.startswith("#"):
                    label = first.lstrip("#").strip() or p.name
            except Exception:
                label = p.name
            out.append((f"{prefix}{label}", str(p)))

    return out


def _read_markdown(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"Failed to read docs: {e}"


def show_help_page():
    set_page_config("Help")

    st.title("Help & Tutorials")
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="help_lang")

    docs = _docs_index(lang)
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]

    # Table of contents (TOC)
    toc_col, content_col = st.columns([0.32, 0.68], gap="large")
    with toc_col:
        st.subheader("Contents")
        flt = st.text_input("Filter", value=str(st.session_state.get("help_filter", "")), key="help_filter")
        flt_norm = str(flt or "").strip().lower()

        visible = list(range(len(labels)))
        if flt_norm:
            visible = [i for i, lbl in enumerate(labels) if flt_norm in str(lbl).lower()]

        if not visible:
            st.info("No topics match your filter.")
            return

        if "help_sel" not in st.session_state:
            st.session_state.help_sel = 0

        # Keep selection valid if the filter hides it
        if int(st.session_state.help_sel) not in visible:
            st.session_state.help_sel = int(visible[0])

        sel = st.radio(
            "",
            options=visible,
            format_func=lambda i: labels[int(i)],
            index=visible.index(int(st.session_state.help_sel)),
            key="help_sel",
        )

    with content_col:
        path = docs[int(sel)][1]
        md = _read_markdown(path)
        st.markdown(md, unsafe_allow_html=True)


if __name__ == "__main__":
    show_help_page()
