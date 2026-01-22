from pbgui_func import set_page_config
import streamlit as st
from pathlib import Path


def _docs_index(lang: str) -> list[tuple[str, str]]:
    ln = str(lang or "EN").strip().upper()
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = Path(__file__).resolve().parents[1] / "docs" / folder
    if not docs_dir.is_dir():
        return []
    out = []
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


def show_help_page():
    set_page_config("Help")

    st.title("Help & Tutorials")
    lang = st.radio("Language", options=["EN", "DE"], horizontal=True, key="help_lang")

    docs = _docs_index(lang)
    if not docs:
        st.info("No help docs found.")
        return

    labels = [d[0] for d in docs]
    sel = st.selectbox("Select Topic", options=list(range(len(labels))), format_func=lambda i: labels[int(i)], index=0, key="help_sel")
    path = docs[int(sel)][1]
    md = _read_markdown(path)
    st.markdown(md, unsafe_allow_html=True)


if __name__ == "__main__":
    show_help_page()
