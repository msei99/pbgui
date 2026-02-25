"""Bidirectional Streamlit component for OHLCV chart with lazy TF loading.

The component is declared at module-level so ``inspect.getmodule()``
returns a real module (unlike Streamlit page files which are exec()'d).
"""

from pathlib import Path
import json
import streamlit.components.v1 as components

_FRONTEND_DIR = str(Path(__file__).parent / "frontend")
_component_func = components.declare_component("ohlcv_chart", path=_FRONTEND_DIR)


def ohlcv_chart(
    layers: dict,
    zoom_range: list | None = None,
    show_volume: bool = True,
    height: int = 620,
    split_dates: list | None = None,
    coin_name: str = "",
    key: str | None = None,
):
    """Render OHLCV chart with multi-resolution candle layers.

    Parameters
    ----------
    layers : dict
        ``{"1d": {ts,o,h,l,c,v}, "1h": {...}, ...}`` columnar data.
    zoom_range : list | None
        ``[start_iso, end_iso]`` to restore zoom after rerun.
    show_volume : bool
        Whether to render volume subplot.
    height : int
        Iframe height in pixels.
    split_dates : list | None
        ``[{"date": "2020-08-31", "factor": 4.0}, ...]``  Stock split events
        to render as dashed vertical lines in the chart.
    coin_name : str
        Display name shown in the chart title area (e.g. ``"GOOGL"``).
    key : str | None
        Streamlit widget key (keeps iframe alive across reruns).

    Returns
    -------
    dict | None
        ``{"need_tf": "5m", "range_start": "...", "range_end": "..."}``
        when the JS needs finer data, else ``None``.
    """
    layers_json = json.dumps(layers, separators=(",", ":"))
    return _component_func(
        layers_json=layers_json,
        zoom_range=zoom_range,
        show_volume=show_volume,
        split_dates=split_dates or [],
        coin_name=coin_name,
        height=height,
        key=key,
        default=None,
    )
