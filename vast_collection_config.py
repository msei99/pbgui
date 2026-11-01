"""Operator settings for bounded, incremental Vast result collection."""
import math

from pbgui_purefunc import load_ini_snapshot
from vast_provider import VastError

SERVICE = "VastCollectionConfig"


def collection_setting(name, *, integer=False):
    """Read a positive [vast] option from pbgui.ini, without changing it."""
    value = load_ini_snapshot().parser.get('vast', name, fallback=None)
    if value is None:
        return None
    try:
        number = int(value) if integer else float(value)
        if not math.isfinite(number) or number <= 0:
            raise ValueError
    except (ValueError, OverflowError):
        raise VastError('Invalid vast.' + name + ': expected a positive ' + ('integer' if integer else 'number'), 422) from None
    return number
