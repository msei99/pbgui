"""Rental/automation duration guards.

Vast.ai itself does not cap instance lifetime and bills per second, so pbgui no
longer imposes an upper bound on the requested window either. These tests pin
the remaining invariants: long windows are accepted, non-finite values and
sub-quarter-hour windows are not.
"""
import pytest
from pydantic import ValidationError

from api.vast import RentalPreferences


def test_multi_day_windows_are_accepted():
    assert RentalPreferences(hours=120).hours == 120
    assert RentalPreferences(hours=720).hours == 720


def test_very_long_windows_are_accepted():
    assert RentalPreferences(hours=8760).hours == 8760


def test_non_finite_hours_are_rejected():
    with pytest.raises(ValidationError):
        RentalPreferences(hours=float("inf"))


def test_rental_preferences_reject_too_short_window():
    with pytest.raises(ValidationError):
        RentalPreferences(hours=0.1)

