"""Rental/automation duration guard rails.

Vast.ai does not cap instance lifetime; the bound is a pbgui guard rail that can
be raised deliberately. These tests pin the boundary so it cannot drift silently.
"""
import pytest
from pydantic import ValidationError

from api.vast import MAX_RENTAL_HOURS, RentalPreferences


def test_max_rental_hours_allows_multi_day_loops():
    assert MAX_RENTAL_HOURS >= 120


def test_rental_preferences_accept_120_hours():
    prefs = RentalPreferences(hours=120)
    assert prefs.hours == 120


def test_rental_preferences_accept_upper_bound():
    assert RentalPreferences(hours=MAX_RENTAL_HOURS).hours == MAX_RENTAL_HOURS


def test_rental_preferences_reject_above_upper_bound():
    with pytest.raises(ValidationError):
        RentalPreferences(hours=MAX_RENTAL_HOURS + 0.25)


def test_rental_preferences_reject_too_short_window():
    with pytest.raises(ValidationError):
        RentalPreferences(hours=0.1)
