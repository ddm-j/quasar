"""Unit tests for the TradingCalendar utility."""
import pytest
from datetime import date, datetime, time, timezone
import pandas as pd
import pytz
from quasar.lib.common.calendar import TradingCalendar

def test_is_session_nyse_holiday():
    """Verify that NYSE correctly identifies holidays."""
    # Christmas 2025 (Thursday)
    christmas = date(2025, 12, 25)
    assert TradingCalendar.is_session("XNYS", christmas) is False

def test_is_session_nyse_regular():
    """Verify that NYSE identifies regular trading days."""
    # Monday Jan 6, 2025
    monday = date(2025, 1, 6)
    assert TradingCalendar.is_session("XNYS", monday) is True

def test_has_sessions_in_range_nyse():
    """Verify range detection for NYSE."""
    # Weekend only: Dec 20 (Sat) to Dec 21 (Sun) 2025
    assert TradingCalendar.has_sessions_in_range("XNYS", date(2025, 12, 20), date(2025, 12, 21)) is False
    
    # Friday to Monday: Dec 19 to Dec 22 2025
    assert TradingCalendar.has_sessions_in_range("XNYS", date(2025, 12, 19), date(2025, 12, 22)) is True

def test_has_sessions_in_range_xfx():
    """Verify range detection for Forex (Custom)."""
    # Saturday only (Market closed)
    assert TradingCalendar.has_sessions_in_range("XFX", date(2025, 12, 20), date(2025, 12, 20)) is False
    # Sunday (Market opens at 5pm ET, library session starts)
    assert TradingCalendar.has_sessions_in_range("XFX", date(2025, 12, 21), date(2025, 12, 21)) is True

def test_normalization_types():
    """Verify that the utility handles different date/time types correctly."""
    mic = "XNYS"
    day_date = date(2025, 1, 6)
    day_dt = datetime(2025, 1, 6)
    day_ts = pd.Timestamp("2025-01-06")
    
    assert TradingCalendar.is_session(mic, day_date) is True
    assert TradingCalendar.is_session(mic, day_dt) is True
    assert TradingCalendar.is_session(mic, day_ts) is True

def test_unknown_mic_fallback():
    """Verify that unknown MICs default to 'Always Open' with no crash."""
    assert TradingCalendar.is_open_now("UNKNOWN_EXCHANGE") is True
    assert TradingCalendar.is_session("UNKNOWN_EXCHANGE", date(2025, 1, 1)) is True
    assert TradingCalendar.has_sessions_in_range("UNKNOWN_EXCHANGE", date(2025, 1, 1), date(2025, 1, 2)) is True

