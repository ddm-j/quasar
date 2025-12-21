"""Trading calendar utility wrapping exchange_calendars with custom support for Crypto and Forex.

This module provides a unified interface for checking market status across various
exchanges and asset classes, including standard stock exchanges, cryptocurrencies,
and forex markets.
"""

import logging
from datetime import datetime, date, time, timezone
from typing import Optional, Dict

import pytz
import pandas as pd
import exchange_calendars as xcals
from exchange_calendars import ExchangeCalendar, register_calendar_type

logger = logging.getLogger(__name__)

# --- Custom Calendar Definitions ---

class CryptoCalendar(ExchangeCalendar):
    """24/7 Trading Calendar for Cryptocurrencies."""

    @property
    def name(self) -> str:
        """Return the calendar name.

        Returns:
            str: The calendar name ("CRYPTO").
        """
        return "CRYPTO"

    @property
    def tz(self):
        """Return the timezone for the calendar.

        Returns:
            datetime.tzinfo: The UTC timezone.
        """
        return pytz.utc

    @property
    def open_times(self):
        """Return the daily open times.

        Returns:
            tuple: Tuple containing the open time (00:00).
        """
        return ((None, time(0, 0)),)

    @property
    def close_times(self):
        """Return the daily close times.

        Returns:
            tuple: Tuple containing the close time (23:59).
        """
        return ((None, time(23, 59)),)

    @property
    def regular_holidays(self):
        """Return the list of regular holidays.

        Returns:
            None: Crypto markets have no holidays.
        """
        return None

    @property
    def weekmask(self) -> str:
        """Return the weekmask defining active days (all 7 days).

        Returns:
            str: The weekmask "1111111".
        """
        return "1111111"

class ForexCalendar(ExchangeCalendar):
    """24/5 Trading Calendar for Forex (Standard Sunday 5pm ET to Friday 5pm ET)."""

    @property
    def name(self) -> str:
        """Return the calendar name.

        Returns:
            str: The calendar name ("XFX").
        """
        return "XFX"

    @property
    def tz(self):
        """Return the timezone for the calendar.

        Returns:
            datetime.tzinfo: The America/New_York timezone.
        """
        return pytz.timezone("America/New_York")

    @property
    def open_times(self):
        """Return the daily open times.

        Returns:
            tuple: Tuple containing the daily open time.
        """
        # Mon-Fri: 00:00, Sun: 17:00
        # This is tricky in xcals. Let's stick to full days for now to keep it "not overly complex"
        # but include Sunday so we don't miss the open.
        return ((None, time(0, 0)),)

    @property
    def close_times(self):
        """Return the daily close times.

        Returns:
            tuple: Tuple containing the daily close time.
        """
        return ((None, time(23, 59)),)

    @property
    def regular_holidays(self):
        """Return the list of regular holidays.

        Returns:
            None: Forex markets handled via weekmask for now.
        """
        return None

    @property
    def weekmask(self) -> str:
        """Return the weekmask defining active days (Mon-Fri + Sun).

        Returns:
            str: The weekmask "1111101".
        """
        return "1111101"

# Register custom calendars with the library
try:
    register_calendar_type("CRYPTO", CryptoCalendar, force=True)
    register_calendar_type("XFX", ForexCalendar, force=True)
except Exception as e:
    logger.warning(f"Failed to register custom calendars: {e}")


# --- TradingCalendar Wrapper ---

class TradingCalendar:
    """Wrapper for exchange_calendars providing a unified interface for Quasar."""
    
    _cache: Dict[str, ExchangeCalendar] = {}

    @classmethod
    def _get_calendar(cls, mic: str) -> Optional[ExchangeCalendar]:
        """Lazy-load and cache an exchange calendar by its MIC.

        Args:
            mic (str): The Market Identifier Code (ISO 10383).

        Returns:
            Optional[ExchangeCalendar]: The loaded calendar instance, or None if not found.
        """
        if mic is None:
            return None
        mic = mic.upper()
        if mic not in cls._cache:
            try:
                cls._cache[mic] = xcals.get_calendar(mic)
            except xcals.errors.InvalidCalendarName:
                logger.warning(f"Calendar not found for MIC: {mic}. Defaulting to 'Always Open'.")
                return None
            except Exception as e:
                logger.error(f"Error loading calendar for MIC {mic}: {e}")
                return None
        return cls._cache[mic]

    @classmethod
    def is_open_now(cls, mic: str) -> bool:
        """Check if the market for the given MIC is currently open.
        
        Used primarily by Live Data providers to determine if a connection
        should be established.

        Args:
            mic (str): The Market Identifier Code (ISO 10383).

        Returns:
            bool: True if the market is open, False otherwise. Defaults to True if MIC is unknown.
        """
        cal = cls._get_calendar(mic)
        if cal is None:
            return True
        
        # Get current time in UTC as a pandas Timestamp
        now = pd.Timestamp.now(tz=timezone.utc)
        
        # exchange_calendars is_open_on_minute checks if the market is open at a specific minute
        return cal.is_open_on_minute(now)

    @classmethod
    def is_session(cls, mic: str, day: date) -> bool:
        """Check if the given date was a valid trading session for the MIC.
        
        Used primarily by Historical Data providers to determine if a data
        pull should be executed for a specific date.

        Args:
            mic (str): The Market Identifier Code (ISO 10383).
            day (date): The date to check.

        Returns:
            bool: True if the date was a session, False otherwise. Defaults to True if MIC is unknown.
        """
        cal = cls._get_calendar(mic)
        if cal is None:
            return True
            
        # Normalize to pandas Timestamp for library consistency
        return cal.is_session(pd.Timestamp(day))

    @classmethod
    def has_sessions_in_range(cls, mic: str, start: date, end: date) -> bool:
        """Check if there were any trading sessions between two dates.

        Used primarily by Historical Data providers to determine if a "gap"
        in data contains any actual sessions worth pulling.

        Args:
            mic (str): The Market Identifier Code (ISO 10383).
            start (date): Start of the range (inclusive).
            end (date): End of the range (inclusive).

        Returns:
            bool: True if at least one session occurred, False otherwise.
                Defaults to True if MIC is unknown.
        """
        cal = cls._get_calendar(mic)
        if cal is None:
            return True

        # Normalize to pandas Timestamps for library consistency
        ts_start = pd.Timestamp(start)
        ts_end = pd.Timestamp(end)

        # exchange_calendars.sessions_in_range returns an index of all trading days
        sessions = cal.sessions_in_range(ts_start, ts_end)
        return len(sessions) > 0
