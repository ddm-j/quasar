"""Cron trigger variant that applies a positive or negative offset."""

from apscheduler.triggers.cron import CronTrigger
from datetime import timedelta, tzinfo
from typing import Any


class OffsetCronTrigger(CronTrigger):
    """CronTrigger that fires at a specified offset from the base schedule."""
    
    def __init__(self, offset_seconds: int = 0, **kwargs: Any):
        """Initialize the trigger with an offset.

        Args:
            offset_seconds (int): Seconds to shift the scheduled time. Negative
                values schedule before the base trigger.
            **kwargs: Standard ``CronTrigger`` keyword arguments.
        """
        self._sign = 1 if offset_seconds >= 0 else -1
        self.offset_seconds = abs(offset_seconds)
        super().__init__(**kwargs)
    
    def get_next_fire_time(self, previous_fire_time, now):
        """Return the next fire time adjusted by the configured offset."""
        # If we have a previous fire time, artificially advance it by 1 microsecond
        # beyond what it would normally be without our offset
        
        # If we want a negative offset, we need to trick the scheduler
        if self._sign < 0:
            if previous_fire_time:
                previous_fire_time = previous_fire_time + timedelta(seconds=self.offset_seconds)
            now = now + timedelta(seconds=self.offset_seconds)

        # Calculate the Original Fire Time
        og_fire_time = super().get_next_fire_time(previous_fire_time, now)

        # Offset the Original Fire Time by the specified seconds
        if og_fire_time:
            if self._sign < 0:
                next_fire_time = og_fire_time - timedelta(seconds=self.offset_seconds)
            else:
                next_fire_time = og_fire_time + timedelta(seconds=self.offset_seconds)
            return next_fire_time
        
        return None

    @classmethod
    def from_crontab(cls, expr: str, offset_seconds: int = 0, timezone: tzinfo | str | None = None) -> "OffsetCronTrigger":
        """Create an ``OffsetCronTrigger`` from a crontab expression.

        Args:
            expr (str): Standard crontab fields ``minute hour day month day_of_week``.
            offset_seconds (int): Seconds to offset the trigger time; can be negative.
            timezone: Time zone for calculations; defaults to scheduler timezone.

        Returns:
            OffsetCronTrigger: Configured trigger instance.

        Raises:
            ValueError: If the crontab expression does not contain 5 fields.
        """
        values = expr.split()
        if len(values) != 5:
            raise ValueError(f"Wrong number of fields; got {len(values)}, expected 5")

        return cls(
            offset_seconds=offset_seconds,
            minute=values[0],
            hour=values[1],
            day=values[2],
            month=values[3],
            day_of_week=values[4],
            timezone=timezone,
        )