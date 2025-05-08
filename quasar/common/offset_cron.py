from apscheduler.triggers.cron import CronTrigger
from datetime import timedelta

class OffsetCronTrigger(CronTrigger):
    """Subclassed CronTrigger that fires at a specified offset from the original trigger time."""
    
    def __init__(self, offset_seconds=0, **kwargs):
        self._sign = 1 if offset_seconds >= 0 else -1
        self.offset_seconds = abs(offset_seconds)
        super().__init__(**kwargs)
    
    def get_next_fire_time(self, previous_fire_time, now):
        """Get the next fire time adjusted by the offset."""
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
    def from_crontab(cls, expr, offset_seconds=0, timezone=None):
        """
        Create a :class:`~CronTrigger` from a standard crontab expression.

        See https://en.wikipedia.org/wiki/Cron for more information on the format accepted here.

        :param offset_seconds: seconds to offset the trigger time by (can be negative)
        :param expr: minute, hour, day of month, month, day of week
        :param datetime.tzinfo|str timezone: time zone to use for the date/time calculations (
            defaults to scheduler timezone)
        :return: a :class:`~CronTrigger` instance

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