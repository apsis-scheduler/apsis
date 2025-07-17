import ora

from apsis.lib.json import TypedJso, check_schema
from apsis.lib.parse import parse_duration
from apsis.lib.py import format_ctor

# -------------------------------------------------------------------------------


class StopSchedule(TypedJso):
    TYPE_NAMES = TypedJso.TypeNames()

    def __call__(self, schedule_time) -> ora.Time:
        raise NotImplementedError("StopSchedule.__call__")


class DurationStopSchedule(StopSchedule):
    def __init__(self, duration):
        try:
            duration = float(duration)
        except (TypeError, ValueError):
            duration = parse_duration(duration)
        self.duration = duration

    def __eq__(self, other):
        return other.duration == self.duration

    def __repr__(self):
        return format_ctor(self, self.duration)

    def __str__(self):
        return f"stop {self.duration} s after schedule time"

    def to_jso(self):
        return {
            **super().to_jso(),
            "duration": self.duration,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            duration = pop("duration")
        return cls(duration)

    def __call__(self, schedule_time):
        return schedule_time + self.duration


class DaytimeStopSchedule(StopSchedule):
    """
    Schedules to stop a run on the next occurrence of a daytime.
    """

    # FIXME: Add date_shift and cal_shift, as DailySchedule.

    def __init__(self, daytime, tz):
        """
        Schedules to stop the run on the next occurrence of `daytime` in `tz`
        after the schedule time.
        """
        self.daytime = ora.Daytime(daytime)
        self.tz = ora.TimeZone(tz)

    def __eq__(self, other):
        return other.daytime == self.daytime and other.tz == self.tz

    def __repr__(self):
        return format_ctor(self, self.daytime, self.tz)

    def __str__(self):
        return f"stop at {self.daytime} {self.tz} after schedule time"

    def to_jso(self):
        return {
            **super().to_jso(),
            "daytime": str(self.daytime),
            "tz": str(self.tz),
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            daytime = pop("daytime", ora.Daytime)
            tz = pop("tz", ora.TimeZone)
        return cls(daytime, tz)

    def __call__(self, schedule_time):
        # FIXME: Handle invalid date/daytime pairs.
        date, daytime = schedule_time @ self.tz
        return (date if daytime < self.daytime else date + 1, self.daytime) @ self.tz


StopSchedule.TYPE_NAMES.set(DurationStopSchedule, "duration")
StopSchedule.TYPE_NAMES.set(DaytimeStopSchedule, "daytime")
