import logging
import ora

from apsis.lib.calendar import get_calendar
from apsis.lib.json import check_schema, to_array
from .base import Schedule

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class DailySchedule(Schedule):
    TYPE_NAME = "daily"

    def __init__(
        self,
        tz,
        calendar,
        daytimes,
        args,
        *,
        enabled=True,
        date_shift=0,
        cal_shift=0,
    ):
        super().__init__(enabled=enabled)
        self.tz = ora.TimeZone(tz)
        self.calendar = calendar
        self.daytimes = tuple(sorted(ora.Daytime(t) for t in daytimes))
        self.args = {str(k): str(v) for k, v in args.items()}
        self.date_shift = int(date_shift)
        self.cal_shift = int(cal_shift)

    def __str__(self):
        daytimes = ", ".join(format(y, "%C") for y in self.daytimes)
        res = f"{self.calendar} at {daytimes} {self.tz}"
        if self.cal_shift != 0:
            res += f" {self.cal_shift:+d} cal days"
        if self.date_shift != 0:
            res += f" {self.date_shift:+d} days"
        if len(self.args) > 0:
            args = ", ".join(f"{k}={v}" for k, v in self.args.items())
            res = "(" + args + ") " + res
        return res

    def __call__(self, start: ora.Time):
        """
        Generates scheduled times starting not before `start`.
        """
        start = ora.Time(start)

        start_date, start_daytime = start @ self.tz
        start_date -= self.date_shift

        if start_date in self.calendar:
            date = self.calendar.shift(start_date, -self.cal_shift)
            # Find the next daytime.
            for i, daytime in enumerate(self.daytimes):
                if start_daytime <= daytime:
                    break
            else:
                # All daytimes have passed for this date.
                date = self.calendar.after(date + 1)
                i = 0
        else:
            # Start at the beginning of the next date.
            date = self.calendar.after(start_date)
            date = self.calendar.shift(date, -self.cal_shift)
            i = 0

        # Now generate.
        common_args = {
            "calendar": str(self.calendar),
            "tz": str(self.tz),
            **self.args,
        }
        while True:
            sched_date = self.calendar.shift(date, self.cal_shift)
            sched_date = sched_date + self.date_shift
            daytime = self.daytimes[i]
            try:
                time = (sched_date, daytime) @ self.tz
            except ora.NonexistentDateDaytime:
                # Landed in a DST transition.
                log.warning(f"skipping nonexistent schedule time {sched_date} {daytime} {self.tz}")
            else:
                assert time >= start
                args = {
                    "date": str(date),
                    "time": time,
                    "daytime": str(daytime),
                    **common_args,
                }
                yield time, args

            i += 1
            if i == len(self.daytimes):
                # On to the next day.
                date = self.calendar.after(date + 1)
                i = 0

    def to_jso(self):
        return {
            **super().to_jso(),
            "tz": str(self.tz),
            "calendar": self.calendar.name,  # FIXME: Not necessarily round-trip.
            "daytime": [str(y) for y in self.daytimes],
            "date_shift": self.date_shift,
            "cal_shift": self.cal_shift,
            "args": self.args,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            kw_args = Schedule._from_jso(pop)
            args = pop("args", default={})
            tz = pop("tz", ora.TimeZone)
            calendar = get_calendar(pop("calendar", default="all"))
            daytimes = to_array(pop("daytime"))
            daytimes = [ora.Daytime(d) for d in daytimes]
            date_shift = pop("date_shift", int, default=0)
            cal_shift = pop("cal_shift", int, default=0)
        return cls(
            tz,
            calendar,
            daytimes,
            args,
            date_shift=date_shift,
            cal_shift=cal_shift,
            **kw_args,
        )
