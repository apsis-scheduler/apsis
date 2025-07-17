import logging
import ora

from apsis.lib.calendar import get_calendar
from apsis.lib.json import check_schema
from apsis.lib.parse import parse_duration
from .base import Schedule, DaytimeSpec

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class DailyIntervalSchedule(Schedule):
    def __init__(self, tz, calendar, start, stop, interval, args, *, enabled=True):
        super().__init__(enabled=enabled)
        self.tz = ora.TimeZone(tz)
        self.calendar = calendar
        self.start = DaytimeSpec.ensure(start)
        self.stop = DaytimeSpec.ensure(stop)
        self.interval = parse_duration(interval)
        if not (0 < self.interval < 86400):
            raise ValueError(f"invalid interval: {self.interval}")
        self.args = {str(k): str(v) for k, v in args.items()}

    def __str__(self):
        res = (
            f"{self.calendar} every {self.interval} sec from {self.start} to {self.stop} {self.tz}"
        )
        if len(self.args) > 0:
            args = ", ".join(f"{k}={v}" for k, v in self.args.items())
            res = "(" + args + ") " + res
        return res

    def __call__(self, start: ora.Time):
        """
        Generates scheduled times starting not before `start`.
        """
        start = ora.Time(start)
        # Figure out which date to schedule from.  Make sure we account
        # for date and cal shifts in either the start or stop.
        date = min(
            self.start.get_start_date(start, self.tz, self.calendar),
            self.stop.get_start_date(start, self.tz, self.calendar),
        )

        # Loop over dates.
        while True:
            # Compute the start time for this date.
            try:
                date_start = self.start.to_local(date, self.tz, self.calendar)
            except ora.NonexistentDateDaytime:
                # Landed on a DST transition.
                # FIXME: Use the time right before the transition.
                log.warning(f"skipping {date}: nonexistent start time {self.start} {self.tz}")
                date = self.calendar.after(date + 1)
                continue

            # Compute the stop time for this date.
            try:
                date_stop = self.stop.to_local(date, self.tz, self.calendar)
            except ora.NonexistentDateDaytime:
                # Landed on a DST transition.
                # FIXME: Use the time right after the transition.
                log.warning(f"skipping {date}: nonexistent stop time {self.stop} {self.tz}")
                date = self.calendar.after(date + 1)
                continue

            # Generate times between them with the interval.
            time = date_start
            while time < date_stop:
                if start <= time:
                    sched_date, daytime = time @ self.tz
                    yield (
                        time,
                        {
                            "date": str(date),
                            "sched_date": str(sched_date),
                            "time": str(time),
                            "daytime": str(daytime),
                            "calendar": str(self.calendar),
                            "tz": str(self.tz),
                            **self.args,
                        },
                    )
                time += self.interval

            date = self.calendar.after(date + 1)

    def to_jso(self):
        return {
            **super().to_jso(),
            "tz": str(self.tz),
            "calendar": repr(self.calendar),
            "start": self.start.to_jso(),
            "stop": self.stop.to_jso(),
            "interval": self.interval,
            "args": self.args,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            kw_args = Schedule._from_jso(pop)
            tz = pop("tz", ora.TimeZone)
            calendar = get_calendar(pop("calendar", default="all"))
            start = DaytimeSpec.from_jso(pop("start"))
            stop = DaytimeSpec.from_jso(pop("stop"))
            interval = pop("interval", parse_duration)
            args = pop("args", default={})
        return cls(tz, calendar, start, stop, interval, args, **kw_args)
