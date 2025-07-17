import ora

from apsis.lib.json import check_schema
from apsis.lib.parse import parse_duration
from apsis.lib.py import format_ctor
from .base import Schedule

# -------------------------------------------------------------------------------


class IntervalSchedule(Schedule):
    """
    Schedules at a fixed physical time interval.

    Generates time of the form,

        EPOCH + phase + n * interval

    Produces these additional args:

    - `time`: the schedule time
    - `date`: the UTC date of the schedule time
    - `daytime`: the UTC daytime of the schedule time

    """

    def __init__(self, interval, args, *, enabled=True, phase=0.0):
        super().__init__(enabled=enabled)
        self.interval = parse_duration(interval)
        self.phase = parse_duration(phase)
        self.args = {str(k): str(v) for k, v in args.items()}

        assert 0 <= self.phase < self.interval

    def __repr__(self):
        return format_ctor(self, self.interval, self.args)

    def __str__(self):
        res = f"every {self.interval} sec"
        if len(self.args) > 0:
            args = ", ".join(f"{k}={v}" for k, v in self.args.items())
            res = "(" + args + ") " + res
        return res

    def __call__(self, start: ora.Time):
        # Round to the next interval.
        start -= self.phase
        off = start - ora.Time.EPOCH
        time = (
            start
            if off % self.interval == 0
            else ora.Time.EPOCH + (off // self.interval + 1) * self.interval
        ) + self.phase

        while True:
            date, daytime = time @ ora.UTC
            yield (
                time,
                {
                    "time": str(time),
                    "date": str(date),
                    "daytime": str(daytime),
                    **self.args,
                },
            )
            time += self.interval

    def to_jso(self):
        return {
            **super().to_jso(),
            "interval": self.interval,
            "phase": self.phase,
            "args": self.args,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            kw_args = Schedule._from_jso(pop)
            interval = pop("interval", parse_duration)
            phase = pop("phase", parse_duration, 0)
            assert 0 <= phase < interval, "phase not between 0 and interval"
            args = pop("args", default={})
        return cls(interval, args, phase=phase, **kw_args)
