import bisect
import ora

from apsis.lib.json import check_schema
from .base import Schedule

# -------------------------------------------------------------------------------


class ExplicitSchedule(Schedule):
    def __init__(self, times, args={}, *, enabled=True):
        super().__init__(enabled=enabled)
        self.times = tuple(sorted(ora.Time(t) for t in times))
        self.args = args

    def __str__(self):
        res = ",".join(str(t) for t in self.times)
        if len(self.args) > 0:
            args = ", ".join(f"{k}={v}" for k, v in self.args.items())
            res = "(" + args + ") " + res
        return res

    def __call__(self, start: ora.Time):
        i = bisect.bisect_left(self.times, start)
        return ((t, {"time": t, **self.args}) for t in self.times[i:])

    def to_jso(self):
        return {
            **super().to_jso(),
            "times": [str(t) for t in self.times],
            "args": self.args,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            kw_args = Schedule._from_jso(pop)
            times = pop("times")
            times = times if isinstance(times, list) else [times]
            times = [ora.Time(t) for t in times]
            args = pop("args", default={})
        return cls(times, args, **kw_args)
