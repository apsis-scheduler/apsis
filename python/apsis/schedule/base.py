from dataclasses import dataclass
import ora

from apsis.lib.json import TypedJso, check_schema, nkey
from .stop import StopSchedule

# -------------------------------------------------------------------------------


@dataclass(kw_only=True, order=True)
class DaytimeSpec:
    """
    The specification of a daytime in a schedule.

    The date and time zone are not encapsulated; these are elsewhere in the
    schedule.
    """

    # Order of attributes is important for dataclass(order=True).
    cal_shift: int = 0
    date_shift: int = 0
    daytime: ora.Daytime

    def __str__(self):
        return (
            format(self.daytime, "%C")
            + ("" if self.cal_shift == 0 else f" {self.cal_shift:+d} cal days")
            + ("" if self.date_shift == 0 else f" {self.date_shift:+d} days")
        )

    @classmethod
    def ensure(cls, obj):
        if isinstance(obj, cls):
            return obj

        try:
            daytime = ora.Daytime(obj)
        except (TypeError, ValueError):
            pass
        else:
            return cls(daytime=daytime)

        raise TypeError(f"not a {cls.__name__}: {obj!r}")

    def get_start_date(self, time, tz, cal):
        date, _ = time @ tz
        date -= self.date_shift
        if date not in cal:
            date = cal.after(date)
        date = cal.shift(date, -self.cal_shift)
        return date

    def to_local(self, date, tz, cal):
        """
        Combines this daytime spec with `date` to produce a time.

        :raise ora.NonexistentDateDaytime:
          The localized time doesn't exist, because it lands in a DST
          transition.
        """
        date = cal.shift(date, self.cal_shift) + self.date_shift
        return (date, self.daytime) @ tz

    def to_jso(self):
        if self.date_shift == 0 and self.cal_shift == 0:
            # Simple form: just a daytime.
            return str(self.daytime)
        else:
            return {
                "daytime": str(self.daytime),
                "date_shift": self.date_shift,
                "cal_shift": self.cal_shift,
            }

    @classmethod
    def from_jso(cls, jso):
        if isinstance(jso, dict):
            with check_schema(jso) as pop:
                daytime = pop("daytime", ora.Daytime)
                date_shift = pop("date_shift", int, default=0)
                cal_shift = pop("cal_shift", int, default=0)
                return cls(daytime=daytime, date_shift=date_shift, cal_shift=cal_shift)

        else:
            # Simple form: just a daytime.
            daytime = ora.Daytime(jso)
            return cls(daytime=daytime)


# -------------------------------------------------------------------------------


class Schedule(TypedJso):
    # Note: `stop_schedule` is not included in the JSO representation.

    TYPE_NAMES = TypedJso.TypeNames()

    def __init__(self, *, enabled=True):
        self.enabled = bool(enabled)
        self.stop_schedule = None

    def to_jso(self):
        return super().to_jso() | nkey("enabled", self.enabled)

    @classmethod
    def _from_jso(cls, pop):
        return dict(enabled=pop("enabled", bool, default=True))

    def __call__(self, start: ora.Time):
        raise NotImplementedError("Schedule.__call__")


def schedule_to_jso(schedule):
    jso = schedule.to_jso()
    return (
        jso
        if schedule.stop_schedule is None
        else {
            "start": jso,
            "stop": schedule.stop_schedule.to_jso(),
        }
    )


def schedule_from_jso(jso):
    if set(jso) in ({"start"}, {"start", "stop"}):
        # Explicit start schedule, and possibly a stop schedule.
        schedule = Schedule.from_jso(jso["start"])
        stop_jso = jso.get("stop", None)
        if stop_jso is not None:
            schedule.stop_schedule = StopSchedule.from_jso(stop_jso)
        return schedule
    else:
        # Only a start schedule.
        return Schedule.from_jso(jso)
