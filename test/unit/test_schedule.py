import itertools
import ora
from   ora import Date, Time, Daytime, UTC, get_calendar
import pytest

from   apsis.lib import itr
from   apsis.schedule import DailySchedule, IntervalSchedule, DaytimeSpec
from   apsis.schedule.daily_interval import DailyIntervalSchedule

#-------------------------------------------------------------------------------

def test_daily_schedule_shift():
    z = "America/New_York"
    sched = DailySchedule(
        z,
        ora.get_calendar("Mon,Wed-Fri"),
        ["9:30:00", "16:00:00"],
        {"foo": "bar"},
        date_shift=-1,
    )

    nd = lambda t: next(sched(t))

    # Fri 00:00 -> Sun 09:30 for Mon.
    st, a = nd(Time(2019, 1, 11,  0, 0, 0, z))
    assert st == Time(2019, 1, 13, 9, 30, 0, z)
    assert a["date"] == "2019-01-14"
    assert a["foo"] == "bar"

    # Sat 05:00 -> Sun 09:30 for Mon.
    st, a = nd(Time(2019, 1, 12,  5, 0, 0, z))
    assert st == Time(2019, 1, 13, 9, 30, 0, z)
    assert a["date"] == "2019-01-14"
    assert a["foo"] == "bar"

    # Sun 09:15 -> Sun 09:30 for Mon.
    st, a = nd(Time(2019, 1, 13, 9, 15, 0, z))
    assert st == Time(2019, 1, 13, 9, 30, 0, z)
    assert a["date"] == "2019-01-14"

    # Sun 09:45 -> Sun 16:00 for Mon.
    st, a = nd(Time(2019, 1, 13, 9, 45, 0, z))
    assert st == Time(2019, 1, 13, 16, 0, 0, z)
    assert a["date"] == "2019-01-14"

    # Sun 15:55 -> Sun 16:00 for Mon.
    st, a = nd(Time(2019, 1, 13, 15, 55, 0, z))
    assert st == Time(2019, 1, 13, 16, 0, 0, z)
    assert a["date"] == "2019-01-14"

    # Sun 16:02 -> Tue 09:30 for Wed.
    st, a = nd(Time(2019, 1, 13, 16, 2, 0, z))
    assert st == Time(2019, 1, 15, 9, 30, 0, z)
    assert a["date"] == "2019-01-16"

    # Mon 12:00 -> Tue 09:30 for Wed.
    st, a = nd(Time(2019, 1, 14, 0, 0, 0, z))
    assert st == Time(2019, 1, 15, 9, 30, 0, z)
    assert a["date"] == "2019-01-16"

    # Tue 09:40 -> Tue 16:00 for Wed.
    st, a = nd(Time(2019, 1, 15, 9, 40, 0, z))
    assert st == Time(2019, 1, 15, 16, 0, 0, z)
    assert a["date"] == "2019-01-16"

    # Tue 17:00 -> Wed 09:30 for Thu.
    st, a = nd(Time(2019, 1, 15, 17, 0, 0, z))
    assert st == Time(2019, 1, 16, 9, 30, 0, z)
    assert a["date"] == "2019-01-17"

    # Thu 20:00 -> Sun 09:30 for Mon.
    st, a = nd(Time(2019, 1, 17, 20, 0, 0, z))
    assert st == Time(2019, 1, 20, 9, 30, 0, z)
    assert a["date"] == "2019-01-21"


@pytest.mark.parametrize("date_shift", [-5, -2, -1, 0, 1, 2, 5])
@pytest.mark.parametrize("cal_shift", [-5, -2, -1, 0, 1, 2, 5])
@pytest.mark.parametrize("start_y", ["00:00:00", "02:00:00", "10:00:00", "15:00:00", "22:15:00"])
def test_daily_schedule_cal_shift(date_shift, cal_shift, start_y):
    z = ora.TimeZone("America/New_York")
    c = ora.get_calendar("Mon,Wed-Fri")
    start = ("2019-03-11", start_y) @ z
    sched_y = ["2:30:00", "12:00:00", "22:00:00"]
    args = {"foo": "bar"}
    n = 20

    def shift_date(t, shift):
        d, y = t @ z
        return (d + shift, y) @ z

    def shift_cal(t, shift):
        d, y = t @ z
        return (c.shift(d, shift), y) @ z

    sched0 = DailySchedule(z, c, sched_y, args)
    # Generate a long piece of schedule.
    times0 = sched0(shift_date(start, -20))
    times0 = [
        t
        for t, _ in itertools.islice(times0, n + 300)
    ]

    sched1 = DailySchedule(
        z, c, sched_y, args,
        date_shift=date_shift, cal_shift=cal_shift,
    )
    times1 = [ t for t, _ in itertools.islice(sched1(start), n) ]

    def shift(t):
        try:
            return shift_date(shift_cal(t, cal_shift), date_shift)
        except ora.NonexistentDateDaytime:
            return None

    expected = [ shift(t) for t in times0 ]
    expected = [ t for t in expected if t is not None and t >= start ][: n]
    assert times1 == expected


def test_interval_schedule_phase():
    # Every hour on the 15 min.
    sched = IntervalSchedule(3600, {}, phase=900)

    st, _ = next(sched(Time(2019, 11, 13, 7, 5, 0, UTC)))
    assert st == Time(2019, 11, 13, 7, 15, 0, UTC)

    st, _ = next(sched(Time(2019, 11, 13, 7, 14, 59, UTC)))
    assert st == Time(2019, 11, 13, 7, 15, 0, UTC)

    st, _ = next(sched(Time(2019, 11, 13, 7, 15, 0, UTC)))
    assert st == Time(2019, 11, 13, 7, 15, 0, UTC)

    st, _ = next(sched(Time(2019, 11, 13, 7, 25, 0, UTC)))
    assert st == Time(2019, 11, 13, 8, 15, 0, UTC)

    st, _ = next(sched(Time(2019, 11, 13, 7, 55, 0, UTC)))
    assert st == Time(2019, 11, 13, 8, 15, 0, UTC)


def test_interval_schedule_phase_repeat():
    args = {"foo": "42"}
    date = Date(2019, 11, 13)

    # Every 10 min with phase of 2 min.
    sched = IntervalSchedule(600, args, phase=120)
    start = (date, Daytime(7, 33)) @ UTC
    times = iter(sched(start))
    for y in [
            Daytime(7, 42),
            Daytime(7, 52),
            Daytime(8,  2),
            Daytime(8, 12),
    ]:
        time = (date, y) @ UTC
        assert next(times) == (time, {
            **args,
            "time": str(time),
        })


def test_daily_schedule_eq():
    z1 = ora.TimeZone("America/New_York")

    c0 = ora.get_calendar("Mon-Fri")
    c1 = ora.get_calendar("all")

    a0 = dict(veg="tomato")
    a1 = dict(veg="eggplant")

    s0 = DailySchedule(ora.UTC, c0, ["9:30:00"], a0)
    s1 = DailySchedule(ora.UTC, c0, ["9:30:00"], a0)
    assert s0 == s1

    s2 = DailySchedule(z1, c0, ["9:30:00"], a0)
    assert s0 != s2

    s3 = DailySchedule(ora.UTC, c1, ["9:30:00"], a0)
    assert s0 != s3

    s4 = DailySchedule(ora.UTC, c0, ["9:45:00"], a0)
    assert s0 != s4

    s5 = DailySchedule(ora.UTC, c0, ["9:30:00", "16:00:00"], a0)
    assert s0 != s5

    s6 = DailySchedule(ora.UTC, c0, ["9:30:00"], a1)
    assert s0 != s6

    s7 = DailySchedule(ora.UTC, c0, ["9:30:00"], a1, date_shift=-1)
    assert s0 != s7

    s8 = DailySchedule(ora.UTC, c0, ["9:30:00"], a1, enabled=False)
    assert s0 != s8


def test_interval_schedule_eq():
    s0 = IntervalSchedule(3600, {"foo": 42})
    s1 = IntervalSchedule(3600, {"foo": 42})
    assert s0 == s1

    s2 = IntervalSchedule(1800, {"foo": 42})
    assert s0 != s2

    s3 = IntervalSchedule(3600, {"foo": 17})
    assert s0 != s3

    s4 = IntervalSchedule(3600, {"foo": 42}, enabled=False)
    assert s0 != s4

    s5 = IntervalSchedule(3600, {"foo": 42}, phase=600)
    assert s0 != s5


def test_daily_interval():
    sched = DailyIntervalSchedule(
        "America/New_York",
        ora.get_calendar("Mon-Fri"),
        "12:00:00", "16:00:00",
        2700,
        {"name": "foo"},
    )
    res = list(itertools.islice(sched("2022-05-20T13:00:00-04:00"), 50))

    cal = get_calendar("Mon-Fri")
    for t, _ in res:
        d, y = t @ "America/New_York"
        assert d in cal
        assert Daytime(12, 0, 0) <= y < Daytime(16, 0, 0)

    for (t0, a0), (t1, a1) in itr.pairwise(res):
        assert next(sched(t0 -    1))[0] == t0
        assert next(sched(t0 +    1))[0] == t1
        assert next(sched(t0 + 2699))[0] == t1
        assert next(sched(t0 + 2700))[0] == t1
        assert next(sched(t1 -    1))[0] == t1


def test_daily_interval_wrap():
    sched = DailyIntervalSchedule(
        "America/New_York",
        ora.get_calendar("Mon-Fri"),
        "22:00:00", "23:59:59",
        300,
        {}
    )

    times = sched("2022-06-10T23:45:00-04:00")
    assert next(times)[0] == ora.Time("2022-06-10T23:45:00-04:00")
    assert next(times)[0] == ora.Time("2022-06-10T23:50:00-04:00")
    assert next(times)[0] == ora.Time("2022-06-10T23:55:00-04:00")
    assert next(times)[0] == ora.Time("2022-06-13T22:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-06-13T22:05:00-04:00")


def test_daily_interval_date_shift():
    sched = DailyIntervalSchedule(
        "America/New_York",
        ora.get_calendar("Mon-Fri"),
        # Every 2 hours from 9 PM until 4 AM the next day.
        "21:00:00",
        DaytimeSpec(daytime="05:00:00", date_shift=1),
        2 * 3600,
        {},
    )
    times = sched("2022-11-03T22:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-03T23:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-04T01:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-04T03:00:00-04:00")

    assert next(times)[0] == ora.Time("2022-11-04T21:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-04T23:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-05T01:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-05T03:00:00-04:00")

    # DST ends.
    assert next(times)[0] == ora.Time("2022-11-07T21:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-07T23:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-08T01:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-08T03:00:00-05:00")


def test_daily_interval_cal_shift():
    sched = DailyIntervalSchedule(
        "America/New_York",
        ora.get_calendar("Mon-Fri"),
        # Every 6 hours from 10 PM the previous cal date until noon.
        DaytimeSpec(daytime="22:00:00", cal_shift=-1),
        "12:00:00",
        6 * 3600,
        {},
    )
    times = sched("2022-11-03T02:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-03T04:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-03T10:00:00-04:00")

    assert next(times)[0] == ora.Time("2022-11-03T22:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-04T04:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-04T10:00:00-04:00")

    assert next(times)[0] == ora.Time("2022-11-04T22:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-05T04:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-05T10:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-05T16:00:00-04:00")
    assert next(times)[0] == ora.Time("2022-11-05T22:00:00-04:00")
    # DST ends.
    assert next(times)[0] == ora.Time("2022-11-06T03:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-06T09:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-06T15:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-06T21:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-07T03:00:00-05:00")
    assert next(times)[0] == ora.Time("2022-11-07T09:00:00-05:00")


def test_daily_interval_cal_shift_start():
    sched = DailyIntervalSchedule(
        "America/New_York",
        ora.get_calendar("Mon-Fri"),
        # Noon, 2 PM, and 4 PM the following calendar day.
        DaytimeSpec(daytime="12:00:00", cal_shift=1),
        DaytimeSpec(daytime="18:00:00", cal_shift=1),
        2 * 3600,
        {},
    )
    times = sched("2022-11-04T13:00:00-04:00")

    s = next(times)
    assert s[0] == ora.Time("2022-11-04T14:00:00-04:00")
    assert s[1]["date"] == "2022-11-03"
    s = next(times)
    assert s[0] == ora.Time("2022-11-04T16:00:00-04:00")
    assert s[1]["date"] == "2022-11-03"
    s = next(times)
    # DST ends.
    assert s[0] == ora.Time("2022-11-07T12:00:00-05:00")
    assert s[1]["date"] == "2022-11-04"


