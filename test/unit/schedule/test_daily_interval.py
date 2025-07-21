import itertools
import ora
from ora import Daytime, get_calendar
import yaml

from apsis.lib import itr
from apsis.schedule import Schedule, DailyIntervalSchedule, DaytimeSpec

# -------------------------------------------------------------------------------


def test_daily_interval():
    sched = DailyIntervalSchedule(
        "America/New_York",
        ora.get_calendar("Mon-Fri"),
        "12:00:00",
        "16:00:00",
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
        assert next(sched(t0 - 1))[0] == t0
        assert next(sched(t0 + 1))[0] == t1
        assert next(sched(t0 + 2699))[0] == t1
        assert next(sched(t0 + 2700))[0] == t1
        assert next(sched(t1 - 1))[0] == t1


def test_daily_interval_wrap():
    sched = DailyIntervalSchedule(
        "America/New_York", ora.get_calendar("Mon-Fri"), "22:00:00", "23:59:59", 300, {}
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


JSON_COMPAT = """
schedule:
  type: daily-interval
  calendar: Mon-Fri
  tz: America/New_York
  start: 22:00:00
  stop: "23:59:59"
  interval: 300
"""


def test_from_json_compat():
    """
    Tests loading JSON without the `DaytimeSpec` syntax, i.e. bare start and stop times.
    """
    jso = yaml.load(JSON_COMPAT, Loader=yaml.SafeLoader)
    sched = Schedule.from_jso(jso["schedule"])
    assert isinstance(sched, DailyIntervalSchedule)
    assert "2022-12-11" not in sched.calendar
    assert "2022-12-12" in sched.calendar
    assert sched.start.daytime == Daytime(22, 0, 0)
    assert sched.start.date_shift == sched.start.cal_shift == 0
    assert sched.stop.daytime == Daytime(23, 59, 59)
    assert sched.stop.date_shift == sched.stop.cal_shift == 0
    assert sched.interval == 300


def test_nonexistent():
    tz = ora.TimeZone("America/New_York")
    sched = DailyIntervalSchedule(
        tz,
        ora.get_calendar("all"),
        DaytimeSpec(daytime=ora.Daytime("01:15:00")),
        DaytimeSpec(daytime=ora.Daytime("03:30:00")),
        1800,
        {},
    )

    start = ("2023-03-11", "00:00:00") @ tz
    print(f"start: {start}")
    times = sched(start)

    def check(time):
        time = ora.Time(time)
        next_time, _ = next(times)
        assert next_time == time

    # 2023-03-11 is a normal date.
    check("2023-03-11T01:15:00-05:00")
    check("2023-03-11T01:45:00-05:00")
    check("2023-03-11T02:15:00-05:00")
    check("2023-03-11T02:45:00-05:00")
    check("2023-03-11T03:15:00-05:00")
    # DST starts on 2023-03-12 at 02:00:00 → 03:00:00.
    check("2023-03-12T01:15:00-05:00")
    check("2023-03-12T01:45:00-05:00")
    check("2023-03-12T03:15:00-04:00")
    # 2023-03-11 is a normal date.
    check("2023-03-13T01:15:00-04:00")
    check("2023-03-13T01:45:00-04:00")
    check("2023-03-13T02:15:00-04:00")
    check("2023-03-13T02:45:00-04:00")
    check("2023-03-13T03:15:00-04:00")


def test_nonexistent_start():
    tz = ora.TimeZone("America/New_York")
    sched = DailyIntervalSchedule(
        tz,
        ora.get_calendar("all"),
        DaytimeSpec(daytime=ora.Daytime("02:15:00")),
        DaytimeSpec(daytime=ora.Daytime("05:00:00")),
        1800,
        {},
    )

    start = ("2023-03-11", "00:00:00") @ tz
    print(f"start: {start}")
    times = sched(start)

    def check(time):
        time = ora.Time(time)
        next_time, _ = next(times)
        assert next_time == time

    # 2023-03-11 is a normal date.
    check("2023-03-11T02:15:00-05:00")
    check("2023-03-11T02:45:00-05:00")
    check("2023-03-11T03:15:00-05:00")
    check("2023-03-11T03:45:00-05:00")
    check("2023-03-11T04:15:00-05:00")
    check("2023-03-11T04:45:00-05:00")
    # DST starts on 2023-03-12 at 02:00:00.
    # FIXME: We skip this date.  Maybe we can do better, but not clear.
    # 2023-03-11 is a normal date.
    check("2023-03-13T02:15:00-04:00")
    check("2023-03-13T02:45:00-04:00")
    check("2023-03-13T03:15:00-04:00")
    check("2023-03-13T03:45:00-04:00")
    check("2023-03-13T04:15:00-04:00")
    check("2023-03-13T04:45:00-04:00")
