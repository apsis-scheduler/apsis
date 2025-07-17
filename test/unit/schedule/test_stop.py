import ora

from apsis.schedule.stop import StopSchedule

# -------------------------------------------------------------------------------


def test_stop_duration_schedule():
    sched = StopSchedule.from_jso(
        {
            "type": "duration",
            "duration": "1h",
        }
    )
    assert sched == StopSchedule.from_jso(sched.to_jso())
    assert "3600" in repr(sched)
    assert "3600" in str(sched)
    jso = sched.to_jso()
    assert jso["duration"] == 3600
    schedule_time = ora.Time("2024-12-13T12:00:00Z")
    assert sched(schedule_time) == ora.Time("2024-12-13T13:00:00Z")


def test_stop_daytime_schedule():
    sched = StopSchedule.from_jso(
        {
            "type": "daytime",
            "daytime": "16:00:00",
            "tz": "UTC",
        }
    )
    assert sched == StopSchedule.from_jso(sched.to_jso())
    assert "UTC" in repr(sched)
    assert "16:00:00" in str(sched)
    jso = sched.to_jso()
    assert jso["daytime"] == "16:00:00"
    assert jso["tz"] == "UTC"
    schedule_time = ora.Time("2024-12-13T12:00:00Z")
    assert sched(schedule_time) == ora.Time("2024-12-13T16:00:00Z")

    sched = StopSchedule.from_jso(
        {
            "type": "daytime",
            "daytime": "16:00:00",
            "tz": "Asia/Tokyo",
        }
    )
    assert sched == StopSchedule.from_jso(sched.to_jso())
    assert "Asia/Tokyo" in repr(sched)
    jso = sched.to_jso()
    assert jso["tz"] == "Asia/Tokyo"
    # Next day in Tokyo.
    assert sched(schedule_time) == ora.Time("2024-12-14T07:00:00Z")
