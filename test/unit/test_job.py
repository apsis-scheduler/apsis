import ora

from apsis.jobs import Job, Jobs, JobsDir
from apsis.schedule.stop import DurationStopSchedule

# -------------------------------------------------------------------------------


def test_schedule_jso_start():
    job_jso = {
        "program": {"type": "no-op"},
        "schedule": {
            "type": "daily-interval",
            "start": "09:30:00",
            "stop": "16:00:00",
            "tz": "America/New_York",
            "interval": "30m",
        },
    }
    job = Job.from_jso(job_jso, "test job")
    (sched,) = job.schedules
    assert sched.start.daytime == ora.Daytime(9, 30, 0)
    assert sched.stop.daytime == ora.Daytime(16, 0, 0)
    assert sched.interval == 1800
    assert sched.enabled
    assert sched.stop_schedule is None
    jso = job.to_jso()
    assert isinstance(jso["schedule"], list)
    assert len(jso["schedule"]) == 1
    assert jso["schedule"][0]["interval"] == 1800

    # This produces an idential job as the above.
    job_jso = {
        "program": {"type": "no-op"},
        "schedule": {
            "start": {
                "type": "daily-interval",
                "start": "09:30:00",
                "stop": "16:00:00",
                "tz": "America/New_York",
                "interval": "30m",
            },
        },
    }
    job = Job.from_jso(job_jso, "test job")
    (sched,) = job.schedules
    assert sched.interval == 1800
    assert sched.enabled
    assert sched.stop_schedule is None
    jso = job.to_jso()
    assert isinstance(jso["schedule"], list)
    assert len(jso["schedule"]) == 1
    assert jso["schedule"][0]["interval"] == 1800


def test_schedule_jso_start_stop():
    # Includes a stop schedule.
    job_jso = {
        "program": {"type": "no-op"},
        "schedule": {
            "start": {
                "type": "daily-interval",
                "start": "09:30:00",
                "stop": "16:00:00",
                "tz": "America/New_York",
                "interval": "30m",
            },
            "stop": {
                "type": "duration",
                "duration": "15m",
            },
        },
    }
    job = Job.from_jso(job_jso, "test job")
    (sched,) = job.schedules
    assert sched.interval == 1800
    assert sched.enabled
    assert sched.stop_schedule == DurationStopSchedule(900)
    jso = job.to_jso()
    (jso,) = jso["schedule"]  # one-element list
    assert set(jso.keys()) == {"start", "stop"}
    assert jso["start"]["interval"] == 1800
    assert jso["stop"]["duration"] == 900


def test_get_dependents():
    def job(job_id, dep_job_id, sched_args=(), cond_type="dependency"):
        return Job.from_jso(
            {
                "program": {"type": "no-op"},
                "schedule": [
                    {"type": "daily", "tz": "UTC", "daytime": "12:00:00", "args": a}
                    for a in sched_args
                ],
                "condition": {"type": cond_type, "job_id": dep_job_id},
            },
            job_id,
        )

    jobs = Jobs(
        JobsDir(
            None,
            {
                j.job_id: j
                for j in (
                    job("plain", "up/a"),
                    job("tmpl", "up/{{ region }}", [{"region": "a"}, {"region": "b"}]),
                    job("own", "{{ job_id }}/up", [{}]),
                    job("missing", "up/{{ date }}", [{"region": "a"}]),
                    job("max", "up/a", cond_type="max_running"),
                )
            },
        ),
        None,
    )
    assert jobs.get_dependents("up/a") == {"plain", "tmpl"}
    assert jobs.get_dependents("up/b") == {"tmpl"}
    assert jobs.get_dependents("own/up") == {"own"}
    assert jobs.get_dependents("plain") == frozenset()

    def jso(job_id: str, cond: dict, schedule: list[dict]) -> Job:
        jso = {"program": {"type": "no-op"}, "schedule": schedule, "condition": cond}
        return Job.from_jso(jso, job_id)

    def daily() -> list[dict]:
        return [{"type": "daily", "tz": "UTC", "daytime": "12:00:00", "args": {"region": "a"}}]

    explicit = [{"type": "explicit", "times": ["2026-10-30T12:00:00Z"], "args": {"region": 1}}]
    bad = "unused/{{ region + 1 }}"
    jobs = Jobs(
        JobsDir(
            None,
            {
                j.job_id: j
                for j in (
                    jso("off", {"type": "dependency", "job_id": bad, "enabled": False}, daily()),
                    jso(
                        "off_tmpl",
                        {"type": "dependency", "job_id": bad, "enabled": '{{ region != "a" }}'},
                        daily(),
                    ),
                    jso(
                        "on_tmpl",
                        {
                            "type": "dependency",
                            "job_id": "up/{{ region }}",
                            "enabled": '{{ region == "a" }}',
                        },
                        daily(),
                    ),
                    jso("num", {"type": "dependency", "job_id": '{{ "up/" + region }}'}, explicit),
                    jso("bad", {"type": "dependency", "job_id": bad}, daily()),
                    job("plain", "up/a"),
                )
            },
        ),
        None,
    )
    # disabled and failing dependencies are skipped and the rest are returned
    assert jobs.get_dependents("up/a") == {"on_tmpl", "plain"}
    assert jobs.get_dependents("up/1") == {"num"}
