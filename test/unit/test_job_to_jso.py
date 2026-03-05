"""Tests for job_to_jso() condition grouping and resolution."""

from apsis.cond.dependency import Dependency
from apsis.cond.max_running import MaxRunning
from apsis.jobs import Job, JobsDir
from apsis.lib.api import job_to_jso
from apsis.schedule.interval import IntervalSchedule


def _jobs(*jobs):
    """Create a JobsDir from a list of Job objects."""
    return JobsDir(None, {j.job_id: j for j in jobs})


def _sched(args, *, enabled=True):
    """Create a minimal IntervalSchedule with the given args."""
    return IntervalSchedule(3600, args, enabled=enabled)


# -------------------------------------------------------------------------------
# No schedules


def test_no_schedules_literal_dep():
    """Job with no schedules and a literal dependency."""
    dep = Dependency("target_job", {"date": "2024-01-01"})
    job = Job("myjob", {"date"}, [], conds=[dep])
    jso = job_to_jso(job)

    # No schedules → nothing varies → dep is common.
    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["resolved_job_id"] == "target_job"
    assert jso["resolved_conditions"] == []
    # Raw definitions are always present.
    assert len(jso["condition"]) == 1
    assert jso["condition"][0]["job_id"] == "target_job"


def test_no_schedules_parametrized_dep():
    """Job with no schedules and a parametrized dependency."""
    dep = Dependency("dep/{{ region }}", {"date": "{{ date }}"})
    job = Job("myjob", {"region", "date"}, [], conds=[dep])
    jso = job_to_jso(job)

    # No schedules → no sched_arg_sets → no resolved groups.
    assert jso["common_conditions"] == []
    assert jso["resolved_conditions"] == []


def test_no_schedules_non_dep_cond():
    """Non-dependency conditions (MaxRunning) with no schedules."""
    cond = MaxRunning("1")
    job = Job("myjob", set(), [], conds=[cond])
    jso = job_to_jso(job)

    # No schedules → nothing varies → MaxRunning is common.
    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["type"] == "max_running"
    assert jso["resolved_conditions"] == []
    assert len(jso["condition"]) == 1


# -------------------------------------------------------------------------------
# Single schedule


def test_single_schedule_literal_dep():
    """Single schedule, literal dependency → common, no variable."""
    target = Job("target_job", {"date"}, [])
    dep = Dependency("target_job", {"date": "{{ date }}"})
    sched = _sched({"date": "2024-01-01"})
    job = Job("myjob", {"date"}, [sched], conds=[dep])
    jobs = _jobs(job, target)

    jso = job_to_jso(job, jobs=jobs)

    # With one schedule, _varies_across always returns False, so the dep is common.
    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["resolved_job_id"] == "target_job"
    assert jso["common_conditions"][0]["resolved_args"] == {"date": "2024-01-01"}

    # One resolved group, but empty (no variable deps).
    assert len(jso["resolved_conditions"]) == 1
    assert jso["resolved_conditions"][0]["conditions"] == []


def test_single_schedule_parametrized_dep():
    """Single schedule, parametrized job_id → common (resolves identically)."""
    target = Job("dep/us", {"date"}, [])
    dep = Dependency("dep/{{ region }}", {"date": "{{ date }}"})
    sched = _sched({"region": "us", "date": "2024-01-01"})
    job = Job("myjob", {"region", "date"}, [sched], conds=[dep])
    jobs = _jobs(job, target)

    jso = job_to_jso(job, jobs=jobs)

    # Single schedule → resolved entry is the same for all (one) arg sets → common.
    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["resolved_job_id"] == "dep/us"
    assert jso["common_conditions"][0]["resolved_args"] == {"date": "2024-01-01"}
    assert len(jso["resolved_conditions"]) == 1
    assert jso["resolved_conditions"][0]["conditions"] == []


def test_single_schedule_mixed_conds():
    """Single schedule with a dep and a MaxRunning → both common."""
    target = Job("target_job", set(), [])
    dep = Dependency("target_job", {})
    max_run = MaxRunning("1")
    sched = _sched({})
    job = Job("myjob", set(), [sched], conds=[dep, max_run])
    jobs = _jobs(job, target)

    jso = job_to_jso(job, jobs=jobs)

    # Both are common (single schedule → nothing varies).
    assert len(jso["common_conditions"]) == 2
    assert jso["common_conditions"][0]["type"] == "dependency"
    assert jso["common_conditions"][1]["type"] == "max_running"
    assert jso["resolved_conditions"][0]["conditions"] == []


# -------------------------------------------------------------------------------
# Multiple schedules


def test_multiple_schedules_parametrized_dep():
    """Multiple schedules, parametrized job_id → variable, resolved per group."""
    dep_us = Job("dep/us", {"date"}, [])
    dep_eu = Job("dep/eu", {"date"}, [])
    dep = Dependency("dep/{{ region }}", {"date": "{{ date }}"})
    sched_us = _sched({"region": "us", "date": "2024-01-01"})
    sched_eu = _sched({"region": "eu", "date": "2024-01-01"})
    job = Job("myjob", {"region", "date"}, [sched_us, sched_eu], conds=[dep])
    jobs = _jobs(job, dep_us, dep_eu)

    jso = job_to_jso(job, jobs=jobs)

    assert jso["common_conditions"] == []
    assert len(jso["resolved_conditions"]) == 2

    us_group = jso["resolved_conditions"][0]
    assert us_group["schedule_args"]["region"] == "us"
    assert len(us_group["conditions"]) == 1
    assert us_group["conditions"][0]["resolved_job_id"] == "dep/us"

    eu_group = jso["resolved_conditions"][1]
    assert eu_group["schedule_args"]["region"] == "eu"
    assert len(eu_group["conditions"]) == 1
    assert eu_group["conditions"][0]["resolved_job_id"] == "dep/eu"


def test_multiple_schedules_common_dep():
    """Multiple schedules, literal dep with same args → common."""
    target = Job("shared_dep", {"date"}, [])
    dep = Dependency("shared_dep", {"date": "{{ date }}"})
    sched_us = _sched({"region": "us", "date": "2024-01-01"})
    sched_eu = _sched({"region": "eu", "date": "2024-01-01"})
    job = Job("myjob", {"region", "date"}, [sched_us, sched_eu], conds=[dep])
    jobs = _jobs(job, target)

    jso = job_to_jso(job, jobs=jobs)

    # date is the same in both schedules, so the dep resolves identically → common.
    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["resolved_job_id"] == "shared_dep"
    assert jso["common_conditions"][0]["resolved_args"] == {"date": "2024-01-01"}
    # Variable groups exist but are empty.
    assert all(g["conditions"] == [] for g in jso["resolved_conditions"])


def test_multiple_schedules_common_and_variable():
    """Multiple schedules with both a common dep and a variable dep."""
    shared = Job("shared", set(), [])
    dep_us = Job("dep/us", set(), [])
    dep_eu = Job("dep/eu", set(), [])
    common_dep = Dependency("shared", {})
    variable_dep = Dependency("dep/{{ region }}", {})
    sched_us = _sched({"region": "us"})
    sched_eu = _sched({"region": "eu"})
    job = Job(
        "myjob",
        {"region"},
        [sched_us, sched_eu],
        conds=[common_dep, variable_dep],
    )
    jobs = _jobs(job, shared, dep_us, dep_eu)

    jso = job_to_jso(job, jobs=jobs)

    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["resolved_job_id"] == "shared"

    assert len(jso["resolved_conditions"]) == 2
    assert jso["resolved_conditions"][0]["conditions"][0]["resolved_job_id"] == "dep/us"
    assert jso["resolved_conditions"][1]["conditions"][0]["resolved_job_id"] == "dep/eu"


# -------------------------------------------------------------------------------
# enabled field


def test_enabled_false_excluded_from_display():
    """enabled=False deps are excluded from common/variable, kept in definitions."""
    dep = Dependency("target_job", {}, enabled=False)
    sched = _sched({})
    job = Job("myjob", set(), [sched], conds=[dep])

    jso = job_to_jso(job)

    assert jso["common_conditions"] == []
    assert jso["resolved_conditions"][0]["conditions"] == []
    # Still in raw definitions.
    assert len(jso["condition"]) == 1


def test_enabled_template_varies_across_schedules():
    """enabled template that differs across schedules → variable."""
    target = Job("target_job", set(), [])
    dep = Dependency("target_job", {}, enabled="{{ strat == 'us' }}")
    sched_us = _sched({"strat": "us"})
    sched_eu = _sched({"strat": "eu"})
    job = Job("myjob", {"strat"}, [sched_us, sched_eu], conds=[dep])
    jobs = _jobs(job, target)

    jso = job_to_jso(job, jobs=jobs)

    # enabled differs across arg sets → variable.
    assert jso["common_conditions"] == []
    assert len(jso["resolved_conditions"]) == 2

    # US: enabled=True → dep is present.
    us_conds = jso["resolved_conditions"][0]["conditions"]
    assert len(us_conds) == 1
    assert us_conds[0]["resolved_job_id"] == "target_job"

    # EU: enabled=False → dep is filtered out.
    eu_conds = jso["resolved_conditions"][1]["conditions"]
    assert len(eu_conds) == 0


def test_enabled_template_same_across_schedules():
    """enabled template that evaluates the same for all schedules → common."""
    target = Job("target_job", set(), [])
    dep = Dependency("target_job", {}, enabled="{{ region != 'xx' }}")
    sched_us = _sched({"region": "us"})
    sched_eu = _sched({"region": "eu"})
    job = Job("myjob", {"region"}, [sched_us, sched_eu], conds=[dep])
    jobs = _jobs(job, target)

    jso = job_to_jso(job, jobs=jobs)

    # Both evaluate to True → doesn't vary → common.
    assert len(jso["common_conditions"]) == 1
    assert jso["common_conditions"][0]["resolved_job_id"] == "target_job"
