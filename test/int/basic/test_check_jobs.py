import shutil
from pathlib import Path

import ora
import pytest
import yaml

import apsis.jobs
from apsis.check import check_job_dependencies_scheduled
from apsis.exc import JobsDirErrors, SchemaError

DAY_IN_SEC = 86400

# -------------------------------------------------------------------------------


def dump_yaml_file(obj, path):
    with path.open("w") as file:
        yaml.dump(obj, file)


@pytest.mark.asyncio
async def test_duplicate_key_in_yaml(tmp_path):
    """
    Test that loading a job with a duplicate key raises the appropriate error.
    """
    source_file = Path(__file__).parent / "invalid_jobs" / "duplicate command key.yaml"
    dest_file = tmp_path / "duplicate command key.yaml"
    shutil.copy2(source_file, dest_file)

    with pytest.raises(JobsDirErrors) as exc_info:
        await apsis.jobs.load_jobs_dir(tmp_path)

    errors = exc_info.value.errors
    assert len(errors) == 1, f"Expected 1 error, got {len(errors)}"
    assert isinstance(errors[0], SchemaError)
    assert 'found duplicate key "command" with value' in str(errors[0]).lower()


@pytest.mark.asyncio
async def test_name_error(tmp_path):
    """
    Tests that a job with an unknown param in an expansion is not loaded.
    """
    jobs_path = tmp_path
    job_path = jobs_path / "job.yaml"

    job = {
        "params": ["date"],
        "schedule": {
            "type": "daily",
            "tz": "America/New_York",
            "calendar": "Mon-Fri",
            "daytime": "09:00:00",
        },
        "program": {
            "type": "procstar-shell",
            "command": "today is {{ date }} and my name is {{ name }}",
        },
    }
    dump_yaml_file(job, job_path)

    # The program's command has an invalid expansion 'name'.
    try:
        await apsis.jobs.load_jobs_dir(jobs_path)
    except JobsDirErrors as exc:
        assert len(exc.errors) == 1
        assert "'name'" in str(exc.errors[0])
    else:
        assert False, "expected jobs dir error"

    # Add 'name' to the params and schedule args.  Now it should be fine.
    job["params"].append("name")
    job["schedule"]["args"] = {"name": "bob"}
    dump_yaml_file(job, job_path)
    await apsis.jobs.load_jobs_dir(jobs_path)


@pytest.mark.asyncio
async def test_syntax_error(tmp_path):
    """
    Tests that a job with a syntax error inside an expanded expression is not
    loaded.
    """
    jobs_path = tmp_path
    job_path = jobs_path / "job.yaml"

    job = {
        "params": ["date"],
        "schedule": {
            "type": "interval",
            "interval": "1d",
        },
        "program": {
            "type": "procstar-shell",
            "command": "today is {{ date }} and also {{ date date }}",
        },
    }
    dump_yaml_file(job, job_path)

    # The program's command has an invalid expansion 'name'.
    try:
        await apsis.jobs.load_jobs_dir(jobs_path)
    except JobsDirErrors as exc:
        assert len(exc.errors) == 1
        assert "expected token" in str(exc.errors[0])
    else:
        assert False, "expeceted jobs dir error"

    # Fix the command.  Now it should be fine.
    job["program"]["command"] = "today is {{ date }}"
    dump_yaml_file(job, job_path)
    await apsis.jobs.load_jobs_dir(jobs_path)


@pytest.mark.asyncio
async def test_misspelled_param(tmp_path):
    """
    Tests that a job with a misspelled param name inside an expanded
    expression is not loaded.
    """
    jobs_path = tmp_path
    job_path = jobs_path / "job.yaml"

    # Create a dependency job.
    dump_yaml_file(
        {"params": ["date"], "program": {"type": "no-op"}},
        jobs_path / "dependency.yaml",
    )

    job = {
        "params": ["date"],
        "schedule": {
            "type": "interval",
            "interval": "1d",
        },
        "condition": {
            "type": "dependency",
            "job_id": "dependency",
            "args": {
                "date": "{{ get_calendar('Mon-Fri').before(data) }}",
            },
        },
        "program": {"type": "no-op"},
    }
    dump_yaml_file(job, job_path)

    # The program's command has an invalid expansion 'name'.
    try:
        await apsis.jobs.load_jobs_dir(jobs_path)
    except JobsDirErrors as exc:
        for err in exc.errors:
            import traceback

            traceback.print_exception(exc, chain=True)

        assert len(exc.errors) == 1
        assert "'data' is not defined" in str(exc.errors[0])
    else:
        assert False, "expeceted jobs dir error"

    # Fix the command.  Now it should be fine.
    job["condition"]["args"]["date"] = "{{ get_calendar('Mon-Fri').before(date) }}"
    dump_yaml_file(job, job_path)
    await apsis.jobs.load_jobs_dir(jobs_path)


@pytest.mark.asyncio
async def test_check_job_dependencies_scheduled(tmp_path):
    jobs_path = tmp_path

    dependent = {
        "params": ["label"],
        "condition": {
            "type": "dependency",
            "job_id": "dependency",
        },
        "program": {"type": "no-op"},
    }
    dump_yaml_file(dependent, jobs_path / "dependent.yaml")

    dependency = {
        "params": ["label"],
        "program": {"type": "no-op"},
    }
    dump_yaml_file(dependency, jobs_path / "dependency.yaml")

    async def check():
        jobs_dir = await apsis.jobs.load_jobs_dir(jobs_path)
        return [msg for _, msg in check_job_dependencies_scheduled(jobs_dir, jobs_dir.get_jobs())]

    # The dependent isn't scheduled, so no error.
    results = await check()
    assert len(results) == 0

    # Schedule the dependent.  Now there are errors, because the dependency is
    # not scheduled.
    dependent["schedule"] = [
        {
            "type": "interval",
            "interval": "12h",
            "args": {
                "label": "foo",
            },
        },
        {
            "type": "interval",
            "interval": "12h",
            "args": {
                "label": "bar",
            },
        },
    ]
    dump_yaml_file(dependent, jobs_path / "dependent.yaml")
    results = await check()
    assert any("label=foo" in msg for msg in results)
    assert any("label=bar" in msg for msg in results)

    # Schedule the dependency of one of the scheduled dependents.  There are
    # still errors, because of the dependency of the other scheduled dependent.
    dependency["schedule"] = [
        {
            "type": "interval",
            "interval": "1h",
            "args": {
                "label": "bar",
            },
        },
    ]
    dump_yaml_file(dependency, jobs_path / "dependency.yaml")
    results = await check()
    assert any("label=foo" in msg for msg in results)
    assert not any("label=bar" in msg for msg in results)

    # Schedule the other dependency.  No more errors.
    dependency["schedule"].append(
        {
            "type": "interval",
            "interval": "4h",
            "args": {
                "label": "foo",
            },
        },
    )
    dump_yaml_file(dependency, jobs_path / "dependency.yaml")
    results = await check()
    assert len(results) == 0


@pytest.mark.asyncio
async def test_check_dependency_timing(tmp_path):
    """
    Tests that check_job_dependencies_scheduled with max_wait_time detects
    dependencies scheduled too late relative to the dependent job.
    """
    jobs_path = tmp_path
    now = ora.now()

    dependent = {
        "params": ["date"],
        "schedule": {
            "type": "daily",
            "tz": "UTC",
            "calendar": "Mon-Sun",
            "daytime": "06:00:00",
        },
        "condition": {
            "type": "dependency",
            "job_id": "dependency",
        },
        "program": {"type": "no-op"},
    }

    # Dependency scheduled 2h after dependent (08:00 vs 06:00).
    dependency = {
        "params": ["date"],
        "schedule": {
            "type": "daily",
            "tz": "UTC",
            "calendar": "Mon-Sun",
            "daytime": "08:00:00",
        },
        "program": {"type": "no-op"},
    }

    dump_yaml_file(dependent, jobs_path / "dependent.yaml")
    dump_yaml_file(dependency, jobs_path / "dependency.yaml")

    sched_times = (now, now + DAY_IN_SEC * 30)
    dep_times = (now - DAY_IN_SEC * 8, now + DAY_IN_SEC * 38)

    async def check(max_wait_time=None):
        jobs_dir = await apsis.jobs.load_jobs_dir(jobs_path)
        return [
            msg
            for _, msg in check_job_dependencies_scheduled(
                jobs_dir,
                jobs_dir.get_jobs(),
                sched_times=sched_times,
                dep_times=dep_times,
                max_wait_time=max_wait_time,
            )
        ]

    # No timing parameters: no timing check, no results.
    results = await check()
    assert len(results) == 0

    # max_wait_time=3h: 2h gap is within limit, no errors.
    results = await check(max_wait_time=10800)
    assert len(results) == 0

    # max_wait_time=1h: 2h gap exceeds limit, should error.
    results = await check(max_wait_time=3600)
    assert len(results) == 1
    assert "not expected to start until" in results[0]

    # Dependency scheduled BEFORE dependent (04:00 vs 06:00) — never an issue.
    dependency["schedule"]["daytime"] = "04:00:00"
    dump_yaml_file(dependency, jobs_path / "dependency.yaml")
    results = await check(max_wait_time=3600)
    assert len(results) == 0

    # Dependency scheduled 20h after dependent (02:00 next day vs 06:00).
    # With max_wait_time=23h (82800s), this should pass.
    dependency["schedule"]["daytime"] = "02:00:00"
    dependency["schedule"]["date_shift"] = 1
    dump_yaml_file(dependency, jobs_path / "dependency.yaml")
    results = await check(max_wait_time=82800)
    assert len(results) == 0

    # But with max_wait_time=18h, the 20h gap should fail.
    results = await check(max_wait_time=64800)
    assert len(results) == 1
    assert "not expected to start until" in results[0]


@pytest.mark.asyncio
async def test_check_dependency_timing_transitive(tmp_path):
    """
    Tests that transitive dependency timing is checked.

    A (01:00) -> B (11:00) -> C (21:00), max_wait_time=12h.
    Each direct gap is 10h (< 12h), but A transitively has to wait for C
    (20h > 12h) because B won't succeed until C completes.
    """
    jobs_path = tmp_path
    now = ora.now()

    dump_yaml_file(
        {
            "params": ["date"],
            "schedule": {
                "type": "daily",
                "tz": "UTC",
                "calendar": "Mon-Sun",
                "daytime": "01:00:00",
            },
            "condition": {"type": "dependency", "job_id": "b"},
            "program": {"type": "no-op"},
        },
        jobs_path / "a.yaml",
    )
    dump_yaml_file(
        {
            "params": ["date"],
            "schedule": {
                "type": "daily",
                "tz": "UTC",
                "calendar": "Mon-Sun",
                "daytime": "11:00:00",
            },
            "condition": {"type": "dependency", "job_id": "c"},
            "program": {"type": "no-op"},
        },
        jobs_path / "b.yaml",
    )
    dump_yaml_file(
        {
            "params": ["date"],
            "schedule": {
                "type": "daily",
                "tz": "UTC",
                "calendar": "Mon-Sun",
                "daytime": "21:00:00",
            },
            "program": {"type": "no-op"},
        },
        jobs_path / "c.yaml",
    )

    sched_times = (now, now + DAY_IN_SEC * 30)
    dep_times = (now - DAY_IN_SEC * 8, now + DAY_IN_SEC * 38)

    async def check(max_wait_time):
        jobs_dir = await apsis.jobs.load_jobs_dir(jobs_path)
        return list(
            check_job_dependencies_scheduled(
                jobs_dir,
                jobs_dir.get_jobs(),
                sched_times=sched_times,
                dep_times=dep_times,
                max_wait_time=max_wait_time,
            )
        )

    # With 12h max: direct gaps (10h) are fine, but A must transitively wait
    # for C (20h).  Job A should be flagged.
    results = await check(max_wait_time=43200)
    a_errors = [msg for job, msg in results if job.job_id == "a"]
    assert len(a_errors) > 0, "Expected transitive timing error for job A"
    # The tree should mention c as the transitive bottleneck.
    assert any("dependency: c(" in msg for msg in a_errors)

    # B->C direct gap (10h) is fine, so B itself should not be flagged.
    b_errors = [msg for job, msg in results if job.job_id == "b"]
    assert len(b_errors) == 0, "B's direct dep (10h) is within 12h max"

    # With 22h max: 20h transitive gap is fine.
    results = await check(max_wait_time=79200)
    a_errors = [msg for job, msg in results if job.job_id == "a"]
    assert len(a_errors) == 0


@pytest.mark.asyncio
async def test_check_dependency_cycle(tmp_path):
    """
    Tests that a cycle in the dependency graph yields errors.

    A -> B -> C -> A forms a cycle.
    """
    jobs_path = tmp_path
    now = ora.now()

    for name, dep in [("a", "b"), ("b", "c"), ("c", "a")]:
        dump_yaml_file(
            {
                "params": ["date"],
                "schedule": {
                    "type": "daily",
                    "tz": "UTC",
                    "calendar": "Mon-Sun",
                    "daytime": "06:00:00",
                },
                "condition": {"type": "dependency", "job_id": dep},
                "program": {"type": "no-op"},
            },
            jobs_path / f"{name}.yaml",
        )

    jobs_dir = await apsis.jobs.load_jobs_dir(jobs_path)

    results = list(
        check_job_dependencies_scheduled(
            jobs_dir,
            jobs_dir.get_jobs(),
            sched_times=(now, now + DAY_IN_SEC * 30),
            dep_times=(now - DAY_IN_SEC * 8, now + DAY_IN_SEC * 38),
            max_wait_time=82800,
        )
    )

    # Each job in the cycle should get an error.
    cycle_errors = [(job.job_id, msg) for job, msg in results]
    assert len(cycle_errors) >= 3
    # The message should show the cycle path, not a list of instances.
    assert all("dependency cycle detected:" in msg for _, msg in cycle_errors)
    # The cycle should mention the job names with arrows.
    assert any("→" in msg for _, msg in cycle_errors)
