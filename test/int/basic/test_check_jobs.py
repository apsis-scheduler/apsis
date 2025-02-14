import yaml

import pytest

from   apsis.check import check_job_dependencies_scheduled
from   apsis.exc import JobsDirErrors
import apsis.jobs
from   apsis.lib import itr

#-------------------------------------------------------------------------------

def dump_yaml_file(obj, path):
    with path.open("w") as file:
        yaml.dump(obj, file)


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
        jobs_path / "dependency.yaml"
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
        return list(itr.chain(*(
            check_job_dependencies_scheduled(jobs_dir, j)
            for j in jobs_dir.get_jobs()
        )))

    # The dependent isn't scheduled, so no error.
    errs = await check()
    assert len(errs) == 0

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
    errs = await check()
    assert any( "label=foo" in e for e in errs )
    assert any( "label=bar" in e for e in errs )

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
    errs = await check()
    assert any( "label=foo" in e for e in errs )
    assert not any( "label=bar" in e for e in errs )

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
    errs = await check()
    assert len(errs) == 0


