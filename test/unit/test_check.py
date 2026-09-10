from typing import List

import pytest

import apsis.check
from apsis.cond.dependency import Dependency
import apsis.jobs
from apsis.jobs import InMemoryJobs, Job
from apsis.runs import BIND_ARGS

# -------------------------------------------------------------------------------


def check_job(jobs, job_id) -> List[str]:
    job = jobs.get_job(job_id)
    return list(apsis.check.check_job(jobs, job))


def test_dependency_no_job():
    """
    Test that check fails if a dependency job ID is unknown.
    """
    jobs = InMemoryJobs(
        (
            Job("job0"),
            Job("job1", conds=[Dependency("job0")]),
            Job("job2", conds=[Dependency("not-a-job")]),
        )
    )

    # No deps.
    assert check_job(jobs, "job0") == []
    # Valid dep job ID.
    assert check_job(jobs, "job1") == []
    # Invalid dep job ID.
    assert check_job(jobs, "job2") != []


def test_dependency_missing_arg():
    """
    Test that check fails if a dependency is missing args or has extraneous
    args.
    """
    jobs = InMemoryJobs(
        (
            Job("job0", params=["color", "fruit"]),
            Job(
                "job1",
                conds=[
                    Dependency("job0", args={"color": "red", "fruit": "mango"}),
                ],
            ),
            Job(
                "job2",
                params=["color"],
                conds=[
                    Dependency("job0", args={"fruit": "apple"}),
                ],
            ),
            Job(
                "job3",
                params=["fruit"],
                conds=[
                    Dependency("job0", args={"color": "blue"}),
                ],
            ),
            Job(
                "job4",
                params=["color", "fruit"],
                conds=[
                    Dependency("job0"),
                ],
            ),
            Job(
                "job5",
                params=[],
                conds=[
                    Dependency("job0"),
                ],
            ),
            Job(
                "job6",
                params=["fruit"],
                conds=[
                    Dependency("job0", args={"color": "green"}),
                    Dependency("job0", args={"fruit": "apricot"}),
                ],
            ),
            Job(
                "job7",
                params=["color"],
                conds=[
                    Dependency("job0", args={"fruit": "pear", "bird": "sparrow"}),
                ],
            ),
        )
    )

    # Both args explicit in dep.
    assert check_job(jobs, "job1") == []
    # Color inherited; fruit explicit.
    assert check_job(jobs, "job2") == []
    # Fruit inherited; color explicit.
    assert check_job(jobs, "job3") == []
    # Both args inherited.
    assert check_job(jobs, "job4") == []

    # Both args missing.
    errors = check_job(jobs, "job5")
    assert len(errors) > 0
    assert "missing" in errors[0]
    # First dep OK but color missing in second dep.
    errors = check_job(jobs, "job6")
    assert len(errors) > 0
    assert "missing" in errors[0]

    # Extraneous arg.
    errors = check_job(jobs, "job7")
    assert len(errors) > 0
    assert "extra" in errors[0]


# -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["date", "strat", "a_b", "_x", "x1", "Date_", "run", "job", "TRUE", "global", "for", "and"],
)
def test_param_name_valid(name):
    jobs = InMemoryJobs((Job("job0", params=[name]),))
    assert check_job(jobs, "job0") == []


@pytest.mark.parametrize(
    "name",
    [
        "a=b",  # can't be written as NAME=VAL
        "a b",
        "a-b",
        "a.b",
        "1a",
        "",
        'a"b',
        "True",  # Jinja literals
        "None",
        "true",
        "false",
        "none",
        "not",  # Jinja operator
        "self",  # Jinja template reference
        "run_id",  # provided by Apsis to templates
        "job_id",
        "Date",
        "format",
    ],
)
def test_param_name_invalid(name):
    jobs = InMemoryJobs((Job("job0", params=["date", name]),))
    (error,) = check_job(jobs, "job0")
    assert error.startswith(f"invalid param name {name!r}")


def test_reserved_params_cover_bind_args():
    # Anything `get_bind_args()` adds to the template context must be reserved,
    # else a param would silently shadow it.
    assert {"run_id", "job_id", *BIND_ARGS} <= apsis.check.RESERVED_PARAMS
