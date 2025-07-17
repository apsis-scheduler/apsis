from typing import List

import apsis.check
from apsis.cond.dependency import Dependency
import apsis.jobs
from apsis.jobs import InMemoryJobs, Job

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
