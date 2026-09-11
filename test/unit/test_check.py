from typing import List

import pytest

import apsis.check
from apsis.check import PARAM_NAME_NOT_EXPANDABLE, PARAM_NAME_PROVIDED, check_param_names
from apsis.cond.dependency import Dependency
import apsis.jobs
from apsis.jobs import InMemoryJobs, Job
from apsis.runs import Instance, Run, get_bind_args, template_expand

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
# param names


@pytest.mark.parametrize(
    "name",
    ["a=b", "=", "date=2024-01-01", "a\0b", "with-dash", "with space", "2big", "a,b", ""],
)
def test_param_name_invalid(name):
    """
    A param name that is not an identifier is an error.
    """
    (error,) = check_param_names([name])
    assert repr(name) in error
    assert "letter or underscore" in error


# Each non-expandable name, with a template in which it doesn't expand to the arg.
NOT_EXPANDABLE = [
    ("not", "{{ not }}"),
    ("true", "{{ true }}"),
    ("false", "{{ false }}"),
    ("none", "{{ none }}"),
    ("True", "{{ True }}"),
    ("False", "{{ False }}"),
    ("None", "{{ None }}"),
    ("self", "{{ self }}"),
    ("loop", "{% for _ in [1] %}{{ loop }}{% endfor %}"),
]


def test_param_names_not_expandable():
    """
    Every non-expandable name has a case below demonstrating why.
    """
    assert {n for n, _ in NOT_EXPANDABLE} == PARAM_NAME_NOT_EXPANDABLE


@pytest.mark.parametrize("name,template", NOT_EXPANDABLE)
def test_param_name_not_expandable(name, template):
    """
    A param name that a template expansion doesn't read as the arg is an error.
    """
    (error,) = check_param_names([name])
    assert repr(name) in error
    assert "reserved" in error

    # Confirm the name really is unusable.
    try:
        expanded = template_expand(template, {name: "VAL"})
    except SyntaxError:
        return
    assert expanded != "VAL"


def test_param_names_provided():
    """
    The provided names are exactly what binding makes available.
    """
    run = Run(Instance("job", {}))
    assert PARAM_NAME_PROVIDED == set(get_bind_args(run))


@pytest.mark.parametrize("name", sorted(PARAM_NAME_PROVIDED))
def test_param_name_provided(name):
    """
    A param name that Apsis provides to template expansion is an error.
    """
    (error,) = check_param_names([name])
    assert repr(name) in error
    assert "provided" in error


def test_param_name_provided_shadows():
    """
    A param that shadows a provided name breaks the job's other expansions.
    """
    run = Run(Instance("job", {"format": "2024-01-01", "run_id": "r1"}))
    args = get_bind_args(run)
    # The param wins over what Apsis provides.
    assert template_expand("{{ format }}", args) == "2024-01-01"
    assert template_expand("{{ run_id }}", args) == "r1"
    # So `format` is no longer callable elsewhere in the job.
    with pytest.raises(TypeError):
        template_expand("{{ format(Date(2024, 1, 1), '%Y%m%d') }}", args)


@pytest.mark.parametrize(
    "name", ["date", "with_underscore", "UPPER", "_leading", "a1", "TRUE", "SELF", "nothing"]
)
def test_param_name_valid(name):
    """
    A param name that is an identifier is accepted, and expands to its arg.
    """
    assert list(check_param_names([name])) == []
    assert template_expand("{{ " + name + " }}", {name: "VAL"}) == "VAL"


def test_param_names_all_reported():
    """
    Every unusable param name is reported, with why, not just the first.
    """
    errors = list(check_param_names(["date", "a=b", "not", "2big", "format"]))
    assert len(errors) == 3
    invalid, reserved, provided = errors
    assert "'2big', 'a=b'" in invalid
    assert "letter or underscore" in invalid
    assert "'not'" in reserved
    assert "reserved" in reserved
    assert "'format'" in provided
    assert "provided" in provided
    assert not any("date" in e for e in errors)


def test_param_name_does_not_mask_other_errors():
    """
    A bad param name doesn't hide the job's other errors.
    """
    jobs = InMemoryJobs(
        (
            Job("job0", params=["color"]),
            Job("job1", params=["a=b"], conds=[Dependency("job0")]),
        )
    )
    errors = check_job(jobs, "job1")
    assert any("'a=b'" in e for e in errors)
    assert any("missing" in e for e in errors)
