import pytest

from apsis.cond import Condition
from apsis.cond.dependency import Dependency
from apsis.cond.skip_duplicate import SkipDuplicate
from apsis.jobs import Job
from apsis.runs import Run, Instance
from apsis.states import State

# -------------------------------------------------------------------------------

JOBS = {
    "testjob0": Job("testjob0", {"foo"}, (), None),
    "testjob1": Job("testjob1", {"foo", "bar"}, (), None),
    "dep/us": Job("dep/us", {"date"}, (), None),
    "dep/eu": Job("dep/eu", {"date"}, (), None),
}


def test_bind0():
    # testjob1 depends on testjob0 with explicit arg.
    dep = Dependency("testjob0", {"foo": "banana"})
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "testjob0"
    assert bound.args == {"foo": "banana"}
    assert bound.states == dep.states


def test_bind1():
    # testjob1 depends on testjob0 with inherited arg.
    dep = Dependency("testjob0", {})
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "testjob0"
    assert bound.args == {"foo": "apple"}
    assert bound.states == dep.states


def test_bind2():
    # testjob0 depends on testjob1 with one explicit, one inherited arg.
    dep = Dependency("testjob1", {"bar": "celery"})
    run = Run(Instance("testjob0", {"foo": "apple"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "testjob1"
    assert bound.args == {"foo": "apple", "bar": "celery"}
    assert bound.states == dep.states


def test_bind3():
    # testjob0 depends on testjob1 with one expanded, one inherited arg.
    dep = Dependency("testjob1", {"bar": "{{ foo }}s"})
    run = Run(Instance("testjob0", {"foo": "apple"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "testjob1"
    assert bound.args == {"foo": "apple", "bar": "apples"}
    assert bound.states == dep.states


def test_bind_parametrized_job_id():
    # Dependency job_id is a template expanded from the run's args.
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x", "region": "us"}))
    # Use a jobs dict that includes the expanded job_id.
    jobs = {**JOBS, "dep/us": Job("dep/us", {"foo"}, (), None)}
    bound = dep.bind(run, jobs)

    assert bound.job_id == "dep/us"
    assert bound.args == {"foo": "x"}


def test_bind_parametrized_job_id_with_args():
    # Templated job_id combined with templated args.
    dep = Dependency("dep/{{ region }}", {"date": "{{ date }}"})
    run = Run(Instance("testjob0", {"foo": "x", "region": "eu", "date": "2024-01-01"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "dep/eu"
    assert bound.args == {"date": "2024-01-01"}


def test_bind_parametrized_job_id_inherited_args():
    # Templated job_id with args inherited from the run.
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x", "region": "us", "date": "2024-03-15"}))
    jobs = {**JOBS, "dep/us": Job("dep/us", {"date"}, (), None)}
    bound = dep.bind(run, jobs)

    assert bound.job_id == "dep/us"
    assert bound.args == {"date": "2024-03-15"}


def test_bind_parametrized_job_id_not_found():
    # Templated job_id that expands to a nonexistent job.
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x", "region": "xx"}))
    with pytest.raises(LookupError):
        dep.bind(run, JOBS)


def test_bind_parametrized_job_id_missing_param():
    # Templated job_id referencing a param not in the run's args.
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x"}))
    with pytest.raises(NameError):
        dep.bind(run, JOBS)


def test_bind_literal_job_id_unchanged():
    # A literal job_id (no template) still works as before.
    dep = Dependency("testjob0", {"foo": "banana"})
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "testjob0"
    assert bound.args == {"foo": "banana"}


def test_bind_parametrized_job_id_jso_roundtrip():
    # Verify that a dependency with a templated job_id survives JSO roundtrip
    # (the unbound form preserves the template).
    dep = Dependency("dep/{{ region }}", {"date": "2024-01-01"})
    jso = dep.to_jso()
    restored = Condition.from_jso(jso)

    assert restored.job_id == "dep/{{ region }}"
    assert restored.args == {"date": "2024-01-01"}


def test_skip_duplicate_jso():
    cond = SkipDuplicate()
    cond = Condition.from_jso(cond.to_jso())
    assert cond.check_states == {State.waiting, State.starting, State.running}
    assert cond.target_state == State.skipped

    cond = SkipDuplicate(check_states="running", target_state="error")
    cond = Condition.from_jso(cond.to_jso())
    assert cond.check_states == {State.running}
    assert cond.target_state == State.error

    run = Run(Instance("my job", {"foo": "orange"}))
    run.run_id = "r12345"
    cond = cond.bind(run, jobs=None)  # jobs not used

    cond = Condition.from_jso(cond.to_jso())
    assert cond.check_states == {State.running}
    assert cond.target_state == State.error
    assert cond.inst.job_id == "my job"
    assert cond.inst.args == {"foo": "orange"}
    assert cond.run_id == "r12345"
