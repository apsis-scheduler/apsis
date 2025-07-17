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
