import pytest

from apsis.cond import Condition
from apsis.cond.base import ConstantCondition
from apsis.cond.dependency import Dependency
from apsis.cond.max_running import MaxRunning
from apsis.cond.skip_duplicate import SkipDuplicate
from apsis.cond.test import TestThreadPolledCondition
from apsis.exc import SchemaError
from apsis.jobs import Job
from apsis.runs import Run, Instance
from apsis.states import State

# -------------------------------------------------------------------------------

JOBS = {
    "testjob0": Job("testjob0", {"foo"}, (), None),
    "testjob1": Job("testjob1", {"foo", "bar"}, (), None),
    "dep/us": Job("dep/us", {"date"}, (), None),
    "dep/eu": Job("dep/eu", {"date"}, (), None),
    "ingest/market data": Job("ingest/market data", set(), (), None),
    "risk/compute var": Job("risk/compute var", set(), (), None),
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
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x", "region": "us"}))
    jobs = {**JOBS, "dep/us": Job("dep/us", {"foo"}, (), None)}
    bound = dep.bind(run, jobs)

    assert bound.job_id == "dep/us"
    assert bound.args == {"foo": "x"}


def test_bind_parametrized_job_id_with_args():
    dep = Dependency("dep/{{ region }}", {"date": "{{ date }}"})
    run = Run(Instance("testjob0", {"foo": "x", "region": "eu", "date": "2024-01-01"}))
    bound = dep.bind(run, JOBS)

    assert bound.job_id == "dep/eu"
    assert bound.args == {"date": "2024-01-01"}


def test_bind_parametrized_job_id_not_found():
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x", "region": "xx"}))
    with pytest.raises(KeyError, match=r"dep/xx"):
        dep.bind(run, JOBS)


def test_bind_parametrized_job_id_missing_param():
    dep = Dependency("dep/{{ region }}", {})
    run = Run(Instance("testjob0", {"foo": "x"}))
    with pytest.raises(NameError, match=r"name 'region' is not defined"):
        dep.bind(run, JOBS)


def test_bind_parametrized_job_id_full_name():
    dep = Dependency("{{ prerequisite }}", {})
    run_a = Run(Instance("testjob0", {"foo": "x", "prerequisite": "ingest/market data"}))
    bound_a = dep.bind(run_a, JOBS)
    assert bound_a.job_id == "ingest/market data"

    run_b = Run(Instance("testjob0", {"foo": "x", "prerequisite": "risk/compute var"}))
    bound_b = dep.bind(run_b, JOBS)
    assert bound_b.job_id == "risk/compute var"


def test_bind_parametrized_job_id_jso_roundtrip():
    dep = Dependency("dep/{{ region }}", {"date": "2024-01-01"})
    jso = dep.to_jso()
    restored = Condition.from_jso(jso)

    assert restored.job_id == "dep/{{ region }}"
    assert restored.args == {"date": "2024-01-01"}


def test_bind_parametrized_job_id_block_syntax():
    dep = Dependency(
        '{% if region == "us" %}dep/us{% else %}dep/eu{% endif %}',
        {"date": "{{ date }}"},
    )

    run_us = Run(Instance("testjob0", {"foo": "x", "region": "us", "date": "2024-01-01"}))
    bound = dep.bind(run_us, JOBS)
    assert bound.job_id == "dep/us"
    assert bound.args == {"date": "2024-01-01"}

    run_eu = Run(Instance("testjob0", {"foo": "x", "region": "eu", "date": "2024-01-01"}))
    bound = dep.bind(run_eu, JOBS)
    assert bound.job_id == "dep/eu"
    assert bound.args == {"date": "2024-01-01"}


def test_bind_enabled_true():
    # enabled evaluates to true — dependency is kept.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled="{{ strat == 'us' }}")
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery", "strat": "us"}))
    bound = dep.bind(run, JOBS)

    assert bound is not None
    assert bound.job_id == "testjob0"
    assert bound.args == {"foo": "banana"}
    # The bound dependency should NOT carry enabled — it was already evaluated.
    assert bound.enabled is None


def test_bind_enabled_false():
    # enabled evaluates to false — dependency is skipped.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled="{{ strat == 'us' }}")
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery", "strat": "eu"}))
    bound = dep.bind(run, JOBS)

    assert bound is None


def test_bind_enabled_bool_false():
    # enabled=False unconditionally disables the dependency.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled=False)
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery"}))
    bound = dep.bind(run, JOBS)

    assert bound is None


def test_bind_enabled_bool_true():
    # enabled=True unconditionally enables the dependency.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled=True)
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery"}))
    bound = dep.bind(run, JOBS)

    assert bound is not None
    assert bound.job_id == "testjob0"


def test_enabled_bool_jso_roundtrip():
    # to_jso/from_jso preserves enabled=False.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled=False)
    jso = dep.to_jso()
    assert jso["enabled"] is False

    restored = Condition.from_jso(jso)
    assert isinstance(restored, Dependency)
    assert restored.enabled is False


def test_enabled_jso_roundtrip():
    # to_jso/from_jso preserves enabled.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled="{{ strat == 'us' }}")
    jso = dep.to_jso()
    assert jso["enabled"] == "{{ strat == 'us' }}"

    restored = Condition.from_jso(jso)
    assert isinstance(restored, Dependency)
    assert restored.enabled == "{{ strat == 'us' }}"
    assert restored.job_id == "testjob0"
    assert restored.args == {"foo": "banana"}


def test_enabled_jso_roundtrip_none():
    # When enabled is None, it should not appear in JSO.
    dep = Dependency("testjob0", {"foo": "banana"})
    jso = dep.to_jso()
    assert "enabled" not in jso

    restored = Condition.from_jso(jso)
    assert restored.enabled is None


def test_enabled_from_jso_rejects_non_string():
    # YAML without quotes parses {{ }} as a dict; from_jso should reject it.
    jso = {"type": "dependency", "job_id": "testjob0", "enabled": {"strat": None}}
    with pytest.raises(TypeError, match=r"enabled must be a bool or a quoted string in YAML"):
        Condition.from_jso(jso)


def test_job_id_from_jso_rejects_non_string():
    # YAML without quotes parses {{ }} as a dict; from_jso should reject it.
    jso = {"type": "dependency", "job_id": {"region": None}}
    with pytest.raises(
        SchemaError,
        match=r"job_id must be a string; if using Jinja2 templates, make sure the value is quoted in YAML",
    ):
        Condition.from_jso(jso)


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


def test_skip_duplicate_enabled_jso_roundtrip():
    cond = SkipDuplicate(enabled="{{ strat == 'us' }}")
    jso = cond.to_jso()
    assert jso["enabled"] == "{{ strat == 'us' }}"

    restored = Condition.from_jso(jso)
    assert isinstance(restored, SkipDuplicate)
    assert restored.enabled == "{{ strat == 'us' }}"


def test_max_running_enabled_jso_roundtrip():
    cond = MaxRunning("1", enabled="{{ strat == 'us' }}")
    jso = cond.to_jso()
    assert jso["enabled"] == "{{ strat == 'us' }}"

    restored = Condition.from_jso(jso)
    assert isinstance(restored, MaxRunning)
    assert restored.enabled == "{{ strat == 'us' }}"


def test_constant_condition_enabled_jso_roundtrip():
    cond = ConstantCondition(True, enabled='{{ foo == "apple" }}')
    jso = cond.to_jso()
    assert jso["enabled"] == '{{ foo == "apple" }}'
    assert jso["value"] is True

    restored = Condition.from_jso(jso)
    assert isinstance(restored, ConstantCondition)
    assert restored.enabled == '{{ foo == "apple" }}'


def test_test_condition_enabled_jso_roundtrip():
    cond = TestThreadPolledCondition(0.5, 3, enabled='{{ foo == "apple" }}')
    jso = cond.to_jso()
    assert jso["enabled"] == '{{ foo == "apple" }}'
    assert jso["delay"] == 0.5
    assert jso["count"] == 3

    restored = Condition.from_jso(jso)
    assert isinstance(restored, TestThreadPolledCondition)
    assert restored.enabled == '{{ foo == "apple" }}'
