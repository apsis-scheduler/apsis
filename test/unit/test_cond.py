import pytest

from apsis.cond import Condition
from apsis.cond.dependency import Dependency
from apsis.cond.max_running import MaxRunning
from apsis.cond.skip_duplicate import SkipDuplicate
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


def test_bind_parametrized_job_id_full_name():
    # The parameter IS the entire dependency job_id — two schedules of the same
    # job can depend on completely unrelated jobs by passing the dependency name
    # as a parameter.
    dep = Dependency("{{ prerequisite }}", {})

    # One schedule passes prerequisite="ingest/market data".
    run_a = Run(Instance("testjob0", {"foo": "x", "prerequisite": "ingest/market data"}))
    bound_a = dep.bind(run_a, JOBS)
    assert bound_a.job_id == "ingest/market data"

    # Another schedule passes prerequisite="risk/compute var".
    run_b = Run(Instance("testjob0", {"foo": "x", "prerequisite": "risk/compute var"}))
    bound_b = dep.bind(run_b, JOBS)
    assert bound_b.job_id == "risk/compute var"


def test_bind_parametrized_job_id_jso_roundtrip():
    # Verify that a dependency with a templated job_id survives JSO roundtrip
    # (the unbound form preserves the template).
    dep = Dependency("dep/{{ region }}", {"date": "2024-01-01"})
    jso = dep.to_jso()
    restored = Condition.from_jso(jso)

    assert restored.job_id == "dep/{{ region }}"
    assert restored.args == {"date": "2024-01-01"}


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


def test_bind_enabled_none():
    # No enabled — backward compatibility, dependency is always kept.
    dep = Dependency("testjob0", {"foo": "banana"})
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery"}))
    bound = dep.bind(run, JOBS)

    assert bound is not None
    assert bound.enabled is None


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


def test_enabled_str():
    # __str__ includes enabled when set.
    dep = Dependency("testjob0", {"foo": "banana"}, enabled="{{ strat == 'us' }}")
    s = str(dep)
    assert "[if {{ strat == 'us' }}]" in s

    # Without enabled, no [if ...] suffix.
    dep2 = Dependency("testjob0", {"foo": "banana"})
    s2 = str(dep2)
    assert "[if" not in s2


def test_enabled_from_jso_rejects_non_string():
    # YAML without quotes parses {{ }} as a dict; from_jso should reject it.
    jso = {"type": "dependency", "job_id": "testjob0", "enabled": {"strat": None}}
    with pytest.raises(SchemaError, match="quoted string"):
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


# --- SkipDuplicate enabled tests ---


def test_skip_duplicate_enabled_true():
    cond = SkipDuplicate(enabled="{{ strat == 'us' }}")
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery", "strat": "us"}))
    run.run_id = "r100"
    bound = cond.bind(run, jobs=None)

    assert bound is not None
    # Bound form should not carry enabled.
    assert bound.enabled is None


def test_skip_duplicate_enabled_false():
    cond = SkipDuplicate(enabled="{{ strat == 'us' }}")
    run = Run(Instance("testjob1", {"foo": "apple", "bar": "celery", "strat": "eu"}))
    run.run_id = "r100"
    bound = cond.bind(run, jobs=None)

    assert bound is None


def test_skip_duplicate_enabled_bool_false():
    cond = SkipDuplicate(enabled=False)
    run = Run(Instance("testjob1", {"foo": "apple"}))
    run.run_id = "r100"
    bound = cond.bind(run, jobs=None)

    assert bound is None


def test_skip_duplicate_enabled_jso_roundtrip():
    cond = SkipDuplicate(enabled="{{ strat == 'us' }}")
    jso = cond.to_jso()
    assert jso["enabled"] == "{{ strat == 'us' }}"

    restored = Condition.from_jso(jso)
    assert isinstance(restored, SkipDuplicate)
    assert restored.enabled == "{{ strat == 'us' }}"


def test_skip_duplicate_enabled_none_jso():
    cond = SkipDuplicate()
    jso = cond.to_jso()
    assert "enabled" not in jso


def test_skip_duplicate_enabled_str():
    cond = SkipDuplicate(enabled="{{ strat == 'us' }}")
    assert "[if {{ strat == 'us' }}]" in str(cond)

    cond2 = SkipDuplicate()
    assert "[if" not in str(cond2)


# --- MaxRunning enabled tests ---


def test_max_running_enabled_true():
    cond = MaxRunning("1", enabled="{{ strat == 'us' }}")
    run = Run(Instance("testjob0", {"foo": "x", "strat": "us"}))
    bound = cond.bind(run, jobs=None)

    assert bound is not None
    assert bound.enabled is None


def test_max_running_enabled_false():
    cond = MaxRunning("1", enabled="{{ strat == 'us' }}")
    run = Run(Instance("testjob0", {"foo": "x", "strat": "eu"}))
    bound = cond.bind(run, jobs=None)

    assert bound is None


def test_max_running_enabled_bool_false():
    cond = MaxRunning("1", enabled=False)
    run = Run(Instance("testjob0", {"foo": "x"}))
    bound = cond.bind(run, jobs=None)

    assert bound is None


def test_max_running_enabled_jso_roundtrip():
    cond = MaxRunning("1", enabled="{{ strat == 'us' }}")
    jso = cond.to_jso()
    assert jso["enabled"] == "{{ strat == 'us' }}"

    restored = Condition.from_jso(jso)
    assert isinstance(restored, MaxRunning)
    assert restored.enabled == "{{ strat == 'us' }}"


def test_max_running_enabled_none_jso():
    cond = MaxRunning("1")
    jso = cond.to_jso()
    assert "enabled" not in jso


def test_max_running_enabled_str():
    cond = MaxRunning("1", enabled="{{ strat == 'us' }}")
    assert "[if {{ strat == 'us' }}]" in str(cond)

    cond2 = MaxRunning("1")
    assert "[if" not in str(cond2)


# --- Cross-type enabled validation ---


def test_enabled_rejects_non_string_skip_duplicate():
    jso = {"type": "skip_duplicate", "enabled": {"strat": None}}
    with pytest.raises(SchemaError, match="quoted string"):
        Condition.from_jso(jso)


def test_enabled_rejects_non_string_max_running():
    jso = {"type": "max_running", "count": "1", "enabled": {"strat": None}}
    with pytest.raises(TypeError, match="quoted string"):
        Condition.from_jso(jso)
