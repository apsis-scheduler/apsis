"""
Tests dependency conditions.
"""

from contextlib import closing
import ora
from pathlib import Path
import pytest
import random
import time

from instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


@pytest.fixture
def client(inst, scope="function"):
    return inst.client


def test_args_match(inst):
    """
    Tests that args in dependencies are processed correctly.
    """
    client = inst.client

    res = client.schedule("dependent", {"date": "2022-11-01", "color": "red"})
    run_id = res["run_id"]

    # This job depends on dependency(date=2022-11-01 flavor=vanilla) and
    # dependency(date=2022-11-01 flavor=chocolate).
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # Run the first of the dependencies.
    res = client.schedule("dependency", {"date": "2022-11-01", "flavor": "vanilla"})

    # One dependency satisfied, but not the other one.
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # This is not a dependency, as the date is wrong.
    res = client.schedule("dependency", {"date": "2022-12-25", "flavor": "chocolate"})

    # One dependency satisfied, but not the other one.
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # This is just the first dependency, run again.
    res = client.schedule("dependency", {"date": "2022-11-01", "flavor": "vanilla"})

    # One dependency satisfied, but not the other one.
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # Now run the second dependency.
    res = client.schedule("dependency", {"date": "2022-11-01", "flavor": "chocolate"})
    inst.wait_run(res["run_id"])

    # Both are satisfied.
    res = inst.wait_run(run_id)
    assert res["state"] == "success"


@pytest.mark.parametrize("direction", ["forward", "backward", "shuffle"])
def test_many_deps(inst, direction):
    """
    Tests a job with a large number of conditions.

    :param direction:
      Order in which the dependencies are run, relative to specified.
    """
    client = inst.client

    # Create a job with many dependencies.
    vals = [format(i, "03d") for i in range(200)]
    job = {
        "program": {"type": "no-op"},
        "condition": [
            {
                "type": "dependency",
                "job_id": "many dep",
                "args": {"val": v},
            }
            for v in vals
        ],
    }

    res = client.schedule_adhoc("now", job)
    run_id = res["run_id"]
    assert res["state"] == "waiting"

    # Run the dependencies, in order depending on `direction`.
    match direction:
        case "forward":
            pass
        case "backward":
            vals = vals[::-1]
        case "shuffle":
            random.shuffle(vals)
        case _:
            assert False

    # All dependencies but one.
    for v in vals[:-1]:
        client.schedule("many dep", {"val": v})

    time.sleep(1)
    # The run is still waiting.
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # Schedule the last dependency.
    client.schedule("many dep", {"val": vals[-1]})

    # Now the run can continue.s
    res = inst.wait_run(run_id)
    assert res["state"] == "success"


def test_exist(inst):
    client = inst.client
    date = "2023-12-27"

    # Schedule the dependent without any dependency; this results in error
    # because depedency(flavor=vanilla) and dependency(flavor=chocolate) must
    # exist.
    res = client.schedule("exist", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "error"

    # Schedule the first dependency.
    res = client.schedule("dependency", {"date": date, "flavor": "chocolate"})
    inst.wait_run(res["run_id"])

    # One dependency isn't enough.
    res = client.schedule("exist", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "error"

    # Schedule the second dependency.
    res = client.schedule("dependency", {"date": date, "flavor": "vanilla"})
    inst.wait_run(res["run_id"])

    # Both dependencies should exist, so we can proceed.
    res = client.schedule("exist", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "success"


def test_exist_slow(inst):
    client = inst.client
    date = "2023-12-27"

    # Schedule the dependent without any dependency; this results in error
    # because depedency(flavor=vanilla) and dependency(flavor=chocolate) must
    # exist.
    res = client.schedule("exist slow", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "error"

    # Schedule the first dependency.
    res = client.schedule("slow", {"date": date, "flavor": "chocolate"})

    # Try the dependent again.  The dependency, slow(flavor=chocolate), isn't
    # done yet, but that's OK.
    res = client.schedule("exist slow", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "error"

    # Schedule the second dependency.
    res = client.schedule("slow", {"date": date, "flavor": "vanilla"})

    # Both dependencies exist, whether or not they are successful, so OK.
    res = client.schedule("exist slow", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "success"


def test_exist_fail(inst):
    client = inst.client
    date = "2023-12-27"

    # Schedule the dependent without any dependency; this results in error
    # because depedency(flavor=vanilla) and dependency(flavor=chocolate) must
    # exist.
    res = client.schedule("exist fail", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "error"

    # Schedule the dependency.
    dep_run_id = client.schedule("fail", {"date": date})["run_id"]

    # Now schedule the dependent should work.  The dependency is running, so
    # this should enter waiting.
    res = client.schedule("exist fail", {"date": date, "color": "red"})
    run_id = res["run_id"]
    time.sleep(0.1)
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # Wait for the dependency to fail.
    res = inst.wait_run(dep_run_id)
    assert res["state"] == "failure"

    # The dependent should error too.
    res = inst.wait_run(run_id)
    assert res["state"] == "error"

    # Now the dependent should no longer work; the dependency is running.
    res = client.schedule("exist fail", {"date": date, "color": "red"})
    run_id = res["run_id"]
    res = inst.wait_run(run_id)
    assert res["state"] == "error"


def test_alarm_error(inst):
    """
    Tests error functionality of an alarm run: a job with an `exist:
    [success]` dependency, which errors immediately when scheduled unless the
    dependency is satisfied.
    """
    client = inst.client
    DATE = "2024-04-20"

    # Schedule the dependent for now, and an alarm for 1 sec in the future.
    now = ora.now()
    depd_run_id = client.schedule("dependent", {"date": DATE, "color": "purple"}, time=str(now))[
        "run_id"
    ]
    alrm_run_id = client.schedule("alarm", {"date": DATE}, time=str(now + 1))["run_id"]

    # The alarm errors, because the dependent never left waiting.
    res = inst.wait_run(alrm_run_id)
    assert res["state"] == "error"
    res = client.get_run(depd_run_id)
    assert res["state"] == "waiting"


def test_alarm_success(inst):
    """
    Tests success functionality of an alarm run: a job with an `exist:
    [success]` dependency, which errors immediately when scheduled unless the
    dependency is satisfied.
    """
    client = inst.client
    DATE = "2024-04-13"

    # Schedule the dependent for now, and an alarm for 1 sec in the future.
    now = ora.now()
    depd_run_id = client.schedule("dependent", {"date": DATE, "color": "purple"}, time=str(now))[
        "run_id"
    ]
    alrm_run_id = client.schedule("alarm", {"date": DATE}, time=str(now + 1))["run_id"]
    # Run necessary dependencies.
    client.schedule("dependency", {"date": DATE, "flavor": "vanilla"})
    client.schedule("dependency", {"date": DATE, "flavor": "chocolate"})

    # The alarm succeeds, because the dependent succeeded first.
    res = inst.wait_run(alrm_run_id)
    assert res["state"] == "success"
    res = client.get_run(depd_run_id)
    assert res["state"] == "success"


def test_type_error(inst):
    client = inst.client

    res = client.schedule("type error", args={"color": "purple"})
    # Should get back an error run with a run ID.
    run_id = res["run_id"]
    assert run_id is not None
    assert res["state"] == "error"


def test_api_dependencies():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db(clock=ora.now() + 1)
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        client = inst.client

        res = client.schedule("dependent", {"date": "2024-10-22", "color": "blue"})
        run_id = res["run_id"]

        deps = client.get_run_dependencies(run_id)
        assert len(deps) == 2
        assert deps[0] == {
            "job_id": "dependency",
            "args": {
                "date": "2024-10-22",
                "flavor": "vanilla",
            },
            "run_ids": [],
        }
        assert deps[1] == {
            "job_id": "dependency",
            "args": {
                "date": "2024-10-22",
                "flavor": "chocolate",
            },
            "run_ids": [],
        }

        res = client.schedule("dependency", {"date": "2024-10-22", "flavor": "chocolate"})
        dep_run_id0 = res["run_id"]
        res = inst.wait_run(dep_run_id0)
        assert res["state"] == "success"

        deps = client.get_run_dependencies(run_id)
        assert len(deps) == 2
        assert deps[0]["run_ids"] == []
        assert deps[1]["run_ids"] == [dep_run_id0]

        res = client.schedule("dependency", {"date": "2024-10-22", "flavor": "vanilla"})
        dep_run_id1 = res["run_id"]
        res = client.schedule("dependency", {"date": "2024-10-22", "flavor": "chocolate"})
        dep_run_id2 = res["run_id"]
        res = inst.wait_run(dep_run_id1)
        assert res["state"] == "success"
        res = inst.wait_run(dep_run_id2)
        assert res["state"] == "success"

        deps = client.get_run_dependencies(run_id)
        assert len(deps) == 2
        assert deps[0]["run_ids"] == [dep_run_id1]
        assert set(deps[1]["run_ids"]) == {dep_run_id0, dep_run_id2}
