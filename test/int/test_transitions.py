"""
Tests basic transitions by running an Apsis service and interacting with it
via the HTTP client.
"""

from collections import Counter
from contextlib import closing
import ora
from pathlib import Path
import pytest
import time

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=JOB_DIR)) as inst, inst.agent():
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def test_create_adhoc(inst):
    client = inst.client

    # Create and schedule a run.
    res = client.schedule_adhoc("now", {"program": {"type": "no-op"}})
    job_id = res["job_id"]
    run_id = res["run_id"]

    # Check on the adhoc job.
    job = client.get_job(job_id)
    assert job["program"]["type"] == "no-op"

    # Check on the run.
    run = client.get_run(run_id)
    assert run["job_id"] == job_id
    # It should be done.
    assert run["state"] == "success"


def test_skip(inst):
    """
    Schedules a new run for the future and skips it before it runs.
    """
    client = inst.client

    # Create and schedule a run for a minute from now.
    res = client.schedule_adhoc(ora.now() + 60, {"program": {"type": "no-op"}})
    run_id = res["run_id"]

    # It should be scheduled.
    run = client.get_run(run_id)
    assert run["state"] == "scheduled"

    # Skip the run.
    client.skip(run_id)

    # It should be skipped.
    run = client.get_run(run_id)
    assert run["state"] == "skipped"


def test_max_running(inst):
    client = inst.client

    JOB = {
        "program": {
            "type": "no-op",
            "duration": "1",
        },
        "condition": {
            "type": "max_running",
            "count": 1,
        },
    }

    # Create and schedule a run.
    res = client.schedule_adhoc("now", JOB)
    job_id = res["job_id"]
    run_id0 = res["run_id"]

    job = client.get_job(job_id)
    print(job)

    # Schedule a second run with the same (adhoc) job.
    res = client.schedule(job_id, {}, "now")
    run_id1 = res["run_id"]

    # The first job should be running.
    run = client.get_run(run_id0)
    assert run["state"] == "running"

    # The second job should be waiting.
    run = client.get_run(run_id1)
    assert run["state"] == "waiting"

    # Wait and check again.
    time.sleep(1)

    # The first run should have completed and the second started.
    run = client.get_run(run_id0)
    assert run["state"] == "success"
    run = client.get_run(run_id1)
    assert run["state"] == "running"

    # Wait and check again.
    time.sleep(2)

    # The second run should have completed.
    run = client.get_run(run_id1)
    assert run["state"] == "success"


def test_max_running_terminate(inst):
    client = inst.client

    JOB = {
        "program": {
            "type": "procstar",
            "argv": ["/usr/bin/sleep", "3"],
        },
        "condition": {
            "type": "max_running",
            "count": 1,
        },
    }

    # Create and schedule a run.
    res = client.schedule_adhoc("now", JOB)
    job_id = res["job_id"]
    run_id0 = res["run_id"]
    inst.wait_for_run_to_start(run_id0)

    # Schedule additional runs with the same (adhoc) job.
    run_ids = {client.schedule(job_id, {}, "now")["run_id"] for _ in range(10)}

    # The first job should be running.
    assert client.get_run(run_id0)["state"] == "running"
    # The rest should be waiting.
    assert all(client.get_run(r)["state"] == "waiting" for r in run_ids)

    # Terminate the running run.
    client.signal(run_id0, "SIGTERM")
    res = inst.wait_run(run_id0)
    assert res["state"] == "failure"
    # One other run should be running.
    assert sum(client.get_run(r)["state"] in {"starting", "running"} for r in run_ids) == 1

    # Repeat for remaining runs.
    while len(run_ids) > 0:
        running = [r for r in run_ids if client.get_run(r)["state"] in {"starting", "running"}]
        assert len(running) == 1
        (run_id,) = running
        inst.wait_for_run_to_start(run_id)
        client.signal(run_id, "SIGTERM")
        res = inst.wait_run(run_id)
        assert res["state"] == "failure"
        run_ids.remove(run_id)


@pytest.mark.parametrize("N", (2**i for i in range(1, 8)))
def test_skip_duplicate_race(inst, N):
    client = inst.client

    # Start a bunch of runs that all have the same dependency, followed by a
    # skip_duplicate condition.
    run_ids = [client.schedule("skip duplicate race", {})["run_id"] for _ in range(N)]
    assert all(client.get_run(r)["state"] == "waiting" for r in run_ids)

    # Run the dependency.  All the waiting runs should move past the dependency
    # and evaluate the skip duplicates condition at around the same time.
    client.schedule("dep", {})

    # Exactly one should have run, the rest skipped.
    states = Counter(inst.wait_run(r)["state"] for r in run_ids)
    assert states["success"] == 1
    assert states["skipped"] == N - 1
