"""
Tests conditions.
"""

from contextlib import closing
from pathlib import Path
import pytest
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


def test_args_max_waiting():
    """
    Tests that a run waiting for more than `waiting.max_time` is
    transitioned to error.
    """
    with ApsisService(job_dir=job_dir, port=5006, cfg={"waiting": {"max_time": 1}}) as svc:
        client = svc.client

        res = client.schedule("dependent", {"date": "2022-11-01", "color": "red"})
        run_id = res["run_id"]

        # This job depends on dependency(date=2022-11-01 flavor=vanilla) and
        # dependency(date=2022-11-01 flavor=chocolate).
        res = client.get_run(run_id)
        assert res["state"] == "waiting"

        # Run the first of the dependencies.
        res = client.schedule("dependency", {"date": "2022-11-01", "flavor": "vanilla"})
        time.sleep(1.1)

        # After a second, its second dependency is not yet satisified, so it is
        # transitioned to error.
        res = client.get_run(run_id)
        assert res["state"] == "error"


def test_skip_duplicate(client):
    """
    Tests that a run is skipped if an identical run is running.
    """
    # Start a run, which runs for 1 s.
    res = client.schedule("skippable", {"date": "2022-11-01"})
    run_id = res["run_id"]

    res = client.get_run(run_id)
    assert res["state"] == "running"

    # Start a couple of duplicates.
    res = client.schedule("skippable", {"date": "2022-11-01"})
    run_id0 = res["run_id"]
    res = client.schedule("skippable", {"date": "2022-11-01"})
    run_id1 = res["run_id"]

    # They both should have been skipped.
    res = client.get_run(run_id0)
    assert res["state"] == "skipped"
    res = client.get_run(run_id1)
    assert res["state"] == "skipped"

    # Start a job with a different date.
    res = client.schedule("skippable", {"date": "2022-12-25"})
    run_id2 = res["run_id"]

    # This should not have been skipped; its date is different.
    res = client.get_run(run_id2)
    assert res["state"] == "running"

    # Wait for the original run to complete.
    time.sleep(1)
    res = client.get_run(run_id)
    assert res["state"] == "success"

    # Now a rerun should be fine.
    res = client.schedule("skippable", {"date": "2022-11-01"})
    run_id3 = res["run_id"]

    res = client.get_run(run_id3)
    assert res["state"] == "running"


def test_to_error(client):
    """
    Tests a custom skip_duplicate condition, which transitions runs to error if
    there is already a failure or error run.
    """
    res = client.schedule("to error", {"color": "red"})
    red0 = res["run_id"]
    res = client.schedule("to error", {"color": "red"})
    red1 = res["run_id"]
    res = client.schedule("to error", {"color": "blue"})
    blue0 = res["run_id"]

    # All should run immediately.
    for run_id in (red0, red1, blue0):
        res = client.get_run(run_id)
        assert res["state"] == "running"

    # All should succeed.
    time.sleep(1)
    for run_id in (red0, red1, blue0):
        res = client.get_run(run_id)
        assert res["state"] == "success"

    # Now mark a red one as failed.
    res = client.mark(red1, "failure")

    # Schedule another red and blue run.
    res = client.schedule("to error", {"color": "red"})
    red2 = res["run_id"]
    res = client.schedule("to error", {"color": "blue"})
    blue1 = res["run_id"]

    # The red one should have been transitioned to error.
    res = client.get_run(red2)
    assert res["state"] == "error"
    res = client.get_run(blue1)
    assert res["state"] == "running"

    # Mark both failure/error runs as success.
    res = client.mark(red1, "success")
    res = client.mark(red2, "success")

    # A red run should go again.
    res = client.schedule("to error", {"color": "red"})
    red3 = res["run_id"]
    res = client.get_run(red3)
    assert res["state"] == "running"


def test_parametrized_dependency(client):
    """
    Tests that a dependency with a templated job_id resolves correctly.

    "region dep" depends on "region/{{ region }}" with args date={{ date }}.
    We schedule a "region dep" run with region=us, which should wait for
    "region/us" to succeed.
    """
    # Schedule a run that depends on region/us.
    res = client.schedule("region dep", {"region": "us", "date": "2024-01-01"})
    run_id = res["run_id"]

    # It should be waiting for its dependency.
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # Satisfy the dependency by scheduling and completing region/us.
    res = client.schedule("region/us", {"date": "2024-01-01"})
    dep_run_id = res["run_id"]

    # The dependency run completes immediately (no-op, duration 0).
    time.sleep(0.5)

    # The dependent run should now have succeeded.
    res = client.get_run(dep_run_id)
    assert res["state"] == "success"
    res = client.get_run(run_id)
    assert res["state"] == "success"


def test_parametrized_dependency_different_regions(client):
    """
    Tests that different parameter values resolve to different dependency jobs.
    """
    # Schedule runs for two different regions.
    res_us = client.schedule("region dep", {"region": "us", "date": "2024-01-01"})
    run_us = res_us["run_id"]
    res_eu = client.schedule("region dep", {"region": "eu", "date": "2024-01-01"})
    run_eu = res_eu["run_id"]

    # Both should be waiting.
    assert client.get_run(run_us)["state"] == "waiting"
    assert client.get_run(run_eu)["state"] == "waiting"

    # Satisfy only the US dependency.
    client.schedule("region/us", {"date": "2024-01-01"})
    time.sleep(0.5)

    # Only the US run should proceed; EU remains waiting.
    assert client.get_run(run_us)["state"] == "success"
    assert client.get_run(run_eu)["state"] == "waiting"

    # Now satisfy the EU dependency.
    client.schedule("region/eu", {"date": "2024-01-01"})
    time.sleep(0.5)

    assert client.get_run(run_eu)["state"] == "success"


def test_parametrized_dependency_full_job_id(client):
    """
    Tests that the entire dependency job_id can come from a parameter.

    "full name dep" has a single param `prerequisite` and a dependency on
    "{{ prerequisite }}".  Two schedules can pass completely unrelated job
    names as the dependency.
    """
    # Schedule two runs depending on completely different jobs.
    res_a = client.schedule("full name dep", {"prerequisite": "ingest market data"})
    run_a = res_a["run_id"]
    res_b = client.schedule("full name dep", {"prerequisite": "risk compute var"})
    run_b = res_b["run_id"]

    # Both should be waiting.
    assert client.get_run(run_a)["state"] == "waiting"
    assert client.get_run(run_b)["state"] == "waiting"

    # Satisfy only the first dependency.
    client.schedule("ingest market data", {})
    time.sleep(0.5)

    # Only run_a proceeds; run_b still waits for a different job.
    assert client.get_run(run_a)["state"] == "success"
    assert client.get_run(run_b)["state"] == "waiting"

    # Satisfy the second dependency.
    client.schedule("risk compute var", {})
    time.sleep(0.5)

    assert client.get_run(run_b)["state"] == "success"


def test_thread_cond(inst):
    client = inst.client

    # Each run has a single condition, which is checked twice, each with a 0.5 s
    # sleep.  The poll interval is short.  If the condition checks were serial,
    # these runs' conditions will take 20 s to complete.
    run_ids = [client.schedule("thread poll", {})["run_id"] for _ in range(20)]
    for run_id in run_ids:
        assert client.get_run(run_id)["state"] == "waiting"
    time.sleep(1.5)
    for run_id in run_ids:
        assert client.get_run(run_id)["state"] == "success"


def test_thread_cond_skip(inst):
    client = inst.client

    run_ids = [client.schedule("thread poll", {})["run_id"] for _ in range(20)]
    for run_id in run_ids:
        assert client.get_run(run_id)["state"] == "waiting"
    for run_id in run_ids:
        client.skip(run_id)
    for run_id in run_ids:
        assert client.get_run(run_id)["state"] == "skipped"


def test_thread_cond_start(inst):
    client = inst.client

    run_ids = [client.schedule("thread poll", {})["run_id"] for _ in range(20)]
    for run_id in run_ids:
        assert client.get_run(run_id)["state"] == "waiting"
    for run_id in run_ids:
        client.start(run_id)
    for run_id in run_ids:
        assert client.get_run(run_id)["state"] == "success"


def test_enabled_disabled(inst):
    """
    Tests that a dependency with enabled is skipped when the condition
    evaluates to false.

    "enable if dep" depends on dependency(flavor=vanilla) with
    enabled="{{ strat == 'us' }}".  When strat=eu, the dependency should
    be skipped entirely and the run should succeed without waiting.
    """
    client = inst.client

    res = client.schedule("enable if dep", {"date": "2024-01-01", "strat": "eu"})
    run_id = res["run_id"]

    # With strat=eu, enabled evaluates to false.  The dependency is skipped,
    # so the run should proceed immediately to success.
    res = inst.wait_run(run_id, timeout=5)
    assert res["state"] == "success"


def test_enabled_enabled(inst):
    """
    Tests that a dependency with enabled is active when the condition
    evaluates to true.

    "enable if dep" depends on dependency(flavor=vanilla) with
    enabled="{{ strat == 'us' }}".  When strat=us, the dependency should
    be active and the run should wait for it.
    """
    client = inst.client

    res = client.schedule("enable if dep", {"date": "2024-01-01", "strat": "us"})
    run_id = res["run_id"]

    # With strat=us, enabled evaluates to true.  The run should be waiting
    # for dependency(date=2024-01-01, flavor=vanilla).
    res = client.get_run(run_id)
    assert res["state"] == "waiting"

    # Satisfy the dependency.
    dep_res = client.schedule("dependency", {"date": "2024-01-01", "flavor": "vanilla"})
    inst.wait_run(dep_res["run_id"])

    # Now the dependent run should succeed.
    res = inst.wait_run(run_id)
    assert res["state"] == "success"


def test_enabled_both_args(inst):
    """
    Tests enabled with two runs: one where the condition is true and one
    where it is false, running concurrently.
    """
    client = inst.client

    # Schedule both runs.
    res_eu = client.schedule("enable if dep", {"date": "2024-01-01", "strat": "eu"})
    run_eu = res_eu["run_id"]
    res_us = client.schedule("enable if dep", {"date": "2024-01-01", "strat": "us"})
    run_us = res_us["run_id"]

    # EU run should succeed immediately (dependency disabled).
    res = inst.wait_run(run_eu, timeout=5)
    assert res["state"] == "success"

    # US run should still be waiting (dependency enabled).
    res = client.get_run(run_us)
    assert res["state"] == "waiting"

    # Satisfy the US dependency.
    dep_res = client.schedule("dependency", {"date": "2024-01-01", "flavor": "vanilla"})
    inst.wait_run(dep_res["run_id"])

    # US run should now succeed.
    res = inst.wait_run(run_us)
    assert res["state"] == "success"
