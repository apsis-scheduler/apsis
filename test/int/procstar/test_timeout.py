import pytest
from time import sleep
from pathlib import Path

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"


@pytest.mark.parametrize("job_name", ["timeout", "timeout-shell"])
def test_timeout(job_name):
    """
    Tests agent program timeout.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        # The program runs for 3 s, so these three runs will time out.
        r0 = client.schedule(job_name, {"timeout": 0, "sleep_duration": 3})["run_id"]
        r1 = client.schedule(job_name, {"timeout": 1, "sleep_duration": 3})["run_id"]
        r2 = client.schedule(job_name, {"timeout": 2, "sleep_duration": 3})["run_id"]
        # These two runs will succeed.
        r4 = client.schedule(job_name, {"timeout": 4, "sleep_duration": 3})["run_id"]
        r5 = client.schedule(job_name, {"timeout": 5, "sleep_duration": 3})["run_id"]

        # Check that r2 is running initially
        res = client.get_run(r2)
        assert res["state"] in ("starting", "running")

        # Wait for timeout runs to fail
        res = svc.wait_run(r0)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == ["SIGTERM"]
        assert res["program"]["timeout"]["duration"] == 0.0

        res = svc.wait_run(r1)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == ["SIGTERM"]
        assert res["program"]["timeout"]["duration"] == 1.0

        res = svc.wait_run(r2)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == ["SIGTERM"]
        assert res["program"]["timeout"]["duration"] == 2.0

        # Wait for success runs to complete
        res = svc.wait_run(r4)
        assert res["state"] == "success"
        assert res["program"]["timeout"]["duration"] == 4.0

        res = svc.wait_run(r5)
        assert res["state"] == "success"
        assert res["program"]["timeout"]["duration"] == 5.0


def test_configured_timeout():
    global_timeout = 1
    timeout_signal = "SIGTERM"
    with (
        ApsisService(
            job_dir=JOB_DIR,
            cfg={"program": {"timeout": {"duration": global_timeout, "signal": timeout_signal}}},
        ) as svc,
        svc.agent(serve=True),
    ):
        client = svc.client
        run_id = client.schedule("sleep", {"time": 10})["run_id"]
        res = client.get_run(run_id)
        assert res["state"] in ("starting", "running")

        res = svc.wait_run(run_id)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == [timeout_signal]
        assert 0.9 < res["meta"]["elapsed"] < 1.1
        run_log = client.get_run_log(run_id)
        assert (
            run_log[-1]["message"]
            == f"failure: killed by {timeout_signal} (timeout after {global_timeout} s)"
        )
        assert "timeout" in res["program"]

    # test timeout is disabled by default if not configured
    with (
        ApsisService(
            job_dir=JOB_DIR,
        ) as svc,
        svc.agent(serve=True),
    ):
        client = svc.client
        run_id = client.schedule("sleep", {"time": 1})["run_id"]
        res = client.get_run(run_id)
        assert res["state"] in ("starting", "running")

        res = svc.wait_run(run_id)
        assert res["state"] == "success"    
        assert res["meta"]["program"]["stop"]["signals"] == []
        assert 0.9 < res["meta"]["elapsed"] < 1.1
        assert "timeout" not in res["program"]


@pytest.mark.parametrize("job_name", ["timeout", "timeout-shell"])
def test_job_timeout_overrides_global_configured_default(job_name):
    timeout_signal = "SIGTERM"
    global_timeout = 5
    job_timeout = 1
    with (
        ApsisService(
            job_dir=JOB_DIR,
            cfg={"program": {"timeout": {"duration": global_timeout}}},
        ) as svc,
        svc.agent(serve=True),
    ):
        client = svc.client
        run_id = client.schedule(job_name, {"timeout": job_timeout, "sleep_duration": 10})["run_id"]
        res = client.get_run(run_id)
        assert res["state"] in ("starting", "running")

        res = svc.wait_run(run_id)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == [timeout_signal]
        assert 0.9 < res["meta"]["elapsed"] < 1.1
        run_log = client.get_run_log(run_id)
        assert (
            run_log[-1]["message"]
            == f"failure: killed by {timeout_signal} (timeout after {job_timeout} s)"
        )


@pytest.mark.parametrize("signal", ["SIGTERM", "SIGKILL", "SIGUSR2"])
def test_signal(signal):
    """
    Tests signal when agent program times out.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        run_id = client.schedule("signal", {"signal": signal})["run_id"]

        # Check that run starts
        assert client.get_run(run_id)["state"] in ("starting", "running")

        # Wait for timeout failure and check signal
        res = svc.wait_run(run_id)
        assert res["state"] == "failure"
        assert res["program"]["timeout"]["duration"] == 1.0
        assert res["meta"]["program"]["stop"]["signals"] == [signal]
        # Check that the process was killed by the expected signal
        assert res["meta"]["program"]["status"]["signal"] == signal


@pytest.mark.parametrize("job_name", ["timeout", "timeout-shell"])
def test_timeout_with_reconnect(job_name):
    """
    Tests that timeout is properly enforced after reconnection.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        timeout = 4
        sleep_duration = 12
        run_id = client.schedule(job_name, {"timeout": timeout, "sleep_duration": sleep_duration})[
            "run_id"
        ]

        # Wait for the run to start
        res = svc.wait_run(run_id, wait_states=("starting",))
        assert res["state"] == "running"

        # Restart Apsis
        svc.restart()

        # Wait for the run to timeout and fail
        res = svc.wait_run(run_id, timeout=sleep_duration + 1)

        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == ["SIGTERM"]
        assert res["program"]["timeout"]["duration"] == timeout

        # Check that actual elapsed time is close to expected timeout
        actual_elapsed = res["meta"]["elapsed"]

        tolerance = 0.3
        assert abs(actual_elapsed - timeout) <= tolerance, (
            f"Elapsed time {actual_elapsed:.3f}s is too far from expected timeout {timeout:.3f}s (tolerance: {tolerance}s)"
        )


@pytest.mark.parametrize("job_name", ["timeout", "timeout-shell"])
def test_timeout_with_delayed_reconnect(job_name):
    """
    Tests that timeout is enforced when Apsis reconnects after being down
    longer than the timeout duration. The run should be killed approximately
    when Apsis comes back up, making the total elapsed time roughly equal
    to the downtime.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        timeout = 3
        sleep_duration = 12
        run_id = client.schedule(job_name, {"timeout": timeout, "sleep_duration": sleep_duration})[
            "run_id"
        ]

        # Start a run that will timeout after restart
        res = svc.wait_run(run_id, wait_states=("starting",))
        assert res["state"] == "running"

        # Stop Apsis for longer than the timeout duration
        svc.stop_serve()
        downtime = 5  # (longer than 3s timeout)
        sleep(downtime)
        svc.start_serve()
        svc.wait_for_serve()

        # Wait for the run to timeout and fail
        res = svc.wait_run(run_id, timeout=sleep_duration + 1)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == ["SIGTERM"]
        assert res["program"]["timeout"]["duration"] == timeout

        # Check that actual elapsed time is roughly equal to the downtime
        # Since Apsis was down for 5s, the job should be killed around that time
        actual_elapsed = res["meta"]["elapsed"]

        # needed because Apsis takes ~1s to restart and reconnect
        tolerance = 1
        assert abs(actual_elapsed - downtime) <= tolerance, (
            f"Elapsed time {actual_elapsed:.3f}s should be close to downtime {downtime}s (tolerance: {tolerance}s). Run should have been killed shortly after Apsis reconnected."
        )
