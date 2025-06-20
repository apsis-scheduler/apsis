import pytest
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
        r0 = client.schedule(job_name, {"timeout": 0})["run_id"]
        r1 = client.schedule(job_name, {"timeout": 1})["run_id"]
        r2 = client.schedule(job_name, {"timeout": 2})["run_id"]
        # These two runs will succeed.
        r4 = client.schedule(job_name, {"timeout": 4})["run_id"]
        r5 = client.schedule(job_name, {"timeout": 5})["run_id"]

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
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        # Start a run that will timeout after restart
        run_id = client.schedule(job_name, {"timeout": 2})["run_id"]

        # Wait for the run to start
        res = svc.wait_run(run_id, wait_states=("starting",))
        assert res["state"] == "running"

        # Restart Apsis
        svc.restart()

        # Wait for the run to timeout and fail
        res = svc.wait_run(run_id, timeout=10)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["signals"] == ["SIGTERM"]
        assert res["program"]["timeout"]["duration"] == 2.0
