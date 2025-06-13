from pathlib import Path

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"


# -------------------------------------------------------------------------------
def test_timeout():
    """
    Tests procstar program timeout.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        # The program runs for 3 s, so these three runs will time out.
        r0 = client.schedule("timeout", {"timeout": 0})["run_id"]
        r1 = client.schedule("timeout", {"timeout": 1})["run_id"]
        r2 = client.schedule("timeout", {"timeout": 2})["run_id"]
        # These two runs will succeed.
        r4 = client.schedule("timeout", {"timeout": 4})["run_id"]
        r5 = client.schedule("timeout", {"timeout": 5})["run_id"]

        # Check that r2 is running initially
        res = client.get_run(r2)
        assert res["state"] in ("starting", "running")

        # Wait for timeout runs to fail
        res = svc.wait_run(r0)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True  # Fixed path
        assert res["program"]["timeout"]["duration"] == 0.0

        res = svc.wait_run(r1)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True  # Fixed path
        assert res["program"]["timeout"]["duration"] == 1.0

        res = svc.wait_run(r2)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True  # Fixed path
        assert res["program"]["timeout"]["duration"] == 2.0

        # Wait for success runs to complete
        res = svc.wait_run(r4)
        assert res["state"] == "success"
        assert res["meta"]["program"]["stop"]["timed_out"] == False  # Fixed path
        assert res["program"]["timeout"]["duration"] == 4.0

        res = svc.wait_run(r5)
        assert res["state"] == "success"
        assert res["meta"]["program"]["stop"]["timed_out"] == False  # Fixed path
        assert res["program"]["timeout"]["duration"] == 5.0


def test_timeout_signal():
    """
    Tests signal when procstar program times out.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        r0 = client.schedule("signal", {"signal": "SIGTERM"})["run_id"]
        r1 = client.schedule("signal", {"signal": "SIGKILL"})["run_id"]
        r2 = client.schedule("signal", {"signal": "SIGUSR2"})["run_id"]

        # Check that runs start
        assert client.get_run(r0)["state"] in ("starting", "running")
        assert client.get_run(r1)["state"] in ("starting", "running")
        assert client.get_run(r2)["state"] in ("starting", "running")

        # Wait for timeout failures and check signals
        res = svc.wait_run(r0)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True
        assert res["program"]["timeout"]["duration"] == 1.0
        assert res["program"]["timeout"]["signal"] == "SIGTERM"
        # Check that the process was killed by the expected signal
        assert res["meta"]["program"]["status"]["signal"] == "SIGTERM"

        res = svc.wait_run(r1)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True
        assert res["program"]["timeout"]["duration"] == 1.0
        assert res["program"]["timeout"]["signal"] == "SIGKILL"
        assert res["meta"]["program"]["status"]["signal"] == "SIGKILL"

        res = svc.wait_run(r2)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True
        assert res["program"]["timeout"]["duration"] == 1.0
        assert res["program"]["timeout"]["signal"] == "SIGUSR2"
        assert res["meta"]["program"]["status"]["signal"] == "SIGUSR2"


def test_timeout_with_reconnect():
    """
    Tests that timeout works correctly after Apsis restart/reconnect.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        # Start a run that will timeout after restart
        run_id = client.schedule("timeout", {"timeout": 2})["run_id"]

        # Wait for the run to start
        res = svc.wait_run(run_id, wait_states=("starting",))
        assert res["state"] == "running"

        # Restart Apsis
        svc.restart()

        # Wait for the run to timeout and fail
        res = svc.wait_run(run_id, timeout=10)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True
        assert res["program"]["timeout"]["duration"] == 2.0


def test_timeout_shell_program():
    """
    Tests timeout with procstar shell programs.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        # Test timeout
        r1 = svc.client.schedule("timeout-shell", {"timeout": 1})["run_id"]
        r4 = svc.client.schedule("timeout-shell", {"timeout": 4})["run_id"]

        # Timeout case
        res = svc.wait_run(r1)
        assert res["state"] == "failure"
        assert res["meta"]["program"]["stop"]["timed_out"] == True
        assert res["program"]["timeout"]["duration"] == 1.0

        # Success case
        res = svc.wait_run(r4)
        assert res["state"] == "success"
        assert res["meta"]["program"]["stop"]["timed_out"] == False
        assert res["program"]["timeout"]["duration"] == 4.0
