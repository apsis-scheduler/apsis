from contextlib import closing
from pathlib import Path
import pytest

from procstar_instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        with inst.agent(serve=True):
            yield inst


def test_timeout(inst):
    """
    Tests agent program timeout.
    """
    client = inst.client
    # The program runs for 3 s, so these three runs will time out.
    r0 = client.schedule("timeout", {"timeout": 0})["run_id"]
    r1 = client.schedule("timeout", {"timeout": 1})["run_id"]
    r2 = client.schedule("timeout", {"timeout": 2})["run_id"]
    # These two runs will succeed.
    r4 = client.schedule("timeout", {"timeout": 4})["run_id"]
    r5 = client.schedule("timeout", {"timeout": 5})["run_id"]

    res = inst.wait_run(r0)
    res = client.get_run(r2)["state"] == "running"

    res = inst.wait_run(r1)
    assert res["state"] == "failure"
    res = inst.wait_run(r4)
    assert res["state"] == "success"
    assert inst.wait_run(r2)["state"] == "failure"
    res = inst.wait_run(r5)
    assert res["state"] == "success"


def test_signal(inst):
    """
    Tests signal when agent program times out.
    """
    client = inst.client
    r0 = client.schedule("signal", {"signal": "TERM"})["run_id"]
    r1 = client.schedule("signal", {"signal": "SIGKILL"})["run_id"]
    r2 = client.schedule("signal", {"signal": 12})["run_id"]  # SIGUSR2

    assert client.get_run(r0)["state"] in ("starting", "running")
    assert client.get_run(r1)["state"] in ("starting", "running")
    assert client.get_run(r2)["state"] in ("starting", "running")

    res = inst.wait_run(r0)
    assert res["state"] == "failure"
    assert res["meta"]["program"]["signal"] == "SIGTERM"
    res = inst.wait_run(r1)
    assert res["state"] == "failure"
    assert res["meta"]["program"]["signal"] == "SIGKILL"
    res = inst.wait_run(r2)
    assert res["state"] == "failure"
    assert res["meta"]["program"]["signal"] == "SIGUSR2"
