from pathlib import Path
import time

from instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def test_stop_basic():
    with ApsisService(job_dir=JOB_DIR) as inst:
        run_id = inst.client.schedule("sleep", {"time": "5"}, stop_time="+1s")["run_id"]
        res = inst.wait_run(run_id, wait_states=("new", "scheduled", "waiting", "starting"))
        assert res["state"] == "running"

        res = inst.wait_run(run_id)
        assert res["state"] == "failure"
        assert "stopping" in res["times"]
        assert res["meta"]["elapsed"] < 2


def test_stop_api():
    with ApsisService(job_dir=JOB_DIR) as inst:
        run_id = inst.client.schedule(
            "sleep",
            {"time": "5"},
        )["run_id"]
        res = inst.wait_run(run_id, wait_states=("new", "scheduled", "waiting", "starting"))
        assert res["state"] == "running"

        time.sleep(0.5)
        res = inst.client.get_run(run_id)
        assert res["state"] == "running"
        res = inst.client.stop_run(run_id)
        assert res["state"] == "stopping"

        res = inst.wait_run(run_id)
        assert res["state"] == "failure"
        assert "stopping" in res["times"]
        assert res["meta"]["elapsed"] < 2


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG)
    test_stop_api()
