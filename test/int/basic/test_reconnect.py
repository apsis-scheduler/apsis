from contextlib import closing
import logging
from pathlib import Path

from instance import ApsisService

# -------------------------------------------------------------------------------


def test_reconnect(tmpdir):
    job_dir = Path(__file__).parent / "jobs"
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        run_ids = [inst.client.schedule("sleep", {"time": 2})["run_id"] for _ in range(8)]
        for run_id in run_ids:
            res = inst.wait_run(run_id, wait_states=("new", "starting"))
            assert res["state"] == "running"

        logging.info("restarting")
        inst.stop_serve()
        inst.start_serve()
        inst.wait_for_serve()
        logging.info("restarted")

        for run_id in run_ids:
            res = inst.client.get_run(run_id)
            assert res["state"] == "running"
        for run_id in run_ids:
            res = inst.wait_run(run_id)
            assert res["state"] == "success"
