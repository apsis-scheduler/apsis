import logging
from contextlib import closing
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

        db_path = inst.db_path
        wal_path = Path(str(db_path) + "-wal")
        shm_path = Path(str(db_path) + "-shm")

        run_ids = [
            inst.client.schedule("sleep", {"time": 2})["run_id"] for _ in range(8)
        ]
        for run_id in run_ids:
            res = inst.wait_run(run_id, wait_states=("new", "starting"))
            assert res["state"] == "running"

        logging.info("restarting")
        return_code = inst.stop_serve()
        from time import sleep

        print(inst.db_path)
        sleep(30)
        assert (
            return_code == 0
        ), f"Service should stop cleanly, got return code {return_code}"

        assert not inst.is_running(), "Service should be stopped"

        assert (
            not wal_path.exists()
        ), f"WAL file at {wal_path} should be removed after Apsis stops"
        assert (
            not shm_path.exists()
        ), f"SHM file at {shm_path} should be removed after Apsis stops"
