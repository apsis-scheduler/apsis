from contextlib import closing, contextmanager
import ora
import os
from pathlib import Path
import pytest
import shutil
import signal
import subprocess
from time import sleep

from instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def is_litestream_available():
    return shutil.which("litestream") is not None


@contextmanager
def litestream_replica(db_path, replica_path):
    proc = subprocess.Popen(
        [
            "litestream",
            "replicate",
            db_path,
            f"file://{replica_path}",
        ]
    )
    try:
        yield proc
    finally:
        # Give Litestream a second to do one last sync.
        sleep(1)
        proc.terminate()


def restore_litestream_db(restored_db_path, replica_path):
    subprocess.run(
        [
            "litestream",
            "restore",
            "-o",
            str(restored_db_path),
            f"file://{replica_path}",
        ],
        check=True,
    )


@pytest.mark.skipif(not is_litestream_available(), reason="Litestream is not available")
def test_replica():
    """
    Tests Litestream replica of the SQLite db.

    Steps:
    - start Litestream process;
    - start Apsis and populate its db with some data;
    - stop Apsis (i.e. simulating a db failure) and Litestream processes;
    - restore db from Litestream replica;
    - check that Apsis works fine with the restored db and that data initially written to the original db are there.
    """
    with closing(ApsisService(job_dir=JOB_DIR)) as inst:
        inst.create_db()
        inst.write_cfg()
        client = inst.client

        # start Litestream replica
        replica_path = inst.tmp_dir / "litestream_replica.db"

        with litestream_replica(inst.db_path, replica_path):
            inst.start_serve()
            inst.wait_for_serve()

            # populate apsis db with some data
            run_ids = {
                s: client.schedule("no-op", {})["run_id"]
                for s in ["success", "failure", "error", "skipped"]
            }
            assert all(inst.wait_run(r)["state"] == "success" for r in run_ids.values())
            for state, run_id in run_ids.items():
                if state != "success":
                    client.mark(run_id, state)

            # stop Apsis and Litestream
            inst.stop_serve()

        # restore the db from the replica
        restored_db_name = "restored.db"
        restored_db_path = inst.tmp_dir / restored_db_name

        restore_litestream_db(restored_db_path, replica_path)

        # rewrite the config to use restored db
        inst.db_path = restored_db_path
        inst.write_cfg()

        # restart Apsis
        inst.start_serve()
        inst.wait_for_serve()

        log = inst.get_log_lines()
        assert any(restored_db_name in l for l in log)

        # check runs data is accurate in the restored db
        for state, run_id in run_ids.items():
            assert client.get_run(run_id)["state"] == state

        # run job again to verify the db is in a good state and Apsis can read/write it
        new_run_id = client.schedule("no-op", {})["run_id"]
        inst.wait_run(new_run_id)
        assert client.get_run(new_run_id)["state"] == "success"

        inst.stop_serve()


@pytest.mark.skipif(not is_litestream_available(), reason="Litestream is not available")
def test_replica_killing_apsis_and_litestream():
    """
    Similar to `test_replica`, but here:
    - Apsis and Litestream processes are terminated with SIGKILL signal;
    - Apsis is killed while there is still a run in the running state;

    Steps:
    - start Litestream process;
    - start Apsis and schedule a run;
    - kill Apsis and Litestream using SIGKILL signals;
    - restore db from Litestream replica;
    - check that the run is still in running state using the restored db.
    """
    with closing(ApsisService(job_dir=JOB_DIR)) as inst:
        inst.create_db()
        inst.write_cfg()

        # start Litestream replica
        replica_path = inst.tmp_dir / "litestream_replica.db"

        with litestream_replica(inst.db_path, replica_path) as litestream_proc:
            inst.start_serve()
            inst.wait_for_serve()

            client = inst.client
            run_id = client.schedule("sleep", {"duration": "4"})["run_id"]

            # kill Apsis and Litestream
            sleep(1)
            os.kill(inst.srv_proc.pid, signal.SIGKILL)
            sleep(1)
            os.kill(litestream_proc.pid, signal.SIGKILL)

        # restore the db from the replica
        restored_db_name = "restored.db"
        restored_db_path = inst.tmp_dir / restored_db_name
        restore_litestream_db(restored_db_path, replica_path)

        # rewrite the config to use restored db
        inst.db_path = restored_db_path
        inst.write_cfg()

        # restart Apsis
        inst.srv_proc = None
        inst.start_serve()
        inst.wait_for_serve()

        log = inst.get_log_lines()
        assert any(restored_db_name in l for l in log)

        # check the run is still in the running state after reconnecting to the new Apsis instance
        assert client.get_run(run_id)["state"] == "running"

        # check run eventually completes successfully
        inst.wait_run(run_id)
        assert client.get_run(run_id)["state"] == "success"

        inst.stop_serve()


@pytest.mark.skipif(not is_litestream_available(), reason="Litestream is not available")
def test_replica_killing_apsis_and_litestream_with_heavy_load():
    """
    Very similar to `test_replica_killing_apsis_and_litestream`, but here a much heavier runs load is used.
    """
    num_jobs = 500
    with closing(ApsisService(job_dir=JOB_DIR)) as inst:
        inst.create_db()
        inst.write_cfg()

        # start Litestream replica
        replica_path = inst.tmp_dir / "litestream_replica.db"

        with litestream_replica(inst.db_path, replica_path) as litestream_proc:
            inst.start_serve()
            inst.wait_for_serve()

            client = inst.client
            # populate apsis db with a large number of runs
            runs_ids = [
                client.schedule("sleep", {"duration": "12"})["run_id"] for _ in range(num_jobs)
            ]
            sched_runs_ids = [
                client.schedule("sleep", {"duration": "1"}, time=ora.now() + 14)["run_id"]
                for _ in range(num_jobs)
            ]

            runs = [client.get_run(r) for r in runs_ids]
            sched_runs = [client.get_run(r) for r in sched_runs_ids]
            assert all(run["state"] == "running" for run in runs)
            assert all(run["state"] == "scheduled" for run in sched_runs)

            # kill Apsis and Litestream
            sleep(1)
            os.kill(inst.srv_proc.pid, signal.SIGKILL)
            sleep(1)
            os.kill(litestream_proc.pid, signal.SIGKILL)

        # restore the db from the replica
        restored_db_name = "restored.db"
        restored_db_path = inst.tmp_dir / restored_db_name
        restore_litestream_db(restored_db_path, replica_path)

        # rewrite the config to use restored db
        inst.db_path = restored_db_path
        inst.write_cfg()

        # restart Apsis
        inst.srv_proc = None
        inst.start_serve()
        inst.wait_for_serve()

        log = inst.get_log_lines()
        assert any(restored_db_name in l for l in log)
        sleep(1)

        # check runs are still in the right state after reconnecting to the new Apsis instance
        runs = [client.get_run(r) for r in runs_ids]
        sched_runs = [client.get_run(r) for r in sched_runs_ids]
        assert all(run["state"] == "running" for run in runs)
        assert all(run["state"] == "scheduled" for run in sched_runs)

        # check all runs eventually complete successfully
        all_runs_results = [inst.wait_run(r) for r in runs_ids + sched_runs_ids]
        assert all(run["state"] == "success" for run in all_runs_results)

        inst.stop_serve()
