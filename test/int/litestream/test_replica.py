from pathlib import Path
from contextlib import closing
import subprocess
from time import sleep

from instance import ApsisService


JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def test_replica():
    """
    Tests Litestream replica of the SQLite database.
    Steps:
    - start Litestream process;
    - start Apsis and populate its database with some data;
    - stop Apsis (i.e. simulating a failure) and Litestream processes;
    - restore database from Litestream replica;
    - check that Apsis works fine with the restored db and that data initially written to the original db are there.
    """

    with closing(ApsisService(job_dir=JOB_DIR)) as inst:

        inst.create_db()
        inst.write_cfg()

        # start Litestream replica
        litestream_replica_path = inst.tmp_dir / "litestream_replica.db"
        litestream_process = subprocess.Popen(
            [
                "litestream",
                "replicate",
                inst.db_path,
                f"file://{str(litestream_replica_path)}",
            ]
        )

        inst.start_serve()
        inst.wait_for_serve()

        # populate apsis db with some data
        client = inst.client
        r1 = client.schedule("job1", {})["run_id"]
        inst.wait_run(r1)

        # stop Apsis and Litestream
        inst.stop_serve()
        litestream_process.terminate()

        # restore the database from the replica
        restored_db_name = "restored.db"
        restored_db_path = inst.tmp_dir / restored_db_name
        subprocess.run(
            [
                "litestream",
                "restore",
                "-o",
                str(restored_db_path),
                f"file://{str(litestream_replica_path)}",
            ],
            check=True,
        )

        # rewrite the config to use restored database
        inst.db_path = restored_db_path
        inst.write_cfg()

        # restart Apsis
        inst.start_serve()
        inst.wait_for_serve()

        log = inst.get_log_lines()
        assert any(restored_db_name in l for l in log)

        # check r1 is there
        client = inst.client
        assert client.get_run(r1)["state"] == "success"
        # assert len(agent.client.get_procs()) == 0

        # run job2 to verify the database is in a good state and Apsis can read/write it.
        r2 = client.schedule("job2", {})["run_id"]
        inst.wait_run(r2)
        assert inst.client.get_run(r2)["state"] == "success"

        inst.stop_serve()
        litestream_process.terminate()
        litestream_process.wait()
        # TODO: consider using a config file for Litestream
