import brotli
from pathlib import Path
import sqlite3
from time import sleep

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def test_large_output_compression_procstar():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        client = svc.client
        res = client.schedule("large output", {"size": 1048576})
        run_id = res["run_id"]
        # FIXME
        sleep(2)
        res = client.get_run(run_id)
        assert res["state"] == "success"

        # Get the output from the client.
        output = client.get_output(run_id, "output")
        assert len(output) == 1048576

        # Examine the output directly in the database.
        with sqlite3.connect(svc.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT length, compression, data
                FROM output
                WHERE run_id = ?
                """,
                (run_id,),
            )
            rows = list(cursor)
            assert len(rows) == 1
            ((length, compression, data),) = rows
            # Compressed output should have been stored in the output table.
            assert length == 1048576
            assert compression == "br"
            assert len(data) < 1024  # compresses real nice
            assert len(brotli.decompress(data)) == 1048576
