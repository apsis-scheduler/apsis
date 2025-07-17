import brotli
from contextlib import closing
from pathlib import Path
import pytest
import sqlite3
from time import sleep

from instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


@pytest.fixture
def client(inst, scope="function"):
    return inst.client


# -------------------------------------------------------------------------------


def test_output_basic(client):
    res = client.schedule("printf", {"string": "hello\n"})
    run_id = res["run_id"]
    # FIXME
    sleep(0.5)
    res = client.get_run(run_id)
    assert res["state"] == "success"

    outputs = client.get_outputs(run_id)
    assert len(outputs) == 1
    assert outputs[0]["output_id"] == "output"  # combined stdout and stderr
    assert outputs[0]["output_len"] == 6

    output = client.get_output(run_id, "output")
    assert output == b"hello\n"


def test_large_output_compression(inst):
    client = inst.client
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
    with sqlite3.connect(inst.db_path) as conn:
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
