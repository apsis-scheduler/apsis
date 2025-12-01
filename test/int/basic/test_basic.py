from contextlib import closing
from pathlib import Path
import pytest
import sqlite3

from procstar_instance import ApsisService
from instance import run_apsisctl

# -------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def inst():
    job_dir = Path(__file__).parent / "jobs"
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def test_create_db(tmpdir):
    db_path = Path(tmpdir) / "apsis.db"
    run_apsisctl("create", db_path)
    assert db_path.is_file()
    with sqlite3.connect(db_path) as db:
        with closing(db.cursor()) as cursor:
            cursor.execute("SELECT * FROM runs")
            names = {d[0] for d in cursor.description}
            assert "run_id" in names
            assert len(list(cursor)) == 0


def test_jobs(inst):
    jobs = inst.run_apsis_json("jobs")
    assert len(jobs) > 0

    job_ids = {j["job_id"] for j in jobs}
    assert "job1" in job_ids


def test_jobs_exact_match(inst):
    ret, out = inst.run_apsis_cmd("job", "match pre")
    # Ambiguous: matches "match prefix" and "match prefix suffix".
    assert ret != 0

    ret, out = inst.run_apsis_cmd("job", "match prefix")
    # Exact match despite additional prefix match.
    assert ret == 0


def test_stop_serve(inst):
    ret = inst.stop_serve()
    assert ret == 0
