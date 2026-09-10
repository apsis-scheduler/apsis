from contextlib import closing
from pathlib import Path
import pytest
import sqlite3

from instance import ApsisService, run_apsisctl

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


def test_schedule_cli_arg_syntax(inst):
    """
    The `apsis schedule` CLI splits each NAME=VAL on the first "=": the name
    can't contain "=" but the value can, and an arg without "=" is rejected.
    """
    ret, _ = inst.run_apsis_cmd("schedule", "now", "print time", "color=gr=een", "exit=0", "a=b=c")
    assert ret == 0

    runs = inst.client.get_runs(job_id="print time")
    (run,) = [r for r in runs.values() if "a" in r["args"]]
    assert run["args"] == {"color": "gr=een", "exit": "0", "a": "b=c"}
    # "a" isn't a param of the job.
    assert run["state"] == "error"
    log = inst.client.get_run_log(run["run_id"])
    assert any("extra args (a)" in r["message"] for r in log)

    ret, _ = inst.run_apsis_cmd("schedule", "now", "print time", "color", "exit=0")
    assert ret != 0


def test_stop_serve(inst):
    ret = inst.stop_serve()
    assert ret == 0
