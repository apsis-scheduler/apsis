from contextlib import closing
import pytest
import sqlite3
import subprocess
import ujson

from apsis.service.client import APIError
from instance import ApsisService

# -------------------------------------------------------------------------------

JOB = "program:\n  type: no-op\n"
OLD_JOB_ID = "team/report"
NEW_JOB_ID = "team/sub/report"


@pytest.fixture
def inst(tmp_path):
    job_dir = tmp_path / "jobs"
    (job_dir / "team").mkdir(parents=True)
    (job_dir / "team/report.yaml").write_text(JOB)
    (job_dir / "team/other.yaml").write_text(JOB)
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def move_job(inst):
    (inst.jobs_dir / "team/sub").mkdir()
    (inst.jobs_dir / "team/report.yaml").rename(inst.jobs_dir / "team/sub/report.yaml")
    inst.client.reload_jobs()


def test_rename_job_history(inst):
    client = inst.client
    run_ids = [client.schedule(OLD_JOB_ID, {})["run_id"] for _ in range(2)]
    for run_id in run_ids:
        assert inst.wait_run(run_id)["state"] == "success"
    later_id = client.schedule(OLD_JOB_ID, {}, time="2100-01-01T00:00:00Z")["run_id"]
    other_id = client.schedule("team/other", {})["run_id"]
    inst.wait_run(other_id)

    move_job(inst)

    # the run scheduled for later still belongs to the old job
    with pytest.raises(APIError, match="unfinished runs") as exc:
        client.rename_job_history(OLD_JOB_ID, NEW_JOB_ID)
    assert exc.value.status == 400

    client.skip(later_id)
    run_ids.append(later_id)
    proc = subprocess.run(
        ["apsisctl", "--port", str(inst.port), "rename-job-history", OLD_JOB_ID, NEW_JOB_ID],
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    assert "moved 3 runs" in proc.stdout

    assert set(client.get_job_runs(NEW_JOB_ID)) == set(run_ids)
    assert client.get_run(other_id)["job_id"] == "team/other"
    with closing(sqlite3.connect(inst.db_path)) as db:
        rows = db.execute("SELECT run_id, payload FROM run_summary").fetchall()
    job_ids = {r: ujson.loads(p)["run_summary"]["job_id"] for r, p in rows}
    assert job_ids == {r: NEW_JOB_ID for r in run_ids} | {other_id: "team/other"}


def test_rename_job_history_checks_jobs(inst):
    with pytest.raises(APIError, match="job still exists") as exc:
        inst.client.rename_job_history(OLD_JOB_ID, "team/other")
    assert exc.value.status == 400

    move_job(inst)
    with pytest.raises(APIError, match="unknown job ID") as exc:
        inst.client.rename_job_history(OLD_JOB_ID, "team/missing")
    assert exc.value.status == 404
