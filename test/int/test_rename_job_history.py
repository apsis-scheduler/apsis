from contextlib import closing
import pytest
import requests
import sqlite3
import subprocess
import ujson
from websockets.sync.client import connect

from apsis.service.client import APIError
from instance import ApsisService

# -------------------------------------------------------------------------------

JOB = "program:\n  type: no-op\n"
DOWNSTREAM_JOB = """
condition:
  type: dependency
  job_id: team/sub/report
program:
  type: no-op
"""
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


def move_job(inst, new_job=JOB):
    (inst.jobs_dir / "team/sub").mkdir()
    (inst.jobs_dir / "team/report.yaml").unlink()
    (inst.jobs_dir / "team/sub/report.yaml").write_text(new_job)
    inst.client.reload_jobs()


def test_rename_job_history(inst):
    client = inst.client
    run_ids = [client.schedule(OLD_JOB_ID, {})["run_id"] for _ in range(2)]
    for run_id in run_ids:
        assert inst.wait_run(run_id)["state"] == "success"
    later_id = client.schedule(OLD_JOB_ID, {}, time="2100-01-01T00:00:00Z")["run_id"]
    other_id = client.schedule("team/other", {})["run_id"]
    inst.wait_run(other_id)

    (inst.jobs_dir / "team/downstream.yaml").write_text(DOWNSTREAM_JOB)
    move_job(inst)
    down_id = client.schedule("team/downstream", {})["run_id"]
    assert inst.wait_run(down_id, wait_states=("new", "scheduled"))["state"] == "waiting"

    # the run scheduled for later still belongs to the old job
    with pytest.raises(APIError, match="unfinished runs") as exc:
        client.rename_job_history(OLD_JOB_ID, NEW_JOB_ID)
    assert exc.value.status == 400

    client.skip(later_id)
    run_ids.append(later_id)
    with connect(f"ws://localhost:{inst.port}/api/v1/summary") as ws:
        proc = subprocess.run(
            ["apsisctl", "--port", str(inst.port), "rename-job-history", OLD_JOB_ID, NEW_JOB_ID],
            stdout=subprocess.PIPE,
            text=True,
            check=True,
        )
        assert "moved 3 runs" in proc.stdout

        # an open web ui gets the moved runs under the new job
        ws_job_ids = {}
        while not set(run_ids) <= set(ws_job_ids):
            for msg in ujson.loads(ws.recv(timeout=5)):
                if msg["type"] == "run_summary":
                    ws_job_ids[msg["run_summary"]["run_id"]] = msg["run_summary"]["job_id"]
    assert {r: ws_job_ids[r] for r in run_ids} == {r: NEW_JOB_ID for r in run_ids}

    assert set(client.get_job_runs(NEW_JOB_ID)) == set(run_ids)
    assert client.get_run(other_id)["job_id"] == "team/other"
    # the downstream run wakes up on the moved successes
    assert inst.wait_run(down_id)["state"] == "success"

    with closing(sqlite3.connect(inst.db_path)) as db:
        rows = db.execute("SELECT run_id, payload FROM run_summary").fetchall()
    db_job_ids = {r: ujson.loads(p)["run_summary"]["job_id"] for r, p in rows}
    assert {r: db_job_ids[r] for r in run_ids + [other_id]} == {r: NEW_JOB_ID for r in run_ids} | {
        other_id: "team/other"
    }


def test_rename_job_history_checks_jobs(inst):
    inst.wait_run(inst.client.schedule(OLD_JOB_ID, {})["run_id"])
    with pytest.raises(APIError, match="job still exists") as exc:
        inst.client.rename_job_history(OLD_JOB_ID, "team/other")
    assert exc.value.status == 400

    move_job(inst, new_job="params: [date]\n" + JOB)
    with pytest.raises(APIError, match="unknown job ID") as exc:
        inst.client.rename_job_history(OLD_JOB_ID, "team/missing")
    assert exc.value.status == 404

    # a typo in the old job id moves nothing
    with pytest.raises(APIError, match="no runs of job") as exc:
        inst.client.rename_job_history("team/reprot", NEW_JOB_ID)
    assert exc.value.status == 404

    with pytest.raises(APIError, match=r"has args \[\] but team/sub/report has params \['date'\]"):
        inst.client.rename_job_history(OLD_JOB_ID, NEW_JOB_ID)

    resp = requests.post(f"http://localhost:{inst.port}/api/control/rename_job_history", json={})
    assert resp.status_code == 400
    assert resp.json()["error"] == "old_job_id and new_job_id are required"
