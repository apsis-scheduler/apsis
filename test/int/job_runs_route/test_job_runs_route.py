"""
`GET /jobs/<job_id>` and `GET /jobs/<job_id>/runs` over raw HTTP.

Both X and X/runs exist, with distinct runs, so fuzzy matching cannot disguise
dispatch to the wrong job.  Encoded suffixes must remain job ID data.
"""

from contextlib import closing
import json
from pathlib import Path
import pytest
import urllib.request

from instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"
JOB_ID = "nested/dir/job"
JOB_IDS = (JOB_ID, f"{JOB_ID}/runs", "runs", "literal%2Fruns", "nested/runs/step")

# -------------------------------------------------------------------------------


def get_json(inst, path):
    with urllib.request.urlopen(f"http://localhost:{inst.port}{path}", timeout=10) as rsp:
        return json.loads(rsp.read())


@pytest.fixture(scope="module")
def service():
    with closing(ApsisService(job_dir=JOB_DIR)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        yield inst


@pytest.fixture(scope="module")
def run_ids(service):
    return {job_id: service.client.schedule(job_id, {}, "now")["run_id"] for job_id in JOB_IDS}


@pytest.mark.parametrize(
    "path, job_id",
    [
        (JOB_ID, JOB_ID),
        ("nested%2Fdir%2Fjob", JOB_ID),
        (f"{JOB_ID}%2Fruns", f"{JOB_ID}/runs"),
        (f"{JOB_ID}%2fruns", f"{JOB_ID}/runs"),
        (f"{JOB_ID}/%72uns", f"{JOB_ID}/runs"),
        ("runs", "runs"),
        ("literal%252Fruns", "literal%2Fruns"),
        ("nested/runs/step", "nested/runs/step"),
    ],
)
def test_job(service, path, job_id):
    job = get_json(service, f"/api/v1/jobs/{path}")
    assert job["job_id"] == job_id
    assert job["program"]["type"] == "no-op"
    assert "runs" not in job


@pytest.mark.parametrize(
    "path, job_id",
    [
        (f"{JOB_ID}/runs", JOB_ID),
        (f"{JOB_ID}/runs?unused=1", JOB_ID),
        ("nested%2Fdir%2Fjob/runs", JOB_ID),
        (f"{JOB_ID}/runs/runs", f"{JOB_ID}/runs"),
        (f"{JOB_ID}%2Fruns/runs", f"{JOB_ID}/runs"),
        (f"{JOB_ID}%2fruns/runs", f"{JOB_ID}/runs"),
        ("runs/runs", "runs"),
        ("literal%252Fruns/runs", "literal%2Fruns"),
        ("nested/runs/step/runs", "nested/runs/step"),
    ],
)
def test_job_runs(service, run_ids, path, job_id):
    res = get_json(service, f"/api/v1/jobs/{path}")
    run_id = run_ids[job_id]
    assert set(res["runs"]) == {run_id}
    assert res["runs"][run_id]["job_id"] == job_id
