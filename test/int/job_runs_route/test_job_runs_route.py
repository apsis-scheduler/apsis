"""
`GET /jobs/<job_id>` and `GET /jobs/<job_id>/runs` over raw HTTP.

Each job has exactly one run, so a response is checked for the right job's
identity, not just its shape.  A percent-encoded `%2Fruns` is job ID data, not
the run history suffix; since `check_job` rejects job IDs ending in `/runs`, such
a request is a lookup of a job that cannot exist, and fails as one.
"""

from contextlib import closing
import json
from pathlib import Path
import pytest
import urllib.error
import urllib.request

from instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"
JOB_ID = "nested/dir/job"
JOB_IDS = (
    JOB_ID,
    "runs",
    "literal%2Fruns",
    "nested/runs/step",
    "special/percent% question? hash# plus+",
)

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


@pytest.mark.parametrize(
    "path",
    [
        f"{JOB_ID}%2Fruns",
        f"{JOB_ID}%2fruns",
        f"{JOB_ID}/%72uns",
    ],
)
def test_encoded_suffix_is_job_id(service, path):
    # A job lookup that fails (404), not a request for JOB_ID's runs (200).
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        get_json(service, f"/api/v1/jobs/{path}")
    assert exc_info.value.code == 404
    assert json.loads(exc_info.value.read())["error"] == f"no job_id {path}"


@pytest.mark.parametrize("job_id", JOB_IDS)
def test_client_job_and_runs(service, run_ids, job_id):
    job = service.client.get_job(job_id)
    assert job["job_id"] == job_id
    assert job["program"]["type"] == "no-op"
    assert "runs" not in job

    runs = service.client.get_job_runs(job_id)
    run_id = run_ids[job_id]
    assert set(runs) == {run_id}
    assert runs[run_id]["job_id"] == job_id


@pytest.mark.parametrize("job_id", JOB_IDS)
def test_cli_job(service, monkeypatch, job_id):
    # The CLI takes its hostname from the environment; always use this service.
    monkeypatch.setenv("APSIS_HOST", f"localhost:{service.port}")
    job = service.run_apsis_json("job", job_id)
    assert job["job_id"] == job_id
    assert job["program"]["type"] == "no-op"
    assert "runs" not in job
