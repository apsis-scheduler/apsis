"""
`GET /jobs/<job_id>` and `GET /jobs/<job_id>/runs` over raw HTTP, for a job ID
that contains slashes.

Before the fix in `api.job`, `/jobs/X/runs` resolved to either the job handler
or the job-runs handler depending on how sanic happened to order the two
overlapping path routes in this process, so this test passed or failed by
process.
"""

from contextlib import closing
import json
from pathlib import Path
import urllib.request

from instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"
JOB_ID = "nested/dir/job"

# -------------------------------------------------------------------------------


def get_json(inst, path):
    with urllib.request.urlopen(f"http://localhost:{inst.port}{path}", timeout=10) as rsp:
        return json.loads(rsp.read())


def test_job_and_job_runs():
    with closing(ApsisService(job_dir=JOB_DIR)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        run_id = inst.client.schedule(JOB_ID, {}, "now")["run_id"]

        # The job itself.
        job = get_json(inst, f"/api/v1/jobs/{JOB_ID}")
        assert job["job_id"] == JOB_ID
        assert job["program"]["type"] == "no-op"
        assert "runs" not in job

        # Its runs.
        res = get_json(inst, f"/api/v1/jobs/{JOB_ID}/runs")
        assert set(res["runs"]) == {run_id}
        assert res["runs"][run_id]["job_id"] == JOB_ID
