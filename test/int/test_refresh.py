"""
Tests refreshing a waiting run with the apsis command after a jobs reload.
"""

from instance import ApsisService

# -------------------------------------------------------------------------------

JOB = """
program:
  type: no-op
  success: {success}
condition:
  type: const
  value: {ready}
"""


def write_job(job_dir, *, success: bool, ready: bool) -> None:
    text = JOB.format(success=str(success).lower(), ready=str(ready).lower())
    (job_dir / "job.yaml").write_text(text)


def test_refresh_waiting_run(tmp_path):
    write_job(tmp_path, success=False, ready=False)
    with ApsisService(job_dir=tmp_path) as inst:
        client = inst.client
        run_id = client.schedule("job", {})["run_id"]

        # refreshing with the same config leaves the run waiting as it was
        assert inst.run_apsis_cmd("refresh", run_id)[0] == 0
        run = client.get_run(run_id)
        assert run["state"] == "waiting"
        assert not run["program"]["success"]

        # reload alone doesn't change a waiting run
        write_job(tmp_path, success=True, ready=True)
        client.reload_jobs()
        assert not client.get_run(run_id)["program"]["success"]

        assert inst.run_apsis_cmd("refresh", run_id)[0] == 0
        assert inst.wait_run(run_id)["state"] == "success"
        assert "refreshed from job config" in [r["message"] for r in client.get_run_log(run_id)]

        assert inst.run_apsis_cmd("refresh", run_id)[0] != 0
        assert inst.run_apsis_cmd("refresh", "r999999")[0] != 0
