from ora import Time
import pytest
import time

from procstar_instance import ApsisService
from apsis.jobs import jso_to_job, dump_job

# -------------------------------------------------------------------------------

FINISHED = {"success", "failure", "error", "skipped"}


def _job(
    job_id: str,
    command: str,
    *,
    retry: dict | None = None,
    params: list[str] = [],
    group_id: str = "default",
):
    program = {"type": "procstar-shell", "command": command, "group_id": group_id}
    jso = {"params": params, "program": program}
    if retry is not None:
        jso["retry"] = retry
    return jso_to_job(jso, job_id)


def _wait_runs(svc: ApsisService, job_id: str, count: int, timeout: float = 30) -> list[dict]:
    """Waits for `job_id` to have `count` finished runs and returns them in schedule order."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runs = list(svc.client.get_runs(job_id=job_id).values())
        if len(runs) == count and all(r["state"] in FINISHED for r in runs):
            return sorted(runs, key=lambda r: Time(r["times"]["schedule"]))
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for {count} runs of {job_id}")


def _log_messages(svc: ApsisService, run_id: str) -> list[str]:
    return [r["message"] for r in svc.client.get_run_log(run_id)]


def test_retry_failure():
    job = _job("fail", "exit 1", retry={"count": 2, "delay": 1})
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        r0 = svc.client.schedule(job.job_id, {})["run_id"]
        runs = _wait_runs(svc, job.job_id, 3)
        r1, r2 = runs[1]["run_id"], runs[2]["run_id"]

        assert [r["run_id"] for r in runs] == [r0, r1, r2]
        assert all(r["state"] == "failure" for r in runs)
        assert [r["meta"]["retry"] for r in runs] == [
            {"attempt": 0, "count": 2, "pending": True},
            {"attempt": 1, "count": 2, "original": r0, "pending": True},
            {"attempt": 2, "count": 2, "original": r0, "pending": False},
        ]
        for prev, run in zip(runs, runs[1:]):
            delay = Time(run["times"]["schedule"]) - Time(prev["times"]["failure"])
            assert abs(delay - 1) < 0.01

        assert f"retry scheduled as {r1}" in _log_messages(svc, r0)
        assert f"scheduled as rerun of {r0}" in _log_messages(svc, r1)
        assert f"scheduled as rerun of {r1}" in _log_messages(svc, r2)


def test_retry_success(tmp_path):
    # fails the first time and succeeds once the marker exists
    command = "test -e {{ dir }}/marker && exit 0; touch {{ dir }}/marker; exit 1"
    job = _job("flaky", command, retry={"count": 2, "delay": 1}, params=["dir"])
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        r0 = svc.client.schedule(job.job_id, {"dir": tmp_path})["run_id"]
        runs = _wait_runs(svc, job.job_id, 2)

        assert [r["state"] for r in runs] == ["failure", "success"]
        assert runs[1]["args"] == {"dir": str(tmp_path)}
        assert runs[1]["meta"]["retry"] == {"attempt": 1, "count": 2, "original": r0}


def test_no_retry():
    job = _job("fail", "exit 1")
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        svc.client.schedule(job.job_id, {})
        (run,) = _wait_runs(svc, job.job_id, 1)

        assert run["state"] == "failure"
        assert "retry" not in run["meta"]


def test_retry_job_removed():
    job = _job("fail", "sleep 2; exit 1", retry={"count": 2, "delay": 1})
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        run_id = svc.client.schedule(job.job_id, {})["run_id"]
        svc.wait_for_run_to_start(run_id)
        (svc.jobs_dir / "fail.yaml").unlink()
        svc.client.reload_jobs()
        run = svc.wait_run(run_id)

        assert run["state"] == "failure"
        assert "retry" not in run["meta"]
        assert not any(m.startswith("retry") for m in _log_messages(svc, run_id))


def test_retry_reload_restart():
    job = _job("flaky", "exit 1", retry={"count": 2, "delay": 5})
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        r0 = svc.client.schedule(job.job_id, {})["run_id"]
        assert svc.wait_run(r0)["state"] == "failure"
        (r1,) = (r for r in svc.client.get_runs(job_id=job.job_id) if r != r0)

        # the changed job refreshes the scheduled retry and has no retry setting
        dump_job(svc.jobs_dir, _job("flaky", "exit 2"))
        svc.client.reload_jobs()
        svc.restart()

        run = svc.client.get_run(r1)
        assert run["state"] == "scheduled"
        assert run["meta"]["retry"] == {"attempt": 1, "count": 2, "original": r0}
        assert "refreshed from job config" in _log_messages(svc, r1)

        runs = _wait_runs(svc, job.job_id, 2)
        assert runs[1]["meta"]["state_reason"] == "exit code 2"
        assert runs[1]["meta"]["retry"] == {
            "attempt": 1,
            "count": 2,
            "original": r0,
            "pending": False,
        }


def test_retry_error_after_start(tmp_path):
    # runs until the agent restart kills it and succeeds once the marker exists
    command = "test -e {{ dir }}/marker && exit 0; touch {{ dir }}/marker; sleep 30"
    job = _job("lost", command, retry={"count": 1, "delay": 0}, params=["dir"])
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent() as agent:
        r0 = svc.client.schedule(job.job_id, {"dir": tmp_path})["run_id"]
        svc.wait_for_run_to_start(r0)
        # apsis can't reconnect to the run once its agent is gone
        svc.stop_serve()
        agent.restart()
        svc.start_serve()
        svc.wait_for_serve()
        runs = _wait_runs(svc, job.job_id, 2)

        assert [r["state"] for r in runs] == ["error", "success"]
        assert runs[0]["meta"]["retry"] == {"attempt": 0, "count": 1, "pending": True}


@pytest.mark.parametrize("args, group_id", [({"bogus": "1"}, "default"), ({}, "no agents")])
def test_no_retry_error_before_start(args, group_id):
    job = _job("bad", "exit 0", retry={"count": 1, "delay": 0}, group_id=group_id)
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        svc.client.schedule(job.job_id, args)
        (run,) = _wait_runs(svc, job.job_id, 1)

        assert run["state"] == "error"
        assert "retry" not in run["meta"]


def test_no_retry_stopped():
    job = _job("sleep", "sleep 30", retry={"count": 1, "delay": 0})
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        run_id = svc.client.schedule(job.job_id, {})["run_id"]
        svc.wait_for_run_to_start(run_id)
        svc.client.stop_run(run_id)
        (run,) = _wait_runs(svc, job.job_id, 1)

        assert run["state"] == "failure"
        assert run["meta"]["retry"] == {"attempt": 0, "count": 1, "pending": False}


@pytest.mark.parametrize("command", ["sleep 30", "exit 1"])
def test_no_retry_past_stop_time(command):
    # sleep is killed at its stop time and exit fails with its retry due after it
    job = _job("stop", command, retry={"count": 1, "delay": 5})
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        svc.client.schedule(job.job_id, {}, stop_time="+2s")
        (run,) = _wait_runs(svc, job.job_id, 1)

        assert run["state"] == "failure"
        assert run["meta"]["retry"] == {"attempt": 0, "count": 1, "pending": False}


def test_no_retry_marked():
    job = _job("works", "exit 0", retry={"count": 1, "delay": 0})
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        run_id = svc.client.schedule(job.job_id, {})["run_id"]
        assert svc.wait_run(run_id)["state"] == "success"
        svc.client.mark(run_id, "failure")
        (run,) = _wait_runs(svc, job.job_id, 1)

        assert run["state"] == "failure"
        assert "retry" not in run["meta"]
