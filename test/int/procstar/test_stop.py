from   pathlib import Path
import time

from   procstar_instance import ApsisService
from   apsis.jobs import jso_to_job, dump_job

#-------------------------------------------------------------------------------

SLEEP_JOB = jso_to_job({
    "params": ["time"],
    "program": {
        "type": "procstar",
        "argv": ["/usr/bin/sleep", "{{ time }}"],
    }
}, "sleep")

IGNORE_TERM_PATH = Path(__file__).parent / "ignore-term"
IGNORE_TERM_JOB = jso_to_job({
    "params": ["time"],
    "program": {
        "type": "procstar",
        "argv": [IGNORE_TERM_PATH, "{{ time }}"],
        "stop": {
            "grace_period": 2,
        },
    }
}, "ignore term")

def test_stop():
    svc = ApsisService()
    dump_job(svc.jobs_dir, SLEEP_JOB)
    with svc, svc.agent():
        # Schedule a 3 sec job but tell Apsis to stop it after 1 sec.
        run_id = svc.client.schedule(
            SLEEP_JOB.job_id, {"time": "3"},
            stop_time="+1s",
        )["run_id"]
        res = svc.wait_run(run_id)

        # The run was successfully stopped by Apsis, by sending it SIGTERM.
        assert res["state"] == "success"
        meta = res["meta"]["program"]
        assert meta["status"]["signal"] == "SIGTERM"
        assert meta["stop"]["signals"] == ["SIGTERM"]
        assert meta["times"]["elapsed"] < 2


def test_dont_stop():
    svc = ApsisService()
    dump_job(svc.jobs_dir, IGNORE_TERM_JOB)
    with svc, svc.agent():
        # Schedule a 1 sec run but tell Apsis to stop it after 3 sec.
        run_id = svc.client.schedule(
            IGNORE_TERM_JOB.job_id, {"time": "1"},
            stop_time="+3s"
        )["run_id"]
        res = svc.wait_run(run_id)

        assert res["state"] == "success"
        meta = res["meta"]["program"]
        assert meta["status"]["exit_code"] == 0
        assert meta["stop"]["signals"] == []
        assert meta["times"]["elapsed"] < 2


def test_kill():
    svc = ApsisService()
    dump_job(svc.jobs_dir, IGNORE_TERM_JOB)
    with svc, svc.agent():
        # Schedule a 5 sec run but tell Apsis to stop it after 1 sec.  The
        # process ignores SIGTERM so Apsis will send SIGQUIT after the grace
        # period.
        run_id = svc.client.schedule(
            IGNORE_TERM_JOB.job_id, {"time": "5"},
            stop_time="+1s"
        )["run_id"]

        time.sleep(1.5)
        res = svc.client.get_run(run_id)
        assert res["state"] == "stopping"
        meta = res["meta"]["program"]

        res = svc.wait_run(run_id)

        assert res["state"] == "failure"
        meta = res["meta"]["program"]
        assert meta["status"]["signal"] == "SIGKILL"
        assert meta["stop"]["signals"] == ["SIGTERM", "SIGKILL"]
        assert meta["times"]["elapsed"] > 2.8

        output = svc.client.get_output(run_id, "output").decode()
        assert "ignoring SIGTERM" in output
        assert "done" not in output


