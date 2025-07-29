from ora import Time
from pathlib import Path
import time

import pytest

from procstar_instance import ApsisService
from apsis.jobs import jso_to_job, dump_job

# -------------------------------------------------------------------------------

SLEEP_JOB = jso_to_job(
    {
        "params": ["time"],
        "program": {
            "type": "procstar",
            "argv": ["/usr/bin/sleep", "{{ time }}"],
        },
    },
    "sleep",
)

EXIT_ZERO_ON_TERM_PATH = Path(__file__).parent / "exit-zero-on-term.py"
EXIT_ZERO_ON_TERM_JOB = jso_to_job(
    {
        "params": ["time"],
        "program": {
            "type": "procstar",
            "argv": [EXIT_ZERO_ON_TERM_PATH, "{{ time }}"],
            "stop": {
                "grace_period": 2,
            },
        },
    },
    "exit zero on term",
)

EXIT_ONE_ON_TERM_PATH = Path(__file__).parent / "exit-one-on-term.py"
EXIT_ONE_ON_TERM_JOB = jso_to_job(
    {
        "params": ["time"],
        "program": {
            "type": "procstar",
            "argv": [EXIT_ONE_ON_TERM_PATH, "{{ time }}"],
            "stop": {
                "grace_period": 2,
            },
        },
    },
    "exit one on term",
)

SLOW_CLEANUP_PATH = Path(__file__).parent / "slow-cleanup.py"
SLOW_CLEANUP_JOB = jso_to_job(
    {
        "params": ["time", "cleanup_time"],
        "program": {
            "type": "procstar",
            "argv": [
                SLOW_CLEANUP_PATH,
                "{{ time }}",
                "--cleanup-time",
                "{{ cleanup_time }}",
            ],
            "stop": {
                "grace_period": 2,
            },
        },
    },
    "slow cleanup",
)

IGNORE_TERM_PATH = Path(__file__).parent / "ignore-term.py"
IGNORE_TERM_JOB = jso_to_job(
    {
        "params": ["time"],
        "program": {
            "type": "procstar",
            "argv": [IGNORE_TERM_PATH, "{{ time }}"],
            "stop": {
                "grace_period": 2,
            },
        },
    },
    "ignore term",
)


@pytest.mark.parametrize(
    "job, expected_state, expected_exit_code, expected_signal",
    [
        (SLEEP_JOB, "failure", None, "SIGTERM"),
        (EXIT_ONE_ON_TERM_JOB, "failure", 1, None),
        (EXIT_ZERO_ON_TERM_JOB, "success", 0, None),
    ],
)
def test_stop(job, expected_state, expected_exit_code, expected_signal):
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        # Schedule a 3 sec job but tell Apsis to stop it after 1 sec.
        run_id = svc.client.schedule(
            job.job_id,
            {"time": "3"},
            stop_time="+1s",
        )["run_id"]
        res = svc.wait_run(run_id)

        # The run was stopped by Apsis, by sending it SIGTERM.
        assert res["state"] == expected_state
        meta = res["meta"]["program"]
        assert meta["status"]["exit_code"] == expected_exit_code
        assert meta["status"]["signal"] == expected_signal
        assert meta["stop"]["signals"] == ["SIGTERM"]
        assert meta["times"]["elapsed"] < 2


@pytest.mark.parametrize(
    "job, expected_state, expected_signal",
    [
        (SLEEP_JOB, "failure", "SIGTERM"),
        (EXIT_ZERO_ON_TERM_JOB, "success", None),
        (EXIT_ONE_ON_TERM_JOB, "failure", None),
    ],
)
def test_stop_api(job, expected_state, expected_signal):
    svc = ApsisService()
    dump_job(svc.jobs_dir, job)
    with svc, svc.agent():
        # Schedule a 3 sec job but tell Apsis to stop it after 1 sec.
        run_id = svc.client.schedule(job.job_id, {"time": "3"})["run_id"]
        res = svc.wait_run(run_id, wait_states=("new", "scheduled", "waiting", "starting"))

        time.sleep(0.5)
        res = svc.client.stop_run(run_id)
        assert res["state"] == "stopping"

        res = svc.wait_run(run_id)
        # The run was stopped by Apsis, by sending it SIGTERM.
        assert res["state"] == expected_state
        meta = res["meta"]["program"]
        assert meta["status"]["signal"] == expected_signal
        assert meta["stop"]["signals"] == ["SIGTERM"]
        assert meta["times"]["elapsed"] < 2


def test_dont_stop():
    svc = ApsisService()
    dump_job(svc.jobs_dir, IGNORE_TERM_JOB)
    with svc, svc.agent():
        # Schedule a 1 sec run but tell Apsis to stop it after 3 sec.
        run_id = svc.client.schedule(IGNORE_TERM_JOB.job_id, {"time": "1"}, stop_time="+3s")[
            "run_id"
        ]
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
        # process ignores SIGTERM so Apsis will send SIGKILL after the grace
        # period.
        run_id = svc.client.schedule(IGNORE_TERM_JOB.job_id, {"time": "5"}, stop_time="+1s")[
            "run_id"
        ]

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


def test_job_slow_cleanup_within_grace_period():
    svc = ApsisService()
    dump_job(svc.jobs_dir, SLOW_CLEANUP_JOB)
    with svc, svc.agent():
        run_id = svc.client.schedule(
            SLOW_CLEANUP_JOB.job_id,
            {"time": "10", "cleanup_time": "1"},
            stop_time="+1s",
        )["run_id"]
        res = svc.wait_run(run_id)

        assert res["state"] == "success"
        meta = res["meta"]["program"]
        assert meta["status"]["exit_code"] == 0
        assert meta["status"]["signal"] is None
        assert meta["stop"]["signals"] == ["SIGTERM"]
        # Should take about 2s (1s wait + 1s cleanup)
        assert 1.8 < meta["times"]["elapsed"] < 2.5


def test_rerun_with_stop():
    near = lambda x, y: abs(x - y) < 0.1

    svc = ApsisService()
    dump_job(svc.jobs_dir, SLEEP_JOB)
    with svc, svc.agent():
        run_id = svc.client.schedule("sleep", {"time": "10"}, stop_time="+0.5s")["run_id"]
        res = svc.wait_run(run_id)
        assert res["state"] == "failure"
        assert near(Time(res["times"]["stop"]), Time(res["times"]["schedule"]) + 0.5)
        meta = res["meta"]["program"]
        assert meta["status"]["signal"] == "SIGTERM"

        rerun_id = svc.client.rerun(run_id)["run_id"]
        reres = svc.wait_run(rerun_id)
        assert reres["state"] == "failure"
        # The rerun should use the old stop time.
        assert reres["times"]["stop"] == res["times"]["stop"]
        # Because of the old stop time, the rerun should have been stopped immediately.
        remeta = reres["meta"]["program"]
        assert remeta["times"]["elapsed"] < 0.1
        assert remeta["status"]["signal"] == "SIGTERM"
        assert remeta["proc_stat"]["pid"] != meta["proc_stat"]["pid"]
