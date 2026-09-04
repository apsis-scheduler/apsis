"""
End-to-end tests that a run's args reach the process as APSIS_ARG_* env vars,
through a real Procstar agent.
"""

import json
import sqlite3
from pathlib import Path

import ora
import pytest

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

ARGS = {"date": "2026-09-01", "database": "asd_hoard"}

# -------------------------------------------------------------------------------


def _program_jso(svc, run_id):
    """
    Returns the persisted program JSO for `run_id`, read straight from SQLite.
    """
    con = sqlite3.connect(svc.db_path)
    try:
        (program,) = con.execute("SELECT program FROM runs WHERE run_id = ?", (run_id,)).fetchone()
    finally:
        con.close()
    return json.loads(program)


def test_run_args_env():
    """
    A shell program can read its run's args from the environment.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        run_id = svc.client.schedule("run args", ARGS)["run_id"]
        res = svc.wait_run(run_id, timeout=10)
        assert res["state"] == "success"

        output = svc.client.get_output(run_id, "output").decode()
        assert output == f"date=2026-09-01 database=asd_hoard run={run_id}\n"

        # The args reached the process without being written to the DB: an older
        # Apsis must still be able to read this row.
        assert "args" not in _program_jso(svc, run_id)


def test_run_args_env_survives_apsis_restart():
    """
    The case that makes persisting the args in the program JSO look necessary.

    A scheduled run is persisted, Apsis restarts, and the run is restored from
    the DB -- its program rebuilt from JSO, with no re-bind -- and only then
    started.  The args must still reach the process.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        # Schedule far enough out that the run is still scheduled at restart.
        # A run scheduled explicitly is not "expected", so it is persisted.
        when = ora.now() + 5
        run_id = svc.client.schedule("run args", ARGS, time=when)["run_id"]
        assert svc.client.get_run(run_id)["state"] == "scheduled"

        svc.restart()

        # The restored run starts on its own once its schedule time arrives.
        res = svc.wait_run(run_id, timeout=20)
        assert res["state"] == "success", res

        output = svc.client.get_output(run_id, "output").decode()
        assert output == f"date=2026-09-01 database=asd_hoard run={run_id}\n"


def test_no_run_args_env():
    """
    A job with no params gets no APSIS_ARG_* vars, and APSIS_RUN_ID regardless.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        run_id = svc.client.schedule("env", {})["run_id"]
        res = svc.wait_run(run_id, timeout=10)
        assert res["state"] == "success"

        env = dict(
            line.split("=", 1)
            for line in svc.client.get_output(run_id, "output").decode().splitlines()
            if "=" in line
        )
        assert env["APSIS_RUN_ID"] == run_id
        assert [k for k in env if k.startswith("APSIS_ARG_")] == []


@pytest.mark.parametrize(
    "kind",
    [
        # A NUL aborts the exec in the agent; the run used to stay in `running`
        # forever, with no process behind it.
        "nul",
        # Over MAX_ARG_STRLEN, exec used to fail with E2BIG.
        "oversized",
    ],
)
def test_unrepresentable_args_do_not_break_the_run(kind):
    """
    An arg that can't be an environment variable is skipped, and the run still
    runs.  The other args are unaffected.
    """
    # Build the value here, not as a param: pytest puts the param into
    # PYTEST_CURRENT_TEST, and a huge one stops the harness spawning procstar.
    value = "a\x00b" if kind == "nul" else "x" * 200_000

    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        run_id = svc.client.schedule("run args", {"date": "ok", "database": value})["run_id"]
        res = svc.wait_run(run_id, timeout=15)
        assert res["state"] == "success", f"{kind}: {res['state']}"

        # `date` still arrives; `database` is simply absent from the environment.
        output = svc.client.get_output(run_id, "output").decode()
        assert output == f"date=ok database= run={run_id}\n", f"{kind}: {output!r}"
