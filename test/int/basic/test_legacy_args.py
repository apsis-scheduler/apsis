"""
Runs and ad hoc jobs stored by an older Apsis may carry arg or param names that
check-jobs would now reject.  Stored data is never re-checked, so they must not
prevent Apsis from starting or from serving those runs.
"""

import json
import sqlite3
from pathlib import Path

import ora

from apsis.runs import Instance, Run
from apsis.sqlite import SqliteDB
from apsis.states import State
from instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

LEGACY_ARGS = {"a=b": "x", "color": "red", "exit": "0"}
LEGACY_JOB_ID = "adhoc-legacy"


def _make_run(run_db, run_id, job_id, args, state):
    run = Run(Instance(job_id, args))
    run.run_id = run_id
    run.timestamp = ora.now()
    run.state = state
    run.times = (
        {"schedule": ora.now() + 3600} if state == State.scheduled else {state.name: run.timestamp}
    )
    run.meta = {}
    run_db.upsert(run)


def _seed_legacy_db(inst):
    run_db = SqliteDB.open(inst.db_path).run_db
    _make_run(run_db, "r1", "print time", LEGACY_ARGS, State.success)
    _make_run(run_db, "r2", "print time", LEGACY_ARGS, State.scheduled)
    _make_run(run_db, "r3", LEGACY_JOB_ID, {"weird name": "1"}, State.scheduled)
    job = {
        "job_id": LEGACY_JOB_ID,
        "params": ["weird name"],
        "program": {"type": "no-op"},
        "schedule": [],
        "condition": [],
        "action": [],
        "metadata": {},
        "ad_hoc": True,
    }
    with sqlite3.connect(inst.db_path) as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, job) VALUES (?, ?)", (LEGACY_JOB_ID, json.dumps(job))
        )
        conn.execute("UPDATE next_run_id SET number = 100")


def test_legacy_arg_names():
    inst = ApsisService(job_dir=JOB_DIR)
    inst.create_db()
    _seed_legacy_db(inst)
    inst.write_cfg()
    inst.start_serve()
    try:
        inst.wait_for_serve()
        client = inst.client

        # Legacy runs load, restore, and are served unchanged.
        assert client.get_run("r1")["args"] == LEGACY_ARGS
        assert client.get_run("r2")["state"] == "scheduled"
        assert client.get_run("r3")["state"] == "scheduled"
        assert set(client.get_runs(job_id="print time")) == {"r1", "r2"}

        # Stored ad hoc jobs are never re-checked, so the legacy job is still served.
        assert client.get_job(LEGACY_JOB_ID)["params"] == ["weird name"]

        # Rerunning a legacy run produces a run that errors, as for any arg the job no longer has.
        run = client.rerun("r1")
        assert run["args"] == LEGACY_ARGS
        assert inst.wait_run(run["run_id"])["state"] == "error"
        log = client.get_run_log(run["run_id"])
        assert any("extra args (a=b)" in r["message"] for r in log)

        # The legacy ad hoc job still has its param, so its runs still work.
        run = client.rerun("r3")
        assert inst.wait_run(run["run_id"])["state"] == "success"
    finally:
        assert inst.stop_serve() == 0
