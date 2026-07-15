"""
Durability of RunStore.update() when a DB write fails during eviction.

When a run leaves the scheduled state it stops being "expected", so RunStore
moves it out of the in-memory working set (__expected_runs) and into the DB.
The order of "evict from memory" vs "persist to DB" matters if a write fails:

  - pop-before-upsert (original): a failed upsert drops the run from BOTH memory
    and DB -> data loss.
  - upsert-before-pop (fixed):    a failed upsert leaves the run in memory ->
    recoverable.

This test guards the fixed ordering: a failed `run_db.upsert` must not lose the
run.  It fails on the pop-before-upsert ordering and passes on main and with the
fix.
"""

import ora
import pytest

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State


def _query_ids(store, **kw):
    return [r.run_id for r in store.query(**kw)[1]]


def test_run_db_failure_during_eviction_does_not_lose_run(tmp_path):
    SqliteDB.create(path=tmp_path / "apsis.db")
    db = SqliteDB.open(tmp_path / "apsis.db")
    store = RunStore(db, min_timestamp=None)

    now = ora.now()

    # A scheduled run created from a schedule (expected -> in-memory only).
    run = Run(Instance("job", {"x": "1"}), expected=True)
    store.add(run)
    run._transition(now, State.scheduled)
    store.update(run, now)
    assert _query_ids(store, job_id="job") == [run.run_id]

    # Fail the DB write on the scheduled -> waiting transition, which flips
    # expected to False and evicts the run from the in-memory working set.
    def boom(_run):
        raise RuntimeError("simulated DB write failure")

    db.run_db.upsert = boom

    run.expected = False
    run._transition(now, State.waiting)
    with pytest.raises(RuntimeError):
        store.update(run, now)

    # The upsert failed, so the run must still be retrievable -- not dropped from
    # both memory and DB.
    #
    # NOTE: update() persists (run_db + run_summary) before popping from the
    # in-memory set, so a failed run_db.upsert leaves the run in memory
    # (recoverable). The ordering is not fully atomic, though: if
    # run_summary.upsert fails *after* run_db.upsert succeeds, the run is written
    # to the DB but not popped, so it appears in both -- a duplicate. That
    # duplicate is corrected by the next successful update() only if the run
    # transitions again; on a terminal eviction (scheduled -> skipped/error) it
    # never does, so the duplicate persists until the next restart (which rebuilds
    # __expected_runs). This is an accepted trade: a duplicate is observable,
    # non-destructive, and clears on restart, whereas the alternative ordering
    # loses the run silently and permanently. Making both table writes + the pop
    # a single transaction would remove the window entirely.
    assert _query_ids(store, job_id="job") == [run.run_id], "run lost after run_db upsert failure"
