"""
Tests for RunStore with real SQLite database.
"""

import ora
import random

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State


def test_runs_by_job(tmp_path):
    """Test RunStore query filtering by job_id with expected runs."""
    # Setup database
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    db = SqliteDB.open(db_path)

    rnd = random.Random(0)
    n = 1000

    job_ids = [f"job{i:02d}" for i in range(100)]
    rnd.shuffle(job_ids)

    # Create a random run store and add a bunch of runs.
    run_store = RunStore(db, min_timestamp=ora.now())
    for _ in range(n):
        run = Run(Instance(rnd.choice(job_ids), {}), expected=True)
        run_store.add(run)

    query = lambda *a, **k: run_store.query(*a, **k)[1]

    run_ids = [r.run_id for r in query()]
    assert len(run_ids) == n

    # Confirm that they are all available by job.
    runs = query()
    for run in runs:
        assert run in query(job_id=run.inst.job_id)

    # Now remove some runs.
    for run_id in rnd.sample(run_ids, len(run_ids) // 5):
        run_store.remove(run_id)
        run_ids.remove(run_id)

    assert set(r.run_id for r in query()) == set(run_ids)

    # Confirm that they are all available by job.
    for run_id in run_ids:
        _, run = run_store.get(run_id)
        assert run in query(job_id=run.inst.job_id)

    # Confirm that no extraneous jobs are left.
    r = set.union(*(set(r.run_id for r in query(job_id=j)) for j in job_ids))
    assert r == set(run_ids)

    # Now remove some more runs.
    for run_id in rnd.sample(run_ids, len(run_ids) // 4):
        run_store.remove(run_id)
        run_ids.remove(run_id)

    assert set(r.run_id for r in query()) == set(run_ids)

    # Confirm that they are all available by job.
    for run_id in run_ids:
        _, run = run_store.get(run_id)
        assert run in query(job_id=run.inst.job_id)

    # Confirm that no extraneous jobs are left.
    r = set.union(*(set(r.run_id for r in query(job_id=j)) for j in job_ids))
    assert r == set(run_ids)


def test_run_store_populate(tmp_path):
    """
    Tests a RunStore populated from existing runs in the database.

    Simulates loading runs from SQLite that were persisted in a previous session.
    These runs are NOT in __expected_runs, so remove() doesn't apply.
    """
    # Setup database
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    db = SqliteDB.open(db_path)

    rnd = random.Random(0)
    n = 1000

    job_ids = [f"job{i:02d}" for i in range(100)]
    rnd.shuffle(job_ids)

    # Set a base timestamp for runs that's definitely in the past.
    base_timestamp = ora.now() - 3600  # 1 hour ago

    # Pre-populate the database with runs (simulating previous session).
    # These are persisted (expected=False) so they go into the DB.
    def make_run():
        inst = Instance(rnd.choice(job_ids), {})
        run = Run(inst, expected=False)
        run.run_id = db.next_run_id_db.get_next_run_id()
        run.timestamp = base_timestamp
        run.state = State.success
        run.times = {State.success.name: run.timestamp}
        run.meta = {}
        db.run_db.upsert(run)
        return run

    runs = [make_run() for _ in range(n)]
    run_ids = {r.run_id for r in runs}
    assert len(run_ids) == len(runs)

    # Create a new run store with min_timestamp before the runs.
    run_store = RunStore(db, min_timestamp=base_timestamp - 1)

    # Query each run by run ID.
    for run in runs:
        result = list(run_store.query(run_ids=run.run_id)[1])
        assert len(result) == 1
        assert result[0].run_id == run.run_id
        assert result[0].inst.job_id == run.inst.job_id

        assert run.run_id in run_store

    assert "rNOTEXIST" not in run_store

    # Query runs by job ID.
    for job_id in job_ids:
        q = set(run_store.query(job_id=job_id)[1])
        expected = {r for r in runs if r.inst.job_id == job_id}
        # Compare by run_id since objects may be different instances
        assert {r.run_id for r in q} == {r.run_id for r in expected}
