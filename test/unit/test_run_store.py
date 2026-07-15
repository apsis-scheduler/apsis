"""
Tests for RunStore with real SQLite database.
"""

import ora
import random

import pytest

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


def _make_store(tmp_path, min_timestamp=None):
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    return RunStore(SqliteDB.open(db_path), min_timestamp=min_timestamp)


def _transition(store, run, state, **kw_args):
    """Replicates the relevant part of Apsis._transition."""
    time = ora.now()
    if run.expected and state not in {State.new, State.scheduled}:
        run.expected = False
    run._transition(time, state, **kw_args)
    store.update(run, time)


def _schedule(store, run):
    store.add(run)
    _transition(store, run, State.scheduled)


def test_run_store_query_with_args(tmp_path):
    store = _make_store(tmp_path)
    run = Run(Instance("job", {"k": "1", "j": "2"}), expected=True)
    _schedule(store, run)

    result = list(store.query(with_args={"k": "1"})[1])
    assert [r.run_id for r in result] == [run.run_id]

    result = list(store.query(with_args={"k": "other"})[1])
    assert result == []


def test_run_store_query_no_duplicates_after_transition(tmp_path):
    """After a run leaves scheduled, query() must return it exactly once."""
    store = _make_store(tmp_path)
    run = Run(Instance("job", {"x": "1"}), expected=True)
    _schedule(store, run)

    _transition(store, run, State.waiting)

    result = list(store.query(job_id="job")[1])
    assert [r.run_id for r in result] == [run.run_id]

    for state in (State.starting, State.running, State.success):
        _transition(store, run, state)
    result = list(store.query()[1])
    assert [r.run_id for r in result] == [run.run_id]


def test_run_store_finished_run_not_in_memory(tmp_path):
    """A finished run must not be retained in the in-memory expected map."""
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)
    for state in (State.waiting, State.starting, State.running, State.success):
        _transition(store, run, state)

    expected_map = store._RunStore__expected_runs
    assert run.run_id not in expected_map


def test_run_store_num_runs_no_double_count(tmp_path):
    """get_stats()['num_runs'] must count each physical run once."""
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)
    _transition(store, run, State.waiting)

    assert store.get_stats()["num_runs"] == 1


def test_run_store_query_since_filters_expected(tmp_path):
    """query(since=...) must filter expected (in-memory) runs by timestamp."""
    store = _make_store(tmp_path)

    early = Run(Instance("job", {"n": "0"}), expected=True)
    _schedule(store, early)

    # advance time so second run gets a later timestamp
    late = Run(Instance("job", {"n": "1"}), expected=True)
    _schedule(store, late)

    # since= the later run's timestamp should exclude the earlier one
    result = list(store.query(since=late.timestamp)[1])
    assert [r.run_id for r in result] == [late.run_id]


def test_run_store_count_runs(tmp_path):
    """count_runs() must count both expected (in-memory) and persisted (DB) runs."""
    store = _make_store(tmp_path)

    # two expected runs for "jobA"
    a1 = Run(Instance("jobA", {"k": "1"}), expected=True)
    _schedule(store, a1)
    a2 = Run(Instance("jobA", {"k": "2"}), expected=True)
    _schedule(store, a2)

    # one expected run for "jobB"
    b1 = Run(Instance("jobB", {"k": "1"}), expected=True)
    _schedule(store, b1)

    # all three are expected (in-memory only)
    assert store.count_runs() == 3
    assert store.count_runs(job_id="jobA") == 2
    assert store.count_runs(job_id="jobB") == 1
    assert store.count_runs(job_id="jobC") == 0

    # transition a1 through to success — persists to DB, leaves expected map
    for state in (State.waiting, State.starting, State.running, State.success):
        _transition(store, a1, state)

    # a1 is now in DB, a2 still in memory
    assert store.count_runs(job_id="jobA") == 2
    assert store.count_runs(job_id="jobA", state=(State.success,)) == 1
    assert store.count_runs(job_id="jobA", state=(State.scheduled,)) == 1

    # filter by args
    assert store.count_runs(job_id="jobA", args={"k": "1"}) == 1
    assert store.count_runs(job_id="jobA", args={"k": "2"}) == 1
    assert store.count_runs(job_id="jobA", args={"k": "99"}) == 0


def test_run_store_limit_lookback(tmp_path):
    """Test limit_lookback parameter controls whether lookback window is applied."""
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    db = SqliteDB.open(db_path)

    now = ora.now()
    lookback = 3600  # 1 hour
    min_timestamp = now - lookback

    # Create store with lookback window
    store = RunStore(db, min_timestamp=min_timestamp)

    # Create an old run beyond the lookback window
    old = Run(Instance("job", {"n": "old"}))
    store.add(old)
    _transition(store, old, State.scheduled)
    _transition(store, old, State.waiting)
    _transition(store, old, State.starting)
    _transition(store, old, State.running)

    # Backdate it to beyond lookback window
    old.timestamp = now - 2 * lookback
    store.update(old, old.timestamp)

    # Create a recent run within lookback window
    recent = Run(Instance("job", {"n": "recent"}))
    store.add(recent)
    _transition(store, recent, State.scheduled)

    # With limit_lookback=True (default), should only see recent run from DB
    _, runs_with_lookback = store.query(job_id="job", limit_lookback=True)
    run_ids_with = {r.run_id for r in runs_with_lookback}
    assert recent.run_id in run_ids_with, "recent run should be visible with lookback"
    assert old.run_id not in run_ids_with, "old run should be filtered out by lookback"

    # With limit_lookback=False, should see both runs
    _, runs_no_lookback = store.query(job_id="job", limit_lookback=False)
    run_ids_no = {r.run_id for r in runs_no_lookback}
    assert recent.run_id in run_ids_no, "recent run should be visible"
    assert old.run_id in run_ids_no, "old run should be visible without lookback filter"

    # Test with since parameter and limit_lookback=True
    # since should be combined with lookback using max()
    since_old = now - 1.5 * lookback  # older than lookback, so lookback wins
    _, runs_since_old = store.query(job_id="job", since=since_old, limit_lookback=True)
    run_ids_since_old = {r.run_id for r in runs_since_old}
    assert recent.run_id in run_ids_since_old
    assert old.run_id not in run_ids_since_old, "lookback should be more restrictive than since"

    # since more restrictive than lookback
    since_recent = now - 0.5 * lookback  # newer than lookback, so since wins
    _, runs_since_recent = store.query(job_id="job", since=since_recent, limit_lookback=True)
    run_ids_since_recent = {r.run_id for r in runs_since_recent}
    assert recent.run_id in run_ids_since_recent
    assert old.run_id not in run_ids_since_recent

    # With limit_lookback=False and since, only since is applied
    _, runs_since_no_lookback = store.query(job_id="job", since=since_old, limit_lookback=False)
    run_ids_since_no = {r.run_id for r in runs_since_no_lookback}
    assert recent.run_id in run_ids_since_no
    assert old.run_id not in run_ids_since_no, "since filter alone should exclude old run"

    # count_runs defaults to limit_lookback=True (matches query default)
    count_with_lookback = store.count_runs(job_id="job")
    assert count_with_lookback == 1, "count_runs should respect lookback by default"

    # Conditions should explicitly pass limit_lookback=False
    count_all = store.count_runs(job_id="job", limit_lookback=False)
    assert count_all == 2, "count_runs with limit_lookback=False should see all runs"


def test_upsert_durability(tmp_path):
    SqliteDB.create(path=tmp_path / "apsis.db")
    db = SqliteDB.open(tmp_path / "apsis.db")
    store = RunStore(db, min_timestamp=None)

    now = ora.now()

    # scheduled an expected run run ie created from a schedule
    run = Run(Instance("job", {"x": "1"}), expected=True)
    store.add(run)
    run._transition(now, State.scheduled)
    store.update(run, now)
    assert [r.run_id for r in store.query(job_id="job")[1]] == [run.run_id]

    # fail the DB write on the scheduled -> waiting transition, which sets expected=False and evicts the run from the
    # set of expected runs
    def failing_update(_run):
        raise RuntimeError("simulated DB write failure")

    db.run_db.upsert = failing_update

    run.expected = False
    run._transition(now, State.waiting)
    with pytest.raises(RuntimeError):
        store.update(run, now)

    # the upsert failed, so the run must still be retrievable
    assert [r.run_id for r in store.query(job_id="job")[1]] == [run.run_id], (
        "run lost after run_db upsert failure"
    )
