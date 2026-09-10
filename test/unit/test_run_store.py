"""
Tests for RunStore with real SQLite database.
"""

import asyncio
import json
import threading
from types import SimpleNamespace

import ora
import random

import pytest
import sqlalchemy as sa
from sanic.request import RequestParameters

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

    assert "r666666666" not in run_store

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


def test_run_store_count_runs_no_double_count(tmp_path):
    """count_runs() must count each physical run once."""
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)
    _transition(store, run, State.waiting)

    assert store.count_runs() == 1


def test_run_store_get_stats_does_not_query_db(tmp_path):
    """
    get_stats() must not count runs in the DB.

    That query scans the runs table, which blocks the event loop for tens of
    seconds on a large database.  See RunStore.get_stats().
    """
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)
    _transition(store, run, State.waiting)

    def fail(*args, **kwargs):
        raise AssertionError("get_stats() must not count runs in the DB")

    store._RunStore__run_db.count_runs = fail

    stats = store.get_stats()
    assert stats["num_active_runs"] == 1
    assert "num_runs" not in stats


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


def test_summaries_concurrent_transition(tmp_path):
    """summaries() must not raise RuntimeError if a run transitions mid-iteration."""
    store = _make_store(tmp_path)

    run_a = Run(Instance("job", {"n": "0"}), expected=True)
    _schedule(store, run_a)
    run_b = Run(Instance("job", {"n": "1"}), expected=True)
    _schedule(store, run_b)

    it = store.summaries()
    next(it)

    # transition a run out of expected (simulates concurrent state change)
    _transition(store, run_b, State.waiting)

    # consuming the rest must not raise "dictionary changed size during iteration"
    list(it)


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


def test_active_run_is_singleton_across_get(tmp_path):
    """
    An active run must have a single in-memory identity.

    Every path that resolves an active run (get, query, subsequent get after
    a transition, subsequent get after eviction from expected) must return
    the same Python object.  This is what protects API handlers and
    _process_updates from mutating disjoint copies (see PR #538, findings
    2/5).
    """
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)

    # transition into an active state; run is now persisted and mirrored.
    _transition(store, run, State.waiting)

    # every get() during the active period returns the same object.
    _, g1 = store.get(run.run_id)
    _, g2 = store.get(run.run_id)
    assert g1 is g2 is run

    # a transition within the active set keeps identity.
    _transition(store, run, State.starting)
    _, g3 = store.get(run.run_id)
    assert g3 is run

    # transition to a finished state evicts the run; subsequent get() must
    # deserialize a fresh copy from the DB (not the mirror), since the
    # in-memory identity is no longer needed.
    _transition(store, run, State.error, force=True)
    _, g_after = store.get(run.run_id)
    assert g_after is not run
    assert g_after.run_id == run.run_id
    assert g_after.state == State.error


def test_active_run_singleton_survives_db_backing(tmp_path):
    """
    A run loaded from SQLite (not created via add()) still gets mirrored
    on first get(), so subsequent get()s return the same object.  This
    covers the restore-window race: an API call for an active run whose
    DB row exists but that restore has not yet attach()-ed.
    """
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    db = SqliteDB.open(db_path)

    # persist an active-state run directly, bypassing the store.
    run = Run(Instance("job", {}), expected=False)
    run.run_id = db.next_run_id_db.get_next_run_id()
    run.timestamp = ora.now()
    run.state = State.running
    run.times = {State.running.name: run.timestamp}
    run.meta = {}
    db.run_db.upsert(run)

    store = RunStore(db, min_timestamp=None)

    # first get() has to deserialize from SQLite.
    _, g1 = store.get(run.run_id)
    assert g1.run_id == run.run_id
    assert g1.state == State.running

    # every subsequent get() returns that same object.
    _, g2 = store.get(run.run_id)
    _, g3 = store.get(run.run_id)
    assert g1 is g2 is g3


def test_active_run_singleton_via_attach(tmp_path):
    """
    attach() installs a Run as the authoritative in-memory object for its
    run_id.  restore() uses this to hand the same object to _wait_loop /
    __reconnect and to any API request that lands during the restore window.
    """
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    db = SqliteDB.open(db_path)

    run = Run(Instance("job", {}), expected=False)
    run.run_id = db.next_run_id_db.get_next_run_id()
    run.timestamp = ora.now()
    run.state = State.waiting
    run.times = {State.waiting.name: run.timestamp}
    run.meta = {}
    db.run_db.upsert(run)

    store = RunStore(db, min_timestamp=None)
    store.attach(run)

    _, g = store.get(run.run_id)
    assert g is run

    # attach() rejects non-active states.
    finished = Run(Instance("job", {}), expected=False)
    finished.run_id = db.next_run_id_db.get_next_run_id()
    finished.timestamp = ora.now()
    finished.state = State.success
    with pytest.raises(AssertionError):
        store.attach(finished)


# --- paged querying ---------------------------------------------------------------


def _rowid(run_id):
    return int(run_id[1:])


def _persist_run(store, job_id, args=None, state=State.success, timestamp=None):
    """Insert a finished run straight into the DB (not the in-memory maps)."""
    db = store._RunStore__run_db
    run = Run(Instance(job_id, args or {}), expected=False)
    run.run_id = store._RunStore__next_run_id_db.get_next_run_id()
    run.timestamp = ora.now() if timestamp is None else timestamp
    run.state = state
    run.times = {state.name: run.timestamp}
    run.meta = {}
    db.upsert(run)
    return run


def _make_active(store, job_id="job"):
    """A run mirrored in memory AND persisted in the DB (an ACTIVE_STATES run)."""
    run = Run(Instance(job_id, {}), expected=True)
    _schedule(store, run)
    _transition(store, run, State.waiting)
    return run.run_id


def _page(store, limit, **kwargs):
    """One page, as the API does it: prepare on the loop, then fetch."""
    return store.fetch_page(store.prepare_page(**kwargs), limit)


def _scroll(store, limit, start_cursor=None, **kwargs):
    """Walk all pages from an optional starting cursor, return run_ids in order."""
    seen = []
    cursor = start_cursor
    while True:
        page, nxt = _page(store, limit, cursor=cursor, **kwargs)
        assert len(page) <= limit
        seen.extend(r.run_id for r in page)
        if nxt is None:
            break
        assert cursor is None or _rowid(nxt) < _rowid(cursor)  # cursor must advance
        cursor = nxt
    return seen


# layouts oldest -> newest of "active" (mirror + db) or "db" (db only) rows, plus
# a page size.  each exercises a distinct adversarial merge shape.
_MERGE_LAYOUTS = [
    ("lone_mirrored_run", ["active"], 10),
    ("newest_window_all_active", ["db", "db", "active", "active", "active"], 2),
    ("active_below_newer_db", ["active", "db", "db", "db", "db"], 2),
]


@pytest.mark.parametrize("label, layout, limit", _MERGE_LAYOUTS, ids=[m[0] for m in _MERGE_LAYOUTS])
def test_paged_merge_dedups_and_orders(tmp_path, label, layout, limit):
    """
    The in-memory mirror and the DB merge into one deduped, rowid-descending
    scroll.  Covers a lone mirrored run, a newest window that is all active
    dupes filling limit+1, and an active run below newer DB-only rows.
    """
    store = _make_store(tmp_path)
    ids = [
        _make_active(store) if kind == "active" else _persist_run(store, "job").run_id
        for kind in layout
    ]
    expected = sorted(ids, key=_rowid, reverse=True)  # newest first, as a scroll returns

    # exact sequence catches wrong order, omissions, additions, and duplicates
    assert _scroll(store, limit, job_id="job") == expected


def test_paged_stable_when_expected_run_persists_mid_scroll(tmp_path):
    """
    A run that transitions from expected in memory to persisted in the DB
    between pages still appears exactly once, since its immutable run number
    keeps the cursor stable.
    """
    store = _make_store(tmp_path)
    persisted = [_persist_run(store, "job").run_id for _ in range(5)]
    expected = [Run(Instance("job", {}), expected=True) for _ in range(5)]
    for r in expected:
        _schedule(store, r)
    all_ids = set(persisted) | {r.run_id for r in expected}

    page1, nxt = _page(store, 4, job_id="job")
    # transition a run below the page one cursor into the DB mirror, its rowid holds
    _transition(store, expected[0], State.waiting)
    seen = [r.run_id for r in page1] + _scroll(store, 4, start_cursor=nxt, job_id="job")

    assert len(seen) == len(set(seen))
    assert set(seen) == all_ids


def test_paged_applies_lookback_on_every_page(tmp_path):
    """Runs older than the lookback window never appear on any page, not just page one."""
    min_ts = ora.now() - 3600
    store = _make_store(tmp_path, min_timestamp=min_ts)
    # interleave fresh and too-old runs so old rows land below the first cursor
    # and between later fresh matches, not all bunched above page one
    fresh = set()
    for _ in range(6):
        fresh.add(_persist_run(store, "job", timestamp=ora.now()).run_id)
        _persist_run(store, "job", timestamp=min_ts - 100)  # too old

    seen = _scroll(store, 2, job_id="job")
    assert set(seen) == fresh
    assert len(seen) == len(set(seen))


def test_paged_invalid_cursor_raises(tmp_path):
    store = _make_store(tmp_path)
    _persist_run(store, "job")
    with pytest.raises(ValueError):
        _page(store, 5, job_id="job", cursor="not-a-run-id")


def test_paged_run_ids_filter(tmp_path):
    store = _make_store(tmp_path)
    runs = [_persist_run(store, "job") for _ in range(5)]
    wanted = {runs[0].run_id, runs[3].run_id}
    seen = _scroll(store, 2, run_ids=list(wanted))
    assert set(seen) == wanted


def test_run_number_validation():
    from apsis.runs import run_number

    assert run_number("r1") == 1
    assert run_number("r12345") == 12345
    for bad in ("r-5", "rabc", "r", "", "x1", "1", None, 5, "r1.0", "r 1"):
        with pytest.raises(ValueError):
            run_number(bad)


def test_prepare_page_snapshots_in_memory_runs(tmp_path):
    """
    The in-memory runs handed to the worker thread must not alias the live
    objects, so a transition on the loop during serialization can't race.
    """
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)

    request = store.prepare_page(job_id="job")
    (snap,) = request.in_memory_list
    assert snap.run_id == run.run_id
    assert snap is not run
    assert snap.times is not run.times and snap.meta is not run.meta

    _transition(store, run, State.waiting)  # mutates run.times and run.state
    assert "waiting" not in snap.times
    assert snap.state == State.scheduled


@pytest.mark.parametrize("mode", ["job_id", "run_id"])
@pytest.mark.asyncio
async def test_runs_handler_reads_off_loop_while_write_proceeds(tmp_path, mode):
    """
    The /runs handler fetches and decodes the page on a worker thread, not the
    event loop, over a query only connection, so a scheduler write lands while
    the read is held open.  Covers the job_id and explicit run_id paths.
    """
    from apsis.service import api

    store = _make_store(tmp_path)
    read_engine = store._RunStore__run_db._RunDB__read_engine
    assert read_engine is not store._RunStore__run_db._RunDB__engine  # off-loop engine
    wanted = [_persist_run(store, "job").run_id for _ in range(2)]

    # hold the paged select open on its worker thread until the loop releases it,
    # recording the reader thread and that it stayed parked until then
    seen = {}
    parked = threading.Event()
    release = threading.Event()

    def _park(conn, cursor, statement, params, context, executemany):
        if "FROM runs" in statement and not parked.is_set():
            seen["ident"] = threading.get_ident()
            parked.set()
            seen["released"] = release.wait(timeout=5)

    sa.event.listen(read_engine, "after_cursor_execute", _park)
    query = {"job_id": ["job"]} if mode == "job_id" else {"run_id": list(wanted)}
    jobs = SimpleNamespace(get_job=lambda _: object())  # exact match, returns job_id as is
    app = SimpleNamespace(apsis=SimpleNamespace(run_store=store, jobs=jobs))
    request = SimpleNamespace(app=app, args=RequestParameters(query))
    loop_ident = threading.get_ident()
    task = asyncio.create_task(api.runs(request))
    try:
        # bounded wait so an early handler exit fails fast instead of hanging
        deadline = asyncio.get_running_loop().time() + 5
        while not parked.is_set() and not task.done():
            if asyncio.get_running_loop().time() > deadline:
                break
            await asyncio.sleep(0.01)
        assert parked.is_set(), "reader never reached the select"

        # write a different job while the read is parked open, then release it
        other = _persist_run(store, "other")
        release.set()
        resp = await task
    finally:
        release.set()
        if not task.done():
            task.cancel()
        sa.event.remove(read_engine, "after_cursor_execute", _park)

    assert seen["released"] is True  # the read stayed open until our write landed
    assert seen["ident"] != loop_ident  # the fetch ran off the loop
    assert resp.status == 200
    jso = json.loads(resp.body)
    assert set(jso["runs"]) == set(wanted)
    assert jso["paging"]["next"] is None
    assert other.run_id in {r.run_id for r in store.query(job_id="other")[1]}

    with read_engine.connect() as conn:
        (query_only,) = conn.execute(sa.text("PRAGMA query_only")).one()
    assert query_only == 1
