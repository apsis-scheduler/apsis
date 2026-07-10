"""
Regression tests for RunStore once most runs are served from SQLite
(PR: "RunStore: remove in-memory storage for most runs").

These pin down four deterministic defects on the branch.  Each reproduces on a
single synchronous call -- no timing/concurrency involved -- against a real
RunStore backed by a temp SQLite DB.

  1. test_query_with_args_*        -- query(with_args=...) crashes
  2. test_transitioned_run_* /     -- run kept in memory after leaving scheduled:
     test_finished_run_* /            duplicated in query(), leaked, and
     test_num_runs_*                  double-counted in stats
  3. test_query_returns_*_order    -- query() no longer globally ascending
  4. test_get_missing_run_*        -- missing run -> 500 instead of 404

All currently FAIL on the branch and should pass once the corresponding fixes
land.
"""

import ora
import pytest

import apsis.service.api as api
from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State

# -------------------------------------------------------------------------------


def _make_store(tmp_path):
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    return RunStore(SqliteDB.open(db_path), min_timestamp=None)


def _transition(store, run, state, **kw_args):
    """
    Replicates the relevant part of `Apsis._transition`.

    Crucially, this flips `run.expected` to False when the run leaves the
    scheduled/new states -- exactly as `apsis.py` does -- *before* calling
    `store.update()`, which is what causes the run to be persisted to the DB.
    """
    time = ora.now()
    if run.expected and state not in {State.new, State.scheduled}:
        run.expected = False
    run._transition(time, state, **kw_args)
    store.update(run, time)


def _schedule(store, run):
    """Adds a run and transitions it to scheduled, as the scheduler does."""
    store.add(run)
    _transition(store, run, State.scheduled)


def _query(store, **kw_args):
    return store.query(**kw_args)[1]


# -------------------------------------------------------------------------------
# 1. query(with_args=...) crashes when any expected run is in memory.


def test_query_with_args_returns_matching_run(tmp_path):
    """
    query(with_args=...) must not crash when an expected run is in memory.

    `with_args` is reassigned from a dict to a list of tuples in query(); the
    lazy in-memory `expected` generator then calls `.items()` on the list ->
    AttributeError.  Reachable from GET /api/v1/runs?<arg>=<val>.
    """
    store = _make_store(tmp_path)
    run = Run(Instance("job", {"k": "1"}), expected=True)
    _schedule(store, run)

    result = _query(store, with_args={"k": "1"})

    assert [r.run_id for r in result] == [run.run_id]


def test_query_with_args_no_match(tmp_path):
    """query(with_args=...) with an expected run present but no match: empty."""
    store = _make_store(tmp_path)
    _schedule(store, Run(Instance("job", {"k": "1"}), expected=True))

    result = _query(store, with_args={"k": "other"})

    assert result == []


# -------------------------------------------------------------------------------
# 2. A run that leaves the scheduled state is persisted to the DB but never
#    removed from the in-memory expected map: duplicated in query() and leaked.


def test_transitioned_run_appears_once_in_query(tmp_path):
    """
    After an expected run leaves `scheduled` (and is persisted), query() must
    return it exactly once -- not once from memory and once from the DB.
    """
    store = _make_store(tmp_path)
    run = Run(Instance("job", {"x": "1"}), expected=True)
    _schedule(store, run)

    # Leaving scheduled flips expected=False and persists to the DB.
    _transition(store, run, State.waiting)

    result = _query(store, job_id="job")
    assert [r.run_id for r in result] == [run.run_id]

    # And through a full finished lifecycle.
    for state in (State.starting, State.running, State.success):
        _transition(store, run, state)
    result = _query(store)
    assert [r.run_id for r in result] == [run.run_id]


def test_finished_run_not_retained_in_memory(tmp_path):
    """
    A finished run must not be retained in the in-memory expected map (the
    working set the PR set out to reclaim).
    """
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)
    for state in (State.waiting, State.starting, State.running, State.success):
        _transition(store, run, state)

    # White-box: the expected map should not still hold the finished run.
    expected_map = store._RunStore__expected_runs
    assert run.run_id not in expected_map


def test_num_runs_stat_not_double_counted(tmp_path):
    """get_stats()['num_runs'] must count each physical run once."""
    store = _make_store(tmp_path)
    run = Run(Instance("job", {}), expected=True)
    _schedule(store, run)
    _transition(store, run, State.waiting)

    assert store.get_stats()["num_runs"] == 1


# -------------------------------------------------------------------------------
# 3. query() must return runs in ascending run-id order (documented contract
#    that websocket_summary relies on via runs[::-1]).


def test_query_returns_ascending_order(tmp_path):
    """
    query() chains in-memory expected runs before DB runs; expected runs are
    the newest ids, so the result is no longer globally ascending.
    """
    store = _make_store(tmp_path)

    # Interleave: even indices stay scheduled+expected (in memory), odd indices
    # run to completion (persisted to DB).
    for i in range(6):
        expected = i % 2 == 0
        run = Run(Instance("job", {"n": str(i)}), expected=expected)
        _schedule(store, run)
        if not expected:
            for state in (State.waiting, State.starting, State.running, State.success):
                _transition(store, run, state)

    run_ids = [r.run_id for r in _query(store)]
    assert run_ids == sorted(run_ids, key=lambda rid: int(rid[1:]))


# -------------------------------------------------------------------------------
# 4. RunDB.get() now raises LookupError; API handlers that `except KeyError`
#    around run_store.get() must still turn a missing run into a 404, not a 500.


class _FakeApp:
    def __init__(self, apsis):
        self.apsis = apsis


class _FakeRequest:
    def __init__(self, apsis):
        self.app = _FakeApp(apsis)


class _Apsis:
    def __init__(self, run_store):
        self.run_store = run_store


@pytest.mark.asyncio
async def test_get_missing_run_returns_404(tmp_path):
    """
    GET /runs/<run_id> for a missing (non-expected) run must return 404.

    RunDB.get() now raises LookupError, but the handler catches KeyError.
    KeyError is a subclass of LookupError, not vice versa, so the LookupError
    escapes -> 500 instead of 404.
    """
    store = _make_store(tmp_path)
    request = _FakeRequest(_Apsis(store))

    response = await api.run(request, "rMISSING")

    assert response.status == 404
