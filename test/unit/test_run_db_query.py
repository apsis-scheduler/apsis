"""
Tests for RunDB.query() predicate pushdown to SQLite.
"""

import ora

from apsis.runs import Instance, Run
from apsis.sqlite import SqliteDB
from apsis.states import State


def _make_run(run_db, job_id, args, state=State.success):
    """Create a run, insert it via upsert, and return it."""
    inst = Instance(job_id, args)
    run = Run(inst)
    _make_run._counter += 1
    run.run_id = f"r{_make_run._counter}"
    run.timestamp = ora.now()
    run.state = state
    run.times = {state.name: run.timestamp}
    run.meta = {}
    run_db.upsert(run)
    return run


_make_run._counter = 0


def _setup(tmp_path):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path=path)
    db = SqliteDB.open(path)
    _make_run._counter = 0
    return db.run_db


# -------------------------------------------------------------------------------


def test_query_all(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "2026-01-01"})
    r2 = _make_run(run_db, "job/b", {"date": "2026-01-02"})

    runs = run_db.query()
    assert {r.run_id for r in runs} == {r1.run_id, r2.run_id}


def test_query_by_job_id(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "2026-01-01"})
    _make_run(run_db, "job/b", {"date": "2026-01-02"})

    runs = run_db.query(job_id="job/a")
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_by_state(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {}, state=State.success)
    _make_run(run_db, "job/a", {}, state=State.failure)
    r3 = _make_run(run_db, "job/a", {}, state=State.error)

    runs = run_db.query(state=(State.success, State.error))
    assert {r.run_id for r in runs} == {r1.run_id, r3.run_id}


def test_query_by_run_ids(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {})
    _make_run(run_db, "job/b", {})
    r3 = _make_run(run_db, "job/c", {})

    runs = run_db.query(run_ids=[r1.run_id, r3.run_id])
    assert {r.run_id for r in runs} == {r1.run_id, r3.run_id}


def test_query_by_args_exact(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "us"})
    _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "eu"})
    _make_run(run_db, "job/a", {"date": "2026-01-01"})

    runs = run_db.query(args={"date": "2026-01-01", "strat": "us"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_by_args_no_superset_match(tmp_path):
    """args= requires exact match, not superset."""
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "us"})

    runs = run_db.query(args={"date": "2026-01-01"})
    assert runs == []


def test_query_by_with_args(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "us"})
    r2 = _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "eu"})
    _make_run(run_db, "job/a", {"date": "2026-01-02", "strat": "us"})

    runs = run_db.query(with_args={"date": "2026-01-01"})
    assert {r.run_id for r in runs} == {r1.run_id, r2.run_id}


def test_query_with_args_superset_matches(tmp_path):
    """with_args= matches runs with extra keys beyond those specified."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "us", "extra": "val"})

    runs = run_db.query(with_args={"date": "2026-01-01", "strat": "us"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_args_overrides_with_args(tmp_path):
    """When both args and with_args are provided, args takes precedence."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "2026-01-01"})
    _make_run(run_db, "job/a", {"date": "2026-01-01", "strat": "us"})

    runs = run_db.query(args={"date": "2026-01-01"}, with_args={"date": "2026-01-01"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_combined_predicates(tmp_path):
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {"date": "2026-01-01"}, state=State.success)
    r2 = _make_run(run_db, "job/a", {"date": "2026-01-01"}, state=State.failure)
    _make_run(run_db, "job/b", {"date": "2026-01-01"}, state=State.failure)

    runs = run_db.query(
        job_id="job/a",
        state=(State.failure,),
        args={"date": "2026-01-01"},
    )
    assert [r.run_id for r in runs] == [r2.run_id]


def test_query_empty_args(tmp_path):
    """Runs with empty args match args={}."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {})
    _make_run(run_db, "job/a", {"date": "2026-01-01"})

    runs = run_db.query(args={})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_json_order_independence(tmp_path):
    """Args match regardless of JSON serialization order."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"z_key": "val1", "a_key": "val2"})

    runs = run_db.query(args={"a_key": "val2", "z_key": "val1"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_args_with_dots_in_key(tmp_path):
    """Args with dots in keys should match correctly."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"a.b": "x"})
    _make_run(run_db, "job/a", {"a": {"b": "x"}})  # nested structure, should not match

    runs = run_db.query(args={"a.b": "x"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_with_args_dots_in_key(tmp_path):
    """with_args with dots in keys should match correctly."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"a.b": "x", "other": "val"})

    runs = run_db.query(with_args={"a.b": "x"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_args_with_quotes_in_key(tmp_path):
    """Args with double quotes in keys should match correctly."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {'wei"rd': "y"})

    runs = run_db.query(args={'wei"rd': "y"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_query_args_with_special_chars_in_value(tmp_path):
    """Args with special chars in values should match correctly."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"name": "o'brien"})
    r2 = _make_run(run_db, "job/a", {"path": "a/b/c"})

    runs = run_db.query(args={"name": "o'brien"})
    assert [r.run_id for r in runs] == [r1.run_id]

    runs = run_db.query(args={"path": "a/b/c"})
    assert [r.run_id for r in runs] == [r2.run_id]


def test_query_args_with_spaces_in_key(tmp_path):
    """Args with spaces in keys should match correctly."""
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"key with spaces": "value"})

    runs = run_db.query(args={"key with spaces": "value"})
    assert [r.run_id for r in runs] == [r1.run_id]


def test_count_runs_all(tmp_path):
    """count_runs() returns total count without deserialization."""
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {"date": "2026-01-01"})
    _make_run(run_db, "job/b", {"date": "2026-01-02"})
    _make_run(run_db, "job/c", {"date": "2026-01-03"})

    count = run_db.count_runs()
    assert count == 3


def test_count_runs_by_job_id(tmp_path):
    """count_runs() filters by job_id."""
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {})
    _make_run(run_db, "job/a", {})
    _make_run(run_db, "job/b", {})

    assert run_db.count_runs(job_id="job/a") == 2
    assert run_db.count_runs(job_id="job/b") == 1


def test_count_runs_by_state(tmp_path):
    """count_runs() filters by state."""
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {}, state=State.success)
    _make_run(run_db, "job/a", {}, state=State.failure)
    _make_run(run_db, "job/a", {}, state=State.success)

    assert run_db.count_runs(state=State.success) == 2
    assert run_db.count_runs(state=State.failure) == 1
    assert run_db.count_runs(state=[State.success, State.failure]) == 3


def test_count_runs_by_args(tmp_path):
    """count_runs() filters by exact args match."""
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {"date": "2026-01-01"})
    _make_run(run_db, "job/a", {"date": "2026-01-01"})
    _make_run(run_db, "job/a", {"date": "2026-01-02"})

    assert run_db.count_runs(args={"date": "2026-01-01"}) == 2
    assert run_db.count_runs(args={"date": "2026-01-02"}) == 1


def test_count_runs_matches_query_length(tmp_path):
    """count_runs() returns same count as len(query()) for same filters."""
    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {"date": "2026-01-01"}, state=State.success)
    _make_run(run_db, "job/a", {"date": "2026-01-01"}, state=State.failure)
    _make_run(run_db, "job/b", {"date": "2026-01-02"}, state=State.success)
    _make_run(run_db, "job/b", {"date": "2026-01-03"}, state=State.success)

    # Test various combinations
    assert run_db.count_runs() == len(list(run_db.query()))
    assert run_db.count_runs(job_id="job/a") == len(list(run_db.query(job_id="job/a")))
    assert run_db.count_runs(state=State.success) == len(list(run_db.query(state=State.success)))
    assert run_db.count_runs(job_id="job/b", state=State.success) == len(
        list(run_db.query(job_id="job/b", state=State.success))
    )
