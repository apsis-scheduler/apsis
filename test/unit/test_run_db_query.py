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


def test_get_malformed_run_id(tmp_path):
    """
    RunDB.get() must raise LookupError for malformed IDs, not AssertionError
    or ValueError.  Even under python -O the wrong-run bug ("x123" resolving
    to rowid 123) must not occur, because _parse_run_id checks the prefix
    explicitly rather than via assert.
    """
    import pytest

    run_db = _setup(tmp_path)
    _make_run(run_db, "job/a", {}, state=State.success)  # rowid 1 -> "r1"

    # Well-formed but unknown ID: LookupError.
    with pytest.raises(LookupError):
        run_db.get("r9999")

    # Malformed IDs: also LookupError (not AssertionError or ValueError).
    for bad in ("", "abc", "x1", "r", "rabc", "r-1", None, 5):
        with pytest.raises(LookupError):
            run_db.get(bad)


def test_query_malformed_run_ids(tmp_path):
    """
    RunDB.query(run_ids=[...]) must silently skip malformed IDs rather than
    raising, matching main's behavior for unknown IDs.
    """
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {})

    # Mix of valid and garbage IDs.
    runs = run_db.query(run_ids=[r1.run_id, "not_a_run_id", "r99999"])
    assert [r.run_id for r in runs] == [r1.run_id]

    # All garbage: empty result, no exception.
    runs = run_db.query(run_ids=["abc", "", "x1"])
    assert list(runs) == []


# --- query_paged ---------------------------------------------------------------


def _rowid(run_id):
    return int(run_id[1:])


def test_query_paged_scroll_orders_limits_and_excludes_cursor(tmp_path):
    """
    One bounded scroll covers ordering (rowid desc), the page limit, cursor
    exclusivity (strictly below the last id), and completeness with no dupes or
    cross-job leakage.
    """
    run_db = _setup(tmp_path)
    # other-job noise before, between, and after the matches, so dropping the job
    # filter after page one would leak noise below the first cursor
    made = []
    _make_run(run_db, "job/b", {})
    for _ in range(5):
        made.append(_make_run(run_db, "job/a", {}).run_id)
        _make_run(run_db, "job/b", {})
    expected = sorted(made, key=_rowid, reverse=True)

    seen = []
    lengths = []
    cursor = None
    for _ in range(len(made) + 2):  # bounded so a broken cursor fails, not hangs
        page = run_db.query_paged(job_id="job/a", max_rowid=cursor, limit=2)
        if not page:
            break
        ids = [r.run_id for r in page]
        assert ids == sorted(ids, key=_rowid, reverse=True)  # ordered within the page
        if cursor is not None:
            assert all(_rowid(i) < cursor for i in ids)  # cursor excludes at and above
        lengths.append(len(page))
        seen.extend(ids)
        cursor = _rowid(page[-1].run_id)
    else:
        raise AssertionError("scroll did not terminate")

    assert lengths == [2, 2, 1]  # full, full, partial terminal page under limit 2
    assert seen == expected  # descending, no dupes, no skips, no cross-job leakage


def test_query_paged_with_args_and_state(tmp_path):
    run_db = _setup(tmp_path)
    r1 = _make_run(run_db, "job/a", {"date": "d1"}, state=State.success)
    _make_run(run_db, "job/a", {"date": "d2"}, state=State.success)
    _make_run(run_db, "job/a", {"date": "d1"}, state=State.failure)

    page = run_db.query_paged(
        job_id="job/a", with_args={"date": "d1"}, state=State.success, limit=10
    )
    assert [r.run_id for r in page] == [r1.run_id]


def test_query_paged_min_timestamp(tmp_path):
    run_db = _setup(tmp_path)
    old = _make_run(run_db, "job/a", {})
    old.timestamp = ora.now() - 10000
    old.times = {old.state.name: old.timestamp}
    run_db.upsert(old)
    new = _make_run(run_db, "job/a", {})

    page = run_db.query_paged(job_id="job/a", min_timestamp=ora.now() - 100, limit=10)
    assert [r.run_id for r in page] == [new.run_id]


def test_open_backfills_pagination_index(tmp_path):
    """
    Opening a database that predates the (job_id, rowid) pagination index
    backfills it, so existing deployments get it without a manual migration.
    """
    import sqlite3

    path = str(tmp_path / "apsis.db")
    SqliteDB.create(path=path)

    def has_index():
        with sqlite3.connect(path) as conn:
            return "idx_runs_job_rowid" in {r[1] for r in conn.execute("PRAGMA index_list('runs')")}

    # simulate an older db by dropping the index create() added
    with sqlite3.connect(path) as conn:
        conn.execute("DROP INDEX IF EXISTS idx_runs_job_rowid")
    assert not has_index()

    SqliteDB.open(path)  # backfills it
    assert has_index()


# --- schedule time filter ------------------------------------------------------

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"
T3 = "2026-01-10T00:00:00Z"


def _ids(runs):
    return {r.run_id for r in runs}


def _at(run_db, schedule, **kw):
    """a run with nominal time `schedule`, built on the shared _make_run without editing it"""
    run = _make_run(run_db, kw.pop("job_id", "job/a"), kw.pop("args", {}), **kw)
    run.times["schedule"] = ora.Time(schedule)
    run_db.upsert(run)
    return run


def test_query_schedule_bounds(tmp_path):
    """since inclusive, until exclusive, either/both/none, empty span, missing time, offset string"""
    run_db = _setup(tmp_path)
    r1, r2, r3 = _at(run_db, T1), _at(run_db, T2), _at(run_db, T3)
    r_none = _make_run(run_db, "job/a", {})  # no schedule time

    assert _ids(run_db.query()) == {r1.run_id, r2.run_id, r3.run_id, r_none.run_id}
    assert _ids(run_db.query(schedule_since=ora.Time(T2))) == {r2.run_id, r3.run_id}
    assert _ids(run_db.query(schedule_until=ora.Time(T2))) == {r1.run_id}
    assert _ids(run_db.query(schedule_since=ora.Time(T2), schedule_until=ora.Time(T3))) == {
        r2.run_id
    }
    # a span between runs matches nothing
    assert (
        run_db.query(schedule_since="2026-01-06T00:00:00Z", schedule_until="2026-01-07T00:00:00Z")
        == []
    )
    # the missing-time run only shows up unfiltered
    assert r_none.run_id not in _ids(run_db.query(schedule_until=ora.Time(T3)))
    # an offset string bound is normalized to utc, 14:00+05:00 == T2 which is 09:00Z
    assert _ids(run_db.query(schedule_since="2026-01-05T14:00:00+05:00")) == {r2.run_id, r3.run_id}


def test_query_schedule_second_precision(tmp_path):
    """fractional seconds order correctly through real upsert and query, across string widths"""
    run_db = _setup(tmp_path)
    base = "2026-01-05T09:00:00"
    r = {f: _at(run_db, f"{base}{f}Z") for f in ("", ".00000101", ".05", ".5", ".5000001", ".55")}
    nextsec = _at(run_db, "2026-01-05T09:00:01Z")

    # since=.5 keeps .5 inclusive, the larger fractions, and the next second
    assert _ids(run_db.query(schedule_since=ora.Time(f"{base}.5Z"))) == {
        r[".5"].run_id,
        r[".5000001"].run_id,
        r[".55"].run_id,
        nextsec.run_id,
    }
    # until=.5 keeps everything strictly before it
    assert _ids(run_db.query(schedule_until=ora.Time(f"{base}.5Z"))) == {
        r[""].run_id,
        r[".00000101"].run_id,
        r[".05"].run_id,
    }


def test_query_paged_schedule_range_reapplies_filter(tmp_path):
    """
    scroll a span with below-lower, at-or-above-upper, and missing-time rows
    interleaved below the first cursor, so dropping either bound on a later page
    would surface an excluded run. dropping only since -> the below rows leak,
    dropping only until -> the above row leaks.
    """
    run_db = _setup(tmp_path)
    since = ora.Time("2026-01-05T00:00:00Z")
    until = ora.Time("2026-01-06T00:00:00Z")

    expected = []  # in creation order by rowid, excluded rows sit between matches
    expected.append(_at(run_db, "2026-01-05T01:00:00Z").run_id)
    _at(run_db, "2026-01-04T00:00:00Z")  # below lower
    expected.append(_at(run_db, "2026-01-05T02:00:00Z").run_id)
    _at(run_db, "2026-01-06T12:00:00Z")  # above upper
    expected.append(_at(run_db, "2026-01-05T03:00:00Z").run_id)
    _make_run(run_db, "job/a", {})  # missing schedule time
    expected.append(_at(run_db, "2026-01-05T04:00:00Z").run_id)
    _at(run_db, "2026-01-04T12:00:00Z")  # below lower
    expected.append(_at(run_db, "2026-01-05T05:00:00Z").run_id)

    want = list(reversed(expected))  # scroll is newest rowid first
    seen, cursor = [], None
    for _ in range(len(want) + 5):  # bounded so a broken cursor can't hang
        page = run_db.query_paged(
            job_id="job/a", schedule_since=since, schedule_until=until, max_rowid=cursor, limit=2
        )
        if not page:
            break
        seen.extend(r.run_id for r in page)
        cursor = _rowid(page[-1].run_id)
    assert seen == want
