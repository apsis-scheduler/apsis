import sqlite3
import threading

import ora
import ujson

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State


def make_db(tmp_path):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    return SqliteDB.open(path)


def make_run_store(tmp_path, min_timestamp=ora.now() - 86400):
    return RunStore(make_db(tmp_path), min_timestamp=min_timestamp)


def test_min_timestamp_none(tmp_path):
    run_store = make_run_store(tmp_path, min_timestamp=None)

    run = Run(Instance("test/job", {"date": "2026-01-01"}), expected=True)
    run_store.add(run)

    summaries = list(run_store.summaries())
    assert len(summaries) == 1


def test_timestamp_updated(tmp_path):
    run_store = make_run_store(tmp_path)

    t0 = ora.now()
    run = Run(Instance("test/job", {"date": "2026-01-01"}), expected=False)
    run_store.add(run)

    # transition to scheduled so it gets persisted
    run._transition(t0, State.scheduled, times={"schedule": t0})
    run_store.update(run, t0)

    summary_db = run_store._RunStore__summary_db
    conn = summary_db._RunSummaryDB__connection.connection

    # query summary timestamp after initial insert
    (ts0,) = conn.execute(
        "SELECT timestamp FROM run_summary WHERE run_id = ?", (run.run_id,)
    ).fetchone()

    t1 = t0 + 60
    run._transition(t1, State.starting, times={"starting": t1})
    run_store.update(run, t1)

    # query summary timestamp after transition
    (ts1,) = conn.execute(
        "SELECT timestamp FROM run_summary WHERE run_id = ?", (run.run_id,)
    ).fetchone()

    # assert timestamp is updated
    assert ts1 > ts0


def test_no_db_lock_truncate(tmp_path):
    """ClockDB write must not fail when an external process does TRUNCATE checkpoints
    while a summaries() generator is open.
    """
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(db_path)
    db = SqliteDB.open(db_path, timeout=5)
    run_store = RunStore(db, min_timestamp=ora.now() - 86400)

    for i in range(100):
        run = Run(Instance("test/job", {"date": f"2026-01-{i:02d}"}), expected=False)
        run_store.add(run)
        run._transition(ora.now(), State.scheduled, times={"schedule": ora.now()})
        run_store.update(run, ora.now())

    stop = threading.Event()
    errors = []

    def truncate_loop():
        conn = sqlite3.connect(str(db_path), timeout=1)
        conn.execute("PRAGMA journal_mode=WAL")
        while not stop.is_set():
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.OperationalError:
                pass
            stop.wait(0.01)
        conn.close()

    t = threading.Thread(target=truncate_loop, daemon=True)
    t.start()

    try:
        for _ in range(25):
            gen = run_store.summaries()
            next(gen)
            try:
                db.clock_db.set_time(ora.now())
            except sqlite3.OperationalError as exc:
                errors.append(exc)
    finally:
        stop.set()
        t.join()

    assert not errors, f"ClockDB.set_time() failed {len(errors)} times: {errors[0]}"


def test_summaries_includes_expected_runs(tmp_path):
    """Expected (in-memory only) runs appear in summaries()."""
    run_store = make_run_store(tmp_path)

    run = Run(Instance("test/job", {"date": "2026-01-01"}), expected=True)
    run_store.add(run)

    summaries = list(run_store.summaries())
    assert len(summaries) == 1

    parsed = ujson.loads(summaries[0])
    assert parsed["run_summary"]["run_id"] == run.run_id
    assert parsed["run_summary"]["state"] == "new"


def test_summaries_includes_persisted_runs(tmp_path):
    """Non-expected runs are persisted and appear in summaries() via the DB."""
    run_store = make_run_store(tmp_path)

    run = Run(Instance("test/job", {}), expected=False)
    run_store.add(run)

    # transition to scheduled so it's no longer expected
    run._transition(ora.now(), State.scheduled, times={"schedule": ora.now()})
    run_store.update(run, ora.now())

    summaries = list(run_store.summaries())
    assert len(summaries) == 1

    parsed = ujson.loads(summaries[0])
    assert parsed["run_summary"]["run_id"] == run.run_id
    assert parsed["run_summary"]["state"] == "scheduled"


def test_summaries_updated_on_transition(tmp_path):
    """Summary payload is updated when a run transitions state."""
    run_store = make_run_store(tmp_path)

    run = Run(Instance("test/job", {}), expected=False)
    run_store.add(run)

    # scheduled
    run._transition(ora.now(), State.scheduled, times={"schedule": ora.now()})
    run_store.update(run, ora.now())

    summaries = list(run_store.summaries())
    parsed = ujson.loads(summaries[0])
    assert parsed["run_summary"]["state"] == "scheduled"

    # waiting
    run._transition(ora.now(), State.waiting)
    run_store.update(run, ora.now())

    summaries = list(run_store.summaries())
    parsed = ujson.loads(summaries[0])
    assert parsed["run_summary"]["state"] == "waiting"


def test_summaries_cover_all_runs(tmp_path):
    """Every run in the store has exactly one summary."""
    run_store = make_run_store(tmp_path)

    # mix of expected and non-expected runs
    expected_runs = []
    for i in range(5):
        run = Run(Instance(f"test/expected-{i}", {}), expected=True)
        run_store.add(run)
        expected_runs.append(run)

    persisted_runs = []
    for i in range(5):
        run = Run(Instance(f"test/persisted-{i}", {}), expected=False)
        run_store.add(run)
        run._transition(ora.now(), State.scheduled, times={"schedule": ora.now()})
        run_store.update(run, ora.now())
        persisted_runs.append(run)

    all_runs = expected_runs + persisted_runs
    summaries = list(run_store.summaries())

    # one summary per run
    assert len(summaries) == len(all_runs)

    # every run_id is present
    summary_run_ids = {ujson.loads(s)["run_summary"]["run_id"] for s in summaries}
    expected_run_ids = {r.run_id for r in all_runs}
    assert summary_run_ids == expected_run_ids


def test_summaries_no_duplicates_for_expected_becoming_persisted(tmp_path):
    """A run that starts expected and becomes persisted doesn't appear twice."""
    run_store = make_run_store(tmp_path)

    run = Run(Instance("test/job", {}), expected=True)
    run_store.add(run)

    # expected run becomes non-expected (e.g. transitions out of scheduled)
    run.expected = False
    run._transition(ora.now(), State.scheduled, times={"schedule": ora.now()})
    run_store.update(run, ora.now())

    summaries = list(run_store.summaries())
    summary_run_ids = [ujson.loads(s)["run_summary"]["run_id"] for s in summaries]
    assert summary_run_ids.count(run.run_id) == 1
