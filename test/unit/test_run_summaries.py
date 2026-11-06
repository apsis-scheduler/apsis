import random
import sqlite3
import threading
from types import SimpleNamespace

import ora
import pytest
import ujson

from apsis import procstar, run_window
from apsis.jobs import InMemoryJobs, Job
from apsis.run_window import JobFilter, WindowSpec
from apsis.runs import Instance, Run, RunStore
from apsis.service import summary
from apsis.service.summary import SummaryScope, send_scoped_init
from apsis.sqlite import SqliteDB
from apsis.states import State


def make_db(tmp_path):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    return SqliteDB.open(path)


def make_run_store(tmp_path, lookback=86400):
    return RunStore(make_db(tmp_path), lookback=lookback)


def all_summaries(run_store: RunStore) -> list[str]:
    return [s for page in run_store.summaries() for s in page]


def test_lookback_none(tmp_path):
    run_store = make_run_store(tmp_path, lookback=None)

    run = Run(Instance("test/job", {"date": "2026-01-01"}), expected=True)
    run_store.add(run)

    summaries = all_summaries(run_store)
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
    run_store = RunStore(db, lookback=86400)

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

    summaries = all_summaries(run_store)
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

    summaries = all_summaries(run_store)
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

    summaries = all_summaries(run_store)
    parsed = ujson.loads(summaries[0])
    assert parsed["run_summary"]["state"] == "scheduled"

    # waiting
    run._transition(ora.now(), State.waiting)
    run_store.update(run, ora.now())

    summaries = all_summaries(run_store)
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
    summaries = all_summaries(run_store)

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

    summaries = all_summaries(run_store)
    summary_run_ids = [ujson.loads(s)["run_summary"]["run_id"] for s in summaries]
    assert summary_run_ids.count(run.run_id) == 1


def test_summaries_paged(tmp_path, monkeypatch):
    """Summaries come in pages of at most SUMMARY_PAGE_SIZE, expected runs first."""
    monkeypatch.setattr("apsis.runs.SUMMARY_PAGE_SIZE", 2)
    monkeypatch.setattr("apsis.sqlite.SUMMARY_PAGE_SIZE", 2)
    run_store = make_run_store(tmp_path)

    expected_ids = set()
    for i in range(3):
        run = Run(Instance(f"test/expected-{i}", {}), expected=True)
        run_store.add(run)
        expected_ids.add(run.run_id)

    persisted_ids = set()
    for i in range(3):
        run = Run(Instance(f"test/persisted-{i}", {}), expected=False)
        run_store.add(run)
        persisted_ids.add(run.run_id)

    pages = [
        [ujson.loads(s)["run_summary"]["run_id"] for s in page] for page in run_store.summaries()
    ]
    assert [len(p) for p in pages] == [2, 1, 2, 1]
    assert set(pages[0] + pages[1]) == expected_ids
    assert set(pages[2] + pages[3]) == persisted_ids


def add_runs(run_store, job_id, count):
    runs = []
    for _ in range(count):
        run = Run(Instance(job_id, {}), expected=False)
        run_store.add(run)
        runs.append(run)
    return runs


def run_ids(payloads):
    return [ujson.loads(p)["run_summary"]["run_id"] for p in payloads]


def test_job_summaries_pages(tmp_path):
    run_store = make_run_store(tmp_path)
    a = add_runs(run_store, "a", 5)
    add_runs(run_store, "b", 2)

    # an old timestamp puts a run outside the lookback window
    conn = run_store._RunStore__summary_db._RunSummaryDB__connection.connection
    conn.execute("UPDATE runs SET timestamp = 0 WHERE run_id = ?", (a[2].run_id,))
    conn.commit()

    pages = [run_ids(p) for p in run_store.job_summaries("a", page_size=2)]
    assert pages == [[a[4].run_id, a[3].run_id], [a[1].run_id], [a[0].run_id]]

    # a full page outside the lookback doesn't end the walk
    conn.execute("UPDATE runs SET timestamp = 0 WHERE run_id = ?", (a[1].run_id,))
    conn.commit()
    pages = [run_ids(p) for p in run_store.job_summaries("a", page_size=2)]
    assert pages == [[a[4].run_id, a[3].run_id], [], [a[0].run_id]]

    summary_db = run_store._RunStore__summary_db
    pages = list(summary_db.query_job("a", min_timestamp=None, page_size=10))
    assert run_ids(pages[0]) == [r.run_id for r in reversed(a)]
    assert list(summary_db.query_job("c", min_timestamp=None, page_size=10)) == [[]]


class FakeWs:
    def __init__(self):
        self.frames = []

    async def send(self, data):
        self.frames.append(ujson.loads(data))

    def ids(self):
        """
        Returns the run or job ID of each message, by frame, before the done frame.
        """
        return [
            [m["run_summary"]["run_id"] if "run_summary" in m else m["job"]["job_id"] for m in f]
            for f in self.frames[:-1]
        ]


@pytest.mark.asyncio
async def test_send_scoped_init_runs(tmp_path, monkeypatch):
    monkeypatch.setattr(summary, "PAGE_ROWS", 2)
    run_store = make_run_store(tmp_path)
    expected = Run(Instance("a", {}), expected=True)
    run_store.add(expected)
    run_store.add(Run(Instance("d", {}), expected=True))
    a = add_runs(run_store, "a", 5)
    b = add_runs(run_store, "b", 2)
    apsis = SimpleNamespace(run_store=run_store)

    ws = FakeWs()
    scope = SummaryScope(init=True, types=frozenset({"run"}), job_ids=frozenset({"a", "b", "c"}))
    await send_scoped_init(apsis, ws, scope)
    assert ws.ids() == [
        [expected.run_id],
        [a[4].run_id, a[3].run_id],
        [a[2].run_id, a[1].run_id],
        [a[0].run_id],
        [b[1].run_id, b[0].run_id],
    ]
    assert ws.frames[-1] == [{"type": "summary_init_done"}]


@pytest.mark.asyncio
async def test_send_scoped_init_jobs_and_conns(tmp_path, monkeypatch):
    run_store = make_run_store(tmp_path)
    (run,) = add_runs(run_store, "x", 1)
    apsis = SimpleNamespace(jobs=InMemoryJobs([Job("x"), Job("y")]), run_store=run_store)

    conn = SimpleNamespace(to_jso=lambda: {"conn_id": "c"})
    with monkeypatch.context() as m:
        m.setattr(procstar, "get_agent_server", lambda: SimpleNamespace(connections={"c": conn}))
        ws = FakeWs()
        scope = SummaryScope(
            init=True, types=frozenset({"job", "agent_conn"}), job_ids=frozenset({"x", "missing"})
        )
        await send_scoped_init(apsis, ws, scope)
    assert [m["job"]["job_id"] for m in ws.frames[0]] == ["x"]
    assert ws.frames[1:] == [
        [{"type": "agent_conn", "conn": {"conn_id": "c"}}],
        [{"type": "summary_init_done"}],
    ]

    # no job ids sends all jobs and runs, and no agent server sends no conns
    ws = FakeWs()
    await send_scoped_init(apsis, ws, SummaryScope(init=True))
    assert ws.ids() == [["x", "y"], [run.run_id]]


@pytest.mark.asyncio
async def test_window_summaries(tmp_path):
    run_store = make_run_store(tmp_path)
    t = ora.now()
    expected = Run(Instance("a", {}), expected=True)
    expected.times["schedule"] = t + 60
    run_store.add(expected)
    (run,) = add_runs(run_store, "a", 1)
    run._transition(t, State.scheduled, times={"schedule": t})
    run_store.update(run, t)

    window = await run_store.query_window(WindowSpec(anchor=t, grouping=False))
    assert window.run_ids == [run.run_id, expected.run_id]
    summaries = run_store.get_summaries([expected.run_id, "r999", run.run_id])
    assert run_ids(summaries) == [expected.run_id, run.run_id]


@pytest.mark.asyncio
async def test_window_lookback_keeps_expected_runs(tmp_path):
    """A window drops db runs older than the lookback but keeps old expected runs."""
    run_store = make_run_store(tmp_path, lookback=3600)
    t = ora.now()
    (done,) = add_runs(run_store, "a", 1)
    done._transition(t - 7200, State.success, times={"schedule": t - 7200}, force=True)
    run_store.update(done, t - 7200)
    old, new = Run(Instance("a", {}), expected=True), Run(Instance("a", {}), expected=True)
    for run, ts, sched in (old, t - 7200, t + 60), (new, t, t + 120):
        run_store.add(run)
        run._transition(ts, State.scheduled, times={"schedule": sched})
        run_store.update(run, ts)

    window = await run_store.query_window(WindowSpec(anchor=t))
    assert window.run_ids == [old.run_id]
    assert window.counts == {old.run_id: 2}


def add_labeled_run(
    run_store: RunStore,
    job_id: str,
    labels: list[str],
    sched: ora.Time,
    *,
    state: State = State.scheduled,
    timestamp: ora.Time | None = None,
    expected: bool = False,
) -> Run:
    """Adds a run of `job_id` with job `labels`, scheduled at `sched`."""
    run = Run(Instance(job_id, {}), expected=expected)
    run_store.add(run)
    run.meta["job"] = {"labels": labels}
    run._transition(
        sched if timestamp is None else timestamp, state, times={"schedule": sched}, force=True
    )
    run_store.update(run, run.timestamp)
    return run


@pytest.mark.asyncio
async def test_window_label_jobs(tmp_path, monkeypatch):
    """
    A label window reads only runs of jobs whose runs had the label, saved
    before the store started or updated since, and checks each run's labels.
    """
    monkeypatch.setattr("apsis.sqlite.SUMMARY_PAGE_SIZE", 1)
    db = make_db(tmp_path)
    t = ora.now()
    old_store = RunStore(db, lookback=86400)
    # saved runs with tied timestamps
    add_labeled_run(old_store, "gained", [], t - 60, timestamp=t - 60)
    lost = add_labeled_run(old_store, "lost", ["l"], t - 50, timestamp=t - 60)
    add_labeled_run(old_store, "other", ["m"], t - 40, timestamp=t - 60)
    run_store = RunStore(db, lookback=86400)
    await run_store.load_label_jobs()
    add_labeled_run(run_store, "lost", [], t - 30)
    gained = add_labeled_run(run_store, "gained", ["l"], t - 20)

    read = []
    job_page = db.run_db.job_page
    monkeypatch.setattr(
        db.run_db, "job_page", lambda job_id, **kw: read.append(job_id) or job_page(job_id, **kw)
    )
    window = await run_store.query_window(WindowSpec(anchor=t, job_filter=JobFilter(labels=("l",))))
    assert window.run_ids == [lost.run_id, gained.run_id]
    assert set(read) == {"lost", "gained"}


@pytest.mark.parametrize("job_path_rows", [0, 10_000])
@pytest.mark.asyncio
async def test_window_label_jobs_match_walk(tmp_path, monkeypatch, job_path_rows):
    """
    Random label windows narrowed to the jobs of the label map match walking
    every run.
    """
    monkeypatch.setattr(run_window, "PAGE_ROWS", 3)
    monkeypatch.setattr(run_window, "JOB_PATH_ROWS", job_path_rows)
    monkeypatch.setattr("apsis.sqlite.SUMMARY_PAGE_SIZE", 3)
    rng = random.Random(job_path_rows)
    db = make_db(tmp_path)
    t = ora.now()
    job_labels = {"a": [[], ["l"]], "b": [["m"]], "c": [["l", "m"]], "d": [[]]}
    run_store = RunStore(db, lookback=3600)
    for i in range(80):
        if i == 50:
            # restart so later runs reach the label map only as they update
            run_store = RunStore(db, lookback=3600)
            # job c loses its labels and job d gains one
            job_labels.update(c=[[]], d=[["l"]])
        job_id = rng.choice(list(job_labels))
        expected = i >= 70
        add_labeled_run(
            run_store,
            job_id,
            # expected runs get any labels
            rng.choice([[], ["l"], ["m"], ["l", "m"]] if expected else job_labels[job_id]),
            t + rng.randrange(-30, 30) * 60,
            state=rng.choice([State.scheduled, State.success, State.failure]),
            # timestamps tie and some are past the lookback
            timestamp=t - rng.choice([60, 60, 120, 7200]),
            expected=expected,
        )

    specs = [
        WindowSpec(
            anchor=t + rng.randrange(-35, 35) * 60,
            limit=rng.randint(2, 6),
            grouping=rng.random() < 0.5,
            job_filter=JobFilter(
                path=rng.choice([None, "a"]),
                labels=rng.choice([("l",), ("m",), ("l", "m")]),
            ),
        )
        for _ in range(100)
    ]
    walked = [await run_store.query_window(spec) for spec in specs]
    await run_store.load_label_jobs()
    assert [await run_store.query_window(spec) for spec in specs] == walked
