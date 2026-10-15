import random
import sqlite3

import ora
import pytest

from apsis import run_window
from apsis.run_window import (
    JobFilter,
    WindowSpec,
    WindowUnavailable,
    query_window,
    select_window,
)
from apsis.runs import Instance, Run
from apsis.sqlite import SqliteDB
from apsis.states import State

NOW = ora.now()
MIN_TIMESTAMP = NOW - 86400
# state groups from runs.js
CLIENT_GROUPS = {
    "new": "S",
    "scheduled": "S",
    "waiting": "R",
    "starting": "R",
    "running": "R",
    "stopping": "R",
    "success": "C",
    "failure": "C",
    "error": "C",
    "skipped": "C",
}


def make_run(rowid, job_id, args, state, sched, *, old=False, expected=False, labels=()):
    run = Run(Instance(job_id, args), expected=expected)
    run.run_id = f"r{rowid}"
    run.timestamp = NOW - (2 * 86400 if old else 60)
    run.state = state
    run.times = {} if sched is None else {"schedule": sched}
    run.meta = {"job": {"labels": list(labels)}}
    return run


def make_db(tmp_path, runs):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    db = SqliteDB.open(path)
    for run in runs:
        db.run_db.upsert(run)
    return db.run_db


def client_window(runs, spec, anchor):
    """
    Returns the shown run IDs, group counts, and edges that the web UI runs
    list computes with every run in memory.
    """

    path, keywords, labels = spec.job_filter.path, spec.job_filter.keywords, spec.job_filter.labels

    def match(r):
        args = r.inst.args
        job_id = r.inst.job_id
        return (
            "schedule" in r.times
            and r.timestamp >= MIN_TIMESTAMP
            and (spec.job_ids is None or job_id in spec.job_ids)
            and (spec.states is None or r.state.name in spec.states)
            and all(k in args and (v is None or args[k] == v) for k, v in spec.args)
            and (path is None or job_id == path or job_id.startswith(path + "/"))
            and all(k.lower() in job_id.lower() for k in keywords)
            and set(labels) <= set(r.meta["job"]["labels"])
        )

    def key(r):
        sgrp = CLIENT_GROUPS[r.state.name]
        if not spec.grouping or sgrp == "R":
            return r.run_id
        return sgrp, r.inst.job_id, tuple(sorted(r.inst.args.items()))

    def pos(r):
        return r.times["schedule"], int(r.run_id[1:])

    runs = sorted(filter(match, runs), key=pos)
    groups = {}
    for r in runs:
        groups.setdefault(key(r), []).append(r)
    principals = sorted(
        ((m[0] if k[0] == "S" else m[-1], len(m)) for k, m in groups.items()),
        key=lambda p: pos(p[0]),
    )

    before = [p for p, _ in principals if p.times["schedule"] < anchor]
    after = [p for p, _ in principals if p.times["schedule"] >= anchor]
    nb, na = select_window(len(before), len(after), spec.limit)
    shown = before[len(before) - nb :] + after[:na]

    def edge(omitted, outer, nearest):
        if not omitted:
            return None
        # anchoring at the outer shown run, or the nearest omitted one, moves the window
        return str((outer or nearest).times["schedule"])

    earlier = before[: len(before) - nb]
    later = after[na:]
    return (
        [r.run_id for r in shown],
        {p.run_id: n for p, n in principals if p in shown},
        edge(earlier, shown[0] if nb else None, earlier[-1] if earlier else None),
        edge(later, shown[-1] if na else None, later[0] if later else None),
    )


def random_runs(rng):
    runs = []
    # few distinct times so schedule times tie within and across groups
    for i, t in enumerate(rng.choices(range(40), k=60)):
        expected = i >= 50
        runs.append(
            make_run(
                1000 + i if expected else i + 1,
                rng.choice(["a/x", "a/y", "b"]),
                rng.choice([{}, {"x": "1"}, {"x": "2"}]),
                State.scheduled if expected else rng.choice(list(State)),
                # expected runs tend to come after the db runs
                None if rng.random() < 0.05 else NOW + (t + 20 * expected) * 60,
                old=rng.random() < 0.1,
                expected=expected,
                labels=rng.choice([(), ("l",), ("l", "m")]),
            )
        )
    return runs


@pytest.mark.parametrize("job_path_rows", [0, 10_000])
@pytest.mark.asyncio
async def test_window_matches_client(tmp_path, monkeypatch, job_path_rows):
    """
    Random windows over db and expected runs, in many small pages, match the
    web UI through both the schedule index and the job index.
    """
    monkeypatch.setattr(run_window, "PAGE_ROWS", 3)
    monkeypatch.setattr(run_window, "JOB_PATH_ROWS", job_path_rows)
    rng = random.Random(job_path_rows)
    runs = random_runs(rng)
    run_db = make_db(tmp_path, [r for r in runs if not r.expected])
    expected = [r for r in runs if r.expected]

    for _ in range(300):
        spec = WindowSpec(
            # some anchors fall exactly on a run
            anchor=NOW + rng.randrange(-2, 62) * 60 + rng.choice([0, 0, 30]),
            limit=rng.randint(1, 7),
            grouping=rng.random() < 0.5,
            job_ids=rng.choice([None, frozenset(), frozenset({"a/x"}), frozenset({"a/x", "b"})]),
            states=rng.choice([None, frozenset({"success", "failure"}), frozenset({"scheduled"})]),
            args=rng.choice([(), (("x", "1"),), (("x", None),)]),
            job_filter=rng.choice(
                [
                    JobFilter(),
                    JobFilter(path="a"),
                    JobFilter(keywords=("X",)),
                    JobFilter(labels=("l", "m")),
                    JobFilter(path="c"),
                ]
            ),
        )
        window = await query_window(run_db, expected, spec, min_timestamp=MIN_TIMESTAMP)
        assert window.complete
        assert (
            window.run_ids,
            window.counts,
            window.earlier,
            window.later,
        ) == client_window(runs, spec, spec.anchor), spec


@pytest.mark.parametrize(
    "before, after, limit, shown",
    [
        (10, 10, 5, (2, 3)),
        (1, 10, 5, (1, 4)),
        (10, 1, 5, (4, 1)),
        (1, 2, 5, (1, 2)),
    ],
)
def test_select_window(before, after, limit, shown):
    assert select_window(before, after, limit) == shown


@pytest.mark.asyncio
async def test_window_budget(tmp_path, monkeypatch):
    """
    The walk budget gives an incomplete window whose edge picks up where the walk stopped.
    """
    runs = [make_run(i, "a", {"i": str(i)}, State.success, NOW + i) for i in range(1, 21)]
    run_db = make_db(tmp_path, runs)
    monkeypatch.setattr(run_window, "PAGE_ROWS", 2)
    monkeypatch.setattr(run_window, "SIDE_BUDGET", 4)
    spec = WindowSpec(anchor=NOW + 100, states=frozenset({"failure"}))
    window = await query_window(run_db, [], spec, min_timestamp=None)
    assert window.run_ids == []
    assert window.earlier == str(NOW + 17)
    assert not window.complete


@pytest.mark.asyncio
async def test_edges_move_window(tmp_path):
    """
    Edge times move the window past what's shown, and runs of shown groups
    don't make more earlier.
    """
    runs = [make_run(i, "a", {}, State.success, NOW + i * 60) for i in range(1, 6)]
    runs.append(make_run(6, "b", {}, State.success, NOW))
    run_db = make_db(tmp_path, runs)

    # a's group shows its latest run after the anchor and only b is earlier
    window = await query_window(
        run_db, [], WindowSpec(anchor=NOW + 150, limit=1), min_timestamp=None
    )
    assert window.run_ids == ["r5"]
    assert window.earlier == str(NOW)

    window = await query_window(
        run_db, [], WindowSpec(anchor=NOW + 150, limit=2), min_timestamp=None
    )
    assert window.run_ids == ["r6", "r5"]
    assert window.earlier is None
    assert window.later is None


@pytest.mark.asyncio
async def test_no_jobs_or_index(tmp_path):
    # no jobs needs no db
    window = await query_window(None, [], WindowSpec(job_ids=frozenset()), min_timestamp=None)
    assert window.run_ids == []

    run_db = make_db(tmp_path, [make_run(1, "a", {}, State.success, NOW)])
    run_db._RunDB__connection.connection.execute("DROP INDEX idx_runs_schedule")
    # dropping the index after open makes the window unavailable from then on
    with pytest.raises(WindowUnavailable):
        await query_window(run_db, [], WindowSpec(), min_timestamp=None)
    assert not run_db.has_schedule_index
    with pytest.raises(WindowUnavailable):
        await query_window(run_db, [], WindowSpec(), min_timestamp=None)

    # other db errors pass through
    class LockedDB:
        has_schedule_index = True

        def schedule_page(self, **kw_args):
            raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError):
        await query_window(LockedDB(), [], WindowSpec(), min_timestamp=None)

    # a small job set doesn't need the index
    window = await query_window(run_db, [], WindowSpec(job_ids=frozenset("a")), min_timestamp=None)
    assert window.run_ids == ["r1"]


def test_job_filter():
    assert JobFilter().matches("a/b", [])
    f = JobFilter(path="a/b", keywords=("Lo",), labels=("x",))
    assert f.matches("a/b/slow", ["x", "y"])
    assert not f.matches("a/bslow", ["x"])
    assert not f.matches("a/b/fast", ["x"])
    assert not f.matches("a/b/slow", ["y"])
