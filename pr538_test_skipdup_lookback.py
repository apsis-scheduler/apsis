"""
Regression test: SkipDuplicate must skip a duplicate that has been in a check
state (waiting/starting/running) longer than the runs.lookback window.

On the sqlite-run-store branch, RunStore.count_runs() ignores min_timestamp
(so it *sees* the old duplicate) while RunStore.query() applies min_timestamp
(so it *cannot re-find* it).  SkipDuplicate.check() uses count_runs() to gate a
fast path and then query() to locate the duplicate for its message; when they
disagree, the duplicate is silently NOT skipped.

Passes on main (single in-memory store, no such divergence); fails on the
branch.

REACHABILITY: this is a latent bug, not reachable under every config.  It
requires a duplicate whose last transition (its `timestamp`) is older than
`now - lookback` while it is still in a check state.  A run's timestamp only
advances on a state transition, so this needs a run to occupy a check state
for longer than `lookback`.

  - NOT reachable under the ASD prod config: lookback=21d, but
    program.timeout.duration=24.5h and waiting.max_time=23h cap how long a run
    can stay running/waiting at ~1 day << 21d.  The invariant
    `lookback > max(program timeout, waiting.max_time)` currently masks the bug.
  - Reachable whenever `lookback` is short relative to how long runs can stay
    in a check state (e.g. an hours-long lookback with long-running jobs).

The test uses a short lookback (1h) to exercise the code path directly.  It is
worth fixing so correctness does not silently depend on that config invariant.
"""

import ora

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State
from apsis.cond.skip_duplicate import SkipDuplicate


def test_skip_duplicate_beyond_lookback(tmp_path):
    now = ora.now()
    lookback = 3600  # 1 hour
    min_timestamp = now - lookback

    db_path = tmp_path / "apsis.db"
    SqliteDB.create(path=db_path)
    db = SqliteDB.open(db_path)
    store = RunStore(db, min_timestamp=min_timestamp)

    # An existing duplicate, still running.
    dup = Run(Instance("job", {"x": "1"}))
    store.add(dup)
    for state in (State.scheduled, State.waiting, State.starting, State.running):
        dup._transition(ora.now(), state)
        store.update(dup, ora.now())
    # A run's timestamp is frozen at its last transition, so a long-running run
    # keeps an old timestamp.  Backdating simulates a run that went `running`
    # before the lookback window (equivalent to elapsed wall-clock time), then
    # re-persist so the store/DB reflect it.
    dup.timestamp = now - 2 * lookback
    store.update(dup, dup.timestamp)

    # A new instance of the same job+args, in waiting, evaluating SkipDuplicate.
    new = Run(Instance("job", {"x": "1"}))
    store.add(new)
    for state in (State.scheduled, State.waiting):
        new._transition(ora.now(), state)
        store.update(new, ora.now())

    cond = SkipDuplicate().bind(new, jobs=None)
    result = cond.check(store)

    # A duplicate exists, so check() must force a transition (skip), not return
    # True (proceed).
    assert result is not True, "duplicate beyond lookback was not detected -> not skipped"
