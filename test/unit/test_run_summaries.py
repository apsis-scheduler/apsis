import ora
import ujson

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State


def make_run_store(tmp_path, min_timestamp=None):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    db = SqliteDB.open(path)
    if min_timestamp is None:
        min_timestamp = ora.now() - 86400
    return RunStore(db, min_timestamp=min_timestamp)


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
