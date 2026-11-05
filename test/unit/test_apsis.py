from types import SimpleNamespace

import pytest

from apsis.apsis import _unschedule_runs, get_lookback
from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB
from apsis.states import State


def test_get_lookback():
    assert get_lookback({}) is None
    assert get_lookback({"runs": {"lookback": 3600}}) == 3600
    assert get_lookback({"runs": {"lookback": "21d"}}) == 21 * 86400
    for bad in ["0", -5, "soon"]:
        with pytest.raises(ValueError):
            get_lookback({"runs": {"lookback": bad}})


def test_unschedule_aged_expected_run(tmp_path, monkeypatch):
    """Reloading a job unschedules its expected runs once they're older than the lookback."""
    SqliteDB.create(path=tmp_path / "apsis.db")
    store = RunStore(SqliteDB.open(tmp_path / "apsis.db"), lookback=100)
    run = Run(Instance("job", {}), expected=True)
    store.add(run)
    run._transition(run.timestamp, State.scheduled)
    store.update(run, run.timestamp)
    monkeypatch.setattr("apsis.runs.now", lambda: run.timestamp + 200)

    unscheduled, published = [], []
    apsis = SimpleNamespace(
        run_store=store,
        scheduled=SimpleNamespace(unschedule=unscheduled.append),
        summary_publisher=SimpleNamespace(publish=published.append),
    )
    _unschedule_runs(apsis, "job")
    assert unscheduled == [run]
    assert [m["type"] for m in published] == ["run_delete"]
    assert store.query(job_id="job", limit_lookback=False)[1] == []
