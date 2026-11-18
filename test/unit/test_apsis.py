from types import SimpleNamespace

import ora
import pytest

from apsis import config
from apsis.apsis import Apsis, _unschedule_runs, get_lookback
from apsis.jobs import InMemoryJobs
from apsis.run_window import WindowSpec
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


@pytest.mark.asyncio
async def test_schedule_error_keeps_schedule_time(tmp_path):
    """A run that errors while being scheduled keeps its schedule time and shows in a window."""
    SqliteDB.create(path=tmp_path / "apsis.db")
    apsis = Apsis(
        config.check({}, tmp_path), InMemoryJobs([]), SqliteDB.open(tmp_path / "apsis.db")
    )
    time = ora.now() + 3600
    run = await apsis.schedule(time, Instance("missing", {}))
    assert run.state == State.error
    assert run.times["schedule"] == time

    window = await apsis.run_store.query_window(WindowSpec(anchor=time))
    assert window.run_ids == [run.run_id]
