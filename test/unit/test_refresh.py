"""
Tests that reloading jobs refreshes scheduled and waiting runs of changed jobs.
"""

import asyncio
from pathlib import Path
from typing import Any, Coroutine

import ora
import pytest

from apsis.apsis import Apsis, reload_jobs
from apsis.config import check
from apsis.jobs import Jobs, load_jobs_dir
from apsis.runs import Instance, RunError
from apsis.sqlite import SqliteDB
from apsis.states import State

# -------------------------------------------------------------------------------


def write_job(
    job_dir: Path, *, success: bool, ready: bool | None, params: tuple[str, ...] = ()
) -> None:
    """
    Writes a no-op job whose condition is const `ready`, or has no condition
    if `ready` is none.
    """
    text = f"params: {list(params)}\nprogram:\n  type: no-op\n  success: {str(success).lower()}\n"
    if ready is not None:
        text += f"condition:\n  type: const\n  value: {str(ready).lower()}\n"
    (job_dir / "job.yaml").write_text(text)


async def make_apsis(tmp_path: Path) -> tuple[Apsis, SqliteDB]:
    """
    Builds an apsis instance on the jobs and DB in `tmp_path`, creating, on the
    first call, a job whose program fails and whose condition never passes.
    """
    job_dir = tmp_path / "jobs"
    job_dir.mkdir(exist_ok=True)
    db_path = tmp_path / "apsis.db"
    if not db_path.exists():
        write_job(job_dir, success=False, ready=False)
        SqliteDB.create(db_path)
    db = SqliteDB.open(db_path)
    cfg = check({"job_dir": str(job_dir), "database": str(db_path)}, tmp_path)
    return Apsis(cfg, await load_jobs_dir(job_dir), db), db


async def finished(apsis: Apsis, run_id: str) -> State:
    while not (state := apsis.run_store.get(run_id)[1].state).finished:
        await asyncio.sleep(0.01)
    return state


def num_waiting(apsis: Apsis) -> int:
    return apsis.get_stats()["tasks"]["num_waiting"]


def run_log(apsis: Apsis, run_id: str) -> list[str]:
    return [r["message"] for r in apsis.get_run_log(run_id)]


async def after_yield(coro: Coroutine[Any, Any, None]) -> None:
    """
    Awaits `coro` one event loop step later, so it lands after a concurrent
    cancel has finished the wait task but before the task leaves its group.
    """
    await asyncio.sleep(0)
    await coro


@pytest.mark.asyncio
async def test_reload_refreshes_runs(tmp_path):
    apsis, _ = await make_apsis(tmp_path)
    waiting = (await apsis.schedule(None, Instance("job", {}))).run_id
    scheduled = (await apsis.schedule(ora.now() + 0.5, Instance("job", {}))).run_id

    write_job(tmp_path / "jobs", success=True, ready=True)
    await reload_jobs(apsis)
    apsis.start_loops()

    assert await asyncio.wait_for(finished(apsis, waiting), 5) == State.success
    assert await asyncio.wait_for(finished(apsis, scheduled), 5) == State.success
    await apsis.shut_down()


@pytest.mark.asyncio
async def test_reload_keeps_runs_that_dont_fit(tmp_path):
    apsis, _ = await make_apsis(tmp_path)
    _, run = apsis.run_store.get((await apsis.schedule(None, Instance("job", {}))).run_id)
    scheduled = (await apsis.schedule(ora.now() + 3600, Instance("job", {}))).run_id

    write_job(tmp_path / "jobs", success=True, ready=True, params=("date",))
    await reload_jobs(apsis)
    assert run.state == State.waiting
    assert not run.program.success
    assert any("missing" in m for m in run_log(apsis, run.run_id))
    assert num_waiting(apsis) == 1

    await apsis.skip(run)
    with pytest.raises(RunError, match="in state skipped"):
        await apsis.refresh(run)
    await apsis.shut_down()

    # a scheduled run isn't on the schedule until restore
    apsis, db = await make_apsis(tmp_path)
    write_job(tmp_path / "jobs", success=True, ready=True)
    await reload_jobs(apsis)
    assert not db.run_db.get(scheduled).program.success
    assert any("restored" in m for m in run_log(apsis, scheduled))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op, ready, delayed, state, errors",
    [
        # refresh restarts the wait before the override cancels it
        ("start", False, False, State.success, [False, False]),
        ("skip", False, False, State.skipped, [False, False]),
        # refresh starts a run with no conditions before the override
        ("start", None, False, State.success, [False, True]),
        ("skip", None, False, State.success, [False, True]),
        # the override lands while the cancelled wait task is leaving its group
        ("start", False, True, State.failure, [True, False]),
    ],
)
async def test_refresh_racing_override(tmp_path, op, ready, delayed, state, errors):
    apsis, db = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))

    # load the new job without refreshing so the refresh below races the override
    write_job(tmp_path / "jobs", success=True, ready=ready)
    apsis.jobs = Jobs(await load_jobs_dir(tmp_path / "jobs"), db.job_db)
    override = getattr(apsis, op)(run)
    results = await asyncio.gather(
        apsis.refresh(run), after_yield(override) if delayed else override, return_exceptions=True
    )

    assert [isinstance(r, RunError) for r in results] == errors
    assert await asyncio.wait_for(finished(apsis, run.run_id), 5) == state
    assert num_waiting(apsis) == 0
    await apsis.shut_down()
