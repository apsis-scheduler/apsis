"""
Tests refreshing scheduled and waiting runs from the current config of their job.
"""

import asyncio
from pathlib import Path

import ora
import pytest

from apsis.apsis import Apsis, reload_jobs
from apsis.config import check
from apsis.jobs import load_jobs_dir
from apsis.runs import Instance, Run, RunError
from apsis.sqlite import SqliteDB
from apsis.states import State

# -------------------------------------------------------------------------------


def write_job(job_dir: Path, *, success: bool, ready: bool | None, params: list[str] = []) -> None:
    """
    Writes a no-op job whose condition is const `ready`, or has no condition
    if `ready` is none.
    """
    text = f"params: {params}\nprogram:\n  type: no-op\n  success: {str(success).lower()}\n"
    if ready is not None:
        text += f"condition:\n  type: const\n  value: {str(ready).lower()}\n"
    (job_dir / "job.yaml").write_text(text)


async def make_apsis(tmp_path: Path) -> Apsis:
    """
    Builds an apsis instance with one job whose program fails and whose
    condition never passes.
    """
    job_dir = tmp_path / "jobs"
    job_dir.mkdir(exist_ok=True)
    db_path = tmp_path / "apsis.db"
    if not db_path.exists():
        write_job(job_dir, success=False, ready=False)
        SqliteDB.create(db_path)
    cfg = check({"job_dir": str(job_dir), "database": str(db_path)}, tmp_path)
    return Apsis(cfg, await load_jobs_dir(job_dir), SqliteDB.open(db_path))


async def finished(run: Run) -> State:
    while not run.state.finished:
        await asyncio.sleep(0.01)
    return run.state


def num_waiting(apsis: Apsis) -> int:
    return apsis.get_stats()["tasks"]["num_waiting"]


@pytest.mark.asyncio
async def test_refresh_scheduled_run(tmp_path):
    apsis = await make_apsis(tmp_path)
    run_id = (await apsis.schedule(ora.now() + 0.5, Instance("job", {}))).run_id

    write_job(tmp_path / "jobs", success=True, ready=True)
    await reload_jobs(apsis)
    _, run = apsis.run_store.get(run_id)
    await apsis.refresh(run)
    apsis.start_loops()

    assert await asyncio.wait_for(finished(run), 5) == State.success
    await apsis.shut_down()


@pytest.mark.asyncio
async def test_refresh_errors_leave_run_unchanged(tmp_path):
    apsis = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))
    scheduled = await apsis.schedule(ora.now() + 3600, Instance("job", {}))

    write_job(tmp_path / "jobs", success=True, ready=True, params=["date"])
    await reload_jobs(apsis)
    with pytest.raises(RunError, match="missing"):
        await apsis.refresh(run)
    assert run.state == State.waiting
    assert not run.program.success
    assert num_waiting(apsis) == 1

    await apsis.skip(run)
    with pytest.raises(RunError, match="in state skipped"):
        await apsis.refresh(run)
    await apsis.shut_down()

    # a scheduled run isn't on the schedule until restore
    write_job(tmp_path / "jobs", success=True, ready=True)
    apsis = await make_apsis(tmp_path)
    _, scheduled = apsis.run_store.get(scheduled.run_id)
    with pytest.raises(RunError, match="restored"):
        await apsis.refresh(scheduled)
    assert not scheduled.program.success


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op, ready, state",
    [
        ("start", False, State.success),
        ("skip", False, State.skipped),
        ("start", None, State.success),
        ("skip", None, State.success),
    ],
)
async def test_refresh_racing_override(tmp_path, op, ready, state):
    apsis = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))

    write_job(tmp_path / "jobs", success=True, ready=ready)
    await reload_jobs(apsis)
    _, result = await asyncio.gather(
        apsis.refresh(run), getattr(apsis, op)(run), return_exceptions=True
    )

    # with no conditions refresh starts the run itself
    assert isinstance(result, RunError) == (ready is None)
    assert await asyncio.wait_for(finished(run), 5) == state
    assert num_waiting(apsis) == 0
    await apsis.shut_down()


@pytest.mark.asyncio
async def test_refresh_loses_to_start(tmp_path):
    apsis = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))

    write_job(tmp_path / "jobs", success=True, ready=False)
    await reload_jobs(apsis)
    _, result = await asyncio.gather(apsis.start(run), apsis.refresh(run), return_exceptions=True)

    assert isinstance(result, RunError)
    assert await asyncio.wait_for(finished(run), 5) == State.failure
    await apsis.shut_down()
