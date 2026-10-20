"""
Tests refreshing scheduled and waiting runs from the current config of their job.
"""

import asyncio

import ora
import pytest

from apsis.apsis import Apsis, reload_jobs
from apsis.config import check
from apsis.jobs import load_jobs_dir
from apsis.runs import Instance, RunError
from apsis.sqlite import SqliteDB
from apsis.states import State

# -------------------------------------------------------------------------------

JOB = """
params: {params}
program:
  type: no-op
  success: {success}
condition:
  type: const
  value: {ready}
"""


def write_job(job_dir, *, success: bool, ready: bool, params: list[str] = []) -> None:
    text = JOB.format(params=params, success=str(success).lower(), ready=str(ready).lower())
    (job_dir / "job.yaml").write_text(text)


async def make_apsis(tmp_path) -> tuple[Apsis, SqliteDB]:
    """
    Builds an apsis instance with one job whose program fails and whose
    condition never passes.
    """
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()
    write_job(job_dir, success=False, ready=False)
    db_path = tmp_path / "apsis.db"
    SqliteDB.create(db_path)
    db = SqliteDB.open(db_path)
    cfg = check({"job_dir": str(job_dir), "database": str(db_path)}, tmp_path)
    return Apsis(cfg, await load_jobs_dir(job_dir), db), db


async def finished(run) -> State:
    while not run.state.finished:
        await asyncio.sleep(0.01)
    return run.state


def num_waiting(apsis: Apsis) -> int:
    return apsis.get_stats()["tasks"]["num_waiting"]


@pytest.mark.asyncio
async def test_refresh_scheduled_run(tmp_path):
    apsis, db = await make_apsis(tmp_path)
    run = await apsis.schedule(ora.now() + 3600, Instance("job", {}))

    write_job(tmp_path / "jobs", success=True, ready=False)
    await reload_jobs(apsis)
    await apsis.refresh(run)

    assert run.state == State.scheduled
    assert run.program.success
    assert db.run_db.get(run.run_id).program.success
    await apsis.shut_down()


@pytest.mark.asyncio
async def test_refresh_rejects_bad_config(tmp_path):
    apsis, _ = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))

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


@pytest.mark.asyncio
@pytest.mark.parametrize("op, state", [("start", State.success), ("skip", State.skipped)])
async def test_refresh_racing_override(tmp_path, op, state):
    apsis, _ = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))

    write_job(tmp_path / "jobs", success=True, ready=False)
    await reload_jobs(apsis)
    await asyncio.gather(apsis.refresh(run), getattr(apsis, op)(run))

    assert await asyncio.wait_for(finished(run), 5) == state
    assert num_waiting(apsis) == 0
    await apsis.shut_down()


@pytest.mark.asyncio
async def test_refresh_loses_to_start(tmp_path):
    apsis, _ = await make_apsis(tmp_path)
    run = await apsis.schedule(None, Instance("job", {}))

    write_job(tmp_path / "jobs", success=True, ready=False)
    await reload_jobs(apsis)
    _, result = await asyncio.gather(apsis.start(run), apsis.refresh(run), return_exceptions=True)

    assert isinstance(result, RunError)
    assert await asyncio.wait_for(finished(run), 5) == State.failure
    await apsis.shut_down()
