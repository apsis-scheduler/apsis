import asyncio
import ora
import pytest

from apsis.scheduler import Scheduler

# -------------------------------------------------------------------------------


class _PollSleep(BaseException):
    """
    Signals that the scheduler loop reached the poll sleep ending an iteration.

    Derives from `BaseException`, not `Exception`, so that the loop's own
    `except Exception` handler doesn't swallow it and report a spurious
    scheduler failure.
    """


class _NoJobs:
    """
    Stands in for `Jobs` with nothing to schedule.
    """

    def get_jobs(self):
        return ()


async def _run_iteration(*, stop, max_age=None):
    """
    Runs one iteration of the loop of a `Scheduler` whose scheduler time is
    `stop`, and returns the scheduler.

    Interrupts the loop at the poll sleep that ends an iteration, so that the
    iteration runs to completion without waiting 60 s for the next.

    Awaits the loop coroutine directly, rather than wrapping it in a task:
    asyncio re-raises `SystemExit` from a task into the event loop, which would
    tear down the test session instead of failing one test.

    :raise SystemExit:
      The loop failed, as it does on any unhandled exception.  The underlying
      exception is `SystemExit.__context__`.
    """
    cfg = {"schedule": {} if max_age is None else {"max_age": max_age}}
    scheduler = Scheduler(cfg, _NoJobs(), None, stop)

    sleep = asyncio.sleep

    async def fake_sleep(delay, *args, **kw_args):
        # The loop sleeps only twice: `sleep(0)` to yield while scheduling
        # runs, and a long sleep between iterations.
        if delay > 0:
            raise _PollSleep
        return await sleep(delay, *args, **kw_args)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(asyncio, "sleep", fake_sleep)
        with pytest.raises(_PollSleep):
            await scheduler.loop()

    return scheduler


# -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_age_ok():
    """
    Tests that the `schedule.max_age` check doesn't fail a current scheduler.

    Regression test: the check read a nonexistent attribute, so it raised
    `AttributeError` on the very first loop iteration, which the loop turned
    into `SystemExit`.  Configuring `schedule.max_age` at all made Apsis exit
    seconds after startup.
    """
    await _run_iteration(stop=ora.now(), max_age=3600)


@pytest.mark.asyncio
async def test_max_age_not_configured():
    """
    Tests that a scheduler with no `max_age` doesn't fail, however far behind.
    """
    await _run_iteration(stop=ora.now() - 30 * 86400)


@pytest.mark.asyncio
async def test_max_age_within():
    """
    Tests that a scheduler behind by less than `max_age` doesn't fail.
    """
    await _run_iteration(stop=ora.now() - 3600 + 60, max_age=3600)


@pytest.mark.asyncio
async def test_max_age_exceeded():
    """
    Tests that `schedule.max_age` fails a scheduler that is too far behind.

    On restart, Apsis schedules runs from the time it last started scheduled
    runs, so a long outage would otherwise start runs scheduled long in the
    past; `max_age` is the guard against that.
    """
    # The scheduler last scheduled 3 hours ago, well over max_age.
    with pytest.raises(SystemExit) as exc_info:
        await _run_iteration(stop=ora.now() - 3 * 3600, max_age=3600)

    # The loop converts any exception to SystemExit, so check that it failed
    # for the intended reason: the age check, not an error in the check itself.
    cause = exc_info.value.__context__
    assert isinstance(cause, RuntimeError), f"unexpected cause: {cause!r}"
    assert "last scheduled more than" in str(cause)


@pytest.mark.asyncio
async def test_scheduling_advances_past_max_age():
    """
    Tests that scheduling advances scheduler time past the `max_age` check.

    An iteration schedules up to the horizon, leaving scheduler time in the
    future, so the check can't trip on the following iteration.
    """
    scheduler = await _run_iteration(stop=ora.now() - 60, max_age=3600)
    assert scheduler.get_scheduler_time() > ora.now()
