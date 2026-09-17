"""
Tests that the scheduler doesn't recreate runs that already exist.

The clock (`ClockDB`) is advanced before the runs it covers are persisted, so
after a crash it can be ahead of the runs that actually reached the database.
Scheduling from the clock alone therefore silently drops those runs.  The
scheduler instead schedules from further back and skips any schedule time for
which a run already exists.
"""

import asyncio
from collections import Counter
import ora
import pytest

from apsis.scheduler import Scheduler
from apsis.sqlite import canonical_args_json

# -------------------------------------------------------------------------------

JOB_ID = "job"
INTERVAL = 300


class _Job:
    """
    A job with `count` identical interval schedules and no params.

    More than one schedule produces the same schedule time and args, as a job
    with overlapping schedules does; each is a run in its own right.
    """

    job_id = JOB_ID
    params = frozenset()

    def __init__(self, count=1):
        from apsis.schedule.interval import IntervalSchedule

        self.schedules = [IntervalSchedule(INTERVAL, {}) for _ in range(count)]


class _Jobs:
    def __init__(self, schedules=1):
        self.__jobs = [_Job(schedules)]

    def get_jobs(self):
        return self.__jobs


class _Recorder:
    """
    Stands in for `Apsis.schedule`, recording what would be created.

    Also serves as the store of existing runs, keyed and counted as
    `RunStore.get_schedule_times` does.
    """

    def __init__(self):
        # Counts of runs that exist, by key and schedule time.
        self.existing = {}
        # Schedule times passed to schedule(), in order.
        self.created = []
        # If set, raise after this many creations, to simulate a crash.
        self.crash_after = None

    def get_schedule_times(self):
        # A fresh copy each call, as the real callback returns; the scheduler
        # decrements the counts it is given.
        return {key: Counter(times) for key, times in self.existing.items()}

    def add_existing(self, time, args={}, count=1):
        key = (JOB_ID, canonical_args_json(args))
        self.existing.setdefault(key, Counter())[time] += count

    async def schedule(self, time, inst, *, stop_time=None):
        if self.crash_after is not None and len(self.created) >= self.crash_after:
            raise _Crash
        self.created.append(time)
        # A created run exists from now on, as it would in the run store.
        self.add_existing(time, inst.args)


class _Crash(BaseException):
    """
    Simulates a process crash partway through scheduling.
    """


def _scheduler(recorder, stop, *, reconcile=True, schedules=1):
    return Scheduler(
        {"schedule": {}},
        _Jobs(schedules),
        recorder.schedule,
        stop,
        get_schedule_times=(recorder.get_schedule_times if reconcile else None),
    )


def _boundaries(start, stop):
    """
    Returns the interval-schedule times in `[start, stop)`.
    """
    epoch = ora.Time.EPOCH
    first = int((start - epoch) // INTERVAL) * INTERVAL
    times = []
    t = epoch + first
    while t < stop:
        if t >= start:
            times.append(t)
        t += INTERVAL
    return times


# -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_existing_runs():
    """
    Tests that with no existing runs, every schedule time is created.
    """
    now = ora.now()
    start = now - 3600
    rec = _Recorder()
    await _scheduler(rec, start).schedule(now)

    assert rec.created == _boundaries(start, now)


@pytest.mark.asyncio
async def test_skips_existing_runs():
    """
    Tests that a schedule time with an existing run is not scheduled again.

    This is the case after a crash: the clock is ahead of some runs, so the
    scheduler revisits schedule times it has already handled.
    """
    now = ora.now()
    start = now - 3600
    expected = _boundaries(start, now)

    rec = _Recorder()
    # Half the runs already exist, as they would after a partial catch-up.
    for time in expected[::2]:
        rec.add_existing(time)

    await _scheduler(rec, start).schedule(now)

    # Only the missing ones are created, and nothing is created twice.
    assert rec.created == expected[1::2]
    assert len(set(rec.created)) == len(rec.created)


@pytest.mark.asyncio
async def test_all_runs_exist():
    """
    Tests that nothing is scheduled when every run already exists.
    """
    now = ora.now()
    start = now - 3600
    rec = _Recorder()
    for time in _boundaries(start, now):
        rec.add_existing(time)

    await _scheduler(rec, start).schedule(now)

    assert rec.created == []


@pytest.mark.asyncio
async def test_repeated_crashes_converge():
    """
    Tests that repeated crashes while catching up neither lose nor duplicate.

    Each pass crashes partway through, as Apsis did when it was restarted
    repeatedly while working through a backlog.
    """
    now = ora.now()
    start = now - 6 * 3600
    expected = _boundaries(start, now)

    rec = _Recorder()

    for crash_after in (10, 25, 5):
        rec.crash_after = len(rec.created) + crash_after
        with pytest.raises(_Crash):
            await _scheduler(rec, start).schedule(now)

    # Finally, a pass that completes.
    rec.crash_after = None
    await _scheduler(rec, start).schedule(now)

    # Every schedule time created exactly once, in spite of the crashes.
    assert sorted(rec.created) == expected
    assert len(set(rec.created)) == len(rec.created)


@pytest.mark.asyncio
async def test_without_reconcile_duplicates():
    """
    Tests that without the check, revisiting schedule times duplicates runs.

    Guards the test setup: shows these tests would catch a regression.
    """
    now = ora.now()
    start = now - 3600
    expected = _boundaries(start, now)

    rec = _Recorder()
    for time in expected:
        rec.add_existing(time)

    await _scheduler(rec, start, reconcile=False).schedule(now)

    # Every run is created a second time.
    assert rec.created == expected


@pytest.mark.asyncio
async def test_args_distinguish_runs():
    """
    Tests that runs differing only in args are not conflated.

    A job scheduled per host has several runs at the same schedule time; an
    existing run for one host must not suppress the others.
    """
    now = ora.now()
    start = now - 3600
    expected = _boundaries(start, now)

    rec = _Recorder()
    # Existing runs carry args that the job doesn't produce, so they must not
    # match any candidate.
    for time in expected:
        rec.add_existing(time, {"host": "other"})

    await _scheduler(rec, start).schedule(now)

    # None of the existing runs match, so all candidates are created.
    assert rec.created == expected


@pytest.mark.asyncio
async def test_no_check_when_scheduling_future():
    """
    Tests that the check is skipped when scheduling only future times.

    In steady state the window is ahead of now, where no run can exist yet, so
    the scheduler shouldn't pay for the lookup.
    """
    now = ora.now()
    calls = 0

    rec = _Recorder()
    get = rec.get_schedule_times

    def counting_get():
        nonlocal calls
        calls += 1
        return get()

    rec.get_schedule_times = counting_get

    # Schedule a window starting in the future.
    scheduler = _scheduler(rec, now + 600)
    await scheduler.schedule(now + 3600)

    assert calls == 0, "existing-run lookup should be skipped for future windows"
    assert len(rec.created) > 0


@pytest.mark.asyncio
async def test_overlapping_schedules_all_created():
    """
    Tests that a job whose schedules overlap gets a run for each.

    Two schedules of a job may produce the same schedule time and args, for
    instance schedules on different calendars that share a date.  Each is a run
    in its own right, so the check must not collapse them into one.
    """
    now = ora.now()
    start = now - 3600
    expected = _boundaries(start, now)

    rec = _Recorder()
    await _scheduler(rec, start, schedules=3).schedule(now)

    counts = Counter(rec.created)
    assert sorted(counts) == expected
    assert all(c == 3 for c in counts.values()), f"expected 3 runs each, got {counts}"


@pytest.mark.asyncio
async def test_overlapping_schedules_partial_existing():
    """
    Tests that only the missing runs of an overlapping schedule are created.

    After a crash partway through, some of the runs for a schedule time exist
    and the rest don't.  Each existing run accounts for one schedule, so the
    remainder are still created.
    """
    now = ora.now()
    start = now - 3600
    expected = _boundaries(start, now)

    rec = _Recorder()
    # One of the three runs for each schedule time already exists.
    for time in expected:
        rec.add_existing(time)

    await _scheduler(rec, start, schedules=3).schedule(now)

    # Two more created for each, for three in total.
    counts = Counter(rec.created)
    assert sorted(counts) == expected
    assert all(c == 2 for c in counts.values()), f"expected 2 more each, got {counts}"


@pytest.mark.asyncio
async def test_overlapping_schedules_crash_converges():
    """
    Tests that overlapping schedules converge across crashes.

    Regression test: matching on the presence of a schedule time rather than
    counting dropped every run but the first for each schedule time.
    """
    now = ora.now()
    start = now - 3600
    expected = _boundaries(start, now)

    rec = _Recorder()
    for crash_after in (1, 4, 9):
        rec.crash_after = len(rec.created) + crash_after
        with pytest.raises(_Crash):
            await _scheduler(rec, start, schedules=3).schedule(now)

    rec.crash_after = None
    await _scheduler(rec, start, schedules=3).schedule(now)

    counts = Counter(rec.created)
    assert sorted(counts) == expected
    assert all(c == 3 for c in counts.values()), f"expected 3 runs each, got {counts}"


if __name__ == "__main__":
    asyncio.run(test_repeated_crashes_converge())
