"""
Regression test for the ``GET /api/v1/runs`` endpoint.

PR #539 moved run serialization off the event loop into a background thread
(``asyncio.to_thread``) to avoid blocking for tens of seconds on an unfiltered
query.  The serialization functions (``runs_to_jso`` → ``run_to_summary_jso``)
read and cache mutable ``Run`` state -- notably ``run.times``, which
``Run._transition`` mutates in place.  Running serialization concurrently with
the event loop (which keeps transitioning runs) is therefore a data race: e.g.
``for n, t in run.times.items()`` can raise ``RuntimeError: dictionary changed
size during iteration``.

This test drives the real ``api.runs`` handler in process (no server, no
network, temp/mock DB only -- never a production Apsis) while a concurrent task
transitions the same runs, and asserts the handler never crashes.

It fails against the naive ``asyncio.to_thread(serialize)`` implementation and
passes once serialization is made safe against concurrent mutation (e.g. by
snapshotting run state on the event loop before crossing the thread boundary).
"""

import asyncio
import ora
import pytest
import sys

import apsis.service.api as api
from apsis.runs import Instance, Run, RunStore
from apsis.states import State

# -------------------------------------------------------------------------------
# Minimal in-memory doubles.  Mirrors test/unit/test_run_store.py.


class _MockRunDb:
    def __init__(self, runs=()):
        self.__runs = runs

    def query(self, min_timestamp=None):
        return iter(self.__runs)

    def upsert(self, run):
        pass


class _MockRunIdDb:
    def __init__(self):
        self.__i = 0

    def get_next_run_id(self):
        self.__i += 1
        return f"r{self.__i:06d}"


class _MockDb:
    def __init__(self, runs=()):
        self.run_db = _MockRunDb(runs)
        self.next_run_id_db = _MockRunIdDb()


class _FakeArgs(dict):
    """
    Stand-in for ``sanic.request.args``.

    The handler calls ``.pop(key, default)`` (returning the raw default) and
    iterates ``.items()``; an empty mapping exercises the unfiltered path,
    which is exactly the case PR #539 targets.
    """

    def pop(self, key, default=None):
        return super().pop(key, default)


class _FakeApsis:
    def __init__(self, run_store):
        self.run_store = run_store
        self.jobs = None  # only used when a job_id filter is given


class _FakeApp:
    def __init__(self, apsis):
        self.apsis = apsis


class _FakeRequest:
    def __init__(self, apsis):
        self.app = _FakeApp(apsis)
        self.args = _FakeArgs()


def _make_running_run(store):
    """Adds a run to `store` and transitions it up to `running`."""
    run = Run(Instance("job", {"x": "0"}))
    store.add(run)
    for state in (State.scheduled, State.waiting, State.starting, State.running):
        run._transition(ora.now(), state)
    return run


async def _drive_once(n):
    """
    Runs the endpoint once against `n` running runs while a background task
    churns their state.  Returns the crash exception, or None on success.
    """
    store = RunStore(_MockDb(), min_timestamp=ora.now())
    runs = [_make_running_run(store) for _ in range(n)]
    request = _FakeRequest(_FakeApsis(store))

    stop = False

    async def churn():
        # Mimic the event loop driving the run state machine: each transition
        # mutates `run.times` in place, the dict the serializer iterates.
        while not stop:
            for run in runs:
                if run.state == State.running:
                    run._transition(ora.now(), State.stopping)
                elif run.state == State.stopping:
                    run._transition(ora.now(), State.success)
                else:
                    # Reset back to running to keep the churn going.
                    run.state = State.running
                    run._transition(ora.now(), State.stopping)
            await asyncio.sleep(0)

    churn_task = asyncio.create_task(churn())
    try:
        await api.runs(request)
        return None
    except Exception as exc:
        return exc
    finally:
        stop = True
        await churn_task


@pytest.mark.asyncio
async def test_runs_endpoint_survives_concurrent_transitions():
    """
    Serving `/runs` must not crash when runs transition concurrently.

    A single race window is probabilistic, so drive the endpoint repeatedly.
    To make the window reliable without a huge run count, shorten the GIL
    thread-switch interval so the serialization thread and the event loop
    interleave tightly; this does not change the code under test, only how
    often the two make progress relative to each other.
    """
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        for _ in range(20):
            exc = await _drive_once(3000)
            assert exc is None, f"/runs crashed under concurrent transitions: {exc!r}"
    finally:
        sys.setswitchinterval(old_interval)
