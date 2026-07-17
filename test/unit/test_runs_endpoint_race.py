"""
Regression test for the ``GET /api/v1/runs`` endpoint.

PR #539 moved run serialization off the event loop into a background thread
(``asyncio.to_thread``) to avoid blocking for tens of seconds on an unfiltered
query.  The serialization functions (``runs_to_jso`` → ``run_to_summary_jso``)
read mutable ``Run`` state -- notably ``run.times``, which ``Run._transition``
mutates in place.  Running serialization in a thread while the event loop keeps
transitioning those same runs is therefore a data race: e.g.
``for n, t in run.times.items()`` can observe the dict being mutated and raise
``RuntimeError: dictionary changed size during iteration``.

Before #539 this could not happen: ``runs_to_jso`` has no ``await``, so it ran
atomically with respect to every other coroutine, and no run could transition
partway through serialization.

This test drives the real ``api.runs`` handler in process (no server, no
network, mock DB only -- never a production Apsis) while a concurrent task
transitions the same runs, and asserts the handler never crashes.  It fails
against the ``asyncio.to_thread`` implementation and passes once serialization
is atomic with respect to run transitions again (e.g. by reverting #539, or by
serializing cooperatively on the event loop).
"""

import asyncio
import ora
import pytest

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
    which is exactly the case PR #539 targets.  With no ``summary`` arg the
    handler defaults to ``summary=False`` -- the same as the ``apsis runs`` CLI
    command, whose client does not send ``summary``.
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


# Number of runs to serve per call.  Large enough that serialization spans
# several GIL thread-switch intervals, giving the concurrent transitions a
# window to collide with the serializing thread.
NUM_RUNS = 5000

# Number of times to invoke the endpoint.  The race is probabilistic per call;
# a single buggy call crashes with probability well above 1/2 at this run
# count, so repeating drives the chance of a false pass to negligible while a
# correct (atomic) implementation passes every call.
NUM_CALLS = 12


def _make_running_run(store):
    """Adds a run to `store` and transitions it up to `running`."""
    run = Run(Instance("job", {"x": "0"}))
    store.add(run)
    for state in (State.scheduled, State.waiting, State.starting, State.running):
        run._transition(ora.now(), state)
    return run


async def _drive_once():
    """
    Runs the endpoint once while a background task churns run state.

    :return:
      The exception the handler raised, or None if it completed.
    """
    store = RunStore(_MockDb(), min_timestamp=ora.now())
    runs = [_make_running_run(store) for _ in range(NUM_RUNS)]
    request = _FakeRequest(_FakeApsis(store))

    stop = False

    async def churn():
        # Mimic the event loop driving the run state machine.  Each transition
        # mutates `run.times` in place -- the dict the serializer iterates --
        # and clears the summary JSO cache, forcing the serializer to rebuild
        # (and re-read `times`) rather than return a cached object.
        while not stop:
            for run in runs:
                run._summary_jso_cache = None
                if run.state == State.running:
                    run._transition(ora.now(), State.success)
                else:
                    # Reset back to running to keep the churn going.
                    run.state = State.running
            await asyncio.sleep(0)

    churn_task = asyncio.create_task(churn())
    try:
        await api.runs(request)
        return None
    except Exception as exc:
        return exc
    finally:
        stop = True
        try:
            await churn_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_runs_endpoint_survives_concurrent_transitions():
    """
    Serving `/runs` must not crash when runs transition concurrently.

    Fails on the `asyncio.to_thread` serialization introduced by #539 with
    ``RuntimeError: dictionary changed size during iteration``; passes when
    serialization is atomic with respect to run transitions.
    """
    for _ in range(NUM_CALLS):
        exc = await _drive_once()
        assert exc is None, f"/runs crashed under concurrent transitions: {exc!r}"
