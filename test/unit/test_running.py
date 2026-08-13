"""
Tests processing of program updates for a run, in `apsis.running`.
"""

import ora
import pytest

from apsis.apsis import Apsis
from apsis.lib import memo
from apsis.program.base import (
    ProgramError,
    ProgramFailure,
    ProgramRunning,
    ProgramSuccess,
    ProgramUpdate,
    RunningProgram,
)
from apsis.running import _process_updates
from apsis.runs import Instance, Run
from apsis.states import State

# -------------------------------------------------------------------------------

RUN_STATE = {"conn_id": "conn0", "proc_id": "proc0"}

# Metadata a Procstar program reports once the process is running.
PROGRAM_META = {
    "procstar_proc_id": "proc0",
    "procstar_conn": {"conn_id": "conn0", "hostname": "host0"},
    "proc_stat": {"pid": 12345},
}


class _FixedRunningProgram(RunningProgram):
    """
    Running program that yields a fixed sequence of updates.
    """

    def __init__(self, run_id, updates):
        super().__init__(run_id)
        self.__updates = updates

    @memo.property
    async def updates(self):
        for update in self.__updates:
            yield update


class _RunLog:
    def __init__(self):
        self.messages = []

    def record(self, run, message, **kw_args):
        self.messages.append(str(message))

    info = record
    error = record

    def exc(self, run, message=None, **kw_args):
        self.messages.append(str(message))


class _Publisher:
    def __contains__(self, run_id):
        return False

    def publish(self, *args, **kw_args):
        pass

    def close(self, run_id):
        pass


class _RunStore:
    def update(self, run, time):
        pass


class _FakeApsis:
    """
    Minimal stand-in for `Apsis`, wired to the real transition and metadata
    logic, so that metadata semantics are tested for real.
    """

    _transition = Apsis._transition
    _update_metadata = Apsis._update_metadata

    # `Apsis._transition` uses these two private attributes; Python mangles
    # their names with the class that defines the method, so provide them under
    # the mangled names.  The db is used only for expected runs, which these
    # tests don't create.
    _Apsis__db = None
    _Apsis__start_actions = staticmethod(lambda run: None)

    def __init__(self, run, updates):
        self.run_log = _RunLog()
        self.run_store = _RunStore()
        self.run_update_publisher = _Publisher()
        self.summary_publisher = _Publisher()
        self.output_update_publisher = _Publisher()
        self.outputs = {}
        self._running_programs = {run.run_id: _FixedRunningProgram(run.run_id, updates)}

    def _update_output_data(self, run, outputs, persist):
        self.outputs.update(outputs)


def _make_run(updates):
    """
    Returns a run in the starting state, and a `_FakeApsis` to drive it.

    :param updates:
      The sequence of program updates to feed to the run.
    """
    run = Run(Instance("job", {}))
    run.run_id = "r0"
    apsis = _FakeApsis(run, updates)
    apsis._transition(run, State.scheduled, times={"schedule": ora.now()})
    apsis._transition(run, State.starting)
    return apsis, run


# -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("result", "state"),
    (
        (ProgramSuccess(), State.success),
        (ProgramFailure("exit code 1"), State.failure),
        (ProgramError("reconnect failed: proc0: unknown connection: conn0"), State.error),
    ),
    ids=("success", "failure", "error"),
)
@pytest.mark.asyncio
async def test_result_without_metadata_keeps_program_metadata(result, state):
    """
    A program result with no metadata of its own doesn't erase program metadata.

    The error case is the reconnect failure: the run was running, so its
    metadata records the host and pid of the process, then Apsis fails to
    reconnect to the process and errors the run.  The metadata is exactly
    what's needed to go find the abandoned process, so it must survive.
    """
    apsis, run = _make_run(
        [
            ProgramRunning(RUN_STATE, meta=PROGRAM_META),
            result,
        ]
    )

    await _process_updates(apsis, run)

    assert run.state == state
    assert run.meta["program"] == PROGRAM_META


@pytest.mark.asyncio
async def test_error_keeps_startup_metadata():
    """
    An error while starting doesn't erase metadata reported during startup.

    A program may report metadata about resources it acquired before the
    process is running, e.g. an AWS ECS task; erasing it leaks the resource.
    """
    STARTUP_META = {"aws_ecs": {"task_id": "task0", "cluster_name": "cluster0"}}
    apsis, run = _make_run(
        [
            ProgramUpdate(meta=STARTUP_META),
            ProgramError("start failed: proc0: no open connection in group"),
        ]
    )

    await _process_updates(apsis, run)

    assert run.state == State.error
    assert run.meta["program"] == STARTUP_META


@pytest.mark.asyncio
async def test_update_without_metadata_keeps_program_metadata():
    """
    A program update with no metadata doesn't erase program metadata.
    """
    apsis, run = _make_run(
        [
            ProgramRunning(RUN_STATE, meta=PROGRAM_META),
            ProgramUpdate(outputs={}),
            ProgramSuccess(meta=PROGRAM_META),
        ]
    )

    await _process_updates(apsis, run)

    assert run.state == State.success
    assert run.meta["program"] == PROGRAM_META


@pytest.mark.asyncio
async def test_result_metadata_replaces_program_metadata():
    """
    A program result with metadata of its own replaces the program metadata.
    """
    ERROR_META = {**PROGRAM_META, "status": {"exit_code": None, "signal": "SIGKILL"}}
    apsis, run = _make_run(
        [
            ProgramRunning(RUN_STATE, meta=PROGRAM_META),
            ProgramError("procstar: oh no", meta=ERROR_META),
        ]
    )

    await _process_updates(apsis, run)

    assert run.state == State.error
    assert run.meta["program"] == ERROR_META
