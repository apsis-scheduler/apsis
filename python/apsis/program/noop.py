import asyncio
import logging
import ora

from .base import (
    Program,
    RunningProgram,
    ProgramRunning,
    ProgramSuccess,
    ProgramFailure,
    ProgramError,
)
from apsis.lib import memo
from apsis.lib.json import check_schema
from apsis.lib.parse import parse_duration
from apsis.lib.py import or_none, nstr, nbool
from apsis.runs import template_expand

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class NoOpProgram(Program):
    def __init__(self, *, duration=0, success=True):
        self.duration = nstr(duration)
        self.success = nbool(success)

    def __str__(self):
        return "no-op" + ("" if self.duration is None else f" for {self.duration} s")

    def bind(self, args):
        duration = or_none(template_expand)(self.duration, args)
        return BoundNoOpProgram(duration=duration, success=self.success)

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            duration = pop("duration", nstr, None)
            success = pop("success", nbool, True)
        return cls(duration=duration, success=success)

    def to_jso(self):
        return {
            **super().to_jso(),
            "duration": self.duration,
            "success": self.success,
        }


# -------------------------------------------------------------------------------


class BoundNoOpProgram(Program):
    """
    A program that does nothing.

    `duration` is the time it takes the program to run.  If `success` is true,
    the program always succeeds; if false, it fails; if none, it errors.
    """

    def __init__(self, *, duration=0, success=True):
        self.duration = nstr(duration)
        self.success = None if success is None else bool(success)

    def __str__(self):
        return "no-op" + ("" if self.duration is None else f" for {self.duration} s")

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            duration = pop("duration", nstr, None)
            success = pop("success", nbool, True)
        return cls(duration=duration, success=success)

    def to_jso(self):
        return {
            **super().to_jso(),
            "duration": self.duration,
            "success": self.success,
        }

    def run(self, run_id, cfg) -> RunningProgram:
        return RunningNoopProgram(self, run_id, None)

    def connect(self, run_id, run_state, cfg) -> RunningProgram:
        return RunningNoopProgram(self, run_id, run_state)


# -------------------------------------------------------------------------------


class RunningNoopProgram(RunningProgram):
    """
    A running instance of a no-op program.
    """

    # FIXME: Should run_state belong to RunningProgram?

    def __init__(self, program, run_id, run_state):
        """
        :param run_state:
          Existing run state when connecting to an existing program, else none.
        """
        super().__init__(run_id)
        self.program = program
        self.run_state = run_state
        # Signals that the program was stopped.
        self.stop_event = asyncio.Event()

    @memo.property
    async def updates(self):
        if self.run_state is None:
            # New instance.  Record start time in the run state, so we know when
            # to stop.
            start = ora.now()
            self.run_state = {"start": str(start)}
            yield ProgramRunning(self.run_state)
        else:
            # Existing instance.
            start = ora.Time(self.run_state["start"])

        if self.program.duration is not None:
            duration = parse_duration(self.program.duration)
            timeout = start + duration - ora.now()
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout)
            except asyncio.TimeoutError:
                # OK, duration expired.
                pass

        if self.stop_event.is_set():
            yield ProgramError("program stopped")
        match self.program.success:
            case True:
                yield ProgramSuccess()
            case False:
                yield ProgramFailure("failed")
            case None:
                yield ProgramError("error")
            case _:
                assert False

    async def stop(self):
        self.stop_event.set()

    async def signal(self, run_id, run_state, signal):
        log.info("ignoring signal to no-op program")
