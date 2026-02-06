import logging

from ..base import Program, RunningProgram, ProgramRunning, ProgramSuccess, memo
from apsis.lib.json import check_schema
from apsis.lib.timing import Timer

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class VacuumProgram(Program):
    """
    A program that defragments the Apsis database.

    This program runs within the Apsis process, and blocks all other activities
    while it runs.
    """

    def __str__(self):
        return "vacuum database"

    def bind(self, args):
        return self

    def to_jso(self):
        return {**super().to_jso()}

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            pass
        return cls()

    def run(self, run_id, cfg):
        """Start a new vacuum operation."""
        # For internal programs, cfg is the apsis instance
        return RunningVacuumProgram(run_id, self, cfg, run_state=None)

    def connect(self, run_id, run_state, cfg):
        """Reconnect to an existing vacuum operation."""
        # For internal programs, cfg is the apsis instance
        return RunningVacuumProgram(run_id, self, cfg, run_state=run_state)


class RunningVacuumProgram(RunningProgram):
    """A running instance of the vacuum program."""

    def __init__(self, run_id, program, cfg, run_state):
        super().__init__(run_id)
        self.program = program
        self.run_state = run_state
        # For internal programs, cfg IS the apsis instance
        self.apsis = cfg

    @memo.property
    async def updates(self):
        """Async generator that yields program state updates."""
        if self.run_state is None:
            # Starting fresh
            self.run_state = {}
            yield ProgramRunning(self.run_state)
        # If reconnecting, just restart (vacuum is idempotent)

        # Perform the vacuum operation
        # FIXME: Private attributes.
        db = self.apsis._Apsis__db

        with Timer() as timer:
            db.vacuum()

        meta = {
            "time": timer.elapsed,
        }
        yield ProgramSuccess(meta=meta)

    async def stop(self):
        """Vacuum cannot be stopped once started."""
        pass

    async def signal(self, signal):
        """Vacuum ignores signals."""
        pass
