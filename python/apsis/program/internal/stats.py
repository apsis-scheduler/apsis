import json
import logging

from ..base import Program, RunningProgram, ProgramRunning, ProgramSuccess, program_outputs, memo
from apsis.lib.json import check_schema
from apsis.lib.py import or_none, nstr
from apsis.runs import template_expand

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class StatsProgram(Program):
    """
    A program that collects and dumps Apsis internal stats in JSON format.
    """

    def __init__(self, *, path=None):
        self.__path = path

    def __str__(self):
        res = "internal stats"
        if self.__path is not None:
            res += f"→ {self.__path}"
        return res

    def bind(self, args):
        path = or_none(template_expand)(self.__path, args)
        return type(self)(path=path)

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            path = pop("path", nstr, None)
        return cls(path=path)

    def to_jso(self):
        return {
            **super().to_jso(),
            "path": self.__path,
        }

    def run(self, run_id, cfg):
        """Start a new stats collection run."""
        # For internal programs, cfg is the apsis instance
        return RunningStatsProgram(run_id, self, cfg, run_state=None)

    def connect(self, run_id, run_state, cfg):
        """Reconnect to an existing stats collection run."""
        # For internal programs, cfg is the apsis instance
        return RunningStatsProgram(run_id, self, cfg, run_state=run_state)


class RunningStatsProgram(RunningProgram):
    """A running instance of the stats program."""

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
        # If reconnecting, run_state already exists, but we just restart
        # (same behavior as the legacy reconnect() which just called wait() again)

        # Collect stats (this is the main work)
        stats = json.dumps(self.apsis.get_stats())

        # Write to file if path is specified
        if self.program._StatsProgram__path is not None:
            with open(self.program._StatsProgram__path, "a") as file:
                print(stats, file=file)

        # Return success with the stats as output
        yield ProgramSuccess(outputs=program_outputs(stats.encode()))

    async def stop(self):
        """Stats collection cannot be stopped - it's instantaneous."""
        pass

    async def signal(self, signal):
        """Stats collection ignores signals."""
        pass
