import logging

from ..base import _InternalProgram, ProgramRunning, ProgramSuccess
from apsis.lib.json import check_schema
from apsis.lib.timing import Timer

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class VacuumProgram(_InternalProgram):
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

    async def start(self, run_id, apsis):
        return ProgramRunning({}), self.wait(apsis)

    async def wait(self, apsis):
        # FIXME: Private attributes.
        db = apsis._Apsis__db

        with Timer() as timer:
            db.vacuum()

        meta = {
            "time": timer.elapsed,
        }
        return ProgramSuccess(meta=meta)
