import asyncio

from .base import Program, ProgramRunning, ProgramSuccess
from apsis.lib.json import check_schema
from apsis.runs import template_expand

# -------------------------------------------------------------------------------


class SimpleLegacyProgram(Program):
    """
    Test program that uses the *deprecated* legacy program API, namely the
    `start()` and `reconnect()` methods.
    """

    def __init__(self, time):
        self.time = time

    def to_jso(self):
        return super().to_jso() | {"time": self.time}

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            time = pop("time")
        return cls(time)

    def bind(self, args):
        return type(self)(template_expand(self.time, args))

    async def __finish(self):
        meta = {"color": "green"}
        await asyncio.sleep(float(self.time))
        return ProgramSuccess(meta=meta)

    async def start(self, run_id, cfg):
        run_state = {
            "run_id": run_id,
            "secret": 42,
        }
        meta = {"color": "yellow"}
        running = ProgramRunning(run_state, meta=meta)
        return running, self.__finish()

    def reconnect(self, run_id, run_state):
        assert run_state["run_id"] == run_id
        assert run_state["secret"] == 42
        return self.__finish()
