import time
import pytest

import apsis.program
from apsis.program.base import RunningProgram

# -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duration():
    JSO = {
        "type": "no-op",
        "duration": "0.75",
    }

    prog = apsis.program.Program.from_jso(JSO).bind({})
    start = time.monotonic()
    running = prog.run("testrun", cfg={})
    async for _ in running.updates:
        pass
    elapsed = time.monotonic() - start
    assert elapsed > 0.7


def test_run_args_base():
    """The base RunningProgram provides `args`, empty until the run starts."""
    running = RunningProgram("r1")
    assert running.args == {}
