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


def test_set_run_args_base():
    """set_run_args is provided by the base RunningProgram and records the args."""
    running = RunningProgram("r1")
    assert running.args == {}
    running.set_run_args({"date": "2026-09-01", "database": "asd_hoard"})
    assert running.args == {"date": "2026-09-01", "database": "asd_hoard"}
