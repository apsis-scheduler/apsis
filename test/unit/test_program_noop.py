import time
import pytest

import apsis.program

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


def test_set_run_args_harmless():
    """
    Every running program accepts the run's args, so Apsis can hand them over
    unconditionally; a program that doesn't expose them simply ignores them.
    """
    args = {"date": "2026-09-01", "database": "asd_hoard"}
    prog = apsis.program.Program.from_jso({"type": "no-op", "duration": "0"}).bind({})
    running = prog.run("testrun", cfg={})

    assert running.args == {}
    running.set_run_args(args)
    assert running.args == args

    # The bound program must not grow them: it is serialized into the run, and
    # an older Apsis rejects unknown keys in the program JSO.
    assert not hasattr(prog, "args")
    assert "args" not in prog.to_jso()
