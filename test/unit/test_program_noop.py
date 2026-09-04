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


def test_set_run_args_noop():
    """
    set_run_args is a harmless no-op for a running program that doesn't expose
    args, so Apsis can call it unconditionally for every program type.
    """
    prog = apsis.program.Program.from_jso({"type": "no-op", "duration": "0"}).bind({})
    running = prog.run("testrun", cfg={})
    # Should not raise, and should not stash args anywhere.
    running.set_run_args({"date": "2026-09-01", "database": "asd_hoard"})
    assert not hasattr(running, "args")
    # Nor should the bound program grow them: it is serialized into the run.
    assert not hasattr(prog, "args")
