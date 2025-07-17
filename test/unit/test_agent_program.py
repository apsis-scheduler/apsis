import asyncio
import pytest

import apsis.program
import apsis.agent.client

# -------------------------------------------------------------------------------


async def _run():
    prog = apsis.program.AgentProgram(["/bin/sleep", "1"]).bind({})
    # Start the program.
    running = prog.run("testrun", cfg={})
    # Wait for it to finish.
    async for update in running.updates:
        pass
    return update


@pytest.mark.asyncio
async def test_agent_program_basic():
    result = await _run()
    assert result.meta["status"] == 0


@pytest.mark.asyncio
async def test_agent_program_concurrent():
    results = await asyncio.gather(*(_run() for _ in range(100)))
    assert all(r.meta["status"] == 0 for r in results)
