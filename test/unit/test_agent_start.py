import asyncio
import time
import pytest

import apsis.agent.client

# -------------------------------------------------------------------------------


async def _wait(agent, proc_id):
    # FIXME: Do something better.
    for _ in range(10):
        proc = await agent.get_process(proc_id)
        if proc["state"] == "run":
            time.sleep(0.1)
        else:
            break
    else:
        assert False, "proc failed to complete in 1 s"

    output, _, _ = await agent.get_process_output(proc_id)
    stop = await agent.del_process(proc_id)

    return proc, output, stop


@pytest.mark.asyncio
async def test_start_stop():
    agent = apsis.agent.client.Agent()
    await agent.connect()

    proc_id = (await agent.start_process(["/bin/echo", "Hello, world!"]))["proc_id"]
    proc, output, stop = await _wait(agent, proc_id)
    assert proc["state"] == "done"
    assert proc["status"] == 0
    assert proc["return_code"] == 0
    assert proc["signal"] is None
    assert output == b"Hello, world!\n"
    assert stop

    assert await agent.stop()
    assert not await agent.is_running()


@pytest.mark.asyncio
async def test_connect():
    """
    Tests that a second agent client will connect to the same running agent.
    """
    agent = apsis.agent.client.Agent()
    conn0 = await agent.connect()
    conn1 = await agent.connect()

    # Should be the same.
    assert conn1 == conn0

    proc_id0 = (await agent.start_process(["/bin/echo", "Hello, 0!"]))["proc_id"]
    proc_id1 = (await agent.start_process(["/bin/echo", "Hello, 1!"]))["proc_id"]

    proc, output, stop = await _wait(agent, proc_id0)
    assert proc["status"] == 0
    assert output == b"Hello, 0!\n"
    assert not stop

    proc, output, stop = await _wait(agent, proc_id1)
    assert proc["status"] == 0
    assert output == b"Hello, 1!\n"
    assert stop

    assert await agent.stop()
    assert not await agent.is_running()


@pytest.mark.asyncio
async def test_concurrent_start():
    """
    Checks that concurrent agent starts all use the same agent.
    """
    agents = [apsis.agent.client.Agent() for _ in range(10)]
    res = await asyncio.gather(*(a.connect() for a in agents))
    assert len(res) == len(agents)

    # All connections should be to the same port.
    assert len(set(r[0] for r in res)) == 1

    await agents[0].stop()

    # All should be stopped.
    res = await asyncio.gather(*(a.is_running() for a in agents))
    assert len(res) == len(agents)
    assert all(not r for r in res)
