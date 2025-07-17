import asyncio
import pytest
import subprocess

import apsis.lib.asyn

# -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_communicate_notimeout():
    argv = ["/bin/bash", "-c", "echo foo; echo bar >&2; sleep 0.1"]

    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    out, err = await apsis.lib.asyn.communicate(proc, 0.5)
    assert out == b"foo\n"
    assert err == b"bar\n"


@pytest.mark.asyncio
async def test_communicate_timeout():
    argv = ["/bin/bash", "-c", "echo foo; echo bar >&2; sleep 0.25"]

    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        await apsis.lib.asyn.communicate(proc, 0.1)
    except asyncio.TimeoutError as exc:
        assert exc.stdout == b"foo\n"
        assert exc.stderr == b"bar\n"
        # Clean up the process, so that it doesn't outlive the event loop.
        await proc.wait()
    else:
        assert False, "should have timed out"


@pytest.mark.asyncio
async def test_publisher():
    pub = apsis.lib.asyn.Publisher()

    with (
        pub.subscription() as sub_all,
        pub.subscription(predicate=lambda n: n % 2 == 0) as sub_even,
        pub.subscription(predicate=lambda n: n % 10 == 0) as sub_tens,
    ):
        assert pub.num_subs == 3

        fut = asyncio.ensure_future(anext(sub_all))
        assert not fut.done()
        pub.publish(0)
        assert await fut == 0
        assert await anext(sub_even) == 0
        assert await anext(sub_tens) == 0

        for i in range(1, 11):
            pub.publish(i)
        assert [await anext(sub_even) for _ in range(5)] == [2, 4, 6, 8, 10]
        fut = asyncio.ensure_future(anext(sub_tens))
        assert await fut == 10
        fut = asyncio.ensure_future(anext(sub_tens))
        assert not fut.done()
        assert [await anext(sub_all) for _ in range(10)] == list(range(1, 11))
        pub.publish(20)
        assert await fut == 20
        assert await anext(sub_all) == 20
        assert await anext(sub_even) == 20

        pub.publish(30)
        pub.publish(31)
        pub.close()

        # Can't publish after close.
        with pytest.raises(RuntimeError):
            pub.publish(32)

        # Closing multiple times is OK.
        pub.close()

        assert await anext(sub_tens) == 30
        with pytest.raises(StopAsyncIteration):
            await anext(sub_tens)
        assert await anext(sub_even) == 30
        with pytest.raises(StopAsyncIteration):
            await anext(sub_even)
        assert await anext(sub_all) == 30
        assert await anext(sub_all) == 31
        with pytest.raises(StopAsyncIteration):
            await anext(sub_all)
        with pytest.raises(StopAsyncIteration):
            await anext(sub_all)

    assert pub.num_subs == 0

    # Subscription after close should immediately be closed.
    with pub.subscription() as sub_late:
        assert pub.num_subs == 1
        with pytest.raises(StopAsyncIteration):
            await anext(sub_late)


@pytest.mark.asyncio
async def test_task_group():
    val = 0

    async def good():
        nonlocal val
        val += 1

    async def slow():
        nonlocal val
        await asyncio.sleep(1)
        val += 10

    async def bad():
        await asyncio.sleep(0.25)
        raise RuntimeError("oops")

    group = apsis.lib.asyn.TaskGroup()
    assert len(group) == 0
    group.add("good0", good())
    group.add("slow0", slow())
    group.add("bad0", bad())
    group.add("bad1", bad())
    group.add("good1", good())
    group.add("slow1", slow())

    await asyncio.sleep(0.5)
    assert val == 2
    assert len(group) == 2
    await group.cancel_all()
    assert len(group) == 0
