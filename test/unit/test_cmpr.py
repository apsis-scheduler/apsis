import asyncio
import brotli
import pytest

from apsis.lib.cmpr import compress_async

# -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compress_async():
    data = b"alkjsdhtlkqjhwetrnabsdcvlkjhqaweljkh" * 1024

    compressed = await compress_async(data, None)
    assert compressed == data

    compressed = await compress_async(data, "br")
    assert len(compressed) < len(data)
    assert brotli.decompress(compressed) == data


@pytest.mark.asyncio
async def test_compress_async_timing():
    """
    Tests that compress_async() allows other coros to run concurrently.
    """
    data = b"alkjsdhtlkqjhwetrnabsdcvlkjhqaweljkh" * 1048576

    check = 0

    async def background():
        nonlocal check
        for i in range(1000):
            await asyncio.sleep(0.0001)
            check += i
        return check

    # Start the background task.
    task = asyncio.create_task(background())
    # It hasn't been scheduled yet.
    assert not check

    _ = await compress_async(data, "br")
    # background() should have run concurrently.
    assert check > 0
    assert await task == 499500
