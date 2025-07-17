import asyncio
import brotli
from concurrent.futures import ThreadPoolExecutor
import logging

from .timing import Timer

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


async def compress_async(data, compression) -> bytes:
    """
    Compresses `data` with `compression`.
    """
    if compression is None:
        return data

    elif compression == "br":
        loop = asyncio.get_running_loop()

        with Timer() as timer, ThreadPoolExecutor(1) as executor:
            result = await loop.run_in_executor(executor, lambda: brotli.compress(data, quality=3))
        log.debug(f"compressed: {len(data)} → {len(result)} in {timer.elapsed:.3f} s")
        return result

    else:
        raise NotImplementedError(f"compression: {compression}")
