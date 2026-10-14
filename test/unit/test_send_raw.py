import asyncio

import pytest
import ujson

from apsis.service import api


class FakeWs:
    def __init__(self):
        self.frames = []

    async def send(self, data):
        self.frames.append(ujson.loads(data))


@pytest.mark.asyncio
async def test_send_raw_frames_and_yields_between_pages(monkeypatch):
    """Pages are joined into frames of at least WS_RAW_CHUNK messages and other tasks run between pages."""
    monkeypatch.setattr(api, "WS_RAW_CHUNK", 4)
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticks_at_page = []

    def pages():
        for i in range(3):
            ticks_at_page.append(ticks)
            yield [ujson.dumps({"n": i * 3 + j}) for j in range(3)]

    task = asyncio.create_task(ticker())
    await asyncio.sleep(0)
    ws = FakeWs()
    await api._send_raw(pages(), ws, "test")
    task.cancel()

    assert ws.frames == [[{"n": n} for n in range(6)], [{"n": n} for n in range(6, 9)]]
    assert ticks_at_page == sorted(set(ticks_at_page))
