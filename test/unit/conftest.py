import asyncio
import uuid
from functools import cached_property

import pytest

import apsis.program.procstar.agent


async def iter_queue(queue: asyncio.Queue):
    while True:
        yield await queue.get()


class MockProc:
    """Mock procstar process with programmable update sequences."""

    def __init__(self):
        self.proc_id = str(uuid.uuid4())
        self.conn_id = "mock-conn-123"
        self.errors = []

        self._updates = asyncio.Queue()

    def _inject_updates(self, updates):
        for update in updates:
            self._updates.put_nowait(update)

    @cached_property
    def updates(self):
        """Mock updates generator that yields programmed sequences."""
        return iter_queue(self._updates)

    async def request_result(self, *args, **kwargs):
        pass

    async def request_fd_data(self, *args, **kwargs):
        pass

    async def send_signal(self, *args, **kwargs):
        pass

    async def request_delete(self, *args, **kwargs):
        pass

    async def delete(self, *args, **kwargs):
        pass


class MockAgentServer:
    """Mock agent server for testing."""

    def __init__(self, mock_proc):
        self.mock_proc = mock_proc

    async def start(self, proc_id, group_id, spec, conn_timeout):
        """Mock start method that returns a mock proc and initial result."""
        initial_result = await anext(self.mock_proc.updates)
        return self.mock_proc, initial_result


@pytest.fixture(scope="function")
def mock_proc(monkeypatch):
    proc = MockProc()
    server = MockAgentServer(proc)
    monkeypatch.setattr(apsis.program.procstar.agent, "get_agent_server", lambda: server)
    yield proc
