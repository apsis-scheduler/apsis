import asyncio
import logging
import ssl
from pathlib import Path

import msgpack
import pytest
import websockets
from procstar.testing.agent import TLS_CERT_PATH, TLS_KEY_PATH
from procstar_instance import ApsisService


JOB_DIR = Path(__file__).parent / "jobs"


class FilteringProxy:
    """
    WebSocket proxy that:
    - Drops all ProcStartRequest messages from server to client
    - Closes connections to mimic keep-alive timeout
    """

    def __init__(self, listen_port, target_host, target_port):
        self._listen_port = listen_port
        self._target = (target_host, target_port)
        self._server = None

    async def __aenter__(self):
        tls_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        tls_ctx.load_cert_chain(TLS_CERT_PATH, TLS_KEY_PATH)
        self._server = await websockets.serve(
            self._handle_client, "localhost", self._listen_port, ssl=tls_ctx
        )
        return self

    async def __aexit__(self, *exc):
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle_client(self, client_ws):
        path = getattr(client_ws.request, "path", "/")
        uri = f"wss://{self._target[0]}:{self._target[1]}{path}"
        logging.info("Proxy: client connected -> %s", uri)

        insecure_ctx = ssl.create_default_context()
        insecure_ctx.check_hostname = False
        insecure_ctx.verify_mode = ssl.CERT_NONE

        async with websockets.connect(uri, ssl=insecure_ctx) as server_ws:
            await asyncio.gather(
                self._forward_messages(client_ws, server_ws),  # client -> server
                self._filter_messages(
                    server_ws, client_ws
                ),  # server -> client (filtered)
            )

    async def _forward_messages(self, src, dst):
        """Forward messages unchanged."""
        async for msg in src:
            await dst.send(msg)

    async def _filter_messages(self, src, dst):
        """Drop all ProcStartRequest messages and schedule disconnect."""
        async for msg in src:
            decoded = msgpack.unpackb(msg, strict_map_key=False)
            if decoded.get("type") == "ProcStartRequest":
                proc_id = next(iter(decoded.get("specs", {}) or []), "unknown")
                logging.info("Proxy: Blocked ProcStartRequest for %s", proc_id)
                # Schedule disconnect after 3s
                asyncio.create_task(self._disconnect_after_delay(src, dst))
                continue  # Drop the message

            await dst.send(msg)

    async def _disconnect_after_delay(self, ws1, ws2):
        """Close connections after delay to mimic keep-alive timeout."""
        await asyncio.sleep(3)
        for ws in (ws1, ws2):
            await ws.close()


@pytest.mark.asyncio
async def test_reconnect_after_lost_process_start_request():
    """
    1. Start Apsis + agent (agent connects through the special proxy).
    2. Schedule a "sleep" job; proxy eats the ProcStartRequest.
    3. Proxy disconnects; agent reconnects; Apsis reconciles and finds
       no such process -> run enters "error".
    """
    proxy_port = 8767
    server_port = 8766
    async with FilteringProxy(proxy_port, "localhost", server_port):
        with (
            ApsisService(job_dir=JOB_DIR, agent_port=server_port) as svc,
            svc.agent(port=proxy_port, serve=True, conn_id="test-conn-id"),
        ):
            run_id = svc.client.schedule("sleep", {"time": 30})["run_id"]

            # give the proxy time to drop the message, disconnect, reconnect, ...
            await asyncio.sleep(5)

            run = svc.client.get_run(run_id)
            assert run["state"] == "error"
            run_log = svc.client.get_run_log(run_id)
            assert "error: process unknown" in run_log[-1]["message"]
