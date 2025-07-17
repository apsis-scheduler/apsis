"""
The global WebSocket server that accepts incoming Procstar agent connections.
"""

import logging
import procstar.agent.server

from apsis.lib.parse import nparse_duration
from apsis.service import messages

log = logging.getLogger(__name__)

# The websockets library is too chatty at DEBUG (but remove this for debugging
# low-level WS or TLS problems).
logging.getLogger("websockets.server").setLevel(logging.INFO)

# -------------------------------------------------------------------------------

_SERVER = None


class NoServerError(RuntimeError):
    def __init__(self):
        super().__init__("no agent server running")


def get_agent_server():
    """
    Returns the global agent server.
    """
    if _SERVER is None:
        raise NoServerError()
    return _SERVER


def start_agent_server(cfg):
    """
    Creates and configures the global agent server.

    :return:
      Awaitable that runs the server.
    """
    global _SERVER
    assert _SERVER is None, "server already created"

    # Network/auth stuff.
    FROM_ENV = procstar.agent.server.FROM_ENV
    server_cfg = cfg.get("server", {})
    host = server_cfg.get("host", FROM_ENV)
    port = server_cfg.get("port", FROM_ENV)
    access_token = server_cfg.get("access_token", FROM_ENV)
    tls_cfg = server_cfg.get("tls", {})
    cert_path = tls_cfg.get("cert_path", FROM_ENV)
    key_path = tls_cfg.get("key_path", FROM_ENV)
    tls_cert = FROM_ENV if cert_path is FROM_ENV else (cert_path, key_path)

    conn_cfg = cfg.get("connection", {})
    reconnect_timeout = nparse_duration(conn_cfg.get("reconnect_timeout", None))

    _SERVER = procstar.agent.server.Server()
    return _SERVER.run_forever(
        host=host,
        port=port,
        tls_cert=tls_cert,
        access_token=access_token,
        reconnect_timeout=reconnect_timeout,
    )


# -------------------------------------------------------------------------------


async def agent_conn(apsis):
    """
    Subscribes to and republishes agent connection messages.
    """
    server = get_agent_server()
    with server.connections.subscription() as sub:
        async for conn_id, conn in sub:
            apsis.summary_publisher.publish(
                messages.make_agent_conn_delete(conn_id)
                if conn is None
                else messages.make_agent_conn(conn)
            )
