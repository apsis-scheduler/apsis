import functools
import logging
import os
from   procstar.testing.agent import TLS_CERT_PATH
import procstar.http.client
import random
import secrets
import signal
import subprocess
import uuid

from   apsis.lib.py import merge_mappings
import instance

logger = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

# Default agent port for testing, distinct from the usual default.  Choose at
# random, to reduce the risk of collisions on a test host.
DEFAULT_AGENT_PORT = random.randint(50000, 60000)

# Environment containing auth info for testing.
AUTH_ENV = {
    "PROCSTAR_AGENT_CERT": TLS_CERT_PATH,
    "PROCSTAR_AGENT_TOKEN": secrets.token_urlsafe(32),
}

class Agent:
    """
    A Procstar agent process.
    """

    def __init__(
            self, *,
            host,
            port,
            conn_id     =None,
            group_id    ="default",
            serve       =None,
    ):
        """
        :param serve:
          If not none, run the HTTP service on this port.
        """
        if conn_id is None:
            conn_id = str(uuid.uuid4())

        self.group_id   = group_id
        self.conn_id    = conn_id
        self.host       = host
        self.port       = port
        self.serve      = serve is not None
        self.serve_port = (
            None if serve is None
            else 3000 if serve is True  # FIXME: Choose a port.
            else int(serve)
        )

        self.proc       = None


    def start(self):
        assert self.proc is None, "already started"

        argv = [
            "procstar",
            "--log-level", "trace",
            "--agent",
            "--agent-host", self.host,
            "--agent-port", str(self.port),
            "--group-id", self.group_id,
            "--conn-id", self.conn_id,
            "--connect-interval-start", "0.1",
            "--connect-interval-max", "0.1",
        ]
        if self.serve:
            argv.extend([
                "--serve",
                "--serve-port", str(self.serve_port),
            ])

        env = os.environ | AUTH_ENV | {
            "RUST_BACKTRACE": "1",
        }
        self.proc = subprocess.Popen(argv, env=env)
        logger.info(f"started Procstar agent at pid {self.proc.pid}")


    def close(self):
        if self.proc is not None:
            logging.info("killing Procstar agent")
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
            self.proc = None


    def restart(self, *, signum=signal.SIGTERM, keep_conn_id=False):
        logging.info("killing Procstar agent")
        self.proc.send_signal(signum)
        self.proc.wait()
        self.proc = None

        if not keep_conn_id:
            self.conn_id = str(uuid.uuid4())

        self.start()


    def __enter__(self):
        self.start()
        return self


    def __exit__(self, *exc_info):
        self.close()


    @functools.cached_property
    def client(self):
        assert self.serve, "agent not serving HTTP"
        return procstar.http.client.Client((self.host, self.serve_port))



class ApsisService(instance.ApsisService):
    """
    Apsis service configured to run a Procstar agent service.
    """

    # FIXME: Choose an available port by default, instead of fixed.
    def __init__(self, *, agent_port=DEFAULT_AGENT_PORT, cfg={}, **kw_args):
        cfg = merge_mappings(cfg, {
            "procstar": {
                "agent": {
                    "enable": True,
                    "server": {
                        "port": agent_port,
                    },
                    "connection": {
                        "start_timeout": 5,
                        "reconnect_timeout": 5,
                    },
                },
            },
        })
        super().__init__(cfg=cfg, env=AUTH_ENV, **kw_args)
        self.agent_port = agent_port


    def agent(self, **kw_args):
        return Agent(host="localhost", port=self.agent_port, **kw_args)



