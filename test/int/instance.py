import asyncio
from contextlib import contextmanager
import functools
import httpx
import logging
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import ujson
import websockets.sync.client
import yaml

import apsis.service.client
from apsis.sqlite import SqliteDB

# -------------------------------------------------------------------------------


def find_free_port():
    """Find an available port by binding to port 0 and getting the assigned port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def run_apsisctl(*argv):
    subprocess.run(["apsisctl", *(str(a) for a in argv)], check=True)


# stands in for the instance's own user in http_client and connect_ws
INSTANCE_USER = object()


class ApsisService:
    """
    An Apsis service instance running in a separate process.
    """

    def __init__(
        self, *, port=None, job_dir=None, cfg={}, env={}, unix=False, user: str | None = None
    ):
        """
        :param user:
          X-Remote-User the proxy would send, which clients send on a unix socket.
        """
        if unix and port is not None:
            raise ValueError("an instance serves on either a port or a unix socket")
        self.tmp_dir = Path(tempfile.mkdtemp())
        self.sock_path = self.tmp_dir / "apsis.sock" if unix else None
        self.user = user
        if unix:
            self.port = None
        else:
            self.port = int(port) if port is not None else find_free_port()
        logging.info(f"Apsis instance in {self.tmp_dir}")
        self.db_path = self.tmp_dir / "apsis.db"
        if job_dir is None:
            job_dir = self.tmp_dir / "jobs"
            job_dir.mkdir()
        else:
            job_dir = Path(job_dir)
        self.jobs_dir = job_dir
        self.cfg_path = self.tmp_dir / "config.yaml"
        self.log_path = self.tmp_dir / "apsis.log"
        self.agent_dir = self.tmp_dir / "agent"

        self.cfg = {"adhoc": {"enabled": True}} | cfg
        self.srv_proc = None
        self.env = dict(env)

    def create_db(self, *, clock=None):
        SqliteDB.create(self.db_path, clock=clock)

    def write_cfg(self):
        cfg = self.cfg | {
            "database": {
                "path": str(self.db_path),
            },
            "job_dir": str(self.jobs_dir),
        }
        with open(self.cfg_path, "w") as file:
            yaml.dump(cfg, file)

    def start_serve(self):
        assert self.cfg is not None
        assert self.srv_proc is None

        with open(self.log_path, "w") as log_file:
            self.srv_proc = subprocess.Popen(
                [
                    "apsisctl",
                    "--log",
                    "DEBUG",
                    "serve",
                    "--config",
                    str(self.cfg_path),
                    *(
                        ("--port", str(self.port))
                        if self.sock_path is None
                        else ("--unix", str(self.sock_path))
                    ),
                ],
                env=os.environ
                | self.env
                | {
                    "APSIS_AGENT_DIR": str(self.agent_dir),
                },
                stderr=log_file,
            )

    def wait_for_serve(self):
        if self.sock_path is None:
            self.client.wait_running(2)
            return
        deadline = time.monotonic() + 2
        with self.http_client() as client:
            while True:
                try:
                    client.get("/api/v1/running")
                    return
                except httpx.TransportError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)

    def _user_headers(self, user: str | None | object, headers: dict[str, str]) -> dict[str, str]:
        """
        Returns `headers` with X-Remote-User for `user`, by default the instance's.
        """
        user = self.user if user is INSTANCE_USER else user
        return headers if user is None else {"X-Remote-User": user} | headers

    def http_client(
        self, *, user: str | None | object = INSTANCE_USER, headers: dict[str, str] = {}, **kwargs
    ) -> httpx.Client:
        """
        Returns an HTTP client for this instance over its unix socket or TCP port,
        sending `user` as X-Remote-User on a unix socket.
        """
        if self.sock_path is None:
            return httpx.Client(base_url=f"http://127.0.0.1:{self.port}", headers=headers, **kwargs)
        return httpx.Client(
            transport=httpx.HTTPTransport(uds=str(self.sock_path)),
            base_url="http://apsis",
            headers=self._user_headers(user, headers),
            **kwargs,
        )

    def connect_ws(
        self,
        path: str,
        *,
        user: str | None | object = INSTANCE_USER,
        additional_headers: dict[str, str] = {},
        **kwargs,
    ) -> websockets.sync.client.ClientConnection:
        """
        Opens a websocket to `path` on this instance over its unix socket or TCP port,
        sending `user` as X-Remote-User on a unix socket.
        """
        if self.sock_path is None:
            return websockets.sync.client.connect(
                f"ws://127.0.0.1:{self.port}{path}", additional_headers=additional_headers, **kwargs
            )
        return websockets.sync.client.unix_connect(
            str(self.sock_path),
            f"ws://apsis{path}",
            additional_headers=self._user_headers(user, additional_headers),
            **kwargs,
        )

    def is_running(self):
        if self.srv_proc is None:
            return False
        try:
            self.srv_proc.wait(timeout=0)
        except subprocess.TimeoutExpired:
            return True
        else:
            return False

    def tcp_port(self) -> int:
        """
        Returns the TCP port, or raises if this instance serves on a unix socket.
        """
        if self.sock_path is not None:
            raise RuntimeError("this instance serves on a unix socket, use http_client")
        return self.port

    @functools.cached_property
    def client(self):
        return apsis.service.client.Client(("localhost", self.tcp_port()))

    @contextmanager
    def get_log(self):
        with open(self.log_path) as file:
            yield iter(file)

    def get_log_lines(self):
        with self.get_log() as lines:
            return tuple(lines)

    def stop_serve(self):
        assert self.srv_proc is not None
        self.srv_proc.send_signal(signal.SIGTERM)
        res = self.srv_proc.wait()
        self.srv_proc = None

        # Dump the log file.
        with open(self.log_path) as file:
            sys.stderr.write(file.read())

        return res

    # FIXME: Remove.
    def close(self):
        if self.srv_proc is not None:
            self.stop_serve()

    def __enter__(self):
        try:
            self.create_db()
            self.write_cfg()
            self.start_serve()
            self.wait_for_serve()
        except Exception:
            # Dump the log file.
            with open(self.log_path) as file:
                sys.stderr.write(file.read())
        return self

    def __exit__(self, *exc_info):
        if self.srv_proc is not None:
            self.stop_serve()

    def restart(self):
        logging.info("restarting Apsis service")
        self.stop_serve()
        self.start_serve()
        self.wait_for_serve()

    def run_apsis_cmd(self, *argv):
        """
        Runs an `apsis` subcommand against the running service.

        :return:
          The return code and stdout.
        """
        port = self.tcp_port()
        assert self.srv_proc is not None
        argv = ["apsis", "--port", str(port), *(str(a) for a in argv)]
        proc = subprocess.run(argv, stdout=subprocess.PIPE)
        logging.info(f"apsis returned: {proc.returncode} {proc.stdout}")
        return proc.returncode, proc.stdout

    def run_apsis_json(self, *argv):
        """
        Runs an `apsis` subcommand against the running service.

        Requests JSON output, and parses it.

        :return:
          The parsed JSO output.
        """
        returncode, stdout = self.run_apsis_cmd(*argv, "--format", "json")
        assert returncode == 0, f"'apsis {' '.join(argv)}' failed: {stdout.decode()}"
        return ujson.loads(stdout)

    def get_run(self, run_id: str) -> dict:
        """
        Returns the JSO for run `run_id`.
        """
        with self.http_client() as client:
            res = client.get(f"/api/v1/runs/{run_id}")
        res.raise_for_status()
        return res.json()["runs"][run_id]

    def wait_run(
        self,
        run_id,
        *,
        timeout=60,
        wait_states=("new", "scheduled", "waiting", "starting", "running", "stopping"),
    ):
        """
        Polls for a run to no longer be running.
        """
        for _ in range(int(timeout / 0.1)):
            res = self.get_run(run_id)
            if res["state"] in wait_states:
                time.sleep(0.1)
                continue
            else:
                return res
        else:
            raise RuntimeError("timeout waiting for run")

    async def async_wait_run(
        self,
        run_id,
        *,
        timeout=60,
        wait_states=("new", "scheduled", "waiting", "starting", "running"),
    ):
        # FIXME: There should be a proper slow-poll endpoint for this, instead
        # of polling.
        for _ in range(int(timeout / 0.1)):
            res = self.get_run(run_id)
            if res["state"] in wait_states:
                await asyncio.sleep(0.1)
                continue
            else:
                return res
        else:
            raise RuntimeError("timeout waiting for run")

    async def read_summary(self, query, *, until="summary_init_done", timeout=10):
        """
        Reads summary websocket messages for `query` through the first
        message of type `until`.
        """

        def read():
            deadline = time.monotonic() + timeout
            msgs = []
            with self.connect_ws(f"/api/v1/summary?{query}", max_size=None) as ws:
                while not msgs or msgs[-1]["type"] != until:
                    msgs += ujson.loads(ws.recv(timeout=deadline - time.monotonic()))
            return msgs

        return await asyncio.to_thread(read)

    def wait_for_run_to_start(self, run_id):
        return self.wait_run(run_id, wait_states=("new", "scheduled", "waiting", "starting"))
