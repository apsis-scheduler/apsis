import asyncio
from contextlib import contextmanager
import functools
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


class ApsisService:
    """
    An Apsis service instance running in a separate process.
    """

    def __init__(self, *, port=None, job_dir=None, cfg={}, env={}):
        self.port = int(port) if port is not None else find_free_port()

        self.tmp_dir = Path(tempfile.mkdtemp())
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

        self.cfg = dict(cfg)
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
                    "--port",
                    str(self.port),
                ],
                env=os.environ
                | self.env
                | {
                    "APSIS_AGENT_DIR": str(self.agent_dir),
                },
                stderr=log_file,
            )

    def wait_for_serve(self):
        self.client.wait_running(2)

    def is_running(self):
        if self.srv_proc is None:
            return False
        try:
            self.srv_proc.wait(timeout=0)
        except subprocess.TimeoutExpired:
            return True
        else:
            return False

    @functools.cached_property
    def client(self):
        return apsis.service.client.Client(("localhost", self.port))

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
        assert self.srv_proc is not None
        argv = ["apsis", "--port", str(self.port), *(str(a) for a in argv)]
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
            res = self.client.get_run(run_id)
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
            res = self.client.get_run(run_id)
            if res["state"] in wait_states:
                await asyncio.sleep(0.1)
                continue
            else:
                return res
        else:
            raise RuntimeError("timeout waiting for run")

    def wait_for_run_to_start(self, run_id):
        return self.wait_run(run_id, wait_states=("new", "scheduled", "waiting", "starting"))
