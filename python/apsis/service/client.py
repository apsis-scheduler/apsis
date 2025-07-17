from collections import namedtuple
from contextlib import asynccontextmanager
import http.client
import io
import logging
from ora import Time
import os
import re
import requests
import time
import ujson
from urllib.parse import quote, urlunparse
import websockets.client

import apsis.service
from apsis.lib.json import nkey

# -------------------------------------------------------------------------------

Address = namedtuple("Address", ("host", "port"))


def get_address() -> Address:
    """
    Returns the configured host and port where Apsis runs.
    """
    try:
        loc = os.environ["APSIS_HOST"]
    except KeyError:
        host, port = "localhost", apsis.service.DEFAULT_PORT
    else:
        try:
            host, port = loc.split(":", 1)
            port = int(port)
        except ValueError:
            host, port = loc, apsis.service.DEFAULT_PORT
        else:
            port = int(port)
    return Address(host, port)


@asynccontextmanager
async def get_ws_msgs(url):
    """
    Async context manager of async iterator of messages read from websocket
    at `url`.
    """
    async with websockets.client.connect(url, max_size=None) as conn:

        async def get_msgs():
            while True:
                try:
                    msg = await conn.recv()
                except websockets.ConnectionClosedOK:
                    break
                yield msg

        yield get_msgs()


def parse_content_range(header):
    match = re.match(r"(\w+)=(\d+)-(\d+)/(\d+)", header)
    typ, start, stop, length = match.groups()
    return typ, int(start), int(stop), int(length)


class APIError(RuntimeError):
    def __init__(self, status, error, jso):
        super().__init__(error)
        self.status = status
        self.jso = jso


NO_ARG = object()


class Client:
    def __init__(self, address=None):
        """
        :param addr:
          Apsis hostname and port, or none for default.
        """
        self.__addr = get_address() if address is None else Address(*address)

    def __url(self, *path, scheme="http", **query):
        query = "&".join(
            str(k) if v is NO_ARG else f"{k}={quote(str(v))}"
            for k, v in query.items()
            if v is not None
        )
        return urlunparse(
            (
                scheme,
                f"{self.__addr.host}:{self.__addr.port}",
                "/".join(path),
                "",
                query,
                "",
            )
        )

    def __request(self, method, *path, timeout=None, data=None, **query):
        url = self.__url(*path, **query)
        logging.debug(f"{method} {url} data={data}")
        resp = requests.request(method, url, json=data, timeout=timeout)
        if 200 <= resp.status_code < 300:
            return resp.json()
        else:
            try:
                jso = resp.json()
                error = jso.get("error", "unknown error")
            except Exception:
                jso = {}
                error = "unknown error"
            raise APIError(resp.status_code, error, jso)

    def __get(self, *path, **query):
        return self.__request("GET", *path, **query)

    def __post(self, *path, data=None, **query):
        return self.__request("POST", *path, data=data, **query)

    def __put(self, *path, data=None, **query):
        return self.__request("PUT", *path, data=data, **query)

    def alive(self, timeout):
        """
        Checks liveness of the service.

        :param timeout:
          Timeout in sec.
        """
        _ = self.__get("/api/v1/alive")

    def wait_running(self, timeout):
        """
        Blocks until the service is running.

        :param timeout:
          Tiemout in sec.
        """
        # FIXME: Use live updates instead.
        INTERVAL = 0.05

        deadline = time.time() + timeout
        while True:
            get_timeout = deadline - time.time()
            if get_timeout < 0:
                raise TimeoutError(f"service not running after {timeout}")
            try:
                _ = self.__get("/api/v1/running", timeout=get_timeout)
            except requests.exceptions.ConnectionError:
                time.sleep(INTERVAL)
            else:
                return True

    def stats(self):
        return self.__get("/api/v1/stats")

    def skip(self, run_id):
        """
        Skips a scheduled or waiting run.
        """
        return self.__post("/api/v1/runs", run_id, "skip")

    def start(self, run_id):
        """
        Forces a scheduled or waiting run to start.
        """
        return self.__post("/api/v1/runs", run_id, "start")

    def get_run_log(self, run_id):
        return self.__get("/api/v1/runs", run_id, "log")["run_log"]

    def get_job(self, job_id):
        return self.__get("/api/v1/jobs", job_id)

    def get_job_runs(self, job_id):
        return self.__get("/api/v1/jobs", job_id, "runs")["runs"]

    def get_jobs(self, *, label=None):
        return self.__get("/api/v1/jobs", label=label)

    def get_outputs(self, run_id):
        """
        Returns output metadata for `run_id`.
        """
        url = self.__url("/api/v1/runs", run_id, "outputs")
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json()

    def get_output(self, run_id, output_id) -> bytes:
        url = self.__url("/api/v1/runs", run_id, "output", output_id)
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content

    @asynccontextmanager
    async def get_output_data_updates(self, run_id, output_id, start=None):
        """
        Async context manager of async iterator of `bytes` containing output
        data updates for output `output_id` of run `run_id`.

        :param start:
          If not none, yields output from this position until current, before
          yielding subsequent output updates.
        """
        url = self.__url(
            "/api/v1/runs",
            run_id,
            "output",
            output_id,
            "updates",
            scheme="ws",
            **({} if start is None else {"start": start}),
        )

        async with get_ws_msgs(url) as msgs:
            pos = None if start is None else start

            def conv(msg):
                nonlocal pos
                msg = io.BytesIO(msg)
                header = http.client.parse_headers(msg)
                content_range = header["Content-Range"]
                typ, start, stop, _ = parse_content_range(content_range)
                assert typ == "bytes"
                if pos is None:
                    pos = start
                else:
                    assert pos == start

                body = msg.read()
                assert len(body) == stop - start + 1
                pos += len(body)

                return body

            yield (conv(m) async for m in msgs)

    def signal(self, run_id, signal):
        """
        Sends `signal` to a running processes.
        """
        signal = getattr(signal, "name", str(signal))
        return self.__put("/api/v1/runs", run_id, "signal", signal)

    def mark(self, run_id, state_name):
        """
        Marks a run to different finished state.
        """
        return self.__post("/api/v1/runs", run_id, "mark", state_name)

    def get_runs(self, *, job_id=None, state=None, args={}):
        return self.__get(
            "/api/v1/runs",
            job_id=job_id,
            state=state,
            # Include args, but prefix with underscore any that collide with
            # fixed arg names.
            # FIXME: Oh so hacky.
            **{
                "_" + n if n in {"job_id", "run_id", "state", "since"} else n: a
                for n, a in args.items()
            },
        )["runs"]

    def get_run(self, run_id):
        return self.__get("/api/v1/runs", run_id)["runs"][run_id]

    def get_run_dependencies(self, run_id):
        return self.__get("/api/v1/runs", run_id, "dependencies")[run_id]

    @asynccontextmanager
    async def get_run_updates(self, run_id, *, init=False):
        """
        Async context manager of async iterator of JSO run update messages
        for `run_id`.

        @param init:
          If true, request current run state at the start of the stream.
        """
        url = self.__url(
            "/api/v1/runs",
            run_id,
            "updates",
            scheme="ws",
            **({"init": NO_ARG} if init else {}),
        )
        async with get_ws_msgs(url) as msgs:
            yield (ujson.loads(m) async for m in msgs)

    def rerun(self, run_id):
        (run,) = self.__post("/api/v1/runs", run_id, "rerun", data={})["runs"].values()
        return run

    def __schedule(self, time, job_spec, *, count=None, stop_time=None):
        time = "now" if time == "now" else str(Time(time))
        stop_time = None if stop_time is None else str(stop_time)
        params = {
            "data": job_spec
            | {
                "times": {
                    "schedule": time,
                }
                | nkey("stop", stop_time)
            }
        } | nkey("count", count)
        runs = self.__post("/api/v1/runs", **params)["runs"]
        # FIXME: Hacky.
        return next(iter(runs.values())) if count is None else runs.values()

    def schedule(self, job_id, args, time="now", **kw_args):
        """
        Creates and schedules a new run.
        """
        return self.__schedule(
            time,
            {
                "job_id": str(job_id),
                "args": {str(k): str(v) for k, v in args.items()},
            },
            **kw_args,
        )

    def schedule_adhoc(self, time, job, **kw_args):
        return self.__schedule(time, {"job": job}, **kw_args)

    def schedule_program(self, time, args, **kw_args):
        """
        :param time:
          The schedule time, or "now" for immediate.
        :param args:
          The argument vector.  The first item is the path to the program
          to run.
        """
        return self.__schedule(time, {"job": {"program": [str(a) for a in args]}}, **kw_args)

    def schedule_shell_program(self, time, command, **kw_args):
        """
        :param time:
          The schedule time, or "now" for immediate.
        :param command:
          The shell command to run.
        """
        return self.__schedule(time, {"job": {"program": str(command)}}, **kw_args)

    def stop_run(self, run_id):
        return self.__put("/api/v1/runs", run_id, "stop")["runs"][run_id]

    def reload_jobs(self, *, dry_run=False):
        return self.__post("/api/control/reload_jobs", data={}, dry_run=dry_run)

    def shut_down(self, restart=False):
        query = {"restart": NO_ARG} if restart else {}
        self.__post("/api/control/shut_down", data={}, **query)

    def version(self):
        """
        Returns a version info structure.
        """
        return self.__get("/api/control/version")
