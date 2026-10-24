import base64
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
from typing import Any
import ujson
from urllib.parse import quote, urlsplit, urlunsplit
import websockets.client

try:
    import gssapi
    from requests_gssapi import DISABLED, SPNEGO, HTTPSPNEGOAuth
except ImportError:
    gssapi = HTTPSPNEGOAuth = None

import apsis.service
from apsis.runs import run_number
from apsis.lib.json import nkey

# -------------------------------------------------------------------------------

Address = namedtuple("Address", ("host", "port"))


def get_address() -> Address:
    """
    Returns the configured host and port where Apsis runs, or the defaults if
    APSIS_HOST is a URL.
    """
    loc = os.environ.get("APSIS_HOST")
    if loc is None or "://" in loc:
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


def get_url() -> str:
    """
    Returns the base URL where Apsis runs, from APSIS_HOST if it's a URL, else
    from `get_address` over http.
    """
    loc = os.environ.get("APSIS_HOST", "")
    if "://" in loc:
        return loc.rstrip("/")
    return "http://{}:{}".format(*get_address())


def get_explicit_address(host: str | None, port: str | None) -> Address | None:
    """
    Returns the address for an explicit `host` or `port`, filling in the other
    from `get_address`, or none if neither is given.
    """
    if host is None and port is None:
        return None
    addr = get_address()
    return Address(host or addr.host, port or addr.port)


AUTH_ERROR = "not authenticated, get a Kerberos ticket and check KRB5CCNAME"
NO_KERBEROS_ERROR = "not authenticated, install requests-gssapi for Kerberos"
NO_HTTPS_ERROR = "not authenticated, use an https URL for Kerberos"


def get_auth_error(url: str) -> str:
    """
    Returns the error for a 401 from `url` without a server error message.
    """
    if HTTPSPNEGOAuth is None:
        return NO_KERBEROS_ERROR
    if urlsplit(url).scheme not in ("https", "wss"):
        return NO_HTTPS_ERROR
    return AUTH_ERROR


def has_ticket() -> bool:
    """
    Returns true if the user has unexpired Kerberos credentials to initiate a
    context with.
    """
    try:
        return gssapi.Credentials(usage="initiate").lifetime != 0
    except gssapi.exceptions.GSSError:
        return False


def get_negotiate_headers(url: str) -> dict[str, str]:
    """
    Returns an SPNEGO `Authorization` header for the host of a wss `url`, or
    no headers if the URL isn't wss, requests-gssapi is missing, there's no
    ticket, or the token fails.
    """
    parts = urlsplit(url)
    if parts.scheme != "wss" or HTTPSPNEGOAuth is None or not has_ticket():
        return {}
    try:
        name = gssapi.Name(f"HTTP@{parts.hostname}", gssapi.NameType.hostbased_service)
        token = gssapi.SecurityContext(name=name, mech=SPNEGO, usage="initiate").step()
    except gssapi.exceptions.GSSError as exc:
        logging.debug(f"no negotiate token for {parts.hostname}: {exc}")
        return {}
    return {"Authorization": "Negotiate " + base64.b64encode(token).decode()}


@asynccontextmanager
async def get_ws_msgs(url):
    """
    Async context manager of async iterator of messages read from websocket
    at `url`.
    """
    headers = get_negotiate_headers(url)
    try:
        async with websockets.client.connect(url, max_size=None, extra_headers=headers) as conn:

            async def get_msgs():
                while True:
                    try:
                        msg = await conn.recv()
                    except websockets.ConnectionClosedOK:
                        break
                    yield msg

            yield get_msgs()
    except websockets.exceptions.InvalidStatusCode as exc:
        if exc.status_code == 401:
            raise APIError(401, get_auth_error(url), {}) from None
        raise


def parse_content_range(header):
    match = re.match(r"(\w+)=(\d+)-(\d+)/(\d+)", header)
    typ, start, stop, length = match.groups()
    return typ, int(start), int(stop), int(length)


class APIError(RuntimeError):
    def __init__(self, status, error, jso):
        super().__init__(error)
        self.status = status
        self.jso = jso


def get_api_error(resp: requests.Response, default: str) -> APIError:
    """
    Returns an `APIError` for `resp` with the server's error, or `default`.
    """
    try:
        jso = resp.json()
        error = jso.get("error", default)
    except Exception:
        jso = {}
        error = default
    return APIError(resp.status_code, error, jso)


NO_ARG = object()


class Client:
    def __init__(self, address=None):
        """
        :param address:
          Apsis hostname and port, or none for `get_url`.
        """
        self.__base = get_url() if address is None else "http://{}:{}".format(*address)
        self.__auth = (
            HTTPSPNEGOAuth(mutual_authentication=DISABLED, opportunistic_auth=True)
            if urlsplit(self.__base).scheme == "https"
            and HTTPSPNEGOAuth is not None
            and has_ticket()
            else None
        )

    @property
    def base_url(self) -> str:
        """
        The base URL requests go to.
        """
        return self.__base

    def __url(self, *path, ws=False, query=None, **kw_query):
        # query holds params whose names may collide with these keywords
        query = {**(query or {}), **kw_query}
        query = "&".join(
            str(k) if v is NO_ARG else f"{k}={quote(str(v))}"
            for k, v in query.items()
            if v is not None
        )
        scheme, netloc, base_path, _, _ = urlsplit(self.__base)
        if ws:
            scheme = {"http": "ws", "https": "wss"}[scheme]
        return urlunsplit((scheme, netloc, base_path + "/".join(path), query, ""))

    def __send(self, method: str, url: str, **kw: Any) -> requests.Response:
        resp = requests.request(method, url, auth=self.__auth, **kw)
        if resp.status_code == 401:
            raise get_api_error(resp, get_auth_error(url))
        return resp

    def __request(self, method, *path, timeout=None, data=None, query=None, **kw_query):
        url = self.__url(*path, query={**(query or {}), **kw_query})
        logging.debug(f"{method} {url} data={data}")
        resp = self.__send(method, url, json=data, timeout=timeout)
        if 200 <= resp.status_code < 300:
            return resp.json()
        else:
            raise get_api_error(resp, "unknown error")

    def __get(self, *path, **query):
        return self.__request("GET", *path, **query)

    def __get_paged_runs(
        self, *path, max_runs: int | None = None, query: dict[str, str] | None = None
    ) -> dict:
        """
        Follows the `paging.next` cursor across pages, returning the merged
        {run_id: run} dict, newest first.

        The server chooses the page size and this walks however many pages are
        needed.  `max_runs` caps the total collected.

        :param max_runs:
          If given, stop once `max_runs` runs are collected and return only the
          newest `max_runs`.  Otherwise walk every page (the full history).
        :param query:
          Query params sent with every page, mapping param name to value.
        """
        runs = {}
        cursor = None
        while True:
            # cursor is None on the first request, which __url drops
            resp = self.__get(*path, query={**(query or {}), "cursor": cursor})
            runs.update(resp["runs"])
            if max_runs is not None and len(runs) >= max_runs:
                return dict(list(runs.items())[:max_runs])
            next_cursor = resp.get("paging", {}).get("next")
            if next_cursor is None:
                return runs
            if cursor is not None and run_number(next_cursor) >= run_number(cursor):
                raise RuntimeError(f"paging cursor did not decrease: {cursor} -> {next_cursor}")
            cursor = next_cursor

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
          Timeout in sec.
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
        return self.__get("/api/v1/jobs", quote(job_id, safe=""))

    def get_job_runs(self, job_id) -> dict:
        # walks the paging.next cursor, same as get_runs
        return self.__get_paged_runs("/api/v1/jobs", quote(job_id, safe=""), "runs")

    def get_jobs(self, *, label=None):
        return self.__get("/api/v1/jobs", label=label)

    def get_outputs(self, run_id):
        """
        Returns output metadata for `run_id`.
        """
        url = self.__url("/api/v1/runs", run_id, "outputs")
        resp = self.__send("GET", url)
        resp.raise_for_status()
        return resp.json()

    def get_output(self, run_id, output_id) -> bytes:
        url = self.__url("/api/v1/runs", run_id, "output", output_id)
        resp = self.__send("GET", url)
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
            ws=True,
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
        Sends `signal` to a running process.
        """
        signal = getattr(signal, "name", str(signal))
        return self.__put("/api/v1/runs", run_id, "signal", signal)

    def mark(self, run_id, state_name):
        """
        Marks a run to a different finished state.
        """
        return self.__post("/api/v1/runs", run_id, "mark", state_name)

    # query params a run arg name can't shadow
    _RUNS_QUERY_PARAMS = frozenset(
        {
            "job_id",
            "run_id",
            "state",
            "since",
            "summary",
            "cursor",
            "schedule_since",
            "schedule_until",
        }
    )

    def get_runs(
        self,
        *,
        job_id=None,
        state=None,
        args={},
        limit: int | None = None,
        schedule_since: Time | str | None = None,
        schedule_until: Time | str | None = None,
    ) -> dict:
        """
        :param schedule_since:
          If not none, lower bound on nominal schedule time, inclusive.
        :param schedule_until:
          If not none, upper bound on nominal schedule time, exclusive.
        """
        # limit is the total runs to return not the page size
        # walk the server pages and stop once we have that many
        if limit is not None and limit < 1:
            raise ValueError(f"limit must be at least 1: {limit}")
        return self.__get_paged_runs(
            "/api/v1/runs",
            max_runs=limit,
            query={
                "job_id": job_id,
                "state": state,
                "schedule_since": None if schedule_since is None else str(Time(schedule_since)),
                "schedule_until": None if schedule_until is None else str(Time(schedule_until)),
                # escape reserved names and existing leading underscores
                **{
                    "_" + n if n in self._RUNS_QUERY_PARAMS or n.startswith("_") else n: a
                    for n, a in args.items()
                },
            },
        )

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
            ws=True,
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
        return self.schedule_adhoc(
            time, {"program": {"type": "procstar", "argv": [str(a) for a in args]}}, **kw_args
        )

    def schedule_shell_program(self, time, command, **kw_args):
        """
        :param time:
          The schedule time, or "now" for immediate.
        :param command:
          The shell command to run.
        """
        return self.schedule_adhoc(
            time, {"program": {"type": "procstar-shell", "command": str(command)}}, **kw_args
        )

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
