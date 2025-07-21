"""
Client wrapper for the Apsis Agent.

Uses async HTTPX for communication, including connection reuse.  Therefore,

- You can only use it in a single asyncio event loop.

- Before shutting down the event loop, you must call `get_session().close()`, to
  shut down all open connections cleanly.

"""

import aiohttp
import asyncio
import contextlib
import errno
import functools
import httpx
import itertools
import logging
import os
import shlex
import subprocess
import sys
import tempfile
import ujson
from urllib.parse import quote_plus
import warnings

import apsis.lib.asyn
from apsis.lib.py import if_none
from apsis.lib.sys import get_username
from apsis.lib.test import in_test

log = logging.getLogger("agent.client")
# Turn down logging for httpx and its dependencies.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)

DEFAULT = object()

HTTP_IMPL = os.environ.get("APSIS_ASYNC_HTTP", "httpx")
if HTTP_IMPL not in {"httpx", "aiohttp"}:
    warnings.warn(f"unknown APSIS_ASYNC_HTTP={HTTP_IMPL}; using httpx")
    HTTP_IMPL = "httpx"

# -------------------------------------------------------------------------------


class NoAgentError(RuntimeError):
    """
    The agent is not running.
    """

    def __init__(self, host, user):
        super().__init__(f"no agent: {user} @ {host}")


class NoSuchProcessError(LookupError):
    """
    No process with the given proc_id.
    """

    def __init__(self, proc_id):
        super().__init__(f"no such process: {proc_id}")


class AgentStartError(RuntimeError):
    """
    The agent failed to start.
    """

    def __init__(self, returncode, err):
        super().__init__(f"agent start failed with returncode={returncode}: {err}")


class RequestError(RuntimeError):
    """
    The service returned 4xx.
    """

    def __init__(self, url, status, error, exception):
        msg = f"request error {status}: {url}: {error}"
        if exception is not None:
            msg += "\n" + exception
        super().__init__(msg)
        self.status = status


class NotFoundError(RuntimeError):
    """
    The service returned 404.
    """

    def __init__(self, url, status, error, exception):
        assert status == 404
        super().__init__(url, status, error, exception)


class InternalServiceError(RuntimeError):
    """
    The service returned 5xx.
    """

    def __init__(self, url, status, error, exception):
        msg = f"internal service error {status}: {url}: {error}"
        if exception is not None:
            msg += "\n" + exception
        super().__init__(msg)
        self.status = status


class RequestJsonError(RuntimeError):
    """
    The response did not contain expected well-formed JSON.
    """

    def __init__(self, url, status, error):
        super().__init__(f"request JSON error {status}: {url}: {error}")
        self.status = status


async def _load_data(rsp):
    match HTTP_IMPL:
        case "httpx":
            return await rsp.aread()
        case "aiohttp":
            return await rsp.read()


async def _get_jso(rsp):
    match HTTP_IMPL:
        case "httpx":
            data = await rsp.aread()
        case "aiohttp":
            data = await rsp.read()
    try:
        return ujson.loads(data)
    except ujson.JSONDecodeError as exc:
        log.error(f"JSON error in {rsp}: {exc}")
        raise RequestJsonError(rsp.url, rsp.status_code, exc)


# -------------------------------------------------------------------------------


@functools.lru_cache(1)
def _get_http_client(loop):
    """
    Returns an HTTP client for use with `loop`.
    """
    match HTTP_IMPL:
        case "httpx":
            client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                # FIXME: For now, we use no server verification when
                # establishing the TLS connection to the agent.  The agent uses
                # a generic SSL cert with no real host name, so host
                # verification cannot work; we'd have to generate a certificate
                # for each agent host.  For now at least we have connection
                # encryption.
                verify=False,
                # FIXME: Don't use keepalive in general, until we understand the
                # disconnect issues.
                limits=httpx.Limits(
                    max_keepalive_connections=0,
                ),
            )
            # Note: loop.on_close() was removed in Python 3.10
            # For now, we'll let the client be cleaned up by normal garbage collection
            # TODO: Consider implementing proper cleanup using asyncio context managers

        case "aiohttp":
            client = aiohttp.ClientSession(
                loop=loop,
                timeout=aiohttp.ClientTimeout(total=30),
            )
            # Note: loop.on_close() was removed in Python 3.10
            # For now, we'll let the client be cleaned up by normal garbage collection
            # TODO: Consider implementing proper cleanup using asyncio context managers

    return client


def get_http_client():
    """
    Returns an HTTP client for use with the current event loop.
    """
    return _get_http_client(asyncio.get_event_loop())


# -------------------------------------------------------------------------------

# FIXME-CONFIG: Configure how we become other users and log in to other hosts.

SSH_OPTIONS = dict(
    BatchMode="yes",
    CheckHostIP="no",  # FIXME-CONFIG
    ClearAllForwardings="yes",
    ForwardAgent="no",
    ForwardX11="no",
    StrictHostKeyChecking="no",  # FIXME-CONFIG
)


def _get_agent_argv(*, host=None, user=None, connect=None, state_dir=None):
    """
    Returns the argument vector to start the agent on `host` as `user`.
    """
    # FIXME-CONFIG: Configure how to start remote agents.
    argv = [sys.executable, "-m", "apsis.agent.main", "--log", "DEBUG"]

    if state_dir is not None:
        argv.extend(["--state-dir", str(state_dir)])

    try:
        pythonpath = os.environ.get("PYTHONPATH", "")
        argv = ["/usr/bin/env", "PYTHONPATH=" + pythonpath, *argv]
    except KeyError:
        pass

    if connect is True:
        argv.append("--connect")
    elif connect is False:
        argv.append("--no-connect")

    if host is not None:
        command = " ".join(argv)
        argv = [
            "/usr/bin/ssh",
            *itertools.chain.from_iterable(["-o", f"{k}={v}"] for k, v in SSH_OPTIONS.items()),
        ]
        if user is not None:
            argv.extend(["-l", user])
        argv.extend(
            [
                host,
                "exec",
                "/bin/bash",
                "-lc",
                shlex.quote(command),
            ]
        )

    elif user is not None:
        argv = ["/usr/bin/sudo", "-u", user, *argv]

    return argv


def _get_agent_name(user, host, port):
    user = "" if user is None else user + "@"
    port = "" if port is None else f":{port}"
    return f"agent {user}{host}{port}"


async def start_agent(*, host=None, user=None, connect=None, timeout=60, state_dir=None):
    """
    Starts the agent on `host` as `user`.

    :param connect:
      If true, connect to a running instance only.  If false, fail if an
      instance is already running.  If None, either start or connect.
    :param timeout:
      Timeout in sec.
    :return:
      The agent port and token.
    """
    name = _get_agent_name(user, host, None)
    argv = _get_agent_argv(host=host, user=user, connect=connect, state_dir=state_dir)
    log.debug(f"{name}: command: {' '.join(argv)}")
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        out, err = await apsis.lib.asyn.communicate(proc, timeout)
    except asyncio.CancelledError:
        raise AgentStartError(-1, "agent start canceled") from None
    except asyncio.TimeoutError as exc:
        raise AgentStartError(-1, f"timeout after {timeout} s\n" + exc.stderr.decode()) from None

    # Show the agent's log output.
    for line in err.decode().splitlines():
        log.debug(f"{name}: log | {line}")

    if proc.returncode == 0:
        # The agent is running.  Whether it just started or not, it prints
        # out its port and secret token.
        try:
            (data,) = out.decode().splitlines()
            port, token = data.split()
            log.debug(f"{name}: running on port {port}")
            return int(port), token
        except (TypeError, ValueError):
            raise AgentStartError(proc.returncode, out.decode())

    else:
        raise AgentStartError(proc.returncode, err.decode())


# -------------------------------------------------------------------------------


class Agent:
    # Delays between attempts to start the agent-- back off.  The number of
    # attempts is the number of delays.
    START_DELAYS = [0.5 * i**2 for i in range(6)]

    def __init__(self, host=None, user=None, *, connect=None, state_dir=DEFAULT):
        """
        :param host:
          Host to run on, or none for local.
        :param user:
          User to run as, or none for the current user.
        :param connect:
          If true, connect to a running instance only.  If false, fail if an
          instance is already running.
        """
        if state_dir is DEFAULT:
            try:
                state_dir = os.environ["APSIS_AGENT_DIR"]
            except KeyError:
                state_dir = get_test_state_dir() if in_test() else None

        self.__host = host
        self.__user = user
        self.__connect = connect
        self.__state_dir = state_dir

        self.__lock = asyncio.Lock()
        self.__conn = None

    def __str__(self):
        port = None if self.__conn is None else self.__conn[0]
        return _get_agent_name(self.__user, self.__host, port)

    async def connect(self):
        """
        Attempts to start or connect to the agent.

        :return:
          The agent port and token.
        """
        async with self.__lock:
            if self.__conn is None:
                log.debug(f"{self}: starting")
                port, token = await start_agent(
                    host=self.__host,
                    user=self.__user,
                    connect=self.__connect,
                    state_dir=self.__state_dir,
                )
                log.debug(f"{self}: started")

                self.__conn = port, token

            return self.__conn

    async def disconnect(self, port, token):
        log.debug(f"{self}: waiting to disconnect")
        async with self.__lock:
            if self.__conn == (port, token):
                log.debug(f"{self}: disconnecting")
                self.__conn = None
            else:
                log.debug(f"{self}: conn changed; not disconnecting")

    @contextlib.asynccontextmanager
    async def __request(
        self,
        method,
        endpoint,
        data=None,
        *,
        args={},
        restart=False,
        client=None,
    ):
        """
        Context manager for an HTTP request to the agent.  The value is the
        response object.

        :param method:
          HTTP method.
        :param endpoint:
          API endpoint path fragment.
        :param data:
          Payload data, to send as JSON.
        :param restart:
          If true, the client will attempt to start an agent automatically
          if the agent conenction fails.
        :raise RequestError:
          The request returned 4xx.
        :raise InternalServiceError:
          The request returned 5xx.
        """
        # Delays in sec before each attempt to connect.
        delays = self.START_DELAYS if restart else [0]

        if client is None:
            client = get_http_client()

        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)

            port, token = await self.connect()
            url_host = if_none(self.__host, "localhost")
            # FIXME: Use library.
            url = f"https://{url_host}:{port}/api/v1" + endpoint
            if len(args) > 0:
                url += "?" + "&".join(f"{k}={quote_plus(v)}" for k, v in args.items())

            try:
                match HTTP_IMPL:
                    case "httpx":
                        rsp = await client.request(
                            method,
                            url,
                            headers={
                                # The auth header, so the agent accepts us.
                                "X-Auth-Token": token,
                                "Content-Type": "application/json",
                            },
                            content=ujson.dumps(data),
                        )
                        status = rsp.status_code

                    case "aiohttp":
                        rsp = await client.request(
                            method,
                            url,
                            verify_ssl=False,
                            headers={
                                # The auth header, so the agent accepts us.
                                "X-Auth-Token": token,
                                "Content-Type": "application/json",
                            },
                            data=ujson.dumps(data),
                        )
                        status = rsp.status

                try:
                    log.debug(f"{self}: {method} {url} → {status}")

                    if status == 403:
                        # Forbidden.  A different agent is running on that port.  We
                        # should start our own.
                        log.debug(f"{self}: wrong agent")
                        await self.disconnect(port, token)
                        continue

                    elif 200 <= status < 300:
                        # Request submitted successfully.
                        yield rsp
                        return

                    elif 400 <= status < 600:
                        jso = await _get_jso(rsp)
                        raise (
                            NotFoundError
                            if status == 400
                            else RequestError
                            if status < 500
                            else InternalServiceError
                        )(
                            url,
                            status,
                            (jso or {}).get("error", None),
                            (jso or {}).get("exception", None),
                        )

                    else:
                        raise RuntimeError(f"unexpected status code: {status}")

                finally:
                    match HTTP_IMPL:
                        case "httpx":
                            await rsp.aclose()
                        case "aiohttp":
                            pass
                            # await rsp.release()

            except httpx.RequestError as exc:
                # We want to show the entire exception stack, unless the
                # underlying exception is garden-variety 'Connection refused'.
                while exc.__context__ is not None:
                    exc = exc.__context__
                is_ecr = getattr(exc, "errno", None) == errno.ECONNREFUSED
                err = "refused" if is_ecr else "error"

                log.info(
                    f"{self}: {method} {url} → connection {err}",
                    exc_info=not is_ecr,
                )
                # Try again with a new connection.
                await self.disconnect(port, token)
                continue

            except aiohttp.ClientError as exc:
                log.info(f"{self}: {method} {url} → {exc}", exc_info=True)
                # Try again with a new connection.
                await self.disconnect(port, token)
                continue

        else:
            # Ran out of connection attempts.
            raise NoAgentError(self.__host, self.__user)

    async def is_running(self):
        try:
            async with self.__request("GET", "/running"):
                pass
        except NoAgentError:
            return False
        else:
            return True

    async def get_processes(self):
        async with self.__request("GET", "/processes") as rsp:
            return (await _get_jso(rsp))["processes"]

    async def start_process(self, argv, cwd="/", env={}, stdin=None, restart=False):
        """
        Starts a process.

        :return:
          The new process, which will either be in state "run" or "err".
        """
        username = get_username() if self.__user is None else self.__user
        data = {
            "program": {
                "username": username,
                "argv": [str(a) for a in argv],
                "cwd": str(cwd),
                "env": env,
                "stdin": stdin,
            },
        }
        async with self.__request("POST", "/processes", data=data, restart=restart) as rsp:
            return (await _get_jso(rsp))["process"]

    async def get_process(self, proc_id, *, restart=False, client=None):
        """
        Returns information about a process.
        """
        path = f"/processes/{proc_id}"
        try:
            async with self.__request("GET", path, restart=restart, client=client) as rsp:
                return (await _get_jso(rsp))["process"]

        except NotFoundError:
            raise NoSuchProcessError(proc_id)

    async def get_process_output(self, proc_id, *, compression=None, client=None):
        """
        Returns process output.

        :return:
          The output, its uncompressed length, and the actual compression
          format.
        """
        path = f"/processes/{proc_id}/output"
        args = {} if compression is None else {"compression": compression}
        try:
            async with self.__request("GET", path, args=args, client=client) as rsp:
                length = int(rsp.headers["X-Raw-Length"])
                cmpr = rsp.headers.get("X-Compression", None)
                assert cmpr == compression
                return await _load_data(rsp), length, compression

        except NotFoundError:
            raise NoSuchProcessError(proc_id)

    async def del_process(self, proc_id, *, client=None):
        """
        Deltes a process.  The process may not be running.
        """
        path = f"/processes/{proc_id}"
        try:
            async with self.__request("DELETE", path, client=client) as rsp:
                return (await _get_jso(rsp))["stop"]

        except NotFoundError:
            raise NoSuchProcessError(proc_id)

    async def signal(self, proc_id, signal):
        """
        Sends a signal to a process.
        """
        path = f"/processes/{proc_id}/signal/{signal}"
        try:
            async with self.__request("PUT", path) as rsp:
                return (await _get_jso(rsp))["delivered"]

        except NotFoundError:
            raise NoSuchProcessError(proc_id)

    async def stop(self):
        """
        Shuts down an agent, if there are no remaining processes.
        """
        async with self.__request("POST", "/stop") as rsp:
            return (await _get_jso(rsp))["stop"]


@functools.cache
def get_test_state_dir():
    """
    Use a unique private agent state dir for automated tests.
    """
    state_dir = tempfile.mkdtemp(prefix="apsis-agent-test-")
    log.info(f"test agent state dir: {state_dir}")
    return state_dir
