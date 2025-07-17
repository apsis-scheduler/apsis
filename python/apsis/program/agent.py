import asyncio
import functools
import httpx
import logging
import ora
import os
from signal import Signals
import socket

from .base import (
    Program,
    ProgramRunning,
    ProgramSuccess,
    ProgramFailure,
    ProgramError,
    program_outputs,
    Timeout,
    RunningProgram,
)
from .process import Stop, BoundStop
from apsis.agent.client import Agent, NoSuchProcessError
from apsis.host_group import expand_host
from apsis.lib import memo
from apsis.lib.cmpr import compress_async
from apsis.lib.json import check_schema, ifkey
from apsis.lib.py import or_none, nstr
from apsis.lib.sys import get_username
from apsis.runs import template_expand, join_args

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


@functools.cache
def _get_agent(host, user):
    return Agent(host=host, user=user)


class AgentProgram(Program):
    def __init__(
        self,
        argv,
        *,
        host=None,
        user=None,
        timeout=None,
        stop=Stop(),
    ):
        self.argv = tuple(str(a) for a in argv)
        self.host = nstr(host)
        self.user = nstr(user)
        self.timeout = timeout
        self.stop = stop

    def __str__(self):
        return join_args(self.argv)

    def to_jso(self):
        jso = (
            {
                **super().to_jso(),
                "argv": list(self.argv),
            }
            | ifkey("host", self.host, None)
            | ifkey("user", self.user, None)
            | ifkey("stop", self.stop.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            host = pop("host", nstr, None)
            user = pop("user", nstr, None)
            timeout = pop("timeout", Timeout.from_jso, None)
            stop = pop("stop", Stop.from_jso, default=Stop())
        return cls(argv, host=host, user=user, timeout=timeout, stop=stop)

    def bind(self, args):
        argv = tuple(template_expand(a, args) for a in self.argv)
        host = or_none(template_expand)(self.host, args)
        user = or_none(template_expand)(self.user, args)
        timeout = None if self.timeout is None else self.timeout.bind(args)
        stop = self.stop.bind(args)
        return BoundAgentProgram(
            argv,
            host=host,
            user=user,
            timeout=timeout,
            stop=stop,
        )


class AgentShellProgram(Program):
    def __init__(
        self,
        command,
        *,
        host=None,
        user=None,
        timeout=None,
        stop=Stop(),
    ):
        self.command = str(command)
        self.host = nstr(host)
        self.user = nstr(user)
        self.timeout = timeout
        self.stop = stop

    def __str__(self):
        return self.command

    def to_jso(self):
        jso = (
            {
                **super().to_jso(),
                "command": self.command,
            }
            | ifkey("host", self.host, None)
            | ifkey("user", self.user, None)
            | ifkey("stop", self.stop.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command = pop("command", str)
            host = pop("host", nstr, None)
            user = pop("user", nstr, None)
            timeout = pop("timeout", Timeout.from_jso, None)
            stop = pop("stop", Stop.from_jso, Stop())
        return cls(command, host=host, user=user, timeout=timeout, stop=stop)

    def bind(self, args):
        command = template_expand(self.command, args)
        host = or_none(template_expand)(self.host, args)
        user = or_none(template_expand)(self.user, args)
        timeout = None if self.timeout is None else self.timeout.bind(args)
        stop = self.stop.bind(args)
        argv = ["/bin/bash", "-c", command]
        return BoundAgentProgram(
            argv,
            host=host,
            user=user,
            timeout=timeout,
            stop=stop,
        )


# -------------------------------------------------------------------------------


class BoundAgentProgram(Program):
    def __init__(
        self,
        argv,
        *,
        host=None,
        user=None,
        timeout=None,
        stop=BoundStop(),
    ):
        self.argv = tuple(str(a) for a in argv)
        self.host = nstr(host)
        self.user = nstr(user)
        self.timeout = timeout
        self.stop = stop

    def __str__(self):
        return join_args(self.argv)

    def to_jso(self):
        jso = {
            **super().to_jso(),
            "argv": list(self.argv),
            "host": self.host,
            "user": self.user,
        } | ifkey("stop", self.stop.to_jso(), {})
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            host = pop("host", nstr, None)
            user = pop("user", nstr, None)
            timeout = pop("timeout", Timeout.from_jso, None)
            stop = pop("stop", BoundStop.from_jso, BoundStop())
        return cls(argv, host=host, user=user, timeout=timeout, stop=stop)

    def run(self, run_id, cfg):
        return RunningAgentProgram(run_id, self, cfg)

    def connect(self, run_id, run_state, cfg):
        return RunningAgentProgram(run_id, self, cfg, run_state)


# -------------------------------------------------------------------------------


class RunningAgentProgram(RunningProgram):
    def __init__(self, run_id, program, cfg, run_state=None):
        super().__init__(run_id)
        self.program = program
        self.cfg = cfg
        self.run_state = run_state

        self.stopping = False

    def __get_agent(self, host):
        host = None if host is None else socket.getfqdn(host)
        return _get_agent(host, self.program.user)

    @memo.property
    async def updates(self):
        if self.run_state is None:
            # Start the proc.
            host = expand_host(self.program.host, self.cfg)
            argv = self.program.argv

            loc = "" if host is None else " on " + host
            cmd = join_args(argv)
            log.debug(f"starting program{loc}: {cmd}")

            env = {
                "inherit": True,
                "vars": {
                    "APSIS_RUN_ID": self.run_id,
                },
            }

            meta = {
                "apsis_hostname": socket.gethostname(),
                "apsis_username": get_username(),
            }

            agent = self.__get_agent(host)
            proc = await agent.start_process(argv, env=env, restart=True)
            state = proc["state"]
            if state == "run":
                log.debug(f"program running: {self.run_id} as {proc['proc_id']}")

                start = ora.now()
                proc_id = proc["proc_id"]
                self.run_state = {
                    "host": host,
                    "proc_id": proc_id,
                    "pid": proc["pid"],
                    "start": str(start),
                }
                meta.update(self.run_state)
                # FIXME: Propagate times from agent.
                yield ProgramRunning(self.run_state, meta=meta)

            elif state == "err":
                message = proc.get("exception", "program error")
                log.info(f"program error: {self.run_id}: {message}")
                # Clean up the process from the agent.
                await agent.del_process(proc["proc_id"])

                yield ProgramError(message)
                return

            else:
                assert False, f"unknown state: {state}"

        else:
            # Poll an existing proc.
            proc_id = self.run_state["proc_id"]
            host = self.run_state["host"]
            start = ora.Time(self.run_state["start"])
            agent = self.__get_agent(host)

        # ------------

        explanation = ""

        # FIXME: This is so embarrassing.
        POLL_INTERVAL = 1

        TIMEOUT = 60
        client_ctx = httpx.AsyncClient(
            verify=False,
            timeout=httpx.Timeout(TIMEOUT),
            limits=httpx.Limits(
                max_keepalive_connections=1,
                keepalive_expiry=TIMEOUT,
            ),
        )

        async with client_ctx as client:
            while True:
                if self.program.timeout is not None:
                    elapsed = ora.now() - start
                    if self.program.timeout.duration < elapsed:
                        msg = f"timeout after {elapsed:.0f} s"
                        log.info(f"{self.run_id}: {msg}")
                        explanation = f" ({msg})"
                        # FIXME: Note timeout in run log.
                        await self.signal(self.program.timeout.signal)
                        await asyncio.sleep(POLL_INTERVAL)

                log.debug(f"polling proc: {self.run_id}: {proc_id} @ {host}")
                try:
                    proc = await agent.get_process(proc_id, restart=True, client=client)
                except NoSuchProcessError:
                    # Agent doesn't know about this process anymore.
                    raise ProgramError(f"program lost: {self.run_id}")
                if proc["state"] == "run":
                    await asyncio.sleep(POLL_INTERVAL)
                else:
                    break

            status = proc["status"]
            output, length, compression = await agent.get_process_output(proc_id, client=client)

            if compression is None and len(output) > 16384:
                # Compress the output.
                try:
                    output, compression = await compress_async(output, "br"), "br"
                except RuntimeError as exc:
                    log.error(f"{exc}; not compressing")

            outputs = program_outputs(output, length=length, compression=compression)
            log.debug(f"got output: {length} bytes, {compression or 'uncompressed'}")

            try:
                if status == 0:
                    yield ProgramSuccess(meta=proc, outputs=outputs)

                elif (
                    self.stopping
                    and os.WIFSIGNALED(status)
                    and Signals(os.WTERMSIG(status)) == self.program.stop.signal
                ):
                    # Program stopped as expected.
                    log.info("EXPECTED SIGTERM WHEN STOPPING")
                    yield ProgramSuccess(meta=proc, outputs=outputs)

                else:
                    message = f"program failed: status {status}{explanation}"
                    yield ProgramFailure(message, meta=proc, outputs=outputs)

            finally:
                # Clean up the process from the agent.
                await agent.del_process(proc_id, client=client)

    async def signal(self, signal):
        """
        :type signal:
          Signal name or number.
        """
        if self.run_state is None:
            raise RuntimeError("can't signal; not running yet")

        log.info(f"sending signal: {self.run_id}: {signal}")
        proc_id = self.run_state["proc_id"]
        agent = self.__get_agent(self.run_state["host"])
        await agent.signal(proc_id, signal)

    async def stop(self):
        stop = self.program.stop
        self.stopping = True

        # Send the stop signal.
        await self.signal(stop.signal)

        if stop.grace_period is not None:
            try:
                # Wait for the grace period to expire.
                await asyncio.sleep(stop.grace_period)
            except asyncio.CancelledError:
                # That's what we were hoping for.
                pass
            else:
                # Send a kill signal.
                try:
                    self.stop_signals.append(Signals.SIGKILL)
                    await self.signal(Signals.SIGKILL)
                except ValueError:
                    # Proc is gone; that's OK.
                    pass
