import asyncio
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import pwd
from signal import Signals
import socket

from .base import (
    Program,
    RunningProgram,
    ProgramRunning,
    ProgramSuccess,
    ProgramFailure,
    ProgramError,
    program_outputs,
)
from apsis.lib import memo
from apsis.lib.json import check_schema, ifkey
from apsis.lib.parse import nparse_duration
from apsis.lib.py import or_none
from apsis.lib.sys import get_username, to_signal
from apsis.runs import template_expand, join_args

log = logging.getLogger(__name__)

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%kZ"

ntemplate_expand = or_none(template_expand)

# -------------------------------------------------------------------------------


@dataclass
class Stop:
    """
    Specification for how to stop a running process.

    1. Send `signal` to the process.
    2. Wait up to `grace_period` sec.
    3. If the process has not terminated, send SIGKILL.
    """

    signal: str = "SIGTERM"
    grace_period: str = "60"

    def to_jso(self):
        cls = type(self)
        return ifkey("signal", self.signal, cls.signal) | ifkey(
            "grace_period", self.grace_period, cls.grace_period
        )

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso or {}) as pop:
            signal = pop("signal", str, default=cls.signal)
            grace_period = pop("grace_period", default=cls.grace_period)
        return cls(signal, grace_period)

    def bind(self, args):
        return BoundStop(
            to_signal(template_expand(self.signal, args)),
            nparse_duration(ntemplate_expand(self.grace_period, args)),
        )


@dataclass
class BoundStop:
    signal: Signals = Signals.SIGTERM
    grace_period: float = 60

    def to_jso(self):
        cls = type(self)
        return ifkey("signal", self.signal, cls.signal) | ifkey(
            "grace_period", self.grace_period, cls.grace_period
        )

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso or {}) as pop:
            signal = pop("signal", to_signal, cls.signal)
            grace_period = pop("grace_period", int, cls.grace_period)
        return cls(signal, grace_period)


# -------------------------------------------------------------------------------


class ProcessProgram(Program):
    def __init__(self, argv, *, stop=Stop()):
        self.argv = tuple(str(a) for a in argv)
        self.stop = stop

    def __str__(self):
        return join_args(self.argv)

    def bind(self, args):
        argv = tuple(template_expand(a, args) for a in self.argv)
        stop = self.stop.bind(args)
        return BoundProcessProgram(argv, stop=stop)

    def to_jso(self):
        return {
            **super().to_jso(),
            "argv": list(self.argv),
        } | ifkey("stop", self.stop.to_jso(), {})

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            stop = pop("stop", Stop.from_jso, Stop())
        return cls(argv, stop=stop)


# -------------------------------------------------------------------------------


class ShellCommandProgram(Program):
    def __init__(self, command, *, stop=Stop()):
        self.command = str(command)
        self.stop = stop

    def bind(self, args):
        command = template_expand(self.command, args)
        argv = ["/bin/bash", "-c", command]
        stop = self.stop.bind(args)
        return BoundProcessProgram(argv, stop=stop)

    def __str__(self):
        return self.command

    def to_jso(self):
        return {
            **super().to_jso(),
            "command": self.command,
        } | ifkey("stop", self.stop.to_jso(), {})

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command = pop("command", str)
            stop = pop("stop", Stop.from_jso, default=Stop())
        return cls(command, stop=stop)


# -------------------------------------------------------------------------------


class BoundProcessProgram(Program):
    def __init__(self, argv, *, stop=BoundStop()):
        self.argv = tuple(str(a) for a in argv)
        self.stop = stop

    def __str__(self):
        return join_args(self.argv)

    def to_jso(self):
        return {
            **super().to_jso(),
            "argv": self.argv,
        } | ifkey("stop", self.stop.to_jso(), {})

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            stop = pop("stop", BoundStop.from_jso, BoundStop())
        return cls(argv, stop=stop)

    def run(self, run_id, cfg) -> RunningProgram:
        return RunningProcessProgram(self, run_id)


# -------------------------------------------------------------------------------


class RunningProcessProgram(RunningProgram):
    # FIXME: Configure?
    grace_period = 60

    def __init__(self, program, run_id):
        super().__init__(run_id)
        self.program = program
        self.process = None
        self.stopping = False

    @memo.property
    async def updates(self):
        argv = self.program.argv
        log.info(f"starting program: {join_args(argv)}")

        meta = {
            "hostname": socket.gethostname(),
            "username": get_username(),
            "euid": pwd.getpwuid(os.geteuid()).pw_name,
        }

        try:
            with open("/dev/null") as stdin:
                self.proc = await asyncio.create_subprocess_exec(
                    *argv,
                    executable=Path(argv[0]),
                    stdin=stdin,
                    # Merge stderr with stdin.
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )

        except OSError as exc:
            # Error starting.
            raise ProgramError(str(exc), meta=meta)

        # Started successfully.
        yield ProgramRunning({"pid": self.proc.pid}, meta=meta)

        stdout, stderr = await self.proc.communicate()
        return_code = self.proc.returncode
        self.proc = None
        log.info(f"complete with return code {return_code}")
        assert stderr is None
        assert return_code is not None

        meta = {
            "return_code": return_code,
        }
        outputs = program_outputs(stdout)

        if return_code == 0:
            yield ProgramSuccess(meta=meta, outputs=outputs)

        elif (
            self.stopping
            and self.returncode < 0
            and Signals(self.returncode) == self.program.stop.signal
        ):
            # Program stopped as expected.
            yield ProgramFailure(meta=meta, outputs=outputs)

        else:
            message = f"program failed: return code {return_code}"
            yield ProgramFailure(message, meta=meta, outputs=outputs)

    def connect(self, run_id, run_state, cfg) -> RunningProgram:
        pid = run_state["pid"]
        raise NotImplementedError(f"can't reconnect to running proc {pid}")

    async def signal(self, signal):
        assert self.process is not None
        self.process.send_signal(signal)

    async def stop(self):
        self.stopping = True

        stop = self.program.stop
        self.process.send_signal(stop.signal)
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
                    await self.signal(Signals.SIGKILL)
                except ValueError:
                    # Proc is gone; that's OK.
                    pass
