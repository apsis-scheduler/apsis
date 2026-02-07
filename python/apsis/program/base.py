import logging
from dataclasses import dataclass
from signal import Signals

from apsis.exc import SchemaError
from apsis.lib import memo
from apsis.lib.api import decompress
from apsis.lib.json import TypedJso, check_schema
from apsis.lib.parse import parse_duration
from apsis.lib.py import format_repr, get_cfg
from apsis.lib.sys import to_signal
from apsis.runs import template_expand

log = logging.getLogger(__name__)

TIMEOUT_SIGNAL = Signals.SIGTERM.name

# -------------------------------------------------------------------------------


class OutputMetadata:
    def __init__(self, name: str, length: int, *, content_type="application/octet-stream"):
        """
        :param name:
          User-visible output name.
        :param length:
          Length in bytes.
        :param content_type:
          MIME type of output.
        """
        self.name = str(name)
        self.length = int(length)
        self.content_type = str(content_type)

    def to_jso(self):
        return {
            "name": self.name,
            "length": self.length,
            "content_type": self.content_type,
        }


class Output:
    def __init__(self, metadata: OutputMetadata, data: bytes, compression=None):
        """
        :param metadata:
          Information about the data.
        :param data:
          The data bytes; these may be compressed.
        :param compression:
          The compresison type, or `None` for uncompressed.
        """
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        self.metadata = metadata
        self.data = data
        self.compression = compression

    def get_uncompressed_data(self) -> bytes:
        """
        Returns the output data, decompressing if necessary.
        """
        return decompress(self.data, self.compression)


def program_outputs(output: bytes, *, length=None, compression=None):
    if length is None:
        length = len(output)
    return {
        "output": Output(
            OutputMetadata("combined stdout & stderr", length=length),
            output,
            compression=compression,
        ),
    }


# -------------------------------------------------------------------------------


class ProgramRunning:
    def __init__(self, run_state, *, meta={}, times={}):
        self.run_state = run_state
        self.meta = meta
        self.times = times

    def __repr__(self):
        return format_repr(self)


class ProgramUpdate:
    def __init__(self, *, meta=None, outputs=None):
        self.meta = meta
        self.outputs = outputs

    def __repr__(self):
        return format_repr(self)


# FIXME: Not an exception.
class ProgramError(RuntimeError):
    def __init__(self, message, *, meta={}, times={}, outputs={}):
        super().__init__(message)
        self.message = message
        self.meta = meta
        self.times = times
        self.outputs = outputs

    def __repr__(self):
        return format_repr(self)


class ProgramSuccess:
    def __init__(self, *, meta={}, times={}, outputs={}):
        self.meta = meta
        self.times = times
        self.outputs = outputs

    def __repr__(self):
        return format_repr(self)


# FIXME: Not an exception.
class ProgramFailure(RuntimeError):
    def __init__(self, message, *, meta={}, times={}, outputs={}):
        self.message = message
        self.meta = meta
        self.times = times
        self.outputs = outputs

    def __repr__(self):
        return format_repr(self)


# -------------------------------------------------------------------------------


@dataclass
class Timeout:
    duration: float
    signal: str

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            duration = pop("duration")
            signal = pop("signal", str, default="SIGTERM")
        return cls(duration=duration, signal=signal)

    def to_jso(self):
        return {
            "duration": self.duration,
            "signal": self.signal,
        }

    def bind(self, args):
        duration = parse_duration(template_expand(self.duration, args))
        signal = to_signal(template_expand(self.signal, args)).name
        return type(self)(duration=duration, signal=signal)


def get_global_runtime_timeout(cfg):
    timeout_duration = get_cfg(cfg, "program.timeout.duration", None)
    if timeout_duration is None:
        return None
    timeout_signal = get_cfg(cfg, "program.timeout.signal", TIMEOUT_SIGNAL)
    return Timeout(duration=timeout_duration, signal=timeout_signal)


# -------------------------------------------------------------------------------

# FIXME: Apsis should take run_state from RunningProgram, and serialize it on
# each transition. (??)


class RunningProgram:
    """
    A running instance of a program.

    An instance (of a subclass) represents a program while it is running, i.e.
    for a run in the starting, running, and stopping states.

    The async iterable `updates` drives the program through the event loop.
    Apsis will await this iterator to completion.
    """

    def __init__(self, run_id):
        self.run_id = run_id

    @property
    def updates(self):
        """
        A singleton async iterable of program updates.

        Apsis async-iterates this to exhaustion, to drive the program through
        the event loop.  Exhaustion indicates the program is done.
        """

    async def stop(self):
        raise NotImplementedError("not implemented: stop()")

    async def signal(self, signal):
        raise NotImplementedError("not implemented: signal()")


class LegacyRunningProgram(RunningProgram):
    def __init__(self, run_id, program, cfg, run_state=None):
        super().__init__(run_id)
        self.program = program
        self.cfg = cfg
        self.run_state = run_state

    @memo.property
    async def updates(self):
        if self.run_state is None:
            # Starting.
            try:
                running, done = await self.program.start(self.run_id, self.cfg)
            except ProgramError as err:
                yield err
            else:
                assert isinstance(running, ProgramRunning)
                yield running

        else:
            done = self.program.reconnect(self.run_id, self.run_state)

        # Running.
        try:
            success = await done
        except (ProgramError, ProgramFailure) as err:
            yield err
        else:
            assert isinstance(success, ProgramSuccess), f"wrong result msg: {success!r}"
            yield success


# -------------------------------------------------------------------------------


class Program(TypedJso):
    """
    Program base class.
    """

    TYPE_NAMES = TypedJso.TypeNames()

    # Program types that can be safely rolled back. See UnavailableProgram.
    ROLLBACKABLE_PROGRAM_TYPES = frozenset(
        ("apsis.program.procstar.aws.ecs_agent.BoundProcstarECSProgram",)
    )

    def bind(self, args):
        """
        Returns a new program with parameters bound to `args`.
        """

    # FIXME: Find a better way to get run_id into logging without passing it in.

    async def start(self, run_id, cfg):
        """
        Starts the run.

        :deprecated:
          Implement `run()` instead.
        """

    def reconnect(self, run_id, run_state):
        """
        Reconnects to an already running run.

        :deprecated:
          Implement `connect()` instead.
        """

    async def signal(self, run_id, run_state, signal):
        """
        Sends a signal to the running program.

        :param run_id:
          The run ID; used for logging only.
        :param signal:
          Signal name or number.
        """
        raise NotImplementedError("program signal not implemented")

    @classmethod
    def from_jso(cls, jso):
        try:
            type_name = jso.pop("type")
        except KeyError:
            raise SchemaError("missing type")

        try:
            type_cls = cls.TYPE_NAMES.get_type(type_name)
        except LookupError:
            if type_name in cls.ROLLBACKABLE_PROGRAM_TYPES:
                # Known-but-unavailable program type - return placeholder.
                # This allows Apsis to start even if there are runs in the database
                # with program types that are not available (e.g., after rollback).
                log.warning(f"unavailable program type: {type_name}")
                return UnavailableProgram(type_name, jso)
            else:
                raise SchemaError(f"unknown program type: {type_name}")

        if not issubclass(type_cls, cls):
            raise SchemaError(f"type {type_cls} not a {cls}")

        return type_cls.from_jso(jso)

    def run(self, run_id, cfg) -> RunningProgram:
        """
        Runs the program.

        The default implementation is a facade for `start()`, for legacy
        compatibility.  Subclasses should override this method.

        :param run_id:
          Used for logging only.
        :return:
          `RunningProgram` instance.
        """
        return LegacyRunningProgram(run_id, self, cfg)

    # FIXME: Remove `run_id` from API; the running program carries it.
    def connect(self, run_id, run_state, cfg) -> RunningProgram:
        """
        Connects to the running program specified by `run_state`.

        The default implementation is a facade for `reconnect()`, for backward
        compatibility.  Subclasses should override this method.

        :param run_id:
          Used for logging only.
        :return:
          Async iterator that yields `Program*` objects.
        """
        return LegacyRunningProgram(run_id, self, cfg, run_state)


# -------------------------------------------------------------------------------


class _InternalProgram(Program):
    """
    Program type for internal use.

    Not API.  Do not use in extension code.
    """

    def bind(self, args):
        pass

    def start(self, run_id, apsis):
        pass

    def reconnect(self, run_id, run_state):
        pass

    async def signal(self, run_id, signum: str):
        pass

    async def stop(self):
        pass


class _UnavailableRunningProgram(RunningProgram):
    """Running program that immediately yields an error for unavailable program types."""

    def __init__(self, run_id, type_name):
        super().__init__(run_id)
        self.type_name = type_name

    @memo.property
    async def updates(self):
        yield ProgramError(f"cannot run unavailable program type: {self.type_name}")


class UnavailableProgram(Program):
    """
    Placeholder for known program types that cannot be loaded.

    This is a safety net for rollback scenarios. When a program type is listed
    in ROLLBACKABLE_PROGRAM_TYPES but cannot be loaded (e.g., after rolling
    back a feature), this placeholder is used instead of crashing Apsis.

    IMPORTANT: This only applies to types explicitly listed in
    ROLLBACKABLE_PROGRAM_TYPES. Unknown/unexpected types will still raise
    SchemaError - we never silently ignore truly unknown program types.

    To add rollback support for a new program type:
    1. Add the fully-qualified type name to ROLLBACKABLE_PROGRAM_TYPES
    2. Deploy this change BEFORE deploying the feature that introduces the type
    3. If the feature is later rolled back, existing runs will be loaded as
       UnavailableProgram instead of crashing

    Behavior:
    - Runs are visible in the UI with their original metadata
    - Attempting to run/restart transitions the run to error state
    - Original program data is preserved in the database (via to_jso)
    - Once the feature is re-deployed, runs will work normally again
    """

    def __init__(self, type_name: str, jso: dict):
        self.type_name = type_name
        self._jso = jso

    def __str__(self):
        return f"UnavailableProgram({self.type_name})"

    def bind(self, args):
        raise ProgramError(f"cannot bind unavailable program type: {self.type_name}")

    def run(self, run_id, cfg):
        return _UnavailableRunningProgram(run_id, self.type_name)

    def connect(self, run_id, run_state, cfg):
        return _UnavailableRunningProgram(run_id, self.type_name)

    def to_jso(self):
        # Preserve original data so it can be serialized back to the database
        return {"type": self.type_name, **self._jso}
