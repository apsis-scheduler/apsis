from dataclasses import dataclass
import logging
import ora
import procstar.spec
from procstar.agent.exc import (
    NoOpenConnectionInGroup,
)
from procstar.agent.proc import FdData, Interval, Process
import uuid

from apsis.lib.json import check_schema, ifkey
from apsis.lib.parse import nparse_duration
from apsis.lib.py import or_none, nstr, get_cfg
from apsis.procstar import get_agent_server
from apsis.program import base
from apsis.program.base import (
    ProgramError,
    Timeout,
    get_global_runtime_timeout,
)
from apsis.program.procstar.base import BaseRunningProcstarProgram
from apsis.program.process import Stop, BoundStop
from apsis.runs import join_args, template_expand

log = logging.getLogger(__name__)

ntemplate_expand = or_none(template_expand)

# -------------------------------------------------------------------------------

SUDO_ARGV_DEFAULT = ["/usr/bin/sudo", "--preserve-env", "--set-home"]

FD_DATA_TIMEOUT = 60

if_not_none = lambda k, v: {} if v is None else {k: v}


def _sudo_wrap(cfg, argv, sudo_user):
    if sudo_user is None:
        return argv
    else:
        sudo_argv = get_cfg(cfg, "sudo.argv", SUDO_ARGV_DEFAULT)
        return (
            [str(a) for a in sudo_argv]
            + ["--non-interactive", "--user", str(sudo_user), "--"]
            + list(argv)
        )


def _make_systemd(cfg, *, resources) -> procstar.spec.Proc.SystemdProperties:
    ngb_to_bytes = or_none(lambda gb: int(gb * 10**9))
    memory_max = ngb_to_bytes(
        get_cfg(cfg, "resource_defaults.mem_max_gb", None)
        if resources.mem_max_gb is None
        else resources.mem_max_gb
    )
    SystemdProperties = procstar.spec.Proc.SystemdProperties
    return SystemdProperties(
        slice=SystemdProperties.Slice(
            tasks_accounting=True,
            memory_accounting=True,
            memory_max=memory_max,
            memory_swap_max=0,
        )
    )


def _make_metadata(proc_id, res: dict):
    """
    Extracts run metadata from a proc result message.

    - `status`: Process raw status (see `man 2 wait`) and decoded exit code
      and signal info.

    - `times`: Process timing from the Procstar agent on the host running the
      program.  `elapsed` is computed from a monotonic clock.

    - `rusage`: Process resource usage.  See `man 2 getrusage` for details.

    - `proc_stat`: Process information collected from `/proc/<pid>/stat`.  See
      the `proc(5)` man page for details.

    - `proc_statm`: Process memory use collected from `/proc/<pid>/statm`.  See
      the `proc(5)` man page for details.

    - `proc_id`: The Procstar process ID.

    - `conn`: Connection info the the Procstar agent.

    - `procstar_proc`: Process information about the Procstar agent itself.

    """
    meta = {
        "errors": res.errors,
    } | {
        k: dict(v.__dict__)
        for k in (
            "status",
            "times",
            "rusage",
            "proc_stat",
            "proc_statm",
            "cgroup_accounting",
        )
        if (v := getattr(res, k, None)) is not None
    }

    meta["procstar_proc_id"] = proc_id
    try:
        meta["procstar_conn"] = dict(res.procstar.conn.__dict__)
        meta["procstar_agent"] = dict(res.procstar.proc.__dict__)
    except AttributeError:
        pass

    return meta


def _combine_fd_data(old, new):
    if old is None:
        return new

    assert new.fd == old.fd
    assert new.encoding == old.encoding
    assert new.interval.start <= new.interval.stop
    assert new.interval.stop - new.interval.start == len(new.data)

    # FIXME: Should be Interval.__str__().
    fi = lambda i: f"[{i.start}, {i.stop})"

    # Check for a gap in the data.
    if old.interval.stop < new.interval.start:
        raise RuntimeError(f"fd data gap: {fi(old.interval)} + {fi(new.interval)}")

    elif old.interval.stop == new.interval.start:
        return FdData(
            fd=old.fd,
            encoding=old.encoding,
            interval=Interval(old.interval.start, new.interval.stop),
            data=old.data + new.data,
        )

    else:
        # Partial overlap of data.
        log.warning(f"fd data overlap: {fi(old.interval)} + {fi(new.interval)}")
        length = new.interval.stop - old.interval.stop
        if length > 0:
            # Partial overlap.  Patch intervals together.
            interval = Interval(old.interval.start, new.interval.stop)
            data = old.data + new.data[-length:]
            assert interval.stop - interval.start == len(data)
            return FdData(
                fd=old.fd,
                encoding=old.encoding,
                interval=interval,
                data=data,
            )
        else:
            # Complete overlap.
            return old


async def _make_outputs(fd_data):
    """
    Constructs program outputs from combined output fd data.
    """
    if fd_data is None:
        return {}

    assert fd_data.fd == "stdout"
    assert fd_data.interval.start == 0
    assert fd_data.encoding is None

    output = fd_data.data
    length = fd_data.interval.stop
    return base.program_outputs(output, length=length, compression=None)


# -------------------------------------------------------------------------------


@dataclass
class Resources:
    """
    Specification for resource limits for a running job.
    """

    mem_max_gb: int | float | None = None

    def to_jso(self):
        return if_not_none("mem_max_gb", self.mem_max_gb)

    @classmethod
    def from_jso(cls, jso: dict):
        with check_schema(jso) as pop:
            mem_max_gb = pop("mem_max_gb", type=float, default=None)
            if mem_max_gb is not None:
                assert mem_max_gb > 0, "mem_max_gb must be postive"
        return cls(mem_max_gb=mem_max_gb)

    def bind(self, args):
        return BoundResources(mem_max_gb=self.mem_max_gb)


@dataclass
class BoundResources:
    mem_max_gb: int | float | None = None

    def to_jso(self):
        return if_not_none("mem_max_gb", self.mem_max_gb)

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso or {}) as pop:
            mem_max_gb = pop("mem_max_gb", default=None)
        return cls(mem_max_gb=mem_max_gb)


# -------------------------------------------------------------------------------


class _ProcstarProgram(base.Program):
    """
    Base class for (unbound) Procstar program types.
    """

    def __init__(
        self,
        *,
        group_id=procstar.proto.DEFAULT_GROUP,
        sudo_user=None,
        stop=Stop(),
        timeout=None,
        resources=Resources(),
    ):
        super().__init__()
        self.group_id = str(group_id)
        self.sudo_user = None if sudo_user is None else str(sudo_user)
        self.stop = stop
        self.timeout = timeout
        self.resources = resources

    def _bind(self, argv, args):
        return BoundProcstarProgram(
            argv,
            group_id=ntemplate_expand(self.group_id, args),
            sudo_user=ntemplate_expand(self.sudo_user, args),
            stop=self.stop.bind(args),
            timeout=None if self.timeout is None else self.timeout.bind(args),
            resources=self.resources.bind(args),
        )

    def to_jso(self):
        jso = (
            super().to_jso()
            | {
                "group_id": self.group_id,
            }
            | if_not_none("sudo_user", self.sudo_user)
            | ifkey("stop", self.stop.to_jso(), {})
            | ifkey("resources", self.resources.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @staticmethod
    def _from_jso(pop):
        return dict(
            group_id=pop("group_id", default=procstar.proto.DEFAULT_GROUP),
            sudo_user=pop("sudo_user", default=None),
            stop=pop("stop", Stop.from_jso, Stop()),
            timeout=pop("timeout", Timeout.from_jso, None),
            resources=pop("resources", Resources.from_jso, Resources()),
        )


# -------------------------------------------------------------------------------


class ProcstarProgram(_ProcstarProgram):
    def __init__(self, argv, **kw_args):
        super().__init__(**kw_args)
        self.argv = [str(a) for a in argv]

    def __str__(self):
        return join_args(self.argv)

    def bind(self, args):
        argv = tuple(template_expand(a, args) for a in self.argv)
        return super()._bind(argv, args)

    def to_jso(self):
        return super().to_jso() | {"argv": self.argv}

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            kw_args = cls._from_jso(pop)
        return cls(argv, **kw_args)


# -------------------------------------------------------------------------------


class ProcstarShellProgram(_ProcstarProgram):
    SHELL = "/usr/bin/bash"

    def __init__(self, command, **kw_args):
        super().__init__(**kw_args)
        self.command = str(command)

    def __str__(self):
        return self.command

    def bind(self, args):
        argv = [self.SHELL, "-c", template_expand(self.command, args)]
        return super()._bind(argv, args)

    def to_jso(self):
        return super().to_jso() | {"command": self.command}

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command = pop("command")
            kw_args = cls._from_jso(pop)
        return cls(command, **kw_args)


# -------------------------------------------------------------------------------


class BoundProcstarProgram(base.Program):
    def __init__(
        self,
        argv,
        *,
        group_id,
        sudo_user=None,
        stop=BoundStop(),
        timeout=None,
        resources=BoundResources(),
    ):
        self.argv = [str(a) for a in argv]
        self.group_id = str(group_id)
        self.sudo_user = nstr(sudo_user)
        self.stop = stop
        self.timeout = timeout
        self.resources = resources

    def __str__(self):
        return join_args(self.argv)

    def to_jso(self):
        jso = (
            super().to_jso()
            | {
                "argv": self.argv,
                "group_id": self.group_id,
            }
            | if_not_none("sudo_user", self.sudo_user)
            | ifkey("resources", self.resources.to_jso(), {})
            | ifkey("stop", self.stop.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            group_id = pop("group_id", default=procstar.proto.DEFAULT_GROUP)
            sudo_user = pop("sudo_user", default=None)
            stop = pop("stop", BoundStop.from_jso, BoundStop())
            timeout = pop("timeout", Timeout.from_jso, None)
            resources = pop("resources", BoundResources.from_jso, BoundResources())
        return cls(
            argv,
            group_id=group_id,
            sudo_user=sudo_user,
            stop=stop,
            timeout=timeout,
            resources=resources,
        )

    def run(self, run_id, cfg):
        return RunningProcstarProgram(run_id, self, cfg)

    def connect(self, run_id, run_state, cfg):
        return RunningProcstarProgram(run_id, self, cfg, run_state)


# -------------------------------------------------------------------------------


async def collect_final_fd_data(
    initial_fd_data: FdData | None, proc: Process, expected_length: int
) -> FdData | None:
    async for update in proc.updates:
        match update:
            case FdData():
                fd_data_local = _combine_fd_data(initial_fd_data, update)
                # Confirm that we've accumulated all the output as
                # specified in the result.
                assert fd_data_local.interval.start == 0
                if fd_data_local.interval.stop != expected_length:
                    log.warning(
                        f"FdData length mismatch: expected {expected_length}, got {fd_data_local.interval.stop}."
                    )
                return fd_data_local

            case _:
                log.debug("expected final FdData")

    # If we get here, the async iterator ended without FdData
    return initial_fd_data


class RunningProcstarProgram(BaseRunningProcstarProgram):
    def __init__(self, run_id, program, cfg, run_state=None):
        """
        :param res:
          The most recent `Result`, if any.
        """
        if program.timeout is None:
            program.timeout = get_global_runtime_timeout(cfg)
        super().__init__(run_id, program, cfg, run_state)
        self.proc_id = None

    @property
    def _spec(self):
        """
        Returns the procstar proc spec for the program.
        """
        argv = _sudo_wrap(self.cfg, self.program.argv, self.program.sudo_user)
        systemd = _make_systemd(self.cfg, resources=self.program.resources)
        return procstar.spec.Proc(
            argv,
            env=procstar.spec.Proc.Env(
                vars={
                    "APSIS_RUN_ID": self.run_id,
                },
                # Inherit the entire environment from procstar, since it probably
                # includes important configuration.
                inherit=True,
            ),
            fds={
                # Capture stdout to a temporary file.
                "stdout": procstar.spec.Proc.Fd.Capture(
                    "tempfile",
                    encoding=None,
                    # Don't attach output to results, so we can poll quickly.
                    attached=False,
                ),
                # Merge stderr into stdin.
                "stderr": procstar.spec.Proc.Fd.Dup(1),
            },
            systemd_properties=systemd,
        )

    async def _start_new_execution(self, update_interval, output_interval):
        """Start new execution by connecting to Procstar agent."""
        conn_timeout = get_cfg(self.cfg, "connection.start_timeout", 0)
        conn_timeout = nparse_duration(conn_timeout)
        self.proc_id = str(uuid.uuid4())

        try:
            # Start the proc.
            self.proc, res = await get_agent_server().start(
                proc_id=self.proc_id,
                group_id=self.program.group_id,
                spec=self._spec,
                conn_timeout=conn_timeout,
            )
        except NoOpenConnectionInGroup as exc:
            msg = f"start failed: {self.proc_id}: {exc}"
            log.warning(msg)
            yield ProgramError(msg)
            return

        conn_id = self.proc.conn_id
        log.info(f"started: {self.proc_id} on conn {conn_id}")

        start = ora.now()
        self.run_state = {
            "conn_id": conn_id,
            "proc_id": self.proc_id,
            "start": str(start),
        }
        yield base.ProgramRunning(run_state=self.run_state, meta=_make_metadata(self.proc_id, res))

        # Monitor execution
        async for update in self._monitor_procstar_execution(start, update_interval, output_interval):
            yield update

    async def _restore_reconnection_state(self):
        """Restore state needed for reconnection."""
        self.conn_id = self.run_state["conn_id"]
        self.proc_id = self.run_state["proc_id"]

    def _get_result_metadata(self, res):
        """Override to use legacy _make_metadata function."""
        return _make_metadata(self.proc_id, res)

    async def _cleanup(self):
        """Cleanup resources after execution."""
        if self.proc is not None:
            # Done with this proc; ask the agent to delete it.
            try:
                # Request deletion.
                await self.proc.delete()
            except Exception as exc:
                # Just log this; for Apsis, the proc is done.
                log.error(f"delete {self.proc.proc_id}: {exc}")
            self.proc = None
