import asyncio
import logging
import ora
import procstar.spec
from   procstar.agent.exc import NoConnectionError, NoOpenConnectionInGroup, ProcessUnknownError
from   procstar.agent.proc import FdData, Interval, Result
from   signal import Signals
import uuid

from   apsis.lib import asyn
from   apsis.lib import memo
from   apsis.lib.json import check_schema, ifkey
from   apsis.lib.parse import nparse_duration
from   apsis.lib.py import or_none, nstr, get_cfg
from   apsis.procstar import get_agent_server
from   apsis.program import base
from   apsis.program.base import (ProgramSuccess, ProgramFailure, ProgramError, Timeout)
from   apsis.program.process import Stop, BoundStop
from   apsis.runs import join_args, template_expand
import time

log = logging.getLogger(__name__)

ntemplate_expand = or_none(template_expand)

#-------------------------------------------------------------------------------

SUDO_ARGV_DEFAULT = ["/usr/bin/sudo", "--preserve-env", "--set-home"]

if_not_none = lambda k, v: {} if v is None else {k: v}

def _sudo_wrap(cfg, argv, sudo_user):
    if sudo_user is None:
        return argv
    else:
        sudo_argv = get_cfg(cfg, "sudo.argv", SUDO_ARGV_DEFAULT)
        return [ str(a) for a in sudo_argv ] + [
            "--non-interactive",
            "--user", str(sudo_user),
            "--"
        ] + list(argv)


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
        for k in ("status", "times", "rusage", "proc_stat", "proc_statm", )
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
        raise RuntimeError(
            f"fd data gap: {fi(old.interval)} + {fi(new.interval)}")

    elif old.interval.stop == new.interval.start:
        return FdData(
            fd      =old.fd,
            encoding=old.encoding,
            interval=Interval(old.interval.start, new.interval.stop),
            data    =old.data + new.data,
        )

    else:
        # Partial overlap of data.
        log.warning(f"fd data overlap: {fi(old.interval)} + {fi(new.interval)}")
        length = new.interval.stop - old.interval.stop
        if length > 0:
            # Partial overlap.  Patch intervals together.
            interval = Interval(old.interval.start, new.interval.stop)
            data = old.data + new.data[-length :]
            assert interval.stop - interval.start == len(data)
            return FdData(
                fd      =old.fd,
                encoding=old.encoding,
                interval=interval,
                data    =data,
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


#-------------------------------------------------------------------------------

class _ProcstarProgram(base.Program):
    """
    Base class for (unbound) Procstar program types.
    """

    def __init__(
            self, *,
            group_id    =procstar.proto.DEFAULT_GROUP,
            sudo_user   =None,
            stop        =Stop(),
            timeout     =None,
    ):
        super().__init__()
        self.group_id   = str(group_id)
        self.sudo_user  = None if sudo_user is None else str(sudo_user)
        self.stop       = stop
        self.timeout    = timeout

    def _bind(self, argv, args):
        return BoundProcstarProgram(
            argv,
            group_id    =ntemplate_expand(self.group_id, args),
            sudo_user   =ntemplate_expand(self.sudo_user, args),
            stop        =self.stop.bind(args),
            timeout     =None if self.timeout is None else self.timeout.bind(args),
        )


    def to_jso(self):
        jso = (
            super().to_jso()
            | {
                "group_id"  : self.group_id,
            }
            | if_not_none("sudo_user", self.sudo_user)
            | ifkey("stop", self.stop.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @staticmethod
    def _from_jso(pop):
        return dict(
            group_id    =pop("group_id", default=procstar.proto.DEFAULT_GROUP),
            sudo_user   =pop("sudo_user", default=None),
            stop        =pop("stop", Stop.from_jso, Stop()),
            timeout     =pop("timeout", Timeout.from_jso, None),
        )



#-------------------------------------------------------------------------------

class ProcstarProgram(_ProcstarProgram):

    def __init__(self, argv, **kw_args):
        super().__init__(**kw_args)
        self.argv = [ str(a) for a in argv ]


    def __str__(self):
        return join_args(self.argv)


    def bind(self, args):
        argv = tuple( template_expand(a, args) for a in self.argv )
        return super()._bind(argv, args)


    def to_jso(self):
        return super().to_jso() | {"argv" : self.argv}


    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv        = pop("argv")
            kw_args     = cls._from_jso(pop)
        return cls(argv, **kw_args)



#-------------------------------------------------------------------------------

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
        return super().to_jso() | {"command" : self.command}


    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command     = pop("command")
            kw_args     = cls._from_jso(pop)
        return cls(command, **kw_args)



#-------------------------------------------------------------------------------

class BoundProcstarProgram(base.Program):

    def __init__(
            self, argv, *, group_id,
            sudo_user   =None,
            stop        =BoundStop(),
            timeout     =None,
    ):
        self.argv = [ str(a) for a in argv ]
        self.group_id   = str(group_id)
        self.sudo_user  = nstr(sudo_user)
        self.stop       = stop
        self.timeout    = timeout

    def __str__(self):
        return join_args(self.argv)


    def to_jso(self):
        jso = (
            super().to_jso()
            | {
                "argv"      : self.argv,
                "group_id"  : self.group_id,
            }
            | if_not_none("sudo_user", self.sudo_user)
            | ifkey("stop", self.stop.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv        = pop("argv")
            group_id    = pop("group_id", default=procstar.proto.DEFAULT_GROUP)
            sudo_user   = pop("sudo_user", default=None)
            stop        = pop("stop", BoundStop.from_jso, BoundStop())
            timeout     = pop("timeout", Timeout.from_jso, None)
        return cls(argv, group_id=group_id, sudo_user=sudo_user, stop=stop, timeout=timeout)


    def run(self, run_id, cfg):
        return RunningProcstarProgram(run_id, self, cfg)


    def connect(self, run_id, run_state, cfg):
        return RunningProcstarProgram(run_id, self, cfg, run_state)



#-------------------------------------------------------------------------------

class RunningProcstarProgram(base.RunningProgram):

    def __init__(self, run_id, program, cfg, run_state=None):
        """
        :param res:
          The most recent `Result`, if any.
        """
        super().__init__(run_id)
        self.program        = program
        self.cfg            = get_cfg(cfg, "procstar.agent", {})
        self.run_state      = run_state

        self.proc           = None
        self.stopping       = False
        self.stop_signals   = []
        self.timed_out      = False
        self.health_check_task = None


    @property
    def _spec(self):
        """
        Returns the procstar proc spec for the program.
        """
        argv = _sudo_wrap(self.cfg, self.program.argv, self.program.sudo_user)
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
        )


    @memo.property
    async def updates(self):
        """
        Handles running `inst` until termination.
        """
        run_cfg         = get_cfg(self.cfg, "run", {})
        update_interval = run_cfg.get("update_interval", None)
        update_interval = nparse_duration(update_interval)
        output_interval = run_cfg.get("output_interval", None)
        output_interval = nparse_duration(output_interval)

        if self.run_state is None:
            # Start the proc.

            conn_timeout = get_cfg(self.cfg, "connection.start_timeout", 0)
            conn_timeout = nparse_duration(conn_timeout)
            proc_id = str(uuid.uuid4())

            try:
                # Start the proc.
                self.proc, res = await get_agent_server().start(
                    proc_id     =proc_id,
                    group_id    =self.program.group_id,
                    spec        =self._spec,
                    conn_timeout=conn_timeout,
                )
            except NoOpenConnectionInGroup as exc:
                msg = f"start failed: {proc_id}: {exc}"
                log.warning(msg)
                yield ProgramError(msg)
                return

            conn_id = self.proc.conn_id
            log.info(f"started: {proc_id} on conn {conn_id}")

            start = ora.now()
            self.run_state = {
                "conn_id": conn_id,
                "proc_id": proc_id,
                "start": str(start),
            }
            yield base.ProgramRunning(
                run_state   =self.run_state,
                meta        =_make_metadata(proc_id, res)
            )

        else:
            # Reconnect to the proc.
            conn_timeout = get_cfg(self.cfg, "connection.reconnect_timeout", None)
            conn_timeout = nparse_duration(conn_timeout)

            conn_id = self.run_state["conn_id"]
            proc_id = self.run_state["proc_id"]
            start = ora.Time(self.run_state["start"])
            log.info(f"reconnecting: {proc_id} on conn {conn_id}")

            try:
                self.proc = await get_agent_server().reconnect(
                    conn_id     =conn_id,
                    proc_id     =proc_id,
                    conn_timeout=conn_timeout,
                )
            except NoConnectionError as exc:
                msg = f"reconnect failed: {proc_id}: {exc}"
                log.error(msg)
                yield ProgramError(msg)
                return

            # Request a result immediately.
            await self.proc.request_result()
            res = None
            
            # Attempt to get the process status immediately after reconnection
            try:
                # Use asyncio.wait_for to add a timeout for the initial response
                status_check_timeout = get_cfg(self.cfg, "connection.status_check_timeout", 5)  # 5 second default
                initial_update = await asyncio.wait_for(
                    anext(self.proc.updates),
                    timeout=status_check_timeout
                )
                
                if isinstance(initial_update, Result) and initial_update.state == "terminated":
                    # Process has already terminated but Apsis didn't know
                    log.info(f"reconnected to terminated process: {proc_id} on conn {conn_id}")
                    res = initial_update
                    # We'll handle this termination in the regular processing loop
                else:
                    log.info(f"reconnected: {proc_id} on conn {conn_id}")
            except asyncio.TimeoutError:
                # If we timeout, just continue with normal operation
                log.info(f"reconnected, but status check timed out: {proc_id} on conn {conn_id}")
            except ProcessUnknownError as exc:
                # If the process is unknown, it likely terminated while we were disconnected
                log.warning(f"Process unknown after reconnection: {proc_id} on conn {conn_id}: {exc}")
                meta = {"errors": [f"Process unknown after reconnection: {exc}"]}
                yield ProgramFailure(f"Process {proc_id} not found on reconnection", meta=meta)
                return

        # We now have a proc running on the agent.

        try:
            tasks = asyn.TaskGroup()
            
            # Start a health check task that periodically checks process status
            health_check_interval = get_cfg(self.cfg, "health_check.interval", 60)  # 60 seconds default
            health_check_enabled = get_cfg(self.cfg, "health_check.enabled", True)
            if health_check_enabled and health_check_interval > 0:
                async def health_check_handler():
                    while self.proc is not None and not self.stopping:
                        try:
                            await asyncio.sleep(health_check_interval)
                            if self.proc is None:
                                break
                            # Request the current status
                            await self.proc.request_result()
                        except Exception as exc:
                            log.warning(f"Health check for {self.run_id} failed: {exc}")
                
                tasks.add("health_check", health_check_handler())

            if self.program.timeout is not None:
                async def timeout_handler():
                    elapsed_so_far = ora.now() - start
                    remaining = self.program.timeout.duration - elapsed_so_far
                    sleep_duration = max(0, remaining)
                    
                    if sleep_duration > 0:
                        await asyncio.sleep(sleep_duration)

                    if not self.stopping and self.proc is not None:
                        elapsed = ora.now() - start
                        log.info(f"{self.run_id}: timeout")
                        self.timed_out = True
                        timeout_signal = Signals[self.program.timeout.signal]
                        self.stop_signals.append(timeout_signal)
                        await self.proc.send_signal(timeout_signal)

                tasks.add("timeout", timeout_handler())

            # Output collected so far.
            fd_data = None

            # Start tasks to request periodic updates of results and output.

            if update_interval is not None:
                # Start a task that periodically requests the current result.
                tasks.add(
                    "poll update",
                    asyn.poll(self.proc.request_result, update_interval)
                )

            if output_interval is not None:
                # Start a task that periodically requests additional output.
                def more_output():
                    # From the current position to the end.
                    start = 0 if fd_data is None else fd_data.interval.stop
                    interval = Interval(start, None)
                    return self.proc.request_fd_data("stdout", interval=interval)

                tasks.add("poll output", asyn.poll(more_output, output_interval))

            # Process further updates, until the process terminates.
            async for update in self.proc.updates:
                match update:
                    case FdData():
                        fd_data = _combine_fd_data(fd_data, update)
                        yield base.ProgramUpdate(outputs=await _make_outputs(fd_data))

                    case Result() as res:
                        meta = _make_metadata(proc_id, res)

                        if res.state == "running":
                            # Intermediate result.
                            yield base.ProgramUpdate(meta=meta)
                        else:
                            # Process terminated.
                            break

            else:
                # Proc was deleted--but we didn't delete it.
                assert False, "proc deleted"

            # Stop update tasks.
            await tasks.cancel_all()

            # Do we have the complete output?
            length = res.fds.stdout.length
            if length > 0 and (
                    fd_data is None
                    or fd_data.interval.stop < length
            ):
                # Request any remaining output.
                await self.proc.request_fd_data(
                    "stdout",
                    interval=Interval(
                        0 if fd_data is None else fd_data.interval.stop,
                        None
                    )
                )
                # Wait for it.
                async for update in self.proc.updates:
                    match update:
                        case FdData():
                            fd_data = _combine_fd_data(fd_data, update)
                            # Confirm that we've accumulated all the output as
                            # specified in the result.
                            assert fd_data.interval.start == 0
                            assert fd_data.interval.stop == res.fds.stdout.length
                            break

                        case _:
                            log.debug("expected final FdData")

            outputs = await _make_outputs(fd_data)
            meta["stop"] = {"signals": [ s.name for s in self.stop_signals ]}

            if res.status.exit_code == 0 and not self.timed_out:
                # The process terminated successfully.
                yield ProgramSuccess(meta=meta, outputs=outputs)
            elif (
                        self.stopping
                    and res.status.signal is not None
                    and Signals[res.status.signal] == self.program.stop.signal
            ):
                # The process stopped with the expected signal.
                yield ProgramSuccess(meta=meta, outputs=outputs)
            else:
                message = (
                    f"exit code {res.status.exit_code}" if res.status.signal is None
                    else f"killed by {res.status.signal}"
                )
                if self.timed_out:
                    elapsed = ora.now() - start
                    message += f" (timeout after {elapsed:.0f} s)"
                yield ProgramFailure(message, meta=meta, outputs=outputs)

        except asyncio.CancelledError:
            # Don't clean up the proc; we can reconnect.
            self.proc = None

        except ProcessUnknownError as exc:
            # Process is already gone - handle this case gracefully
            log.warning(f"Process unknown error for {self.run_id}: {exc}")
            self.proc = None
            
            # Return a failed state if we know the process is gone
            if res is None:
                yield ProgramFailure(
                    f"Process {proc_id} not found on reconnection",
                    meta={"errors": [f"Process unknown: {exc}"]}
                )

        except Exception as exc:
            log.error("procstar", exc_info=True)
            yield ProgramError(
                f"procstar: {exc}",
                meta={} if res is None else _make_metadata(proc_id, res),
            )

        finally:
            # Cancel our helper tasks.
            await tasks.cancel_all()
            if self.proc is not None:
                # Done with this proc; ask the agent to delete it.
                try:
                    # Request deletion.
                    await self.proc.delete()
                except ProcessUnknownError as exc:
                    # Process is already gone, which is fine
                    log.info(f"Cannot delete {self.run_id} - process already gone: {exc}")
                except Exception as exc:
                    # Just log this; for Apsis, the proc is done.
                    log.error(f"delete {self.proc.proc_id}: {exc}")
                self.proc = None


    async def stop(self):
        if self.proc is None:
            log.warning("no more proc to stop")
            return

        stop = self.program.stop
        self.stopping = True

        # Send the stop signal.
        self.stop_signals.append(stop.signal)
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


    async def signal(self, signal):
        if self.proc is None:
            log.warning("no more proc to signal")
            return

        try:
            await self.proc.send_signal(int(signal))
        except ProcessUnknownError as exc:
            # Process is already gone, which is fine when trying to signal it
            log.info(f"Cannot signal process {self.run_id} - process already gone: {exc}")



