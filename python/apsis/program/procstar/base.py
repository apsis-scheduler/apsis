"""Base classes for Procstar program implementations."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

import ora
from procstar.agent.proc import FdData, Interval, Result
from signal import Signals

from apsis.lib import asyn, memo
from apsis.lib.parse import nparse_duration
from apsis.lib.py import get_cfg
from apsis.program import base
from apsis.procstar import get_agent_server
from apsis.program.base import (
    ProgramSuccess,
    ProgramFailure,
    ProgramError,
    ProgramUpdate,
)
from procstar.agent.exc import NoConnectionError

log = logging.getLogger(__name__)

FD_DATA_TIMEOUT = 60


def _combine_fd_data(old, new):
    """Combine old and new fd_data from Procstar agent output."""
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
    """Constructs program outputs from combined output fd data."""
    if fd_data is None:
        return {}

    assert fd_data.fd == "stdout"
    assert fd_data.interval.start == 0
    assert fd_data.encoding is None

    output = fd_data.data
    length = fd_data.interval.stop
    return base.program_outputs(output, length=length, compression=None)


class BaseRunningProcstarProgram(base.RunningProgram, ABC):
    """Base class for running Procstar programs with shared functionality."""

    def __init__(self, run_id, program, cfg, run_state=None):
        super().__init__(run_id)
        self.program = program
        self.cfg = get_cfg(cfg, "procstar.agent", {})
        self.run_state = run_state

        self.proc = None
        self.stopping = False
        self.stop_signals = []
        self.timed_out = False

    @abstractmethod
    async def _start_new_execution(self, update_interval, output_interval):
        """Start new execution. Must yield program updates."""
        pass

    @abstractmethod
    async def _restore_reconnection_state(self):
        """Restore state needed for reconnection. Override in subclasses."""
        pass

    async def _reconnect_execution(self, update_interval, output_interval):
        conn_timeout = get_cfg(self.cfg, "connection.reconnect_timeout", None)
        conn_timeout = nparse_duration(conn_timeout)
        
        # Hook for subclasses to restore state
        await self._restore_reconnection_state()
        
        log.info(f"reconnecting: {self.proc_id} on conn {self.conn_id}")
        
        try:
           
            self.proc = await get_agent_server().reconnect(
                conn_id=self.conn_id,
                proc_id=self.proc_id,
                conn_timeout=conn_timeout,
            )
        except NoConnectionError as exc:
            msg = f"reconnect failed: {self.proc_id}: {exc}"
            log.error(msg)
            yield ProgramError(msg)
            return
        
        await self.proc.request_result()
        log.info(f"reconnected: {self.proc_id} on conn {self.conn_id}")
        
        # Continue monitoring
        start = ora.Time(self.run_state["start"])
        async for update in self._monitor_procstar_execution(start, update_interval, output_interval):
            yield update

    @abstractmethod
    async def _cleanup(self):
        """Cleanup resources after execution."""
        pass

    async def _collect_final_fd_data(
        self, initial_fd_data: FdData | None, expected_length: int
    ) -> FdData | None:
        """Collect final fd_data from the proc."""
        async for update in self.proc.updates:
            match update:
                case FdData():
                    fd_data_local = _combine_fd_data(initial_fd_data, update)
                    # Confirm that we've accumulated all the output as
                    # specified in the result.
                    assert fd_data_local.interval.start == 0
                    assert fd_data_local.interval.stop == expected_length
                    return fd_data_local

                case _:
                    log.debug("expected final FdData")

        # If we get here, the async iterator ended without FdData
        return initial_fd_data

    async def _monitor_procstar_execution(self, start, update_interval, output_interval):
        """Monitor Procstar execution until completion."""
        tasks = asyn.TaskGroup()

        try:
            if hasattr(self.program, "timeout") and self.program.timeout is not None:

                async def timeout_handler():
                    elapsed_so_far = ora.now() - start
                    remaining = self.program.timeout.duration - elapsed_so_far
                    sleep_duration = max(0, remaining)

                    if sleep_duration > 0:
                        await asyncio.sleep(sleep_duration)

                    if not self.stopping and self.proc is not None:
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
                tasks.add("poll update", asyn.poll(self.proc.request_result, update_interval))

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
                        yield ProgramUpdate(outputs=await _make_outputs(fd_data))

                    case Result() as res:
                        meta = self._get_result_metadata(res)

                        if res.state == "running":
                            # Intermediate result.
                            yield ProgramUpdate(meta=meta)
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
            if length > 0 and (fd_data is None or fd_data.interval.stop < length):
                # Request any remaining output.
                await self.proc.request_fd_data(
                    "stdout",
                    interval=Interval(0 if fd_data is None else fd_data.interval.stop, None),
                )
                try:
                    fd_data = await asyncio.wait_for(
                        self._collect_final_fd_data(fd_data, expected_length=length),
                        timeout=FD_DATA_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    error_msg = (
                        f"Timeout waiting for final FdData after {FD_DATA_TIMEOUT}s."
                        f"Run completed with exit_code={res.status.exit_code}."
                    )
                    log.error(error_msg)
                    yield ProgramError(
                        error_msg,
                        meta=self._get_result_metadata(res),
                        outputs=await _make_outputs(fd_data),
                    )
                    return

            outputs = await _make_outputs(fd_data)
            meta = self._get_result_metadata(res)
            meta["stop"] = {"signals": [s.name for s in self.stop_signals]}

            if res.status.exit_code == 0 and not self.timed_out:
                # The process terminated successfully.
                yield ProgramSuccess(meta=meta, outputs=outputs)
            else:
                message = (
                    f"exit code {res.status.exit_code}"
                    if res.status.signal is None
                    else f"killed by {res.status.signal}"
                )
                if self.timed_out:
                    elapsed = ora.now() - start
                    message += f" (timeout after {elapsed:.0f} s)"
                yield ProgramFailure(message, meta=meta, outputs=outputs)

        finally:
            # Cancel our helper tasks.
            await tasks.cancel_all()

    def _get_result_metadata(self, res: Result) -> Dict[str, Any]:
        """Extract metadata from a proc result. Override for custom metadata."""
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

        if hasattr(self, "proc_id"):
            meta["procstar_proc_id"] = self.proc_id
        try:
            meta["procstar_conn"] = dict(res.procstar.conn.__dict__)
            meta["procstar_agent"] = dict(res.procstar.proc.__dict__)
        except AttributeError:
            pass

        return meta

    @memo.property
    async def updates(self):
        """Main entry point for program execution."""
        run_cfg = get_cfg(self.cfg, "run", {})
        update_interval = run_cfg.get("update_interval", None)
        update_interval = nparse_duration(update_interval)
        output_interval = run_cfg.get("output_interval", None)
        output_interval = nparse_duration(output_interval)

        try:
            if self.run_state is None:
                # Start new execution
                async for update in self._start_new_execution(update_interval, output_interval):
                    yield update
            else:
                # Reconnect to existing execution
                async for update in self._reconnect_execution(update_interval, output_interval):
                    yield update

        except asyncio.CancelledError:
            # Don't clean up the proc; we can reconnect.
            self.proc = None
            raise
        except Exception as exc:
            log.error("procstar execution error", exc_info=True)
            yield ProgramError(f"procstar: {exc}")
        finally:
            # Cleanup resources
            await self._cleanup()

    async def stop(self):
        """Stop the running program gracefully."""
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
        """Send a signal to the running program."""
        if self.proc is None:
            log.warning("no more proc to signal")
            return

        await self.proc.send_signal(int(signal))