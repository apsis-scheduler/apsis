"""Base classes for Procstar program implementations."""

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict

import ora
from procstar.agent.proc import FdData, Interval, Result
from signal import Signals

from apsis.lib import asyn, memo
from apsis.lib.parse import nparse_duration
from apsis.lib.py import get_cfg
from apsis.procstar import get_agent_server
from apsis.program import base
from apsis.program.base import (
    ProgramError,
    ProgramFailure,
    ProgramSuccess,
    ProgramUpdate,
)
from apsis.program.procstar.utils import (
    FD_DATA_TIMEOUT,
    _combine_fd_data,
    _make_metadata,
    _make_outputs,
    collect_final_fd_data,
)
from procstar.agent.exc import NoConnectionError

log = logging.getLogger(__name__)


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

    async def _restore_reconnection_state(self):
        """Restore state needed for reconnection. Subclasses should call super()."""
        self.conn_id = self.run_state["conn_id"]
        self.proc_id = self.run_state["proc_id"]

    async def _start_proc_on_agent(
        self,
        group_id: str,
        extra_run_state: Dict[str, Any] = None,
    ):
        """Start a proc on a Procstar agent.

        Connects to an agent in the specified group and starts the proc.
        Sets self.proc, self.conn_id, self.proc_id, and self.run_state.

        Args:
            group_id: The Procstar agent group to connect to.
            extra_run_state: Additional fields to include in run_state.

        Returns:
            Tuple of (start_time, initial_result) for use in monitoring.

        Raises:
            NoOpenConnectionInGroup: If no agent connects in time.
        """
        conn_timeout = get_cfg(self.cfg, "connection.start_timeout", 0)
        conn_timeout = nparse_duration(conn_timeout) or 300

        self.proc_id = str(uuid.uuid4())

        # This may raise NoOpenConnectionInGroup
        self.proc, res = await get_agent_server().start(
            proc_id=self.proc_id,
            group_id=group_id,
            spec=self._spec,
            conn_timeout=conn_timeout,
        )

        self.conn_id = self.proc.conn_id
        log.info(f"started: {self.proc_id} on conn {self.conn_id}")

        start = ora.now()
        self.run_state = {
            "conn_id": self.conn_id,
            "proc_id": self.proc_id,
            "start": str(start),
            **(extra_run_state or {}),
        }

        return start, res

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
        async for update in self._monitor_procstar_execution(
            start, update_interval, output_interval
        ):
            yield update

    async def _cleanup(self):
        """Cleanup resources after execution. Override for custom cleanup."""
        if self.proc is not None:
            # Done with this proc; ask the agent to delete it.
            try:
                await self.proc.delete()
            except Exception as exc:
                # Just log this; for Apsis, the proc is done.
                log.error(f"delete {self.proc.proc_id}: {exc}")
            self.proc = None

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
                        collect_final_fd_data(fd_data, self.proc, length), timeout=FD_DATA_TIMEOUT
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
        return _make_metadata(self.proc_id, res)

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
