"""Shared utilities for Procstar program implementations."""

import logging

from procstar.agent.proc import FdData, Interval, Process

from apsis.program import base

log = logging.getLogger(__name__)

FD_DATA_TIMEOUT = 60


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
                assert fd_data_local.interval.stop == expected_length
                return fd_data_local

            case _:
                log.debug("expected final FdData")

    # If we get here, the async iterator ended without FdData
    return initial_fd_data
