from procstar.agent.proc import FdData, Interval, Result


def create_running_result(proc, stdout_length=0):
    """Helper to create a running process result."""
    return Result(
        {
            "proc_id": proc.proc_id,
            "state": "running",
            "status": {"exit_code": None, "signal": None},
            "fds": {"stdout": {"length": stdout_length}},
            "errors": [],
        }
    )


def create_success_result(proc, stdout_length=0):
    """Helper to create a successful process result."""
    return Result(
        {
            "proc_id": proc.proc_id,
            "state": "success",
            "status": {"exit_code": 0, "signal": None},
            "fds": {"stdout": {"length": stdout_length}},
            "errors": [],
        }
    )


def create_fddata(length, data=None, start=0):
    """Helper to create FdData for stdout."""
    if data is None:
        data = b"x" * length

    return FdData(
        "stdout",
        Interval(start, start + len(data)),
        None,
        data,
    )
