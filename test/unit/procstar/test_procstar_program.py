from signal import Signals

import pytest

import apsis.program.procstar.agent
from test.unit.util import create_fddata, create_running_result, create_success_result
from apsis.exc import SchemaError
from apsis.jobs import InMemoryJobs, Job
from apsis.program import Program
from apsis.program.procstar.agent import (
    RunningProcstarProgram,
    ProcstarProgram,
    ProcstarShellProgram,
)
from apsis.program.base import (
    ProgramSuccess,
    ProgramFailure,
    ProgramError,
    ProgramRunning,
)
from apsis.runs import Instance, Run, bind

# -------------------------------------------------------------------------------


def test_process_program_jso():
    program = Program.from_jso(
        {
            "type": "apsis.program.procstar.agent.ProcstarProgram",
            "argv": ["/usr/bin/echo", "Hello, {{ name }}!"],
            "stop": {"signal": "SIGUSR1"},
            "group_id": "prod",
            "resources": {"mem_max_gb": 1.5},
        }
    )

    # JSO round trip.
    program = Program.from_jso(program.to_jso())
    assert list(program.argv) == ["/usr/bin/echo", "Hello, {{ name }}!"]
    assert program.group_id == "prod"
    assert program.sudo_user is None
    assert program.stop.signal == "SIGUSR1"
    assert program.stop.grace_period == "60"
    assert program.resources.mem_max_gb == 1.5

    # Bind and do it again.
    program = program.bind({"name": "Bob"})
    program = Program.from_jso(program.to_jso())
    assert list(program.argv) == ["/usr/bin/echo", "Hello, Bob!"]
    assert program.group_id == "prod"
    assert program.sudo_user is None
    assert program.stop.signal == Signals.SIGUSR1
    assert program.stop.grace_period == 60
    assert program.resources.mem_max_gb == 1.5


def test_shell_command_program_jso():
    program = Program.from_jso(
        {
            "type": "apsis.program.procstar.agent.ProcstarShellProgram",
            "command": "echo 'Hello, {{ name }}!'",
            "sudo_user": "produser",
        }
    )

    # JSO round trip.
    program = Program.from_jso(program.to_jso())
    assert program.command == "echo 'Hello, {{ name }}!'"
    assert program.group_id == "default"
    assert program.sudo_user == "produser"
    assert program.stop.signal == "SIGTERM"
    assert program.stop.grace_period == "60"

    # Bind and do it again.
    program = program.bind({"name": "Bob"})
    program = Program.from_jso(program.to_jso())
    assert "echo 'Hello, Bob!'" in program.argv[2]
    assert program.group_id == "default"
    assert program.sudo_user == "produser"
    assert program.stop.signal == Signals.SIGTERM
    assert program.stop.grace_period == 60


def test_systemd_properties():
    program = Program.from_jso(
        {
            "type": "apsis.program.procstar.agent.ProcstarShellProgram",
            "command": "/usr/bin/true",
            "resources": {"mem_max_gb": 2},
        }
    )
    running_program = program.bind({}).run("r123", {})
    systemd = running_program._spec.to_jso()["systemd_properties"]
    assert systemd["slice"]["memory_max"] == 2 * 10**9
    assert systemd["slice"]["memory_swap_max"] == 0

    # test default
    program = Program.from_jso(
        {
            "type": "apsis.program.procstar.agent.ProcstarShellProgram",
            "command": "/usr/bin/true",
        }
    )
    running_program = program.bind({}).run(
        "r123", {"procstar": {"agent": {"resource_defaults": {"mem_max_gb": 64}}}}
    )
    systemd = running_program._spec.to_jso()["systemd_properties"]
    assert systemd["slice"]["memory_max"] == 64 * 10**9

    with pytest.raises(SchemaError):
        program = Program.from_jso(
            {
                "type": "apsis.program.procstar.agent.ProcstarShellProgram",
                "command": "/usr/bin/true",
                "resources": {"mem_max_gb": -1},
            }
        )


def _spec_env(bound, args=None, run_id="r123", cfg={}):
    """Runs `bound`, applies run args (as `_start` does), returns the proc spec env."""
    running = bound.run(run_id, cfg)
    running.args = args or {}
    return running._spec.to_jso()["env"]["vars"]


def test_run_args_env_vars():
    """
    A run's bound args are exposed to the process as APSIS_ARG_* env vars,
    including args that aren't referenced in the command.
    """
    args = {"date": "2026-09-01", "database": "asd_hoard"}
    # `date` is not referenced in argv; it should still be exported.
    bound = ProcstarProgram(argv=["/usr/bin/echo", "{{ database }}"]).bind(args)

    env = _spec_env(bound, args)
    assert env["APSIS_RUN_ID"] == "r123"
    assert env["APSIS_ARG_date"] == "2026-09-01"
    assert env["APSIS_ARG_database"] == "asd_hoard"


@pytest.mark.parametrize(
    "args",
    [
        # Real param combinations from asd/jobs.
        {"date": "2026-09-01"},
        {"time": "2026-09-01T09:00:00Z"},
        {"date": "2026-09-01", "daytime": "09:00:00"},
        {"date": "2026-09-01", "instance": "eq-us-01"},
        {"strat": "st_stat", "date": "2026-09-01"},
        {"date": "2026-09-01", "macro_portfolio": "global_macro"},  # underscore
        # Robustness: digits and underscores in names.
        {"model_v2": "abc", "region3": "us", "run_7": "x"},
    ],
)
def test_run_args_env_names(args):
    """A variety of realistic param names map to APSIS_ARG_<name>."""
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})

    env = _spec_env(bound, args)
    assert env["APSIS_RUN_ID"] == "r123"
    for name, value in args.items():
        assert env[f"APSIS_ARG_{name}"] == value


def test_shell_program_run_args_env():
    """ProcstarShellProgram exposes run args as env vars too."""
    args = {"database": "asd_hoard"}
    bound = ProcstarShellProgram(command="echo {{ database }}").bind(args)

    assert _spec_env(bound, args)["APSIS_ARG_database"] == "asd_hoard"


def test_start_applies_run_args():
    """
    The bound program from runs.bind carries no args; _start applies them to the
    running program, which emits them.  Mirrors production.
    """
    program = ProcstarProgram(argv=["/usr/bin/echo", "{{ database }}"])
    job = Job("job1", {"date", "database"}, program=program)
    jobs = InMemoryJobs([job])
    run = Run(Instance("job1", {"date": "2026-09-01", "database": "asd_hoard"}))

    bind(run, job, jobs)

    # What _start does: run the program, then apply the run's args.
    running = run.program.run("r1", {})
    running.args = run.inst.args
    env = running._spec.to_jso()["env"]["vars"]
    assert env["APSIS_ARG_date"] == "2026-09-01"
    assert env["APSIS_ARG_database"] == "asd_hoard"


def test_run_args_stringified():
    """
    Non-string arg values reach the environment as strings.  `Instance`
    stringifies args on construction, so a run's args are always `{str: str}`.
    """
    program = ProcstarProgram(argv=["/usr/bin/echo", "hi"])
    job = Job("job1", {"count", "ratio"}, program=program)
    run = Run(Instance("job1", {"count": 5, "ratio": 1.5}))
    assert run.inst.args == {"count": "5", "ratio": "1.5"}

    bind(run, job, InMemoryJobs([job]))
    running = run.program.run("r1", {})
    running.args = run.inst.args

    env = running._spec.to_jso()["env"]["vars"]
    assert env["APSIS_ARG_count"] == "5"
    assert env["APSIS_ARG_ratio"] == "1.5"


def test_run_args_env_inherits():
    """Adding run args doesn't disable environment inheritance."""
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    running = bound.run("r1", {})
    running.args = {"database": "asd_hoard"}

    assert running._spec.to_jso()["env"]["inherit"] is True


def test_no_run_args_env():
    """Without bound args, only APSIS_RUN_ID is set (no APSIS_ARG_* vars)."""
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    assert _spec_env(bound) == {"APSIS_RUN_ID": "r123"}


def test_run_args_not_in_jso():
    """Run args are never serialized on the program (rollback safety)."""
    program = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    assert "args" not in program.to_jso()


def test_run_args_env_after_restore_and_start():
    """
    A program restored from the DB (args are not persisted) still emits
    APSIS_ARG_* when _start applies the run's args to the running program.
    """
    args = {"date": "2026-09-01", "database": "asd_hoard"}
    program = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    restored = Program.from_jso(program.to_jso())

    env = _spec_env(restored, args)
    assert env["APSIS_ARG_date"] == "2026-09-01"
    assert env["APSIS_ARG_database"] == "asd_hoard"


def test_run_args_run_id_not_clobbered():
    """An arg named RUN_ID becomes APSIS_ARG_RUN_ID, distinct from APSIS_RUN_ID."""
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})

    env = _spec_env(bound, {"RUN_ID": "not-the-run-id"})
    assert env["APSIS_RUN_ID"] == "r123"
    assert env["APSIS_ARG_RUN_ID"] == "not-the-run-id"


def test_run_args_empty_string_value():
    """An empty-string arg value is exported as an empty env var."""
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    assert _spec_env(bound, {"date": ""})["APSIS_ARG_date"] == ""


@pytest.mark.asyncio
async def test_final_fddata_normal_case(mock_proc):
    """
    Test the normal case where final FdData arrives properly. This should complete
    successfully with a ProgramSuccess result.
    """
    program = ProcstarProgram(argv=["/bin/echo", "test"]).bind({})
    running_program = RunningProcstarProgram(
        run_id="test-run-123", program=program, cfg={}, run_state=None
    )

    mock_proc._inject_updates(
        [
            create_running_result(mock_proc, stdout_length=0),
            create_success_result(mock_proc, stdout_length=1000),
            create_fddata(1000),  # Final output data arrives
        ]
    )

    assert isinstance(await anext(running_program.updates), ProgramRunning)

    updates_received = []
    async for update in running_program.updates:
        updates_received.append(update)
        if isinstance(update, (ProgramSuccess, ProgramFailure, ProgramError)):
            break

    final_result = updates_received[-1]
    assert isinstance(final_result, ProgramSuccess)


@pytest.mark.asyncio
async def test_missing_final_fddata(mock_proc, monkeypatch):
    """
    Test that verifies the the program will eventually terminate even if no final
    FdData is received.
    """
    # let's not wait 30s
    monkeypatch.setattr(apsis.program.procstar.agent, "FD_DATA_TIMEOUT", 0.1)

    program = ProcstarProgram(argv=["/bin/echo", "test"]).bind({})
    running_program = RunningProcstarProgram(
        run_id="test-run-123", program=program, cfg={}, run_state=None
    )

    mock_proc._inject_updates(
        [
            create_running_result(mock_proc, stdout_length=0),
            create_success_result(
                mock_proc, stdout_length=1000
            ),  # has output to collect, but no FdData will come
        ]
    )

    # first update should be ProgramRunning
    assert isinstance(await anext(running_program.updates), ProgramRunning)

    # Third update should now be ProgramError (after timeout) instead of hanging
    final_update = await anext(running_program.updates)
    assert isinstance(final_update, ProgramError), (
        f"Expected ProgramError, got {type(final_update)}"
    )
    assert "Timeout waiting for final FdData" in final_update.message
    assert "exit_code=0" in final_update.message


# -------------------------------------------------------------------------------
# Memory limit resolution and OOM evidence


def _res(**fields):
    from procstar.agent.proc import Result

    return Result(fields)


def _acct(*, max_="max", kills=0, local_oom=0, hits=0, peak=10**9):
    return {
        "memory": {
            "current": 0,
            "peak": peak,
            "swap_current": 0,
            "swap_peak": 0,
            "max": max_,
            "events": {"low": 0, "high": 0, "max": hits, "oom": local_oom, "oom_kill": kills},
            "events_local": {"low": 0, "high": 0, "max": hits, "oom": local_oom, "oom_kill": 0},
        }
    }


def _running(resources=None, cfg=None):
    jso = {
        "type": "apsis.program.procstar.agent.ProcstarShellProgram",
        "command": "/usr/bin/true",
    }
    if resources is not None:
        jso["resources"] = resources
    running = Program.from_jso(jso).bind({}).run("r123", cfg or {})
    # As at start: the request is recorded in run_state.
    running.run_state = {"proc_id": "p0", **running._get_extra_run_state()}
    return running


DEFAULT_CFG = {"procstar": {"agent": {"resource_defaults": {"mem_max_gb": 64}}}}


@pytest.mark.parametrize(
    ("resources", "cfg", "requested", "source"),
    [
        (None, {}, None, "none"),
        (None, DEFAULT_CFG, 64 * 10**9, "default"),
        ({"mem_max_gb": 2}, DEFAULT_CFG, 2 * 10**9, "job"),
    ],
)
def test_memory_limit_requested(resources, cfg, requested, source):
    running = _running(resources, cfg)
    meta = running._get_result_metadata(_res(errors=[]))
    assert meta["memory_limit"]["requested_bytes"] == requested
    assert meta["memory_limit"]["source"] == source
    # Nothing reported by the agent: unknown, never "none".
    assert meta["memory_limit"]["observed"] == {"state": "unknown"}
    assert meta["oom"]["evidence"] == "unknown"


def test_memory_limit_observed_states():
    running = _running({"mem_max_gb": 2}, {})
    meta = running._get_result_metadata(_res(errors=[], systemd_enabled=False))
    assert meta["memory_limit"]["observed"] == {"state": "unavailable"}

    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, cgroup_accounting=_acct(max_="max"))
    )
    assert meta["memory_limit"]["observed"] == {"state": "unlimited"}

    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, cgroup_accounting=_acct(max_=1999998976))
    )
    assert meta["memory_limit"]["observed"] == {"state": "finite", "bytes": 1999998976}


@pytest.mark.parametrize(
    ("acct", "evidence", "message"),
    [
        (
            _acct(max_=2 * 10**9, kills=1, local_oom=1, hits=40, peak=2 * 10**9),
            "job_limit_likely",
            "OOM kill recorded in this run; likely caused by its 2.0 GB memory limit, peak 2.0 GB",
        ),
        (
            _acct(max_=2 * 10**9, kills=1, local_oom=0, hits=0),
            "oom_kill_observed",
            "OOM kill recorded in this run; cause not attributed, host-wide OOM possible",
        ),
        # A kill under no finite limit cannot be attributed to the job limit.
        (_acct(max_="max", kills=1, local_oom=1, hits=1), "oom_kill_observed", None),
        (
            _acct(max_=2 * 10**9, kills=0, local_oom=0, hits=3),
            "limit_hit_no_kill",
            "memory limit reached, no OOM kill recorded",
        ),
        (_acct(max_=2 * 10**9), "none", None),
    ],
)
def test_oom_evidence(acct, evidence, message):
    from apsis.program.procstar.agent import _oom_message

    running = _running({"mem_max_gb": 2}, {})
    # systemd marks the scope oom-kill only when a kill happened.
    scope_result = "oom-kill" if acct["memory"]["events"]["oom_kill"] else "success"
    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, scope_result=scope_result, cgroup_accounting=acct)
    )
    assert meta["oom"]["evidence"] == evidence
    assert meta["oom"]["scope_result"] == scope_result
    assert meta["oom"]["kill_count"] == acct["memory"]["events"]["oom_kill"]
    got = _oom_message(meta)
    if message is None:
        assert got is None or evidence == "oom_kill_observed"
    else:
        assert got == message


def test_oom_counters_missing_is_unknown():
    running = _running({"mem_max_gb": 2}, {})
    acct = _acct(max_=2 * 10**9)
    del acct["memory"]["events"]
    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, cgroup_accounting=acct)
    )
    assert meta["oom"]["evidence"] == "unknown"
    assert meta["oom"]["kill_count"] is None


def test_kill_without_local_counters_is_still_observed():
    running = _running({"mem_max_gb": 2}, {})
    acct = _acct(max_=2 * 10**9, kills=1)
    del acct["memory"]["events_local"]
    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, cgroup_accounting=acct)
    )
    assert meta["oom"]["evidence"] == "oom_kill_observed"


def test_null_memory_accounting_is_tolerated():
    # The agent reports memory: null when the memory controller is unavailable.
    from apsis.program.procstar.agent import _oom_message

    running = _running({"mem_max_gb": 2}, {})
    acct = {"memory": None, "cpu_stat": {"usage_usec": 1}}
    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, cgroup_accounting=acct)
    )
    assert meta["memory_limit"]["observed"] == {"state": "unknown"}
    assert meta["oom"]["evidence"] == "unknown"
    assert _oom_message(meta) is None


def test_requested_limit_survives_reconnect():
    # Started under a 100 GB default, reconnected under a 128 GB default.
    started = _running(None, {"procstar": {"agent": {"resource_defaults": {"mem_max_gb": 100}}}})
    reconnected = _running(
        None, {"procstar": {"agent": {"resource_defaults": {"mem_max_gb": 128}}}}
    )
    reconnected.run_state = started.run_state
    meta = reconnected._get_result_metadata(_res(errors=[]))
    assert meta["memory_limit"]["requested_bytes"] == 100 * 10**9
    assert meta["memory_limit"]["source"] == "default"


def test_requested_limit_unknown_for_old_run_state():
    # Started before the request was recorded: only an explicit job value is certain.
    running = _running(None, DEFAULT_CFG)
    running.run_state = {"proc_id": "p0"}
    meta = running._get_result_metadata(_res(errors=[]))
    assert meta["memory_limit"] == {
        "requested_bytes": None,
        "source": "unknown",
        "observed": {"state": "unknown"},
    }

    running = _running({"mem_max_gb": 2}, DEFAULT_CFG)
    running.run_state = {"proc_id": "p0"}
    meta = running._get_result_metadata(_res(errors=[]))
    assert meta["memory_limit"]["requested_bytes"] == 2 * 10**9
    assert meta["memory_limit"]["source"] == "job"


def test_systemd_oom_kill_without_kernel_counters():
    from apsis.program.procstar.agent import _oom_message

    running = _running({"mem_max_gb": 2}, {})
    # systemd-oomd style: scope ended by oom-kill, kernel counters zero.
    meta = running._get_result_metadata(
        _res(
            errors=[],
            systemd_enabled=True,
            scope_result="oom-kill",
            cgroup_accounting=_acct(max_=2 * 10**9),
        )
    )
    assert meta["oom"]["evidence"] == "systemd_oom_kill"
    assert _oom_message(meta) == "systemd reports OOM-related termination; cause not attributed"
    # ... and with no counters at all.
    acct = _acct(max_=2 * 10**9)
    del acct["memory"]["events"]
    del acct["memory"]["events_local"]
    meta = running._get_result_metadata(
        _res(errors=[], systemd_enabled=True, scope_result="oom-kill", cgroup_accounting=acct)
    )
    assert meta["oom"]["evidence"] == "systemd_oom_kill"
    # A kernel kill still wins over the scope result.
    meta = running._get_result_metadata(
        _res(
            errors=[],
            systemd_enabled=True,
            scope_result="oom-kill",
            cgroup_accounting=_acct(max_=2 * 10**9, kills=1, local_oom=1),
        )
    )
    assert meta["oom"]["evidence"] == "job_limit_likely"


def test_timeout_applies():
    import ora

    from apsis.program.procstar.agent import _timeout_applies

    sent = ora.Time("2026-10-19T12:00:00+00:00")
    exited = {"exit_code": 0, "signal": None}
    killed = {"exit_code": None, "signal": "SIGTERM"}

    def res(status, stop):
        return _res(status=status, times={"stop": stop})

    # exited clearly before the signal was requested: not a timeout, even if the
    # same signal name appears (e.g. systemd's SIGTERM after a child OOM)
    assert _timeout_applies(res(exited, "2026-10-19T11:59:30+00:00"), "SIGTERM", sent) is False
    assert _timeout_applies(res(killed, "2026-10-19T11:59:30+00:00"), "SIGTERM", sent) is False
    # stopped clearly after the request: a timeout, whatever the status
    assert _timeout_applies(res(exited, "2026-10-19T12:00:30+00:00"), "SIGTERM", sent) is True
    # within the clock tolerance: decided by the signal
    assert _timeout_applies(res(killed, "2026-10-19T11:59:59+00:00"), "SIGTERM", sent) is True
    assert _timeout_applies(res(exited, "2026-10-19T11:59:59+00:00"), "SIGTERM", sent) is False
    # cannot tell: the timeout stands
    assert _timeout_applies(_res(status=exited), "SIGTERM", sent) is True
    assert _timeout_applies(res(exited, "2026-10-19T11:59:30+00:00"), "SIGTERM", None) is True
