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


def _spec_env(bound, args={}, *, run_id="r123", cfg=None):
    """
    Returns the env vars of the proc spec, built the way Apsis builds it: run
    the bound program, hand the running program the run's args, read the spec.
    """
    running = bound.run(run_id, {} if cfg is None else cfg)
    running.set_run_args(args)
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
        # Param combinations from real jobs.  These all take the same path, so
        # a couple of shapes is enough; the interesting cases are the ones that
        # can't be represented, below.
        {"date": "2026-09-01", "instance": "eq-us-01"},
        {"date": "2026-09-01", "macro_portfolio": "global_macro"},  # underscore
        {"model_v2": "abc", "region3": "us"},  # digits
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


def test_runs_bind_does_not_apply_run_args():
    """
    runs.bind builds the bound program; the args are not part of it.  Apsis
    applies them to the *running* program, so a program restored from the DB
    picks them up too.
    """
    program = ProcstarProgram(argv=["/usr/bin/echo", "{{ database }}"])
    job = Job("job1", {"date", "database"}, program=program)
    jobs = InMemoryJobs([job])
    run = Run(Instance("job1", {"date": "2026-09-01", "database": "asd_hoard"}))

    bind(run, job, jobs)
    assert not hasattr(run.program, "args")

    # What _start() does: run the program, then hand it the run's args.
    env = _spec_env(run.program, run.inst.args, run_id="r1")
    assert env["APSIS_ARG_date"] == "2026-09-01"
    assert env["APSIS_ARG_database"] == "asd_hoard"


def test_run_args_stringified():
    """
    Non-string arg values are stringified for the environment.

    Defensive only: `Instance.__init__` already coerces every arg to `str`, so
    Apsis never reaches `set_run_args` with a non-string on the production path.
    """
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})

    env = _spec_env(bound, {"count": 5, "ratio": 1.5})
    assert env["APSIS_ARG_count"] == "5"
    assert env["APSIS_ARG_ratio"] == "1.5"


def test_run_args_env_inherits():
    """Adding run args doesn't disable environment inheritance."""
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    running = bound.run("r1", {})
    running.set_run_args({"database": "asd_hoard"})

    assert running._spec.to_jso()["env"]["inherit"] is True


def test_no_run_args_env():
    """Without args, only APSIS_RUN_ID is set (no APSIS_ARG_* vars)."""
    running = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({}).run("r123", {})
    env = running._spec.to_jso()["env"]["vars"]
    assert env == {"APSIS_RUN_ID": "r123"}


def test_run_args_not_on_bound_program():
    """
    The args are not part of the bound program at all, so they cannot be
    serialized into the run: an older Apsis rejects unknown keys, so persisting
    them would stop it loading rows written by a newer Apsis.
    """
    program = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    assert not hasattr(program, "args")
    assert not hasattr(program, "set_run_args")

    jso = program.to_jso()
    assert "args" not in jso
    assert not hasattr(Program.from_jso(jso), "args")


def test_run_args_env_after_restore_and_start():
    """
    A run scheduled before a restart has its program rebuilt from JSO, with no
    re-bind; _start() still supplies the args, so the process gets APSIS_ARG_*.
    """
    args = {"date": "2026-09-01", "database": "asd_hoard"}
    program = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    restored = Program.from_jso(program.to_jso())

    env = _spec_env(restored, args, run_id="r1")
    assert env["APSIS_ARG_date"] == "2026-09-01"
    assert env["APSIS_ARG_database"] == "asd_hoard"


def test_set_run_args_overwrites():
    """set_run_args replaces any previously set args."""
    running = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({}).run("r1", {})
    running.set_run_args({"database": "asd_hoard"})
    running.set_run_args({"date": "2026-09-01"})
    assert running.args == {"date": "2026-09-01"}
    assert running._spec.to_jso()["env"]["vars"] == {
        "APSIS_RUN_ID": "r1",
        "APSIS_ARG_date": "2026-09-01",
    }


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


@pytest.mark.parametrize(
    "args, expected",
    [
        # A name that isn't a shell identifier is still a valid env var name,
        # so it is passed through; see the environment section of the docs.
        ({"my-param": "x"}, {"APSIS_ARG_my-param": "x"}),
        ({"a.b": "x"}, {"APSIS_ARG_a.b": "x"}),
        # "=" in a name would be read back as part of the value.
        ({"a=b": "c"}, {}),
        # NUL cannot be passed to execve at all: it aborts the exec in the
        # agent, leaving the run running with no process behind it.
        ({"blob": "a\x00b"}, {}),
        ({"a\x00b": "x"}, {}),
        # An empty name would give a bare "APSIS_ARG_".
        ({"": "x"}, {}),
        # Too long for a single env string.
        ({"blob": "x" * (1 << 17)}, {}),
        # The kernel limit is on bytes, so a value of multi-byte characters is
        # over it well before len() is.
        ({"blob": "é" * ((1 << 16) + 1)}, {}),
        # A multi-byte name counts too.
        ({"é" * ((1 << 16) + 1): "x"}, {}),
        # Skipping one arg doesn't drop the others.
        ({"date": "ok", "a=b": "c"}, {"APSIS_ARG_date": "ok"}),
    ],
)
def test_run_args_unrepresentable_skipped(args, expected):
    """
    An arg that can't be an environment variable is dropped, not allowed to
    break the process.  APSIS_RUN_ID is always set.
    """
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})

    env = _spec_env(bound, args)
    assert env == {"APSIS_RUN_ID": "r123", **expected}


@pytest.mark.parametrize("char", ["x", "é", "中"])
def test_run_args_length_limit_boundary(char):
    """
    An arg whose encoded `NAME=value` just fits is passed; one byte more is not.
    Parametrized over 1-, 2- and 3-byte characters, since the limit is on bytes.
    """
    bound = ProcstarProgram(argv=["/usr/bin/echo", "hi"]).bind({})
    width = len(char.encode())
    room = (1 << 17) - len(b"APSIS_ARG_blob=") - 1

    fits = char * (room // width)
    assert _spec_env(bound, {"blob": fits})["APSIS_ARG_blob"] == fits

    over = char * (room // width + 1)
    assert "APSIS_ARG_blob" not in _spec_env(bound, {"blob": over})


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
