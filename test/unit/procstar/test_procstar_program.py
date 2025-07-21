from signal import Signals

import pytest

import apsis.program.procstar.agent
from test.unit.util import create_fddata, create_running_result, create_success_result
from apsis.exc import SchemaError
from apsis.program import Program
from apsis.program.procstar.agent import RunningProcstarProgram, ProcstarProgram
from apsis.program.base import (
    ProgramSuccess,
    ProgramFailure,
    ProgramError,
    ProgramRunning,
)

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


@pytest.mark.asyncio
async def test_final_fddata_normal_case(mock_proc):
    """
    Test the normal case where final FdData arrives properly. This should complete
    successfully with a ProgramSuccess result.
    """
    program = ProcstarProgram(argv=["/bin/echo", "test"])
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

    program = ProcstarProgram(argv=["/bin/echo", "test"])
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
