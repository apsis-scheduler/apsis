import pytest
from signal import Signals

from apsis.program import Program, ProgramSuccess

# -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_program():
    program = Program.from_jso(
        {
            "type": "apsis.program.process.ProcessProgram",
            "argv": ["/usr/bin/echo", "Hello, {{ name }}!"],
        }
    )
    program = program.bind({"name": "world"})

    running = program.run("testrun", cfg={})
    async for update in running.updates:
        pass

    assert isinstance(update, ProgramSuccess)
    assert update.meta["return_code"] == 0
    output = update.outputs["output"]
    assert output.data == b"Hello, world!\n"


@pytest.mark.asyncio
async def test_shell_command_program():
    program = Program.from_jso(
        {
            "type": "apsis.program.process.ShellCommandProgram",
            "command": "echo 'Hello, {{ name }}!'",
        }
    )
    program = program.bind({"name": "world"})

    running = program.run("testrun", cfg={})
    async for update in running.updates:
        pass

    assert isinstance(update, ProgramSuccess)
    assert update.meta["return_code"] == 0
    output = update.outputs["output"]
    assert output.data == b"Hello, world!\n"


def test_process_program_jso():
    program = Program.from_jso(
        {
            "type": "apsis.program.process.ProcessProgram",
            "argv": ["/usr/bin/echo", "Hello, {{ name }}!"],
            "stop": {"grace_period": 30},
        }
    )

    # JSO round trip.
    program = Program.from_jso(program.to_jso())
    assert list(program.argv) == ["/usr/bin/echo", "Hello, {{ name }}!"]
    assert program.stop.signal == "SIGTERM"  # default
    assert program.stop.grace_period == 30

    # Bind and do it again.
    program = program.bind({"name": "Bob"})
    program = Program.from_jso(program.to_jso())
    assert list(program.argv) == ["/usr/bin/echo", "Hello, Bob!"]
    assert program.stop.signal == Signals.SIGTERM  # default
    assert program.stop.grace_period == 30


def test_shell_command_program_jso():
    program = Program.from_jso(
        {
            "type": "apsis.program.process.ShellCommandProgram",
            "command": "echo 'Hello, {{ name }}!'",
            "stop": {"grace_period": 30},
        }
    )

    # JSO round trip.
    program = Program.from_jso(program.to_jso())
    assert program.command == "echo 'Hello, {{ name }}!'"
    assert program.stop.signal == "SIGTERM"  # default
    assert program.stop.grace_period == 30

    # Bind and do it again.
    program = program.bind({"name": "Bob"})
    program = Program.from_jso(program.to_jso())
    assert "echo 'Hello, Bob!'" in program.argv[2]
    assert program.stop.signal == Signals.SIGTERM  # default
    assert program.stop.grace_period == 30
