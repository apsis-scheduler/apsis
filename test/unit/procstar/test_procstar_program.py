from   signal import Signals

from   apsis.program import Program

#-------------------------------------------------------------------------------

def test_process_program_jso():
    program = Program.from_jso({
        "type"      : "apsis.program.procstar.agent.ProcstarProgram",
        "argv"      : ["/usr/bin/echo", "Hello, {{ name }}!"],
        "stop"      : {"signal": "SIGUSR1"},
        "group_id"  : "prod",
    })

    # JSO round trip.
    program = Program.from_jso(program.to_jso())
    assert list(program.argv) == ["/usr/bin/echo", "Hello, {{ name }}!"]
    assert program.group_id == "prod"
    assert program.sudo_user is None
    assert program.stop.signal == "SIGUSR1"
    assert program.stop.grace_period == "60"

    # Bind and do it again.
    program = program.bind({"name": "Bob"})
    program = Program.from_jso(program.to_jso())
    assert list(program.argv) == ["/usr/bin/echo", "Hello, Bob!"]
    assert program.group_id == "prod"
    assert program.sudo_user is None
    assert program.stop.signal == Signals.SIGUSR1
    assert program.stop.grace_period == 60


def test_shell_command_program_jso():
    program = Program.from_jso({
        "type"      : "apsis.program.procstar.agent.ProcstarShellProgram",
        "command"   : "echo 'Hello, {{ name }}!'",
        "sudo_user" : "produser",
    })

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


