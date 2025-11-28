from .base import (
    Program,
    Output,
    OutputMetadata,
    ProgramRunning,
    ProgramError,
    ProgramSuccess,
    ProgramFailure,
)

from .agent import AgentProgram, AgentShellProgram
from .noop import NoOpProgram
from .procstar.agent import ProcstarProgram, ProcstarShellProgram

# -------------------------------------------------------------------------------

Program.TYPE_NAMES.set(NoOpProgram, "no-op")
Program.TYPE_NAMES.set(AgentProgram, "program")
Program.TYPE_NAMES.set(AgentShellProgram, "shell")
Program.TYPE_NAMES.set(ProcstarProgram, "procstar")
Program.TYPE_NAMES.set(ProcstarShellProgram, "procstar-shell")
