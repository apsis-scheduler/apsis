from .base import (
    Program,
    Output,
    OutputMetadata,
    ProgramRunning,
    ProgramError,
    ProgramSuccess,
    ProgramFailure,
)

from .noop import NoOpProgram
from .process import ProcessProgram, ShellCommandProgram
from .procstar.agent import ProcstarProgram, ProcstarShellProgram

# -------------------------------------------------------------------------------

Program.TYPE_NAMES.set(NoOpProgram, "no-op")
Program.TYPE_NAMES.set(ProcstarProgram, "procstar")
Program.TYPE_NAMES.set(ProcstarShellProgram, "procstar-shell")
