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
from .procstar.agent import ProcstarProgram, ProcstarShellProgram
from .procstar.aws.ecs_agent import ProcstarECSProgram

# -------------------------------------------------------------------------------

Program.TYPE_NAMES.set(NoOpProgram, "no-op")
Program.TYPE_NAMES.set(ProcstarProgram, "procstar")
Program.TYPE_NAMES.set(ProcstarShellProgram, "procstar-shell")
Program.TYPE_NAMES.set(ProcstarECSProgram, "procstar-ecs")
