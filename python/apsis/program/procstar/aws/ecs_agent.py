"""ProcstarECS program implementations for executing jobs on AWS ECS with Procstar agents."""

import logging
from typing import Any, Dict, Optional

import procstar.spec
from procstar.agent.exc import NoOpenConnectionInGroup
from procstar.agent.proc import Result

from apsis.lib.json import check_schema, ifkey
from apsis.lib.py import get_cfg, or_none
from apsis.program.base import (
    Program,
    ProgramError,
    ProgramRunning,
    ProgramUpdate,
    Timeout,
    get_global_runtime_timeout,
)
from apsis.program.process import BoundStop, Stop
from apsis.program.procstar.agent import BaseRunningProcstarProgram, _make_metadata
from apsis.runs import template_expand

from .ecs import ECSTaskManager

log = logging.getLogger(__name__)


class _BaseProcstarECSProgram(Program):
    """Base class for ECS Procstar programs with common functionality."""

    def __init__(
        self,
        *,
        stop=Stop(),
        timeout=None,
        mem_gb=None,
        vcpu=None,
        disk_gb=None,
        role: Optional[str] = None,
    ):
        """Initialize base ECS program.

        Args:
            stop: Stop configuration for graceful shutdown
            timeout: Timeout configuration
            mem_gb: Memory limit in GiB for the ECS task container
            vcpu: CPU limit in vCPUs (e.g., 1.0 = 1 vCPU, 0.5 = half vCPU)
            disk_gb: Ephemeral storage in GiB for the ECS task
            role: Optional IAM role name to assume (role ARN will be constructed)
        """
        super().__init__()
        self.stop = stop
        self.timeout = timeout
        self.mem_gb = mem_gb
        self.vcpu = vcpu
        self.disk_gb = disk_gb
        self.role = role

    def _bind(self, argv, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        """Common logic for building a bound program."""
        expanded_mem_gb = None
        if self.mem_gb is not None:
            expanded_mem_gb = template_expand(str(self.mem_gb), args)
            expanded_mem_gb = float(expanded_mem_gb) if expanded_mem_gb else None

        expanded_vcpu = None
        if self.vcpu is not None:
            expanded_vcpu = template_expand(str(self.vcpu), args)
            expanded_vcpu = float(expanded_vcpu) if expanded_vcpu else None

        expanded_disk_gb = None
        if self.disk_gb is not None:
            expanded_disk_gb = template_expand(str(self.disk_gb), args)
            expanded_disk_gb = int(expanded_disk_gb) if expanded_disk_gb else None

        return BoundProcstarECSProgram(
            argv,
            stop=self.stop.bind(args),
            timeout=None if self.timeout is None else self.timeout.bind(args),
            mem_gb=expanded_mem_gb,
            vcpu=expanded_vcpu,
            disk_gb=expanded_disk_gb,
            role=or_none(template_expand)(self.role, args),
        )

    def _base_to_jso(self):
        """Common to_jso logic for ECS programs."""
        jso = super().to_jso() | ifkey("stop", self.stop.to_jso(), {})
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        if self.mem_gb is not None:
            jso["mem_gb"] = self.mem_gb
        if self.vcpu is not None:
            jso["vcpu"] = self.vcpu
        if self.disk_gb is not None:
            jso["disk_gb"] = self.disk_gb
        if self.role is not None:
            jso["role"] = self.role
        return jso

    @staticmethod
    def _from_jso(pop):
        """Parse ECS-specific fields."""
        return dict(
            stop=pop("stop", Stop.from_jso, Stop()),
            timeout=pop("timeout", Timeout.from_jso, None),
            mem_gb=pop("mem_gb", default=None),
            vcpu=pop("vcpu", default=None),
            disk_gb=pop("disk_gb", default=None),
            role=pop("role", default=None),
        )


class ProcstarECSShellProgram(_BaseProcstarECSProgram):
    """Program that executes shell commands on AWS ECS using Procstar agents.

    This is similar to ProcstarECSProgram but uses a shell command string instead of argv.
    The command is executed via SHELL -c.
    """

    SHELL = "/usr/bin/bash"

    def __init__(self, command: str, **kwargs):
        """Initialize ProcstarECSShellProgram with a shell command."""
        super().__init__(**kwargs)
        self.command = command

    def bind(self, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        """Bind template parameters in the program configuration."""
        argv = [self.SHELL, "-c", template_expand(self.command, args)]
        return self._bind(argv, args)

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command = pop("command")
            kw_args = cls._from_jso(pop)
        return cls(command=command, **kw_args)

    def to_jso(self):
        jso = self._base_to_jso()
        jso["command"] = self.command
        return jso


class ProcstarECSProgram(_BaseProcstarECSProgram):
    """Program that executes commands on AWS ECS using Procstar agents with argv.

    This program launches an ECS task that runs a Procstar agent, then
    connects to that agent via websocket to execute the specified command.
    This follows the proper Procstar architecture for distributed job execution.
    """

    def __init__(self, argv, **kwargs):
        """Initialize ProcstarECSProgram with argv list."""
        super().__init__(**kwargs)
        self.argv = [str(a) for a in argv]

    def bind(self, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        """Bind template parameters in the program configuration."""
        argv = tuple(template_expand(a, args) for a in self.argv)
        return self._bind(argv, args)

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            kw_args = cls._from_jso(pop)
        return cls(argv=argv, **kw_args)

    def to_jso(self):
        jso = self._base_to_jso()
        jso["argv"] = self.argv
        return jso


# -------------------------------------------------------------------------------


class BoundProcstarECSProgram(Program):
    """Bound version of ProcstarECSProgram with template parameters resolved."""

    def __init__(
        self,
        argv,
        *,
        stop=BoundStop(),
        timeout=None,
        mem_gb: Optional[float] = None,
        vcpu: Optional[float] = None,
        disk_gb: Optional[int] = None,
        role: Optional[str] = None,
    ):
        self.argv = [str(a) for a in argv]
        self.stop = stop
        self.timeout = timeout
        self.mem_gb = mem_gb
        self.vcpu = vcpu
        self.disk_gb = disk_gb
        self.role = role

    def __str__(self):
        return self.argv[2] if len(self.argv) >= 3 else " ".join(self.argv)

    def to_jso(self):
        jso = (
            super().to_jso()
            | {"argv": self.argv}
            | ifkey("stop", self.stop.to_jso(), {})
            | ifkey("mem_gb", self.mem_gb, None)
            | ifkey("vcpu", self.vcpu, None)
            | ifkey("disk_gb", self.disk_gb, None)
            | ifkey("role", self.role, None)
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            stop = pop("stop", BoundStop.from_jso, BoundStop())
            timeout = pop("timeout", Timeout.from_jso, None)
            mem_gb = pop("mem_gb", default=None)
            vcpu = pop("vcpu", default=None)
            disk_gb = pop("disk_gb", default=None)
            role = pop("role", default=None)
        return cls(
            argv,
            stop=stop,
            timeout=timeout,
            mem_gb=mem_gb,
            vcpu=vcpu,
            disk_gb=disk_gb,
            role=role,
        )

    def run(self, run_id: str, cfg) -> "RunningProcstarECSProgram":
        return RunningProcstarECSProgram(run_id, self, cfg)

    def connect(self, run_id: str, run_state: Dict, cfg) -> "RunningProcstarECSProgram":
        return RunningProcstarECSProgram(run_id, self, cfg, run_state)


class RunningProcstarECSProgram(BaseRunningProcstarProgram):
    """Running instance of ProcstarECS programs using websocket communication with Procstar agents."""

    def __init__(
        self,
        run_id: str,
        program: BoundProcstarECSProgram,
        cfg,
        run_state: Optional[Dict] = None,
    ):
        if hasattr(program, "timeout") and program.timeout is None:
            program.timeout = get_global_runtime_timeout(cfg)
        super().__init__(run_id, program, cfg, run_state)

        ecs_cfg = get_cfg(cfg, "procstar.agent.ecs", None)
        if ecs_cfg is None:
            raise ValueError("procstar.agent.ecs config section is required to run ECS programs")

        self.cluster_name = ecs_cfg["cluster_name"]
        self.container_name = ecs_cfg["container_name"]
        self.task_definition = ecs_cfg["task_definition"]
        self.region = ecs_cfg["region"]
        self.capacity_provider = ecs_cfg["capacity_provider"]
        self.log_group = ecs_cfg["log_group"]
        self.log_stream_prefix = ecs_cfg["log_stream_prefix"]
        self.aws_account_number = ecs_cfg["aws_account_number"]
        ebs_volume_role = ecs_cfg["ebs_volume_role"]
        ebs_volume_role_arn = f"arn:aws:iam::{self.aws_account_number}:role/{ebs_volume_role}"
        self.iam_role_arn = None
        if self.program.role:
            self.iam_role_arn = f"arn:aws:iam::{self.aws_account_number}:role/{self.program.role}"

        # Resource defaults from config
        default_mem_gb = ecs_cfg["default_mem_gb"]
        default_vcpu = ecs_cfg["default_vcpu"]
        default_disk_gb = ecs_cfg["default_disk_gb"]

        self.task_arn: Optional[str] = None
        self.ecs_manager = ECSTaskManager(
            self.cluster_name,
            self.region,
            ebs_volume_role_arn,
            self.container_name,
            self.capacity_provider,
            default_mem_gb=default_mem_gb,
            default_vcpu=default_vcpu,
            default_disk_gb=default_disk_gb,
        )
        self.proc_id = None
        self.conn_id = None
        self.unique_group_id = f"aws-ecs-{self.run_id}"

    def _get_base_metadata(self):
        task_arn = getattr(self, "task_arn", None)
        task_id = task_arn.split("/")[-1] if task_arn else None
        log_stream = f"{self.log_stream_prefix}/{task_id}" if task_id else None

        return {
            "aws_ecs": {
                "account": self.aws_account_number,
                "region": self.region,
                "cluster_name": self.cluster_name,
                "capacity_provider": self.capacity_provider,
                "task_definition": self.task_definition,
                "task_id": task_id,
                "container_name": self.container_name,
                "log_group": self.log_group,
                "log_stream": log_stream,
            },
            "proc_id": getattr(self, "proc_id", None),
            "conn_id": getattr(self, "conn_id", None),
        }

    @property
    def _spec(self):
        return procstar.spec.Proc(
            self.program.argv,
            env=procstar.spec.Proc.Env(
                vars={
                    "APSIS_RUN_ID": self.run_id,
                },
                # Inherit the entire environment from procstar
                inherit=True,
            ),
            fds={
                # Capture stdout to a temporary file
                "stdout": procstar.spec.Proc.Fd.Capture(
                    "tempfile",
                    encoding=None,
                    # Don't attach output to results, so we can poll quickly
                    attached=False,
                ),
                # Merge stderr into stdout
                "stderr": procstar.spec.Proc.Fd.Dup(1),
            },
        )

    async def _start_new_execution(self, update_interval, output_interval):
        """Start new execution by launching ECS task and connecting to agent."""
        try:
            log.info(f"Launching ECS task for run {self.run_id} on cluster {self.cluster_name}")
            env_vars = {
                "APSIS_RUN_ID": self.run_id,
                "PROCSTAR_GROUP_ID": self.unique_group_id,
            }
            self.task_arn = await self.ecs_manager.start_task(
                task_definition=self.task_definition,
                environment_overrides=env_vars,
                mem_gb=self.program.mem_gb,
                vcpu=self.program.vcpu,
                disk_gb=self.program.disk_gb,
                tags=[{"key": "apsis-run-id", "value": self.run_id}],
                task_role_arn=self.iam_role_arn,
            )
            log.info(f"ECS task started with ARN: {self.task_arn}")
            yield ProgramUpdate(meta=self._get_base_metadata())

            # Connect to agent
            start, res = await self._start_proc_on_agent(
                self.unique_group_id,
                extra_run_state={
                    "task_arn": self.task_arn,
                    "group_id": self.unique_group_id,
                },
            )

            yield ProgramRunning(run_state=self.run_state, meta=self._get_result_metadata(res))

            async for update in self._monitor_procstar_execution(
                start, update_interval, output_interval
            ):
                yield update

        except NoOpenConnectionInGroup:
            task_id = self.task_arn.split("/")[-1] if self.task_arn else "unknown"
            msg = f"on-demand ECS (task {task_id}) Procstar agent didn't connect in time."
            log.error(msg)
            meta = self._get_base_metadata()
            meta["error"] = msg
            yield ProgramError(msg, meta=meta)

        except Exception as exc:
            log.error(f"Error in _start_new_execution for run {self.run_id}", exc_info=True)
            meta = self._get_base_metadata()
            meta["error"] = f"Failed to start execution: {exc}"
            meta["error_type"] = type(exc).__name__
            yield ProgramError(f"Failed to start execution: {exc}", meta=meta)

    async def _restore_reconnection_state(self):
        """Restore state needed for reconnection."""
        await super()._restore_reconnection_state()
        self.task_arn = self.run_state.get("task_arn")
        # Restore the unique group_id for reconnection (fallback to run-specific ID for older runs)
        self.unique_group_id = self.run_state.get("group_id", f"aws-ecs-{self.run_id}")

    async def stop(self):
        """Stop the running program gracefully."""
        # Call parent stop method first - this sends signals to terminate the process.
        # Once the process dies and the proc is deleted, the Procstar agent will
        # exit immediately (via --wait mode), which terminates the ECS task.
        await super().stop()

    def _get_result_metadata(self, res: Result):
        """Override to include ECS-specific metadata."""
        meta = super()._get_result_metadata(res)
        meta.update(self._get_base_metadata())
        meta["proc_status"] = res.state
        return meta

    # Note: _cleanup is inherited from BaseRunningProcstarProgram.
    # We don't explicitly stop the ECS task - the Procstar agent is configured
    # with --wait and --wait-timeout, so it will automatically exit when idle
    # (after proc deletion or if no work is assigned).
