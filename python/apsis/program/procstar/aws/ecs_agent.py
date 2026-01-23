"""ProcstarECSProgram implementation for executing jobs on AWS ECS with Procstar agents."""

import logging
import uuid
from typing import Any, Dict, Optional

import ora
import procstar.spec
from procstar.agent.exc import (
    NoOpenConnectionInGroup,
)
from procstar.agent.proc import Result

from apsis.lib.json import check_schema, ifkey
from apsis.lib.parse import nparse_duration
from apsis.lib.py import get_cfg, nstr, or_none
from apsis.procstar import get_agent_server
from apsis.program.base import (
    Program,
    ProgramError,
    ProgramRunning,
    ProgramUpdate,
    Timeout,
    get_global_runtime_timeout,
)
from apsis.program.process import BoundStop
from apsis.program.procstar.agent import BoundResources, _ProcstarProgram
from apsis.program.procstar.base import BaseRunningProcstarProgram
from apsis.runs import template_expand

from .ecs import ECSTaskManager

logger = logging.getLogger(__name__)


class _BaseProcstarECSProgram(_ProcstarProgram):
    """Base class for ECS Procstar programs with common functionality."""

    def __init__(
        self,
        *,
        environment: Optional[Dict[str, str]] = None,
        memory=None,
        cpu=None,
        disk_space=None,
        role: Optional[str] = None,
        **kwargs,
    ):
        """Initialize base ECS program.

        Args:
            environment: Additional environment variables
            memory: Memory limit in MiB for the ECS task container
            cpu: CPU limit in CPU units (1 vCPU = 1024 units) for the ECS task container
            disk_space: Ephemeral storage in GiB for the ECS task
            role: Optional IAM role name to assume (role ARN will be constructed)
            **kwargs: Passed to _ProcstarProgram (timeout, stop, resources)
        """
        kwargs.pop("group_id", None)
        super().__init__(group_id=None, **kwargs)
        self.environment = environment or {}
        self.memory = memory
        self.cpu = cpu
        self.disk_space = disk_space
        self.role = role

    def _build_bound_program(
        self, argv, args: Dict[str, Any]
    ) -> "BoundProcstarECSProgram":
        """Common logic for building a bound program."""
        # Expand memory, CPU, and disk_space templates, convert to int if not None
        expanded_memory = None
        if self.memory is not None:
            expanded_memory = template_expand(str(self.memory), args)
            expanded_memory = int(expanded_memory) if expanded_memory else None

        expanded_cpu = None
        if self.cpu is not None:
            expanded_cpu = template_expand(str(self.cpu), args)
            expanded_cpu = int(expanded_cpu) if expanded_cpu else None

        expanded_disk_space = None
        if self.disk_space is not None:
            expanded_disk_space = template_expand(str(self.disk_space), args)
            expanded_disk_space = (
                int(expanded_disk_space) if expanded_disk_space else None
            )

        return BoundProcstarECSProgram(
            argv,
            environment={
                k: template_expand(v, args) for k, v in self.environment.items()
            },
            sudo_user=or_none(template_expand)(self.sudo_user, args),
            stop=self.stop.bind(args),
            timeout=None if self.timeout is None else self.timeout.bind(args),
            resources=self.resources.bind(args),
            memory=expanded_memory,
            cpu=expanded_cpu,
            disk_space=expanded_disk_space,
            role=or_none(template_expand)(self.role, args),
        )

    @classmethod
    def _check_group_id(cls, jso):
        """Check that group_id is not set for ECS programs."""
        if "group_id" in jso:
            raise ValueError(
                "group_id cannot be set for procstar-ecs programs. "
                "ECS tasks require isolated execution - each run automatically gets a unique group ID "
                "in the format 'aws-ecs-{run_id}' to ensure proper isolation and security."
            )

    def _base_to_jso(self):
        """Common to_jso logic for ECS programs."""
        parent_jso = super().to_jso()
        parent_jso.pop("group_id", None)

        jso = {
            **parent_jso,
            "environment": self.environment,
        }
        if self.memory is not None:
            jso["memory"] = self.memory
        if self.cpu is not None:
            jso["cpu"] = self.cpu
        if self.disk_space is not None:
            jso["disk_space"] = self.disk_space
        if self.role is not None:
            jso["role"] = self.role
        return jso

    def run(self, run_id: str, cfg) -> "RunningProcstarECSProgram":
        return RunningProcstarECSProgram(run_id, self, cfg)

    def connect(self, run_id: str, run_state: Dict, cfg) -> "RunningProcstarECSProgram":
        return RunningProcstarECSProgram(run_id, self, cfg, run_state)


class ProcstarECSProgram(_BaseProcstarECSProgram):
    """Program that executes commands on AWS ECS using Procstar agents.

    This program launches an ECS task that runs a Procstar agent, then
    connects to that agent via websocket to execute the specified command.
    This follows the proper Procstar architecture for distributed job execution.
    """

    def __init__(self, command: str, **kwargs):
        """Initialize ProcstarECSProgram with a shell command."""
        super().__init__(**kwargs)
        self.command = command

    def bind(self, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        """Bind template parameters in the program configuration."""
        argv = ["/usr/bin/bash", "-c", template_expand(self.command, args)]
        return self._build_bound_program(argv, args)

    @classmethod
    def from_jso(cls, jso):
        cls._check_group_id(jso)
        with check_schema(jso) as pop:
            command = pop("command")
            environment = pop("environment", default={})
            memory = pop("memory", default=None)
            cpu = pop("cpu", default=None)
            disk_space = pop("disk", default=None)
            role = pop("role", default=None)
            kw_args = cls._from_jso(pop)

        return cls(
            command=command,
            environment=environment,
            memory=memory,
            cpu=cpu,
            disk_space=disk_space,
            role=role,
            **kw_args,
        )

    def to_jso(self):
        jso = self._base_to_jso()
        jso["command"] = self.command
        return jso


class ProcstarECSArgvProgram(_BaseProcstarECSProgram):
    """Program that executes commands on AWS ECS using Procstar agents with argv.

    This is similar to ProcstarECSProgram but uses argv directly instead of a shell command.
    Useful for restricted execution environments where the executable must be called directly.
    """

    def __init__(self, argv, **kwargs):
        """Initialize ProcstarECSArgvProgram with argv list."""
        super().__init__(**kwargs)
        self.argv = [str(a) for a in argv]

    def bind(self, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        """Bind template parameters in the program configuration."""
        argv = tuple(template_expand(a, args) for a in self.argv)
        return self._build_bound_program(argv, args)

    @classmethod
    def from_jso(cls, jso):
        cls._check_group_id(jso)
        with check_schema(jso) as pop:
            argv = pop("argv")
            environment = pop("environment", default={})
            memory = pop("memory", default=None)
            cpu = pop("cpu", default=None)
            disk_space = pop("disk", default=None)
            role = pop("role", default=None)
            kw_args = cls._from_jso(pop)

        return cls(
            argv=argv,
            environment=environment,
            memory=memory,
            cpu=cpu,
            disk_space=disk_space,
            role=role,
            **kw_args,
        )

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
        environment: Optional[Dict[str, str]] = None,
        sudo_user=None,
        stop=BoundStop(),
        timeout=None,
        resources=BoundResources(),
        memory: Optional[int] = None,
        cpu: Optional[int] = None,
        disk_space: Optional[int] = None,
        role: Optional[str] = None,
    ):
        self.argv = [str(a) for a in argv]
        self.environment = environment or {}
        self.group_id = None
        self.sudo_user = nstr(sudo_user)
        self.stop = stop
        self.timeout = timeout
        self.resources = resources
        self.memory = memory
        self.cpu = cpu
        self.disk_space = disk_space
        self.role = role

    def __str__(self):
        return self.argv[2] if len(self.argv) >= 3 else " ".join(self.argv)

    def to_jso(self):
        jso = (
            super().to_jso()
            | {
                "argv": self.argv,
                "environment": self.environment,
            }
            | ifkey("sudo_user", self.sudo_user, None)
            | ifkey("resources", self.resources.to_jso(), {})
            | ifkey("stop", self.stop.to_jso(), {})
            | ifkey("memory", self.memory, None)
            | ifkey("cpu", self.cpu, None)
            | ifkey("disk_space", self.disk_space, None)
            | ifkey("role", self.role, None)
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            environment = pop("environment", default={})
            pop("group_id", default=None)
            sudo_user = pop("sudo_user", default=None)
            stop = pop("stop", BoundStop.from_jso, BoundStop())
            timeout = pop("timeout", Timeout.from_jso, None)
            resources = pop("resources", BoundResources.from_jso, BoundResources())
            memory = pop("memory", default=None)
            cpu = pop("cpu", default=None)
            disk_space = pop("disk_space", default=None)
            role = pop("role", default=None)
        return cls(
            argv,
            environment=environment,
            sudo_user=sudo_user,
            stop=stop,
            timeout=timeout,
            resources=resources,
            memory=memory,
            cpu=cpu,
            disk_space=disk_space,
            role=role,
        )

    def run(self, run_id: str, cfg) -> "RunningProcstarECSProgram":
        return RunningProcstarECSProgram(run_id, self, cfg)

    def connect(self, run_id: str, run_state: Dict, cfg) -> "RunningProcstarECSProgram":
        return RunningProcstarECSProgram(run_id, self, cfg, run_state)


class RunningProcstarECSProgram(BaseRunningProcstarProgram):
    """Running instance of ProcstarECSProgram using websocket communication with Procstar agents."""

    def __init__(
        self,
        run_id: str,
        program,  # Can be ProcstarECSProgram or BoundProcstarECSProgram
        cfg,
        run_state: Optional[Dict] = None,
    ):
        if hasattr(program, "timeout") and program.timeout is None:
            program.timeout = get_global_runtime_timeout(cfg)
        super().__init__(run_id, program, cfg, run_state)

        ecs_cfg = get_cfg(cfg, "procstar.agent.ecs", None)
        if ecs_cfg is None:
            raise ValueError(
                "procstar.agent.ecs config section is required to run ECS programs"
            )

        self.cluster_name = ecs_cfg["cluster_name"]
        self.container_name = ecs_cfg["container_name"]
        self.task_definition = ecs_cfg["task_definition"]
        self.region = ecs_cfg["region"]
        self.capacity_provider = ecs_cfg["capacity_provider"]
        self.log_group = ecs_cfg["log_group"]
        self.log_stream_prefix = ecs_cfg["log_stream_prefix"]
        self.aws_account_number = ecs_cfg["aws_account_number"]
        ebs_volume_role = ecs_cfg["ebs_volume_role"]
        self.task_startup_timeout = ecs_cfg["task_startup_timeout"]
        default_disk_space_gb = ecs_cfg["default_disk_space_gb"]
        ebs_volume_role_arn = (
            f"arn:aws:iam::{self.aws_account_number}:role/{ebs_volume_role}"
        )
        self.iam_role_arn = None
        if self.program.role:
            self.iam_role_arn = (
                f"arn:aws:iam::{self.aws_account_number}:role/{self.program.role}"
            )

        self.task_arn: Optional[str] = None
        self.ecs_manager = ECSTaskManager(
            self.cluster_name,
            self.region,
            default_disk_space_gb,
            ebs_volume_role_arn,
            self.container_name,
            self.capacity_provider,
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
        # Handle both unbound and bound programs
        if hasattr(self.program, "command"):
            # Unbound ProcstarECSProgram
            argv = ["/usr/bin/bash", "-c", self.program.command]
        else:
            # Bound ProcstarECSProgram - argv is already prepared
            argv = self.program.argv

        return procstar.spec.Proc(
            argv,
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
            logger.info(
                f"Launching ECS task for run {self.run_id} on cluster {self.cluster_name}"
            )
            yield ProgramUpdate(meta=self._get_base_metadata())

            # Prepare environment for the ECS task
            env_vars = dict(self.program.environment)
            env_vars["APSIS_RUN_ID"] = self.run_id
            env_vars["PROCSTAR_GROUP_ID"] = (
                self.unique_group_id
            )  # Set unique group for this run
            self.proc_id = str(uuid.uuid4())
            self.task_arn = await self.ecs_manager.start_task(
                task_definition=self.task_definition,
                environment_overrides=env_vars,
                memory=self.program.memory,
                cpu=self.program.cpu,
                disk_space=self.program.disk_space,
                tags=[{"key": "apsis-run-id", "value": self.run_id}],
                task_role_arn=self.iam_role_arn,
            )
            logger.info(f"ECS task started with ARN: {self.task_arn}")

            yield ProgramUpdate(meta=self._get_base_metadata())

            # Wait for ECS task to reach RUNNING state
            def task_progress_callback(status, elapsed, timeout):
                logger.debug(
                    f"ECS task status: {status} (elapsed: {elapsed:.0f}s)"
                )

            task_running = await self.ecs_manager.wait_for_task_status(
                self.task_arn,
                "RUNNING",
                timeout=self.task_startup_timeout,
                progress_callback=task_progress_callback,
            )

            if not task_running:
                logger.error(
                    f"ECS task failed to reach RUNNING state within {self.task_startup_timeout}s"
                )
                meta = self._get_base_metadata()
                meta["error"] = "ECS task failed to reach RUNNING state"
                yield ProgramError("ECS task failed to start", meta=meta)
                return

            logger.info(f"ECS task is now RUNNING: {self.task_arn}")
            yield ProgramUpdate(meta=self._get_base_metadata())

            # Wait for agent to connect to Apsis and start the process
            conn_timeout = get_cfg(self.cfg, "connection.start_timeout", 0)
            conn_timeout = (
                nparse_duration(conn_timeout) or 300
            )  # 5 minutes for agent connection

            try:
                self.proc, res = await get_agent_server().start(
                    proc_id=self.proc_id,
                    group_id=self.unique_group_id,  # Use unique group_id for perfect isolation
                    spec=self._spec,
                    conn_timeout=conn_timeout,
                )
            except NoOpenConnectionInGroup as exc:
                msg = f"start failed: {self.proc_id}: {exc} (agent may not have connected yet)"
                logger.error(msg)
                meta = self._get_base_metadata()
                meta["error"] = "Failed to start process on agent"
                yield ProgramError(msg, meta=meta)
                return

            self.conn_id = self.proc.conn_id
            start = ora.now()
            self.run_state = {
                "conn_id": self.conn_id,
                "proc_id": self.proc_id,
                "task_arn": self.task_arn,
                "group_id": self.unique_group_id,
                "start": str(start),
            }

            meta = self._get_base_metadata()
            meta["proc_status"] = "running"
            yield ProgramRunning(run_state=self.run_state, meta=meta)

            async for update in self._monitor_procstar_execution(
                start, update_interval, output_interval
            ):
                yield update

        except Exception as exc:
            logger.error(
                f"Error in _start_new_execution for run {self.run_id}", exc_info=True
            )
            meta = self._get_base_metadata()
            meta["error"] = f"Failed to start execution: {exc}"
            meta["error_type"] = type(exc).__name__
            yield ProgramError(f"Failed to start execution: {exc}", meta=meta)

    async def _restore_reconnection_state(self):
        """Restore state needed for reconnection."""
        self.conn_id = self.run_state["conn_id"]
        self.proc_id = self.run_state["proc_id"]
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

    async def _cleanup(self):
        """Cleanup resources."""
        # Delete the proc
        if self.proc is not None:
            try:
                await self.proc.delete()
            except Exception as exc:
                logger.error(f"delete {self.proc.proc_id}: {exc}")
            self.proc = None

        # Don't explicitly stop the ECS task - the Procstar agent is configured
        # with --wait and --wait-timeout, so it will automatically exit when idle
        # (after proc deletion or if no work is assigned).
