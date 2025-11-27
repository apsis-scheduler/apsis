"""ProcstarECSProgram implementation for executing jobs on AWS ECS with Procstar agents."""

import logging
import uuid
from typing import Dict, Optional, Any

import ora

from apsis.lib.py import get_cfg
from apsis.lib.json import check_schema
from apsis.lib.parse import nparse_duration
from apsis.program.base import (
    Program,
    ProgramRunning,
    ProgramError,
    ProgramUpdate,
    Timeout,
    get_global_runtime_timeout,
)
from apsis.program.procstar.agent import _ProcstarProgram, BoundResources
from apsis.program.procstar.base import BaseRunningProcstarProgram
from apsis.program.process import BoundStop
from apsis.lib.py import nstr, or_none
from apsis.lib.json import ifkey
from .ecs import ECSTaskManager
from apsis.procstar import get_agent_server
from procstar.agent.exc import (
    NoOpenConnectionInGroup,
)
import procstar.spec
import procstar.proto
from procstar.agent.proc import Result
from apsis.runs import template_expand


logger = logging.getLogger(__name__)


class ProcstarECSProgram(_ProcstarProgram):
    """Program that executes commands on AWS ECS using Procstar agents.

    This program launches an ECS task that runs a Procstar agent, then
    connects to that agent via websocket to execute the specified command.
    This follows the proper Procstar architecture for distributed job execution.
    """

    def __init__(
        self,
        command: str,
        *,
        environment: Optional[Dict[str, str]] = None,
        memory = None,
        cpu = None,
        disk_space = None,
        role: Optional[str] = None,
        **kwargs,
    ):
        """Initialize ProcstarECSProgram.

        Args:
            command: Command to execute via Procstar agent
            environment: Additional environment variables
            memory: Memory limit in MiB for the ECS task container
            cpu: CPU limit in CPU units (1 vCPU = 1024 units) for the ECS task container
            disk_space: Ephemeral storage in GiB for the ECS task
            role: Optional IAM role name to assume (role ARN will be constructed)
            **kwargs: Passed to _ProcstarProgram (timeout, stop, resources)
                     Note: group_id is ignored for ECS - each run gets a unique group
        """
        # Remove group_id from kwargs if provided - we'll set our own
        kwargs.pop('group_id', None)
        # Always use a placeholder - the actual group will be set at runtime
        super().__init__(group_id="ECS_DYNAMIC", **kwargs)
        self.command = command
        self.environment = environment or {}
        self.memory = memory
        self.cpu = cpu
        self.disk_space = disk_space
        self.role = role

    def bind(self, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        """Bind template parameters in the program configuration."""
        argv = ["/usr/bin/bash", "-c", template_expand(self.command, args)]
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
            expanded_disk_space = int(expanded_disk_space) if expanded_disk_space else None

        return BoundProcstarECSProgram(
            argv,
            environment={
                k: template_expand(v, args) for k, v in self.environment.items()
                },
            group_id="ECS_DYNAMIC",  # Always use placeholder - runtime will set actual group
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
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command = pop("command")
            environment = pop("environment", default={})
            memory = pop("memory", default=None)
            cpu = pop("cpu", default=None)
            disk_space = pop("disk", default=None)
            role = pop("role", default=None)
            kw_args = cls._from_jso(pop)
            # Remove group_id if present - ECS always uses dynamic groups
            if 'group_id' in kw_args:
                logger.warning(f"Ignoring group_id '{kw_args['group_id']}' in ProcstarECSProgram - each run uses a unique group")
                kw_args.pop('group_id')

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
        jso = {
            **super().to_jso(),
            "command": self.command,
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


# -------------------------------------------------------------------------------


class BoundProcstarECSProgram(Program):
    """Bound version of ProcstarECSProgram with template parameters resolved."""

    def __init__(
        self,
        argv,
        *,
        environment: Optional[Dict[str, str]] = None,
        group_id,
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
        self.group_id = str(group_id)
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
                "group_id": self.group_id,
            }
            | ifkey("sudo_user", self.sudo_user, None)
            | ifkey("resources", self.resources.to_jso(), {})
            | ifkey("stop", self.stop.to_jso(), {})
        )
        if self.timeout is not None:
            jso["timeout"] = self.timeout.to_jso()
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            argv = pop("argv")
            environment = pop("environment", default={})
            group_id = pop("group_id", default=procstar.proto.DEFAULT_GROUP)
            sudo_user = pop("sudo_user", default=None)
            stop = pop("stop", BoundStop.from_jso, BoundStop())
            timeout = pop("timeout", Timeout.from_jso, None)
            resources = pop("resources", BoundResources.from_jso, BoundResources())
        return cls(
            argv,
            environment=environment,
            group_id=group_id,
            sudo_user=sudo_user,
            stop=stop,
            timeout=timeout,
            resources=resources,
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

        ecs_cfg = get_cfg(cfg, "procstar.ecs", {})
        self.cluster_name = get_cfg(ecs_cfg, "cluster_name", None)
        self.task_definition = get_cfg(ecs_cfg, "task_definition", "run-procstar-agent")
        self.region = get_cfg(ecs_cfg, "region", "us-east-1")
        self.startup_timeout = get_cfg(ecs_cfg, "startup_timeout", 900)
        default_disk_space_gb = get_cfg(ecs_cfg, "default_disk_space_gb", 20)
        aws_account_number = get_cfg(ecs_cfg, "aws_account_number", None)
        ebs_volume_role = get_cfg(ecs_cfg, "ebs_volume_role", None)
        ebs_volume_role_arn = f"arn:aws:iam::{aws_account_number}:role/{ebs_volume_role}"
        
        # Construct IAM role ARN from role if specified
        self.iam_role_arn = None
        if self.program.role:
            if not aws_account_number:
                raise ValueError("aws_account_number must be configured in procstar.ecs config when using role parameter")
            self.iam_role_arn = f"arn:aws:iam::{aws_account_number}:role/{self.program.role}"

        self.task_arn: Optional[str] = None
        self.ecs_manager = ECSTaskManager(self.cluster_name, self.region, default_disk_space_gb, ebs_volume_role_arn)

        self.proc_id = None
        self.conn_id = None

        # Generate unique group_id for this run to ensure perfect isolation
        self.unique_group_id = f"ecs-run-{self.run_id}"

    def _get_base_metadata(self):
        return {
            "cluster_name": self.cluster_name,
            "task_definition": self.task_definition,
            "region": self.region,
            "task_arn": getattr(self, "task_arn", None),
            "proc_id": getattr(self, "proc_id", None),
            "conn_id": getattr(self, "conn_id", None),
            "agent_group": getattr(self, "unique_group_id", None),
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
        logger.info(f"Starting new execution for run {self.run_id}")

        try:
            # Start ECS task with Procstar agent
            logger.info(
                f"Launching ECS task for run {self.run_id} on cluster {self.cluster_name}"
                )
            meta = self._get_base_metadata()
            meta["message"] = "Launching ECS task with Procstar agent"
            yield ProgramUpdate(meta=meta)

            # Prepare environment for the ECS task
            env_vars = dict(self.program.environment)
            env_vars["APSIS_RUN_ID"] = self.run_id
            env_vars["PROCSTAR_GROUP_ID"] = self.unique_group_id  # Set unique group for this run
            # Generate unique proc ID
            self.proc_id = str(uuid.uuid4())
            logger.info(f"Generated proc_id {self.proc_id} for run {self.run_id}")

            # Start the ECS task (which runs the Procstar agent)
            logger.info(
                f"Starting ECS task with task_definition={self.task_definition}, "
                f"memory={self.program.memory}, cpu={self.program.cpu}, disk_space={self.program.disk_space}, "
                f"task_role={self.iam_role_arn if self.iam_role_arn else 'default'}"
            )
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

            meta = self._get_base_metadata()
            meta["message"] = f"ECS task started: {self.task_arn}"
            meta["task_status"] = "PENDING"
            yield ProgramUpdate(meta=meta)

            # Wait for ECS task to reach RUNNING state first
            logger.info(
                f"Waiting for ECS task to reach RUNNING state (timeout: {self.startup_timeout}s)"
            )

            # Create a simple progress callback for logging only
            def task_progress_callback(status, elapsed, timeout):
                remaining = timeout - elapsed
                logger.info(
                    f"ECS task status: {status} (elapsed: {elapsed:.0f}s, remaining: {remaining:.0f}s)"
                )

            task_running = await self.ecs_manager.wait_for_task_status(
                self.task_arn,
                "RUNNING",
                timeout=self.startup_timeout,
                progress_callback=task_progress_callback,
            )

            if not task_running:
                logger.error(
                    f"ECS task failed to reach RUNNING state within {self.startup_timeout}s"
                )
                meta = self._get_base_metadata()
                meta["error"] = "ECS task failed to reach RUNNING state"
                meta["task_status"] = "STOPPED"
                yield ProgramError("ECS task failed to start", meta=meta)
                return

            logger.info(f"ECS task is now RUNNING: {self.task_arn}")
            meta = self._get_base_metadata()
            meta["message"] = "ECS task is running, waiting for Procstar agent to connect"
            meta["task_status"] = "RUNNING"
            yield ProgramUpdate(meta=meta)

            # Wait for agent to connect to Apsis and start the process
            conn_timeout = get_cfg(self.cfg, "connection.start_timeout", 0)
            conn_timeout = nparse_duration(conn_timeout) or 300  # 5 minutes for agent connection
            logger.info(
                f"Waiting for agent connection with timeout {conn_timeout}s for group {self.unique_group_id}"
            )

            try:
                # Start the proc via the agent server
                logger.info(f"Starting proc {self.proc_id} via agent server")
                self.proc, res = await get_agent_server().start(
                    proc_id=self.proc_id,
                    group_id=self.unique_group_id,  # Use unique group_id for perfect isolation
                    spec=self._spec,
                    conn_timeout=conn_timeout,
                )
            except NoOpenConnectionInGroup as exc:
                msg = f"start failed: {self.proc_id}: {exc} (agent may not have connected yet)"
                logger.error(msg)
                # Include task status in error for debugging
                task_info = await self.ecs_manager.describe_task(self.task_arn)
                task_status = task_info.get("lastStatus", "UNKNOWN") if task_info else "UNKNOWN"
                meta = self._get_base_metadata()
                meta["error"] = "Failed to start process on agent"
                meta["task_status"] = task_status
                yield ProgramError(msg, meta=meta)
                return

            self.conn_id = self.proc.conn_id
            logger.info(
                f"started: {self.proc_id} on conn {self.conn_id} in group {self.unique_group_id}"
            )

            start = ora.now()
            self.run_state = {
                "conn_id": self.conn_id,
                "proc_id": self.proc_id,
                "task_arn": self.task_arn,
                "group_id": self.unique_group_id,  # Store group_id for reconnection
                "start": str(start),
            }

            meta = self._get_base_metadata()
            meta["message"] = "Process started on ECS Procstar agent"
            meta["task_status"] = "RUNNING"
            meta["proc_status"] = "running"
            yield ProgramRunning(run_state=self.run_state, meta=meta)

            # Monitor execution
            logger.info(f"Starting execution monitoring for run {self.run_id}")
            async for update in self._monitor_procstar_execution(
                start, update_interval, output_interval
            ):
                yield update

        except Exception as exc:
            logger.error(f"Error in _start_new_execution for run {self.run_id}", exc_info=True)
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
        self.unique_group_id = self.run_state.get("group_id", f"ecs-run-{self.run_id}")
        logger.info(f"restored state for group {self.unique_group_id}")

    async def stop(self):
        """Stop the running program gracefully."""
        # Call parent stop method first
        await super().stop()

        # Also stop the ECS task
        if self.task_arn:
            await self.ecs_manager.stop_task(self.task_arn, "Stopped by Apsis")

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

        # Don't stop the ECS task - let the agent remain running for future jobs
        # The ECS task should continue running to serve future requests
        # Only stop the task if explicitly requested via the stop() method
        logger.info(f"Leaving ECS task {self.task_arn} running for future jobs")
