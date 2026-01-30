"""ProcstarECS program implementation for executing jobs on AWS ECS with Procstar agents."""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union

from procstar.agent.proc import Result

from apsis.exc import SchemaError
from apsis.lib.json import check_schema, ifkey
from apsis.lib.py import get_cfg, or_none
from apsis.program.base import (
    Program,
    Timeout,
)
from apsis.program.process import BoundStop, Stop
from apsis.program.procstar.agent import BaseRunningProcstarProgram
from apsis.runs import template_expand

from .ecs import ECSTaskManager

log = logging.getLogger(__name__)

SHELL = "/usr/bin/bash"


@dataclass(frozen=True)
class ShellCommand:
    command: str

    def get_argv(self, args: Dict[str, Any]) -> list[str]:
        return [SHELL, "-c", template_expand(self.command, args)]

    def to_jso(self) -> dict:
        return {"command": self.command}


@dataclass(frozen=True)
class Argv:
    argv: tuple[str, ...]

    def __init__(self, argv):
        object.__setattr__(self, "argv", tuple(str(a) for a in argv))

    def get_argv(self, args: Dict[str, Any]) -> list[str]:
        return [template_expand(a, args) for a in self.argv]

    def to_jso(self) -> dict:
        return {"argv": list(self.argv)}


RunSpec = Union[ShellCommand, Argv]


class ProcstarECSProgram(Program):
    def __init__(
        self,
        *,
        run_spec: RunSpec,
        stop=Stop(),
        timeout=None,
        mem_gb=None,
        vcpu=None,
        disk_gb=None,
        role: Optional[str] = None,
    ):
        super().__init__()
        self.run_spec = run_spec
        self.stop = stop
        self.timeout = timeout
        self.mem_gb = mem_gb
        self.vcpu = vcpu
        self.disk_gb = disk_gb
        self.role = role

    def _bind(self, argv, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
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

    def bind(self, args: Dict[str, Any]) -> "BoundProcstarECSProgram":
        return self._bind(self.run_spec.get_argv(args), args)

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            command = pop("command", default=None)
            argv = pop("argv", default=None)
            stop = pop("stop", Stop.from_jso, Stop())
            timeout = pop("timeout", Timeout.from_jso, None)
            mem_gb = pop("mem_gb", default=None)
            vcpu = pop("vcpu", default=None)
            disk_gb = pop("disk_gb", default=None)
            role = pop("role", default=None)

        if command is not None and argv is not None:
            raise SchemaError("specify either 'command' or 'argv', not both")
        if command is not None:
            run_spec = ShellCommand(command)
        elif argv is not None:
            run_spec = Argv(argv)
        else:
            raise SchemaError("must specify either 'command' or 'argv'")

        return cls(
            run_spec=run_spec,
            stop=stop,
            timeout=timeout,
            mem_gb=mem_gb,
            vcpu=vcpu,
            disk_gb=disk_gb,
            role=role,
        )

    def to_jso(self):
        jso = super().to_jso()
        jso.update(self.run_spec.to_jso())
        stop_jso = self.stop.to_jso()
        if stop_jso:
            jso["stop"] = stop_jso
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


# -------------------------------------------------------------------------------


class BoundProcstarECSProgram(Program):
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
        # ECS programs don't have a predefined group_id - it's generated per-run
        self.group_id = None

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
    def __init__(
        self,
        run_id: str,
        program: BoundProcstarECSProgram,
        cfg,
        run_state: Optional[Dict] = None,
    ):
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

        default_mem_gb = ecs_cfg["default_mem_gb"]
        default_vcpu = ecs_cfg["default_vcpu"]
        default_disk_gb = ecs_cfg["default_disk_gb"]

        self.task_arn: Optional[str] = run_state.get("task_arn") if run_state else None
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
        self.program.group_id = f"aws-ecs-{self.run_id}"

    def _get_base_metadata(self):
        task_id = self.task_arn.split("/")[-1] if self.task_arn else None
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
        }

    async def _pre_start(self, proc_id):
        """Start ECS task before connecting to agent."""
        log.info(f"Launching ECS task for run {self.run_id} on cluster {self.cluster_name}")
        env_vars = {
            "APSIS_RUN_ID": self.run_id,
            "PROCSTAR_GROUP_ID": self.program.group_id,
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
        return self._get_base_metadata()

    def _get_extra_run_state(self):
        """Return ECS-specific fields for run_state."""
        return {
            "task_arn": self.task_arn,
            "group_id": self.program.group_id,
        }

    def _get_result_metadata(self, res: Result):
        meta = super()._get_result_metadata(res)
        meta.update(self._get_base_metadata())
        return meta
