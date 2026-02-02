"""ECS abstraction layer for AWS ECS operations."""

import asyncio
import logging
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Task-level resource overhead to account for ECS agent and container runtime
TASK_MEMORY_OVERHEAD_MIB = 512
TASK_CPU_OVERHEAD_UNITS = 256

TASK_MIN_MEMORY_MIB = 2048
TASK_MIN_CPU_UNITS = 1024


class ECSTaskManager:
    def __init__(
        self,
        cluster_name: str,
        region: str,
        ebs_volume_role_arn: str,
        container_name: str,
        capacity_provider: str,
        default_mem_gb: float,
        default_vcpu: float,
        default_disk_gb: int,
        retain_ebs: bool = False,
    ):
        self.cluster_name = cluster_name
        self.region = region
        self.ebs_volume_role_arn = ebs_volume_role_arn
        self.container_name = container_name
        self.capacity_provider = capacity_provider
        self.default_mem_gb = default_mem_gb
        self.default_vcpu = default_vcpu
        self.default_disk_gb = default_disk_gb
        self.retain_ebs = retain_ebs
        self._ecs_client = None

    @property
    def ecs_client(self):
        if self._ecs_client is None:
            self._ecs_client = boto3.client("ecs", region_name=self.region)
        return self._ecs_client

    def _create_ebs_volume_config(self, disk_gb: int) -> List[Dict]:
        return [
            {
                "name": "procstar-data",
                "managedEBSVolume": {
                    "sizeInGiB": disk_gb,
                    "volumeType": "gp3",
                    "iops": 3000,
                    "throughput": 125,
                    "encrypted": True,
                    "filesystemType": "ext4",
                    "roleArn": self.ebs_volume_role_arn,
                    "terminationPolicy": {
                        "deleteOnTermination": not self.retain_ebs
                    },
                },
            }
        ]

    async def start_task(
        self,
        task_definition: str,
        environment_overrides: Optional[Dict[str, str]] = None,
        command_override: Optional[List[str]] = None,
        mem_gb: Optional[float] = None,
        vcpu: Optional[float] = None,
        disk_gb: Optional[int] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        task_role_arn: Optional[str] = None,
    ) -> str:
        actual_mem_gb = mem_gb if mem_gb is not None else self.default_mem_gb
        actual_vcpu = vcpu if vcpu is not None else self.default_vcpu
        actual_disk_gb = disk_gb if disk_gb is not None else self.default_disk_gb
        memory_mib = int(actual_mem_gb * 1024)
        cpu_units = int(actual_vcpu * 1024)

        container_override = {
            "name": self.container_name,
            "memory": memory_mib,
            "cpu": cpu_units,
        }

        if environment_overrides:
            container_override["environment"] = [
                {"name": k, "value": v} for k, v in environment_overrides.items()
            ]

        if command_override:
            container_override["command"] = command_override

        task_memory = max(memory_mib + TASK_MEMORY_OVERHEAD_MIB, TASK_MIN_MEMORY_MIB)
        task_cpu = max(cpu_units + TASK_CPU_OVERHEAD_UNITS, TASK_MIN_CPU_UNITS)

        overrides = {
            "containerOverrides": [container_override],
            "memory": str(task_memory),
            "cpu": str(task_cpu),
        }

        if task_role_arn is not None:
            overrides["taskRoleArn"] = task_role_arn
            logger.info(f"Overriding task role to: {task_role_arn}")

        volume_configurations = self._create_ebs_volume_config(actual_disk_gb)

        run_task_params = {
            "cluster": self.cluster_name,
            "taskDefinition": task_definition,
            "volumeConfigurations": volume_configurations,
            "capacityProviderStrategy": [
                {
                    "capacityProvider": self.capacity_provider,
                    "weight": 1,
                    "base": 0,
                }
            ],
            "overrides": overrides,
        }

        if tags:
            run_task_params["tags"] = tags

        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self.ecs_client.run_task(**run_task_params),
        )

        if not response.get("tasks"):
            failures = response.get("failures", [])
            failure_details = "; ".join(
                f"{f.get('reason', 'Unknown')}: {f.get('detail', 'No details')}" for f in failures
            )
            raise ClientError(
                {
                    "Error": {
                        "Code": "TaskCreationFailed",
                        "Message": failure_details,
                    }
                },
                "RunTask",
            )

        task_arn = response["tasks"][0]["taskArn"]
        logger.info(f"Started ECS task: {task_arn}")

        return task_arn
