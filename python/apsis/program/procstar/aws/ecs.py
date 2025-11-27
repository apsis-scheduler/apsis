"""ECS abstraction layer for AWS ECS operations."""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
import uuid


logger = logging.getLogger(__name__)


async def assume_role_credentials(role_arn: str, session_name: str = None, region: str = "us-east-1") -> Dict[str, str]:
    """Assume an IAM role and return temporary credentials as environment variables.
    
    Args:
        role_arn: The ARN of the role to assume
        session_name: Optional session name (defaults to UUID)
        region: AWS region
        
    Returns:
        Dict with AWS credential environment variables
        
    Raises:
        ClientError: If assume role operation fails
    """
    if session_name is None:
        session_name = f"apsis-ecs-{str(uuid.uuid4())[:8]}"
    
    try:
        sts_client = boto3.client("sts", region_name=region)
        
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                DurationSeconds=3600  # 1 hour, can be extended to 12 hours if needed
            ),
        )
        
        credentials = response["Credentials"]
        
        # Return as environment variables
        return {
            "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "AWS_SESSION_TOKEN": credentials["SessionToken"],
        }
        
    except ClientError as e:
        logger.error(f"Failed to assume role {role_arn}: {e}")
        raise


class ECSTaskManager:
    """Abstraction layer for managing ECS tasks."""

    def __init__(self, cluster_name: str, region: str, default_disk_space_gb: int,
                 ebs_volume_role_arn: str = None):
        self.cluster_name = cluster_name
        self.region = region
        self.default_disk_space_gb = default_disk_space_gb
        self.ebs_volume_role_arn = ebs_volume_role_arn
        self._ecs_client = None
        self._logs_client = None

    @property
    def ecs_client(self):
        if self._ecs_client is None:
            self._ecs_client = boto3.client("ecs", region_name=self.region)
        return self._ecs_client

    @property
    def logs_client(self):
        if self._logs_client is None:
            self._logs_client = boto3.client("logs", region_name=self.region)
        return self._logs_client

    def _create_ebs_volume_config(self, disk_space_gb: int) -> List[Dict]:
        """Create EBS volume configuration with specified size."""
        
        return [
            {
                "name": "procstar-data",
                "managedEBSVolume": {
                    "sizeInGiB": disk_space_gb,
                    "volumeType": "gp3",
                    "iops": min(3000, max(100, disk_space_gb * 3)),
                    "throughput": 125,
                    "encrypted": True,
                    "filesystemType": "ext4",
                    "roleArn": self.ebs_volume_role_arn,
                    #TODO: check if we want to retain volumes instead
                    # For the moment, just delete them to avoid extra costs during development and testing
                    "terminationPolicy": {"deleteOnTermination": True},
                },
            }
        ]

    async def start_task(
        self,
        task_definition: str,
        environment_overrides: Optional[Dict[str, str]] = None,
        command_override: Optional[List[str]] = None,
        memory: Optional[int] = None,
        cpu: Optional[int] = None,
        disk_space: Optional[int] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        task_role_arn: Optional[str] = None,
    ) -> str:
        # Retry configuration - transparent to users
        max_retries = 3
        base_delay = 30
        
        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                overrides = {}

                container_override = {}

                if environment_overrides:
                    container_override["environment"] = [
                        {"name": k, "value": v} for k, v in environment_overrides.items()
                    ]

                if command_override:
                    container_override["command"] = command_override

                if memory is not None:
                    container_override["memory"] = memory

                if cpu is not None:
                    container_override["cpu"] = cpu

                if container_override:
                    container_override["name"] = "procstar-agent"
                    overrides["containerOverrides"] = [container_override]

                # Also set task-level limits to ensure they're high enough
                if memory is not None:
                    task_memory = max(memory + 512, 2048)
                    overrides["memory"] = str(task_memory)

                if cpu is not None:
                    task_cpu = max(cpu + 256, 1024)
                    overrides["cpu"] = str(task_cpu)
                
                # Override task role if specified
                if task_role_arn is not None:
                    logger.info("5555555555555555555555555555555555")
                    logger.info(task_role_arn)
                    logger.info("xxxxxxxxxxxxxxx66666666666666666666666666")
                    overrides["taskRoleArn"] = task_role_arn
                    logger.info(f"Overriding task role to: {task_role_arn}")

                actual_disk_space = disk_space if (disk_space is not None and disk_space > 0) else self.default_disk_space_gb

                volume_configurations = self._create_ebs_volume_config(actual_disk_space)

                run_task_params = {
                    "cluster": self.cluster_name,
                    "taskDefinition": task_definition,
                    "launchType": "EC2",
                    "volumeConfigurations": volume_configurations,
                }
                            
                if overrides:
                    run_task_params["overrides"] = overrides
                
                if tags:
                    run_task_params["tags"] = tags
                
                # Run the task (bridge mode - no network configuration needed)
                response = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.ecs_client.run_task(**run_task_params),
                )

                # Check for failures
                if not response.get("tasks"):
                    failures = response.get("failures", [])
                    failure_details = "; ".join(
                        f"{f.get('reason', 'Unknown')}: {f.get('detail', 'No details')}"
                        for f in failures
                    )
                    raise ClientError(
                        {"Error": {"Code": "TaskCreationFailed", "Message": failure_details}}, "RunTask"
                    )

                task_arn = response["tasks"][0]["taskArn"]
                logger.info(f"Started ECS task: {task_arn}")

                return task_arn

            except ClientError as e:
                error_message = str(e)
                
                # Check if this is a retryable resource error
                is_retryable = (
                    "RESOURCE:MEMORY" in error_message or
                    "RESOURCE:CPU" in error_message or
                    "TaskCreationFailed" in error_message
                )
                
                # If this is the last attempt or error is not retryable, raise the exception
                if attempt >= max_retries or not is_retryable:
                    logger.error(f"Failed to start ECS task after {attempt + 1} attempts: {e}")
                    raise
                
                # Calculate delay with exponential backoff and jitter
                delay = base_delay * (2 ** attempt) + (asyncio.get_event_loop().time() % 10)
                logger.warning(
                    f"ECS task creation failed (attempt {attempt + 1}/{max_retries + 1}): {error_message}. "
                    f"Retrying in {delay:.1f} seconds..."
                )
                await asyncio.sleep(delay)

    async def stop_task(self, task_arn: str, reason: str = "Stopped by Apsis") -> bool:
        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.ecs_client.stop_task(
                    cluster=self.cluster_name, task=task_arn, reason=reason
                ),
            )
            logger.info(f"Stopped ECS task: {task_arn}")
            return True

        except ClientError as e:
            logger.error(f"Failed to stop ECS task {task_arn}: {e}")
            return False

    async def describe_task(self, task_arn: str) -> Optional[Dict]:
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.ecs_client.describe_tasks(cluster=self.cluster_name, tasks=[task_arn]),
            )

            tasks = response.get("tasks", [])
            return tasks[0] if tasks else None

        except ClientError as e:
            logger.error(f"Failed to describe task {task_arn}: {e}")
            return None

    async def get_task_status(self, task_arn: str) -> Optional[str]:
        task_info = await self.describe_task(task_arn)
        return task_info.get("lastStatus") if task_info else None

    async def wait_for_task_status(
        self, task_arn: str, target_status: str, timeout: float = 300, progress_callback=None
    ) -> bool:
        start_time = asyncio.get_event_loop().time()
        last_status = None

        while (asyncio.get_event_loop().time() - start_time) < timeout:
            status = await self.get_task_status(task_arn)

            # Report progress if status changed OR every 30 seconds for debugging
            elapsed = asyncio.get_event_loop().time() - start_time
            should_report = (status != last_status) or (elapsed > 0 and int(elapsed) % 30 == 0)

            if should_report and progress_callback:
                progress_callback(status, elapsed, timeout)
                last_status = status

            if status == target_status:
                return True

            # If task stopped but we're not waiting for STOPPED, it's failed
            if target_status != "STOPPED" and status == "STOPPED":
                logger.error(
                    f"Task {task_arn} stopped unexpectedly while waiting for {target_status}"
                )
                return False

            await asyncio.sleep(5)  # Check every 5 seconds

        return False

    async def get_task_logs(
        self,
        log_group: str,
        task_arn: Optional[str] = None,
        start_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> List[str]:
        try:
            # If no task_arn provided, we can't get logs
            if not task_arn:
                logger.warning("No task ARN provided, cannot retrieve logs")
                return []

            # Construct log stream name from task ARN
            # ECS log stream format: {stream-prefix}/{container-name}/{task-id}
            # Extract task ID from ARN: arn:aws:ecs:region:account:task/cluster/task-id
            task_id = task_arn.split("/")[-1]
            log_stream_name = f"ecs/procstar-agent/{task_id}"

            kwargs = {"logGroupName": log_group, "logStreamName": log_stream_name, "limit": limit}

            if start_time:
                kwargs["startTime"] = int(start_time.timestamp() * 1000)

            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.logs_client.get_log_events(**kwargs)
            )

            return [event["message"] for event in response.get("events", [])]

        except ClientError as e:
            # If the log stream doesn't exist or other error, log and return empty
            logger.warning(f"Could not retrieve task logs from stream '{log_stream_name}': {e}")
            return []

    def get_task_exit_code(self, task_info: Dict) -> Optional[int]:
        containers = task_info.get("containers", [])
        for container in containers:
            exit_code = container.get("exitCode")
            if exit_code is not None:
                return exit_code
        return None
