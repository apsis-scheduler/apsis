"""ECS abstraction layer for AWS Fargate operations."""

import asyncio
import logging
from typing import Dict, List, Optional, AsyncGenerator
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from apsis.lib.py import or_none


logger = logging.getLogger(__name__)


class ECSTaskManager:
    """Abstraction layer for managing ECS Fargate tasks."""
    
    def __init__(self, cluster_name: str, region: str = "us-east-1"):
        """Initialize ECS task manager.
        
        Args:
            cluster_name: Name of the ECS cluster
            region: AWS region
        """
        self.cluster_name = cluster_name
        self.region = region
        self._ecs_client = None
        self._logs_client = None
    
    @property
    def ecs_client(self):
        """Lazy initialization of ECS client."""
        if self._ecs_client is None:
            self._ecs_client = boto3.client('ecs', region_name=self.region)
        return self._ecs_client
    
    @property
    def logs_client(self):
        """Lazy initialization of CloudWatch Logs client."""
        if self._logs_client is None:
            self._logs_client = boto3.client('logs', region_name=self.region)
        return self._logs_client
    
    async def start_task(
        self,
        task_definition: str,
        subnet_ids: List[str],
        security_group_ids: List[str],
        environment_overrides: Optional[Dict[str, str]] = None,
        command_override: Optional[List[str]] = None,
        assign_public_ip: bool = True,
    ) -> str:
        """Start an ECS Fargate task.
        
        Args:
            task_definition: Task definition ARN or family:revision
            subnet_ids: List of subnet IDs for the task
            security_group_ids: List of security group IDs
            environment_overrides: Optional environment variable overrides
            command_override: Optional command override for the container
            assign_public_ip: Whether to assign a public IP
            
        Returns:
            Task ARN of the started task
            
        Raises:
            ClientError: If task creation fails
        """
        try:
            # Prepare network configuration
            network_config = {
                'awsvpcConfiguration': {
                    'subnets': subnet_ids,
                    'securityGroups': security_group_ids,
                    'assignPublicIp': 'ENABLED' if assign_public_ip else 'DISABLED'
                }
            }
            
            # Prepare overrides if provided
            overrides = {}
            if environment_overrides or command_override:
                container_override = {
                    'name': 'procstar-agent',  # Assuming single container
                }
                
                if environment_overrides:
                    container_override['environment'] = [
                        {'name': k, 'value': v} 
                        for k, v in environment_overrides.items()
                    ]
                
                if command_override:
                    container_override['command'] = command_override
                
                overrides['containerOverrides'] = [container_override]
            
            # Run the task
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.ecs_client.run_task(
                    cluster=self.cluster_name,
                    taskDefinition=task_definition,
                    launchType='FARGATE',
                    networkConfiguration=network_config,
                    overrides=overrides if overrides else {}
                )
            )
            
            # Check for failures
            if not response.get('tasks'):
                failures = response.get('failures', [])
                failure_details = '; '.join(
                    f"{f.get('reason', 'Unknown')}: {f.get('detail', 'No details')}"
                    for f in failures
                )
                raise ClientError(
                    {'Error': {'Code': 'TaskCreationFailed', 'Message': failure_details}},
                    'RunTask'
                )
            
            task_arn = response['tasks'][0]['taskArn']
            logger.info(f"Started ECS task: {task_arn}")
            
            return task_arn
            
        except ClientError as e:
            logger.error(f"Failed to start ECS task: {e}")
            raise
    
    async def stop_task(self, task_arn: str, reason: str = "Stopped by Apsis") -> bool:
        """Stop an ECS task.
        
        Args:
            task_arn: ARN of the task to stop
            reason: Reason for stopping
            
        Returns:
            True if task was stopped successfully
        """
        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.ecs_client.stop_task(
                    cluster=self.cluster_name,
                    task=task_arn,
                    reason=reason
                )
            )
            logger.info(f"Stopped ECS task: {task_arn}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to stop ECS task {task_arn}: {e}")
            return False
    
    async def describe_task(self, task_arn: str) -> Optional[Dict]:
        """Get detailed information about a task.
        
        Args:
            task_arn: ARN of the task
            
        Returns:
            Task description dict or None if not found
        """
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.ecs_client.describe_tasks(
                    cluster=self.cluster_name,
                    tasks=[task_arn]
                )
            )
            
            tasks = response.get('tasks', [])
            return tasks[0] if tasks else None
            
        except ClientError as e:
            logger.error(f"Failed to describe task {task_arn}: {e}")
            return None
    
    async def get_task_status(self, task_arn: str) -> Optional[str]:
        """Get the current status of a task.
        
        Args:
            task_arn: ARN of the task
            
        Returns:
            Task status string or None if not found
        """
        task_info = await self.describe_task(task_arn)
        return task_info.get('lastStatus') if task_info else None
    
    async def wait_for_task_status(
        self, 
        task_arn: str, 
        target_status: str, 
        timeout: float = 300
    ) -> bool:
        """Wait for a task to reach a specific status.
        
        Args:
            task_arn: ARN of the task
            target_status: Target status to wait for (e.g., 'RUNNING', 'STOPPED')
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if status reached, False if timeout
        """
        start_time = asyncio.get_event_loop().time()
        
        while (asyncio.get_event_loop().time() - start_time) < timeout:
            status = await self.get_task_status(task_arn)
            if status == target_status:
                return True
            
            # If task stopped but we're not waiting for STOPPED, it's failed
            if target_status != 'STOPPED' and status == 'STOPPED':
                return False
                
            await asyncio.sleep(2)
        
        return False
    
    async def get_task_logs(
        self, 
        log_group: str,
        log_stream: Optional[str] = None,
        start_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[str]:
        """Retrieve logs for a task.
        
        Args:
            log_group: CloudWatch log group name
            log_stream: Optional specific log stream
            start_time: Optional start time for logs
            limit: Maximum number of log events
            
        Returns:
            List of log messages
        """
        try:
            kwargs = {
                'logGroupName': log_group,
                'limit': limit
            }
            
            if log_stream:
                kwargs['logStreamName'] = log_stream
                
            if start_time:
                kwargs['startTime'] = int(start_time.timestamp() * 1000)
            
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.logs_client.get_log_events(**kwargs)
            )
            
            return [event['message'] for event in response.get('events', [])]
            
        except ClientError as e:
            logger.error(f"Failed to get task logs: {e}")
            return []
    
    def get_task_exit_code(self, task_info: Dict) -> Optional[int]:
        """Extract exit code from task description.
        
        Args:
            task_info: Task description from describe_task
            
        Returns:
            Exit code or None if not available
        """
        containers = task_info.get('containers', [])
        for container in containers:
            exit_code = container.get('exitCode')
            if exit_code is not None:
                return exit_code
        return None
    
    def is_task_healthy(self, task_info: Dict) -> bool:
        """Check if task is healthy based on health checks.
        
        Args:
            task_info: Task description from describe_task
            
        Returns:
            True if task is healthy
        """
        health_status = task_info.get('healthStatus')
        return health_status == 'HEALTHY'