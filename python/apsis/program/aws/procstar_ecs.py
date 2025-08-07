"""ProcstarECSProgram implementation for running jobs on AWS ECS Fargate."""

import asyncio
import logging
from typing import Dict, List, Optional, Any, AsyncGenerator
from datetime import datetime, timedelta

from apsis.lib.py import or_none
# from apsis.lib.exc import NotReadyError  # Not needed
from apsis.program.base import Program, RunningProgram
from apsis.program.base import (
    ProgramRunning, ProgramSuccess, ProgramFailure, ProgramError, ProgramUpdate
)
from apsis.program.aws.ecs import ECSTaskManager


logger = logging.getLogger(__name__)


class ProcstarECSProgram(Program):
    """Program that executes arbitrary commands on AWS ECS Fargate.
    
    This program starts an ECS Fargate task, executes the specified command directly,
    and cleans up the task when done. The command can be any shell command like:
    - "echo 'Hello World'"
    - "python my_script.py"
    - "ls -la && cat file.txt"
    
    The task_definition parameter defaults to "run-procstar-agent" but can be
    customized if needed for specific container images or configurations.
    """
    
    def __init__(
        self,
        *,
        command: str,
        cluster_name: str,
        task_definition: str = "run-procstar-agent",  # Default task definition
        subnet_ids: List[str],
        security_group_ids: List[str],
        region: str = "us-east-1",
        log_group: str = "/ecs/procstar-agent",
        timeout: Optional[float] = 3600,  # 1 hour default
        startup_timeout: float = 600,     # 10 minutes for task startup
        environment: Optional[Dict[str, str]] = None,
        assign_public_ip: bool = True,
        **kwargs
    ):
        """Initialize ProcstarECSProgram.
        
        Args:
            command: Command to execute (e.g., "echo HelloWorld")
            cluster_name: ECS cluster name
            task_definition: ECS task definition ARN or family:revision (defaults to "run-procstar-agent")
            subnet_ids: List of subnet IDs for Fargate task
            security_group_ids: List of security group IDs
            region: AWS region
            log_group: CloudWatch log group for task logs
            timeout: Maximum execution time in seconds
            startup_timeout: Time to wait for task to start
            environment: Additional environment variables
            assign_public_ip: Whether to assign public IP to task
        """
        super().__init__(**kwargs)
        self.command = command
        self.cluster_name = cluster_name
        self.task_definition = task_definition
        self.subnet_ids = subnet_ids
        self.security_group_ids = security_group_ids
        self.region = region
        self.log_group = log_group
        self.timeout = timeout
        self.startup_timeout = startup_timeout
        self.environment = environment or {}
        self.assign_public_ip = assign_public_ip
    
    def bind(self, args: Dict[str, Any]) -> 'ProcstarECSProgram':
        """Bind template parameters in the program configuration.
        
        Args:
            args: Template parameters to substitute
            
        Returns:
            New program instance with parameters bound
        """
        def substitute(value):
            """Recursively substitute template parameters."""
            if isinstance(value, str):
                return value.format(**args) if value else value
            elif isinstance(value, dict):
                return {k: substitute(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [substitute(v) for v in value]
            else:
                return value
        
        return ProcstarECSProgram(
            command=substitute(self.command),
            cluster_name=substitute(self.cluster_name),
            task_definition=substitute(self.task_definition),
            subnet_ids=substitute(self.subnet_ids),
            security_group_ids=substitute(self.security_group_ids),
            region=substitute(self.region),
            log_group=substitute(self.log_group),
            timeout=self.timeout,
            startup_timeout=self.startup_timeout,
            environment=substitute(self.environment),
            assign_public_ip=self.assign_public_ip,
        )
    
    def run(self, run_id: str, cfg) -> 'RunningProcstarECSProgram':
        """Start execution of the program.
        
        Args:
            run_id: Unique identifier for this program run
            cfg: Apsis configuration
            
        Returns:
            RunningProgram instance for monitoring execution
        """
        return RunningProcstarECSProgram(run_id, self, cfg)
    
    def connect(self, run_id: str, run_state: Dict, cfg) -> 'RunningProcstarECSProgram':
        """Reconnect to an already running program.
        
        Args:
            run_id: Unique identifier for the program run
            run_state: Saved state from previous execution
            cfg: Apsis configuration
            
        Returns:
            RunningProgram instance for monitoring execution
        """
        return RunningProcstarECSProgram(run_id, self, cfg, run_state.get('task_arn'))


class RunningProcstarECSProgram(RunningProgram):
    """Running instance of ProcstarECSProgram."""
    
    def __init__(
        self, 
        run_id: str, 
        program: ProcstarECSProgram, 
        cfg,
        task_arn: Optional[str] = None
    ):
        """Initialize running program.
        
        Args:
            run_id: Unique identifier for this run
            program: The program configuration
            cfg: Apsis configuration
            task_arn: Existing task ARN for reconnection
        """
        super().__init__(run_id)
        self.program = program
        self.cfg = cfg
        self.task_arn = task_arn
        self.ecs_manager = ECSTaskManager(program.cluster_name, program.region)
        self.start_time = datetime.utcnow()
        self._updates_queue = asyncio.Queue()
        self._execution_task = None
        self._stopped = False
        
        # Start the execution coroutine
        self._execution_task = asyncio.create_task(self._execute())
    
    @property
    def updates(self) -> AsyncGenerator:
        """Async generator yielding program execution updates."""
        return self._get_updates()
    
    async def _get_updates(self):
        """Internal async generator for updates."""
        try:
            while True:
                update = await self._updates_queue.get()
                yield update
                
                # Stop yielding if we've reached a terminal state
                if isinstance(update, (ProgramSuccess, ProgramFailure, ProgramError)):
                    break
                    
        except asyncio.CancelledError:
            logger.debug(f"Updates cancelled for run {self.run_id}")
            raise
    
    async def _put_update(self, update):
        """Put an update into the queue."""
        await self._updates_queue.put(update)
    
    async def _execute(self):
        """Main execution coroutine."""
        try:
            # Start or reconnect to ECS task
            if self.task_arn is None:
                await self._start_task()
            else:
                await self._put_update(ProgramUpdate(
                    meta={'message': "Reconnecting to existing ECS task", 'task_arn': self.task_arn}
                ))
            
            # Wait for task to be running
            if not await self._wait_for_task_running():
                return
            
            # Monitor task execution
            await self._monitor_execution()
            
        except Exception as e:
            logger.exception(f"Execution failed for run {self.run_id}")
            await self._put_update(ProgramError(
                f"Execution error: {str(e)}",
                meta={'error_type': type(e).__name__}
            ))
        finally:
            # Ensure cleanup happens
            await self._cleanup()
    
    async def _start_task(self):
        """Start the ECS Fargate task."""
        try:
            await self._put_update(ProgramUpdate(
                meta={
                    'message': "Starting ECS Fargate task",
                    'cluster': self.program.cluster_name,
                    'task_definition': self.program.task_definition
                }
            ))
            
            # Prepare environment variables
            env_vars = dict(self.program.environment)
            env_vars['APSIS_RUN_ID'] = self.run_id
            
            # Prepare command override to execute the specified command
            # Use shell to execute the command directly
            command_override = ["/bin/bash", "-c", self.program.command]
            
            # Start the task
            self.task_arn = await self.ecs_manager.start_task(
                task_definition=self.program.task_definition,
                subnet_ids=self.program.subnet_ids,
                security_group_ids=self.program.security_group_ids,
                environment_overrides=env_vars,
                command_override=command_override,
                assign_public_ip=self.program.assign_public_ip
            )
            
            await self._put_update(ProgramRunning(
                run_state={'task_arn': self.task_arn},
                meta={
                    'message': "ECS task started",
                    'task_arn': self.task_arn
                }
            ))
            
        except Exception as e:
            logger.error(f"Failed to start ECS task: {e}")
            await self._put_update(ProgramError(
                f"Failed to start ECS task: {str(e)}",
                meta={'error_type': type(e).__name__}
            ))
            raise
    
    async def _wait_for_task_running(self) -> bool:
        """Wait for the task to reach RUNNING status."""
        if not self.task_arn:
            return False
        
        await self._put_update(ProgramUpdate(
            meta={
                'message': "Waiting for task to start",
                'timeout': self.program.startup_timeout
            }
        ))
        
        success = await self.ecs_manager.wait_for_task_status(
            self.task_arn, 
            'RUNNING', 
            self.program.startup_timeout
        )
        
        if success:
            await self._put_update(ProgramUpdate(
                meta={
                    'message': "Task is now running",
                    'status': 'RUNNING'
                }
            ))
            return True
        else:
            # Check if task stopped with failure
            task_info = await self.ecs_manager.describe_task(self.task_arn)
            if task_info:
                status = task_info.get('lastStatus')
                if status == 'STOPPED':
                    exit_code = self.ecs_manager.get_task_exit_code(task_info)
                    await self._put_update(ProgramFailure(
                        "Task failed to start",
                        meta={
                            'exit_code': exit_code,
                            'task_status': status,
                            'task_info': task_info
                        }
                    ))
                    return False
            
            await self._put_update(ProgramError(
                "Task startup timeout",
                meta={'timeout': self.program.startup_timeout}
            ))
            return False
    
    async def _monitor_execution(self):
        """Monitor the task execution until completion."""
        timeout_time = None
        if self.program.timeout:
            timeout_time = self.start_time + timedelta(seconds=self.program.timeout)
        
        while not self._stopped:
            # Check for timeout
            if timeout_time and datetime.utcnow() > timeout_time:
                await self._put_update(ProgramFailure(
                    "Execution timeout",
                    meta={'timeout': self.program.timeout}
                ))
                return
            
            # Get task status
            task_info = await self.ecs_manager.describe_task(self.task_arn)
            if not task_info:
                await self._put_update(ProgramError(
                    "Lost connection to task"
                ))
                return
            
            status = task_info.get('lastStatus')
            
            # Update health status
            if self.ecs_manager.is_task_healthy(task_info):
                await self._put_update(ProgramUpdate(
                    meta={
                        'message': "Task healthy",
                        'health_status': 'HEALTHY'
                    }
                ))
            
            # Check if task completed
            if status == 'STOPPED':
                await self._handle_task_completion(task_info)
                return
            
            # Wait before next check
            await asyncio.sleep(10)
    
    async def _handle_task_completion(self, task_info: Dict):
        """Handle task completion and determine success/failure."""
        exit_code = self.ecs_manager.get_task_exit_code(task_info)
        
        # Get final logs
        logs = await self.ecs_manager.get_task_logs(
            self.program.log_group,
            start_time=self.start_time,
            limit=50
        )
        
        # Determine success or failure
        if exit_code == 0:
            await self._put_update(ProgramSuccess(
                meta={
                    'message': "Task completed successfully",
                    'exit_code': exit_code,
                    'task_info': task_info,
                    'logs': logs
                }
            ))
        else:
            await self._put_update(ProgramFailure(
                f"Task failed with exit code {exit_code}",
                meta={
                    'exit_code': exit_code,
                    'task_info': task_info,
                    'logs': logs
                }
            ))
    
    async def stop(self):
        """Stop the running program gracefully."""
        if self._stopped:
            return
        
        self._stopped = True
        
        if self.task_arn:
            await self._put_update(ProgramUpdate(
                meta={
                    'message': "Stopping ECS task",
                    'task_arn': self.task_arn
                }
            ))
            
            success = await self.ecs_manager.stop_task(
                self.task_arn, 
                "Stopped by Apsis"
            )
            
            if success:
                await self._put_update(ProgramUpdate(
                    meta={'message': "Task stopped successfully"}
                ))
            else:
                await self._put_update(ProgramUpdate(
                    meta={'message': "Failed to stop task gracefully"}
                ))
        
        # Cancel execution task
        if self._execution_task:
            self._execution_task.cancel()
    
    async def signal(self, signal: str):
        """Send a signal to the running program.
        
        Args:
            signal: Signal to send (limited support in ECS)
        """
        # ECS has limited signal support, mainly stop
        if signal in ('TERM', 'KILL', 'STOP'):
            await self.stop()
        else:
            await self._put_update(ProgramUpdate(
                meta={'message': f"Signal {signal} not supported for ECS tasks"}
            ))
    
    async def _cleanup(self):
        """Cleanup resources."""
        # Ensure task is stopped
        if self.task_arn and not self._stopped:
            await self.ecs_manager.stop_task(
                self.task_arn, 
                "Cleanup on program termination"
            )