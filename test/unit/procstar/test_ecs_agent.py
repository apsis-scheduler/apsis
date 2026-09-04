from apsis.program.procstar.aws.ecs_agent import (
    Argv,
    ProcstarECSProgram,
    RunningProcstarECSProgram,
)

# -------------------------------------------------------------------------------

ECS_CFG = {
    "procstar": {
        "agent": {
            "ecs": {
                "cluster_name": "c",
                "container_name": "ct",
                "default_task_definition": "td",
                "region": "us-east-1",
                "log_group": "lg",
                "log_stream_prefix": "lsp",
                "aws_account_id": "123456789012",
                "ebs_volume_role": "role",
                "default_mem_gb": 4,
                "default_vcpu": 2,
                "default_disk_gb": 20,
            }
        }
    }
}


def test_run_args_env_vars_ecs():
    """ECS jobs expose run args as APSIS_ARG_* via the shared proc spec."""
    args = {"date": "2026-09-01", "database": "asd_hoard"}
    bound = ProcstarECSProgram(run_spec=Argv(["/usr/bin/echo", "hi"])).bind({})
    running = RunningProcstarECSProgram("r1", bound, ECS_CFG)
    running.set_run_args(args)

    env = running._spec.to_jso()["env"]["vars"]
    assert env["APSIS_RUN_ID"] == "r1"
    assert env["APSIS_ARG_date"] == "2026-09-01"
    assert env["APSIS_ARG_database"] == "asd_hoard"


def test_no_run_args_env_ecs():
    """Without applied args, the ECS proc spec sets only APSIS_RUN_ID."""
    bound = ProcstarECSProgram(run_spec=Argv(["/usr/bin/echo", "hi"])).bind({})
    running = RunningProcstarECSProgram("r1", bound, ECS_CFG)
    assert running._spec.to_jso()["env"]["vars"] == {"APSIS_RUN_ID": "r1"}


def test_run_args_not_in_jso():
    """Run args are never serialized on the ECS program (rollback safety)."""
    program = ProcstarECSProgram(run_spec=Argv(["/usr/bin/echo", "hi"])).bind({})
    assert "args" not in program.to_jso()
