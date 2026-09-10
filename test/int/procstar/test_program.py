import asyncio
from pathlib import Path
import signal

import pytest

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def test_program():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True) as agent:
        assert len(agent.client.get_procs()) == 0
        run_id = svc.client.schedule("sleep", {"time": 1})["run_id"]
        res = svc.wait_run(run_id, timeout=5)
        assert res["state"] == "success"
        assert len(agent.client.get_procs()) == 0


def test_command_program():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True):
        job = svc.client.get_job("sleep command")
        assert job["program"]["type"] == "procstar-shell"

        run_id = svc.client.schedule("sleep command", {"time": 1})["run_id"]
        res = svc.wait_run(run_id, timeout=5)
        assert res["state"] == "success"
        assert svc.client.get_outputs(run_id) == [{"output_id": "output", "output_len": 24}]
        output = svc.client.get_output(run_id, "output")
        assert output == b"sleeping for 1 sec\ndone\n"


def test_reconnect():
    """
    Tests reconnecting to a running run after Apsis restart.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True) as agent:
        run_id = svc.client.schedule("sleep", {"time": 1})["run_id"]
        # Wait for the run to start; we can't reconnect to starting runs.
        res = svc.wait_run(run_id, wait_states=("starting",))
        assert res["state"] == "running"

        svc.restart()

        res = svc.wait_run(run_id, timeout=5)
        assert res["state"] == "success"
        assert len(agent.client.get_procs()) == 0


def test_reconnect_failed_keeps_metadata():
    """
    Tests that a run keeps its metadata when Apsis can't reconnect to it.

    A run whose process Apsis has lost track of is precisely the run whose
    metadata is needed, to go find the process, so the metadata must survive
    the transition to error.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent() as agent:
        run_id = svc.client.schedule("sleep", {"time": 30})["run_id"]
        # Wait for the run to start; only a running run has program metadata.
        assert svc.wait_for_run_to_start(run_id)["state"] == "running"
        meta = svc.client.get_run(run_id)["meta"]["program"]
        assert meta["procstar_proc_id"] is not None
        assert meta["procstar_conn"]["conn_id"] == agent.conn_id

        # Take Apsis down, then restart the agent with a new connection ID, so
        # that Apsis can't reconnect to the run's process.
        svc.stop_serve()
        agent.restart()
        svc.start_serve()
        svc.wait_for_serve()

        # The run errors, since Apsis can't reconnect to it.
        res = svc.wait_run(run_id, timeout=30)
        assert res["state"] == "error"
        assert any("reconnect failed" in r["message"] for r in svc.client.get_run_log(run_id))

        # Its metadata is intact.
        assert svc.client.get_run(run_id)["meta"]["program"] == meta


def test_reconnect_many(num=256):
    """
    Tests reconnecting to many running runs after Apsis restart.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True) as agent:
        run_ids = [svc.client.schedule("sleep", {"time": 1})["run_id"] for _ in range(num)]
        # Wait for the runs to start; we can't reconnect to starting runs.
        for run_id in run_ids:
            res = svc.wait_run(run_id, wait_states=("starting",))
            # Some may have completed already.
            assert res["state"] in {"running", "success"}

        svc.restart()

        for run_id in run_ids:
            res = svc.wait_run(run_id, timeout=0.025 * num)
            assert res["state"] == "success"

        assert len(agent.client.get_procs()) == 0


def test_signal():
    SIGNALS = (
        signal.SIGTERM,
        signal.SIGINT,
        signal.SIGKILL,
        signal.SIGUSR1,
        signal.SIGUSR2,
    )
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        # Schedule some runs.
        run_ids = [
            svc.client.schedule("sleep", {"time": 1})["run_id"] for _ in range(len(SIGNALS) + 1)
        ]
        # Wait for them to start.
        for run_id in run_ids:
            svc.wait_run(run_id, wait_states=("scheduled", "waiting", "starting"))
        # Send them signals.
        for sig, run_id in zip(SIGNALS, run_ids):
            svc.client.signal(run_id, sig)
        # Check status.
        for sig, run_id in zip(SIGNALS, run_ids):
            res = svc.wait_run(run_id)
            assert res["state"] == "failure"
            assert res["meta"]["program"]["status"]["signal"] == sig.name
        # The last run didn't get a signal.
        res = svc.wait_run(run_ids[-1])
        assert res["state"] == "success"


def test_run_id_env():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        run_id = svc.client.schedule("env", args={})["run_id"]
        res = svc.wait_run(run_id)
        assert res["state"] == "success"
        output = svc.client.get_output(run_id, "output").decode()
        # Flawed parsing: multiline environment variables will be incomplete,
        # but this works for testing purposes
        env_vars = {}
        for line in output.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key] = value
        assert env_vars["APSIS_RUN_ID"] == run_id


def test_run_args_env():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        run_id = svc.client.schedule("env_args", args={"database": "asd_hoard"})["run_id"]
        res = svc.wait_run(run_id)
        assert res["state"] == "success"
        output = svc.client.get_output(run_id, "output").decode()
        env_vars = {}
        for line in output.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key] = value
        assert env_vars["APSIS_ARG_database"] == "asd_hoard"
        assert env_vars["APSIS_RUN_ID"] == run_id


@pytest.mark.asyncio
async def test_resources():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        run_id = svc.client.schedule("resources", args={})["run_id"]
        res = await asyncio.wait_for(svc.async_wait_run(run_id), 1)
        assert res["state"] == "success"


# FIXME: procstar connection timeout and reconnect: use SIGHUP to pause agent,
# wait for websocket timeout, then resume agent and watch it reconnect.

if __name__ == "__main__":
    from apsis.lib import logging

    logging.configure(level="DEBUG")
    logging.set_log_levels()
    test_program()
