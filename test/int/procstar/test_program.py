import asyncio
from   pathlib import Path
import signal

import pytest

from   procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

#-------------------------------------------------------------------------------

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
        res = svc.wait_run(run_id, wait_states=("starting", ))
        assert res["state"] == "running"

        svc.restart()

        res = svc.wait_run(run_id, timeout=5)
        assert res["state"] == "success"
        assert len(agent.client.get_procs()) == 0


def test_reconnect_many(num=256):
    """
    Tests reconnecting to many running runs after Apsis restart.
    """
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True) as agent:
        run_ids = [
            svc.client.schedule("sleep", {"time": 1})["run_id"]
            for _ in range(num)
        ]
        # Wait for the runs to start; we can't reconnect to starting runs.
        for run_id in run_ids:
            res = svc.wait_run(run_id, wait_states=("starting", ))
            # Some may have completed already.
            assert res["state"] in {"running", "success"}

        svc.restart()

        for run_id in run_ids:
            res = svc.wait_run(run_id, timeout=0.025 * num)
            assert res["state"] == "success"

        assert len(agent.client.get_procs()) == 0


def test_signal():
    SIGNALS = (
        signal.SIGTERM, signal.SIGINT, signal.SIGKILL,
        signal.SIGUSR1, signal.SIGUSR2
    )
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent():
        # Schedule some runs.
        run_ids = [
            svc.client.schedule("sleep", {"time": 1})["run_id"]
            for _ in range(len(SIGNALS) + 1)
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
        env_vars = dict( line.split("=", 1) for line in output.splitlines() )
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

