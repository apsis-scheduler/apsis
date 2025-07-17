from pathlib import Path
import logging
import time

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def _start(svc, job_id, args):
    """
    Starts a run for `job_id` and `args`, and waits for it to start running.
    """
    run_id = svc.client.schedule(job_id, args)["run_id"]
    assert svc.wait_run(run_id, wait_states=("starting",))["state"] == "running"
    return run_id


def test_restart_some():
    """
    Tests restarting some agents but not others, while processes run.
    """
    with (
        ApsisService(job_dir=JOB_DIR) as svc,
        svc.agent(group_id="group0") as agent0,
        svc.agent(group_id="group1"),
        svc.agent(group_id="group2") as agent2,
    ):
        svc.client.wait_running(1)
        # Start runs in groups 1 and 2.
        run_id0 = _start(svc, "group sleep", {"group_id": "group0", "duration": "2"})
        run_id1 = _start(svc, "group sleep", {"group_id": "group1", "duration": "2"})

        # Restart agent 2.
        agent2.restart()

        # The runs should be unaffected.
        assert svc.client.get_run(run_id0)["state"] == "running"
        assert svc.client.get_run(run_id1)["state"] == "running"

        # Restart agent 0.  Run 0 should error.
        agent0.restart()
        time.sleep(0.1)
        assert svc.client.get_run(run_id0)["state"] == "failure"  # SIGTERM
        assert svc.client.get_run(run_id1)["state"] == "running"

        assert svc.wait_run(run_id1)["state"] == "success"


if __name__ == "__main__":
    from apsis.lib import logging

    logging.configure(level="DEBUG")
    logging.set_log_levels()
    test_restart_some()
