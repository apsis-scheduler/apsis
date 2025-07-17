from pathlib import Path

from procstar_instance import ApsisService

JOB_DIR = Path(__file__).parent / "jobs"

# -------------------------------------------------------------------------------


def test_program():
    with ApsisService(job_dir=JOB_DIR) as svc, svc.agent(serve=True) as agent:
        assert len(agent.client.get_procs()) == 0
        run_id = svc.client.schedule("echo", {"msg": "Hello, world!"})["run_id"]
        res = svc.wait_run(run_id, timeout=2)

        assert res["state"] == "success"
        assert len(agent.client.get_procs()) == 0

        output = svc.client.get_output(run_id, "output")
        assert output == b"Hello, world!\n"


if __name__ == "__main__":
    from apsis.lib import logging

    logging.configure(level="DEBUG")
    test_program()
