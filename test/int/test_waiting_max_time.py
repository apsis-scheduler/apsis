from contextlib import closing
import time

from instance import ApsisService

# -------------------------------------------------------------------------------


def test_waiting_max_time(tmp_path):
    CFG = {
        "waiting": {
            "max_time": 0.5,
        }
    }
    with closing(ApsisService(cfg=CFG, job_dir=tmp_path)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        client = inst.client

        res = client.schedule_adhoc(
            "now",
            {
                "program": {"type": "no-op"},
                "condition": {
                    "type": "const",
                    "value": False,
                },
            },
        )
        run_id = res["run_id"]

        run = client.get_run(run_id)
        assert run["state"] == "waiting"

        time.sleep(0.6)

        run = client.get_run(run_id)
        assert run["state"] == "error"
        run_log = client.get_run_log(run_id)
        assert "timeout" in run_log[-1]["message"]
