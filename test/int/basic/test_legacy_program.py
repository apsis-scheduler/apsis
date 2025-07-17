from instance import ApsisService

# -------------------------------------------------------------------------------


def test_reconnect_legacy_program():
    job = {
        "program": {
            "type": "apsis.program.test.SimpleLegacyProgram",
            "time": 2,
        }
    }

    with ApsisService() as svc:
        run_id = svc.client.schedule_adhoc("now", job)["run_id"]
        res = svc.wait_run(run_id, wait_states=("new", "scheduled", "waiting", "starting"))
        assert res["state"] == "running"

        svc.restart()

        res = svc.client.get_run(run_id)
        assert res["state"] == "running"

        res = svc.wait_run(run_id)
        assert res["state"] == "success"
