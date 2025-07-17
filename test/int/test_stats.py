from contextlib import closing
import json
import time

from instance import ApsisService

# -------------------------------------------------------------------------------


def test_stats(tmp_path):
    path = tmp_path / "stats.json"
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()

    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        client = inst.client

        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.stats.StatsProgram",
                    "path": str(path),
                },
            },
        )
        time.sleep(0.2)
        assert client.get_run(res["run_id"])["state"] == "success"

        res = client.schedule_adhoc(
            "now",
            {
                "program": {
                    "type": "apsis.program.internal.stats.StatsProgram",
                    "path": str(path),
                },
            },
        )
        time.sleep(0.2)
        assert client.get_run(res["run_id"])["state"] == "success"

        with open(path) as file:
            stats0 = json.loads(next(file))
            stats1 = json.loads(next(file))
        # FIXME: Pending migration to incremental program style.
        # assert int(stats0["tasks"]["num_running"]) == 1
        assert int(stats0["scheduled"]["num_entries"]) == 0
        # assert int(stats1["tasks"]["num_running"]) == 1
        assert int(stats1["scheduled"]["num_entries"]) == 0
