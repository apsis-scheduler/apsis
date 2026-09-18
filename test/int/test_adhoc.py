from pathlib import Path
import sqlite3

import ora
import pytest
import requests

from apsis.service.client import APIError
from instance import ApsisService


@pytest.mark.parametrize("adhoc", [{}, {"enabled": False}])
def test_disabled_submissions(tmp_path: Path, adhoc: dict[str, bool]) -> None:
    (tmp_path / "registered.yaml").write_text("program:\n  type: no-op\n")
    with ApsisService(job_dir=tmp_path, cfg={"adhoc": adhoc}) as svc:
        response = requests.post(
            f"http://localhost:{svc.port}/api/v1/runs",
            json={"job": {"program": {"type": "invalid"}}, "job_id": "registered"},
            timeout=5,
        )
        assert response.status_code == 403
        assert "ad hoc jobs are disabled" in response.text
        with pytest.raises(APIError) as exc:
            svc.client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        assert exc.value.status == 403
        with sqlite3.connect(svc.db_path) as db:
            assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
            assert db.execute("SELECT count(*) FROM runs").fetchone()[0] == 0

        run = svc.client.schedule("registered", {})
        assert svc.wait_run(run["run_id"])["state"] == "success"
        rerun = svc.client.rerun(run["run_id"])
        assert svc.wait_run(rerun["run_id"])["state"] == "success"


def test_disabled_cli() -> None:
    with ApsisService(cfg={"adhoc": {"enabled": False}}) as svc:
        returncode, _ = svc.run_apsis_cmd("adhoc", "/bin/true")
        assert returncode != 0
        with sqlite3.connect(svc.db_path) as db:
            assert db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0


def test_stored_adhoc() -> None:
    with ApsisService() as svc:
        run = svc.client.schedule_adhoc("now", {"program": {"type": "no-op"}})
        assert svc.wait_run(run["run_id"])["state"] == "success"
        scheduled = svc.client.schedule(run["job_id"], {})
        assert svc.wait_run(scheduled["run_id"])["state"] == "success"

        svc.cfg["adhoc"]["enabled"] = False
        svc.write_cfg()
        svc.restart()

        for rejected in (
            svc.client.schedule(run["job_id"], {}),
            svc.client.rerun(run["run_id"]),
        ):
            assert rejected["state"] == "error"
            assert b"ad hoc jobs are disabled" in svc.client.get_output(
                rejected["run_id"], "output"
            )
        assert svc.client.get_run(run["run_id"])["state"] == "success"


@pytest.mark.parametrize("manual", [False, True])
def test_queued_adhoc(manual: bool) -> None:
    with ApsisService() as svc:
        run = svc.client.schedule_adhoc(
            ora.now() + (3600 if manual else 5), {"program": {"type": "no-op"}}
        )
        assert run["state"] == "scheduled"
        svc.cfg["adhoc"]["enabled"] = False
        svc.write_cfg()
        svc.restart()
        if manual:
            svc.client.start(run["run_id"])
        assert svc.wait_run(run["run_id"], timeout=10)["state"] == "error"
        assert any(
            "ad hoc jobs are disabled" in entry["message"]
            for entry in svc.client.get_run_log(run["run_id"])
        )


def test_waiting_adhoc_and_running_reconnect() -> None:
    with ApsisService() as svc:
        running = svc.client.schedule_adhoc(
            "now",
            {
                "program": {"type": "no-op", "duration": 5},
                "condition": {"type": "max_running", "count": 1},
            },
        )
        assert svc.wait_for_run_to_start(running["run_id"])["state"] == "running"
        waiting = svc.client.schedule(running["job_id"], {})
        assert waiting["state"] == "waiting"
        svc.cfg["adhoc"]["enabled"] = False
        svc.write_cfg()
        svc.restart()
        assert svc.wait_run(running["run_id"], timeout=10)["state"] == "success"
        assert svc.wait_run(waiting["run_id"], timeout=10)["state"] == "error"
