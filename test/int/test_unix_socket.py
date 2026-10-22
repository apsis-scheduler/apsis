import httpx
import json
import pytest
import socket
import websockets.exceptions
import websockets.sync.client

from instance import ApsisService

USER = {"X-Remote-User": "alice@EXAMPLE.COM"}
LATER_RUN = {"job": {"program": {"type": "no-op"}}, "times": {"schedule": "2100-01-01T00:00:00Z"}}


def skip_later_run(client: httpx.Client) -> list[str]:
    """
    Schedules an ad hoc run in the future and skips it, both with `USER`.

    :return:
      The run's log messages.
    """
    (run_id,) = client.post("/api/v1/runs", json=LATER_RUN, headers=USER).json()["runs"]
    assert client.post(f"/api/v1/runs/{run_id}/skip", headers=USER).status_code == 200
    log = client.get(f"/api/v1/runs/{run_id}/log", headers=USER).json()["run_log"]
    return [r["message"] for r in log]


def test_serve_unix():
    inst = ApsisService(unix=True)
    # leave a stale socket file behind for serve to replace
    with socket.socket(socket.AF_UNIX) as stale:
        stale.bind(str(inst.sock_path))

    with inst:
        assert inst.sock_path.stat().st_mode & 0o777 == 0o660

        with (
            websockets.sync.client.unix_connect(
                str(inst.sock_path), "ws://apsis/api/v1/summary", additional_headers=USER
            ) as ws,
            inst.unix_client() as client,
        ):
            res = client.post(
                "/api/v1/runs",
                json={"job": {"program": {"type": "no-op"}}, "times": {"schedule": "now"}},
                headers=USER,
            )
            assert res.status_code == 200
            (run_id,) = res.json()["runs"]
            msgs = json.loads(ws.recv())
            assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)


def test_user_unix():
    with ApsisService(unix=True) as inst, inst.unix_client() as client:
        assert client.get("/api/v1/runs").status_code == 401
        with pytest.raises(websockets.exceptions.InvalidStatus) as exc:
            websockets.sync.client.unix_connect(str(inst.sock_path), "ws://apsis/api/v1/summary")
        assert exc.value.response.status_code == 401
        assert client.get("/api/control/debug", headers=USER).status_code == 403

        assert skip_later_run(client) == [
            "scheduled: 2100-01-01T00:00:00+00:00 by alice",
            "skipped by alice",
        ]

        waiting_run = {
            "job": {"program": {"type": "no-op"}, "condition": [{"type": "const", "value": False}]},
            "times": {"schedule": "now"},
        }
        for jso, state in ((LATER_RUN, "scheduled"), (waiting_run, "waiting")):
            (run_id,) = client.post("/api/v1/runs", json=jso, headers=USER).json()["runs"]
            assert client.post(f"/api/v1/runs/{run_id}/start", headers=USER).status_code == 200
            log = client.get(f"/api/v1/runs/{run_id}/log", headers=USER).json()["run_log"]
            assert f"{state} run started by alice" in [r["message"] for r in log]

        assert client.post("/api/control/reload_jobs?dry_run=true", headers=USER).status_code == 200
        assert client.post("/api/control/shut_down", headers=USER).status_code == 200
        inst.srv_proc.wait()
        log = "".join(inst.get_log_lines())
        assert f"reloading jobs from {inst.jobs_dir} by alice" in log
        assert "shut down starting; restart=False by alice" in log


def test_user_tcp():
    with ApsisService() as inst, httpx.Client(base_url=f"http://localhost:{inst.port}") as client:
        assert skip_later_run(client) == ["scheduled: 2100-01-01T00:00:00+00:00", "skipped"]

        (run_id,) = client.post("/api/v1/runs", json=LATER_RUN, headers=USER).json()["runs"]
        assert client.post(f"/api/v1/runs/{run_id}/start", headers=USER).status_code == 200
        log = client.get(f"/api/v1/runs/{run_id}/log", headers=USER).json()["run_log"]
        assert "scheduled run started by override" in [r["message"] for r in log]
