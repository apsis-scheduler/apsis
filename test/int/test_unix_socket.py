import httpx
import json
import pytest
import socket
import subprocess
import sys
import websockets.exceptions

from instance import ApsisService

USER = "aaronf@EXAMPLE.COM"
EVIL = {"Origin": "https://evil.example"}
LATER_RUN = {"job": {"program": {"type": "no-op"}}, "times": {"schedule": "2100-01-01T00:00:00Z"}}


def run_log(client: httpx.Client, run_id: str) -> list[str]:
    """
    Returns the log messages of run `run_id`.
    """
    return [r["message"] for r in client.get(f"/api/v1/runs/{run_id}/log").json()["run_log"]]


def skip_later_run(client: httpx.Client) -> list[str]:
    """
    Schedules an ad hoc run in the future and skips it.

    :return:
      The run's log messages.
    """
    (run_id,) = client.post("/api/v1/runs", json=LATER_RUN).json()["runs"]
    assert client.post(f"/api/v1/runs/{run_id}/skip").status_code == 200
    return run_log(client, run_id)


def test_serve_unix():
    inst = ApsisService(unix=True, user=USER)
    # leave a stale socket file behind for serve to replace
    with socket.socket(socket.AF_UNIX) as stale:
        stale.bind(str(inst.sock_path))

    with inst:
        assert inst.sock_path.stat().st_mode & 0o777 == 0o660

        with (
            inst.connect_ws("/api/v1/summary", origin="https://apsis:8443") as ws,
            inst.connect_ws(
                "/api/v1/summary",
                origin="http://apsis",
                additional_headers={"X-Request-ID": "req-1"},
            ) as req_ws,
            inst.http_client() as client,
        ):
            res = client.post(
                "/api/v1/runs",
                json={"job": {"program": {"type": "no-op"}}, "times": {"schedule": "now"}},
            )
            assert res.status_code == 200
            (run_id,) = res.json()["runs"]
            for w in (ws, req_ws):
                msgs = json.loads(w.recv(timeout=10))
                assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)
        assert inst.wait_run(run_id)["state"] == "success"
        lines = inst.get_log_lines()
        assert any("/summary unix aaronf:" in l for l in lines)
        assert any("/summary unix aaronf request_id='req-1':" in l for l in lines)


def test_user_unix():
    with (
        ApsisService(unix=True, user=USER) as inst,
        inst.http_client() as client,
        inst.http_client(user=None) as anon,
    ):
        for headers in (
            {},
            [("X-Remote-User", "aaronf"), ("X-Remote-User", "bob")],
            {"X-Remote-User": "mallory, aaronf"},
            # the kelvin sign lowercases to k
            {"X-Remote-User": "\u212aaaronf".encode()},
            {"X-Remote-User": "a" * 33},
        ):
            assert anon.get("/api/v1/runs", headers=headers).status_code == 401

        for origin in (EVIL["Origin"], "http://[::1"):
            assert (
                client.post("/api/v1/runs", json=LATER_RUN, headers={"Origin": origin}).status_code
                == 403
            )
        for origin, user, status in (
            ("http://apsis", None, 401),
            (EVIL["Origin"], USER, 403),
            ("http://[::1", USER, 403),
        ):
            with pytest.raises(websockets.exceptions.InvalidStatus) as exc:
                inst.connect_ws("/api/v1/summary", origin=origin, user=user)
            assert exc.value.response.status_code == status
        assert client.get("/api/control/debug").status_code == 403
        assert client.get("/api/v1/whoami").json() == {"user": "aaronf"}

        bad_args = LATER_RUN | {"args": {"x": "1"}}
        (run,) = client.post("/api/v1/runs", json=bad_args).json()["runs"].values()
        assert run["state"] == "error"
        assert run["meta"]["state_reason"] == f"extra args (x) for job {run['job_id']}"
        assert run_log(client, run["run_id"])[0] == "created by aaronf"

        error_id = run["run_id"]
        assert client.post(f"/api/v1/runs/{error_id}/mark/success").status_code == 200
        assert "marked as success by aaronf" in run_log(client, error_id)
        (rerun_id,) = client.post(f"/api/v1/runs/{error_id}/rerun").json()["runs"]
        assert f"scheduled as rerun of {error_id} by aaronf" in run_log(client, rerun_id)

        assert skip_later_run(client) == [
            "scheduled: 2100-01-01T00:00:00+00:00 by aaronf",
            "skipped by aaronf",
        ]

        waiting_run = {
            "job": {"program": {"type": "no-op"}, "condition": [{"type": "const", "value": False}]},
            "times": {"schedule": "now"},
        }
        for jso, state in ((LATER_RUN, "scheduled"), (waiting_run, "waiting")):
            (run_id,) = client.post("/api/v1/runs", json=jso).json()["runs"]
            assert client.post(f"/api/v1/runs/{run_id}/start").status_code == 200
            assert f"{state} run started by aaronf" in run_log(client, run_id)

        long_run = {
            "job": {"program": {"type": "no-op", "duration": "600"}},
            "times": {"schedule": "now"},
        }
        (run_id,) = client.post("/api/v1/runs", json=long_run).json()["runs"]
        inst.wait_for_run_to_start(run_id)
        client.post(f"/api/v1/runs/{run_id}/signal/SIGTERM")
        assert client.post(f"/api/v1/runs/{run_id}/stop").status_code == 200
        log = run_log(client, run_id)
        assert "sending SIGTERM by aaronf" in log
        assert "stopping by aaronf" in log

        assert client.post("/api/control/reload_jobs?dry_run=true").status_code == 200
        assert client.post("/api/control/shut_down").status_code == 200
        inst.srv_proc.wait()
        log = "".join(inst.get_log_lines())
        assert f"reloading jobs from {inst.jobs_dir} by aaronf" in log
        assert "shut down starting; restart=False by aaronf" in log


def test_user_tcp():
    with (
        ApsisService() as inst,
        inst.http_client(headers=EVIL | {"X-Remote-User": USER}) as client,
    ):
        assert skip_later_run(client) == ["scheduled: 2100-01-01T00:00:00+00:00", "skipped"]

        (run_id,) = client.post("/api/v1/runs", json=LATER_RUN).json()["runs"]
        assert client.post(f"/api/v1/runs/{run_id}/start").status_code == 200
        assert "scheduled run started by override" in run_log(client, run_id)
        assert client.get("/api/v1/whoami").json() == {"user": None}


def test_serve_unix_creates_dir():
    inst = ApsisService(unix=True, user=USER)
    inst.sock_path = inst.tmp_dir / "run" / "apsis.sock"
    with inst, inst.http_client() as client:
        assert client.get("/api/v1/running").status_code == 200


def test_instance_unix_refuses_tcp():
    with pytest.raises(ValueError, match="either a port or a unix socket"):
        ApsisService(port=5000, unix=True)
    with pytest.raises(RuntimeError, match="serves on a unix socket"):
        ApsisService(unix=True).run_apsis_cmd("runs")


def test_summary_tcp():
    with ApsisService() as inst:
        with inst.connect_ws("/api/v1/summary") as ws:
            run_id = inst.client.schedule_adhoc("now", {"program": {"type": "no-op"}})["run_id"]
            msgs = json.loads(ws.recv(timeout=10))
            assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)
        assert any("/summary 127.0.0.1:" in l for l in inst.get_log_lines())


def test_serve_unix_refuses_running_server():
    with ApsisService(unix=True, user=USER) as inst:
        proc = subprocess.run(
            ["apsisctl", "serve", "--config", inst.cfg_path, "--unix", inst.sock_path],
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )
        assert proc.returncode != 0
        assert f"something is already serving on {inst.sock_path}" in proc.stderr
        with inst.http_client() as client:
            assert client.get("/api/v1/running").status_code == 200


def test_serve_unix_refuses_group_writable_dir():
    inst = ApsisService(unix=True)
    inst.write_cfg()
    inst.tmp_dir.chmod(0o770)
    proc = subprocess.run(
        ["apsisctl", "serve", "--config", inst.cfg_path, "--unix", inst.sock_path],
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    assert proc.returncode != 0
    assert f"socket folder {inst.tmp_dir} is writable by group or others" in proc.stderr
    assert not inst.sock_path.exists()


def test_bind_unix_refuses_bound_socket(tmp_path):
    path = tmp_path / "apsis.sock"
    code = (
        "import sys, pathlib, apsis.service.main as m\n"
        "p = pathlib.Path(sys.argv[1])\n"
        "s = m.bind_unix(p)\n"
        "m.bind_unix(p)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code, str(path)],
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    assert f"something is already serving on {path}" in proc.stderr


@pytest.mark.parametrize("opt", [("--host", "0.0.0.0"), ("--port", "5000")])
def test_serve_unix_rejects_host_or_port(opt):
    proc = subprocess.run(
        ["apsisctl", "serve", "--unix", "apsis.sock", *opt],
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert "--unix can't be used with --host or --port" in proc.stderr


def test_summary_log_escapes_request_id():
    with ApsisService(unix=True, user=USER) as inst:
        with socket.socket(socket.AF_UNIX) as sock:
            sock.connect(str(inst.sock_path))
            # a raw handshake, since websocket clients refuse control characters in headers
            sock.sendall(
                b"GET /api/v1/summary HTTP/1.1\r\nHost: apsis\r\nUpgrade: websocket\r\n"
                b"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n"
                b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                b"X-Remote-User: aaronf@EXAMPLE.COM\r\nX-Request-ID: req\x1b[2K\r\n\r\n"
            )
            assert sock.recv(1024).startswith(b"HTTP/1.1 101")
    lines = "".join(inst.get_log_lines())
    assert "/summary unix aaronf request_id='req\\x1b[2K':" in lines
    assert "\x1b" not in lines
