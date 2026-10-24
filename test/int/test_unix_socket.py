import json
import pytest
import socket
import subprocess
import sys
import websockets.sync.client

from instance import ApsisService


def test_serve_unix():
    inst = ApsisService(unix=True)
    # leave a stale socket file behind for serve to replace
    with socket.socket(socket.AF_UNIX) as stale:
        stale.bind(str(inst.sock_path))

    with inst:
        assert inst.sock_path.stat().st_mode & 0o777 == 0o660

        with (
            websockets.sync.client.unix_connect(
                str(inst.sock_path), "ws://apsis/api/v1/summary"
            ) as ws,
            inst.unix_client() as client,
        ):
            res = client.post(
                "/api/v1/runs",
                json={"job": {"program": {"type": "no-op"}}, "times": {"schedule": "now"}},
            )
            assert res.status_code == 200
            (run_id,) = res.json()["runs"]
            msgs = json.loads(ws.recv(timeout=10))
            assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)


def test_summary_tcp():
    with ApsisService() as inst:
        with websockets.sync.client.connect(f"ws://127.0.0.1:{inst.port}/api/v1/summary") as ws:
            run_id = inst.client.schedule_adhoc("now", {"program": {"type": "no-op"}})["run_id"]
            msgs = json.loads(ws.recv(timeout=10))
            assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)
        assert any("/summary 127.0.0.1:" in l for l in inst.get_log_lines())


def test_serve_unix_refuses_running_server():
    with ApsisService(unix=True) as inst:
        proc = subprocess.run(
            ["apsisctl", "serve", "--config", inst.cfg_path, "--unix", inst.sock_path],
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )
        assert proc.returncode != 0
        assert f"something is already serving on {inst.sock_path}" in proc.stderr
        with inst.unix_client() as client:
            assert client.get("/api/v1/running").status_code == 200


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
        [sys.executable, "-m", "apsis.ctl", "serve", "--unix", "apsis.sock", *opt],
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert "--unix can't be used with --host or --port" in proc.stderr
