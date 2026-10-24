import json
import pytest
import socket
import subprocess
import websockets.sync.client

import apsis.ctl

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
            msgs = json.loads(ws.recv())
            assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)


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


@pytest.mark.parametrize("opt", [("--host", "0.0.0.0"), ("--port", "5000")])
def test_serve_unix_rejects_host_or_port(monkeypatch, capsys, opt):
    monkeypatch.setattr("sys.argv", ["apsisctl", "serve", "--unix", "apsis.sock", *opt])
    with pytest.raises(SystemExit) as exc:
        apsis.ctl.main()
    assert exc.value.code == 2
    assert "--unix can't be used with --host or --port" in capsys.readouterr().err
