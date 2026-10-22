import json
import socket
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
            msgs = json.loads(ws.recv())
            assert any(m.get("run_summary", {}).get("run_id") == run_id for m in msgs)
