import asyncio
from contextlib import closing

import pytest
import ujson
import websockets
import yaml

from apsis.service.client import get_ws_msgs
from instance import ApsisService

NO_OP = {"type": "no-op"}
JOBS = {
    "a": {"program": NO_OP},
    "b": {"program": NO_OP},
}


@pytest.fixture(scope="module")
def service():
    with closing(ApsisService()) as inst:
        for job_id, jso in JOBS.items():
            with open(inst.jobs_dir / f"{job_id}.yaml", "w") as file:
                yaml.dump(jso, file)
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


@pytest.fixture(scope="module")
def run_ids(service):
    run_ids = {
        job_id: [service.client.schedule(job_id, {}, "now")["run_id"] for _ in range(count)]
        for job_id, count in (("a", 2), ("b", 1))
    }
    for ids in run_ids.values():
        for run_id in ids:
            service.wait_run(run_id)
    return run_ids


def job_ids(msgs):
    return sorted(m["job"]["job_id"] for m in msgs if m["type"] == "job")


def run_job_ids(msgs):
    return {
        m["run_summary"]["run_id"]: m["run_summary"]["job_id"] for m in msgs if "run_summary" in m
    }


async def read_until(frames, pred):
    msgs = []
    async for frame in frames:
        msgs += ujson.loads(frame)
        if any(pred(m) for m in msgs):
            return msgs


@pytest.mark.asyncio
async def test_job_scope(service, run_ids):
    msgs = await service.read_summary("init&job_id=a")
    assert job_ids(msgs) == ["a"]
    assert run_job_ids(msgs) == {r: "a" for r in run_ids["a"]}
    assert msgs[-1] == {"type": "summary_init_done", "truncated": {}}


@pytest.mark.asyncio
async def test_bad_scope(service):
    url = f"ws://localhost:{service.port}/api/v1/summary?init&types=bogus"
    async with websockets.client.connect(url) as conn:
        (msg,) = ujson.loads(await conn.recv())
        assert msg["type"] == "summary_error"
        with pytest.raises(websockets.ConnectionClosedError):
            await conn.recv()
        assert conn.close_code == 4400


@pytest.mark.asyncio
async def test_live_deltas(service):
    url = f"ws://localhost:{service.port}/api/v1/summary?job_id=a"
    async with get_ws_msgs(url) as frames:
        b_run_id = service.client.schedule("b", {}, "now")["run_id"]
        a_run_id = service.client.schedule("a", {}, "now")["run_id"]
        msgs = await asyncio.wait_for(
            read_until(
                frames,
                lambda m: (
                    m["type"] == "run_transition"
                    and m["run_summary"]["run_id"] == a_run_id
                    and m["run_summary"]["state"] == "success"
                ),
            ),
            10,
        )
    service.wait_run(b_run_id)
    assert set(run_job_ids(msgs).values()) == {"a"}
