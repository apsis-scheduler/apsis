import asyncio
from contextlib import closing
import sqlite3
from urllib.parse import quote

import ora
import pytest
import ujson
import websockets
import yaml

from apsis.service.client import get_ws_msgs
from instance import ApsisService

JOBS = {
    "a": {"params": ["x"], "program": {"type": "no-op"}},
    "b": {"program": {"type": "no-op"}},
}


def start(drop_index=False):
    inst = ApsisService()
    for job_id, jso in JOBS.items():
        with open(inst.jobs_dir / f"{job_id}.yaml", "w") as file:
            yaml.dump(jso, file)
    inst.create_db()
    if drop_index:
        with closing(sqlite3.connect(inst.db_path)) as conn:
            conn.execute("DROP INDEX idx_runs_schedule")
    inst.write_cfg()
    inst.start_serve()
    inst.wait_for_serve()
    return inst


@pytest.fixture(scope="module")
def service():
    with closing(start()) as inst:
        yield inst


@pytest.fixture(scope="module")
def run_ids(service):
    now = ora.now()
    schedule = service.client.schedule
    done = schedule("a", {"x": 1})["run_id"]
    service.wait_run(done)
    return {
        "done": done,
        "a1": [schedule("a", {"x": 1}, now + h * 3600)["run_id"] for h in (1, 2)],
        "a2": schedule("a", {"x": 2}, now + 3 * 3600)["run_id"],
        "b": schedule("b", {}, now + 4 * 3600)["run_id"],
    }


def window_ids(msgs):
    return [m["run_summary"]["run_id"] for m in msgs if m["type"] == "run_summary"]


@pytest.mark.asyncio
async def test_window(service, run_ids):
    msgs = await service.read_summary("init&time=now", until="run_window")
    assert window_ids(msgs) == [run_ids["done"], run_ids["a1"][0], run_ids["a2"], run_ids["b"]]
    assert msgs[-1]["groups"] == {
        run_ids["done"]: 1,
        run_ids["a1"][0]: 2,
        run_ids["a2"]: 1,
        run_ids["b"]: 1,
    }
    assert msgs[-1]["earlier"] is msgs[-1]["later"] is None
    # windowed mode sends runs only by default
    assert {m["type"] for m in msgs} == {"run_summary", "run_window"}

    anchor = msgs[-1]["anchor"]
    msgs = await service.read_summary(
        f"init&time={quote(anchor)}&limit=2&grouping=0", until="run_window"
    )
    assert window_ids(msgs) == [run_ids["done"], run_ids["a1"][0]]
    assert msgs[-1]["anchor"] == anchor
    assert msgs[-1]["groups"] == {}
    assert msgs[-1]["earlier"] is None
    assert msgs[-1]["later"]["time"]


@pytest.mark.asyncio
async def test_window_deltas(service, run_ids):
    url = f"ws://localhost:{service.port}/api/v1/summary?init&time=now&path=a&arg=x=1"
    async with get_ws_msgs(url) as frames:
        async for frame in frames:
            if ujson.loads(frame)[-1]["type"] == "run_window":
                break
        b_run_id = service.client.schedule("b", {})["run_id"]
        a2_run_id = service.client.schedule("a", {"x": 2})["run_id"]
        a1_run_id = service.client.schedule("a", {"x": 1})["run_id"]

        async def read():
            msgs = []
            async for frame in frames:
                msgs += ujson.loads(frame)
                if any(
                    m["run_summary"]["run_id"] == a1_run_id
                    and m["run_summary"]["state"] == "success"
                    for m in msgs
                ):
                    return msgs

        msgs = await asyncio.wait_for(read(), 10)
    for run_id in (b_run_id, a2_run_id):
        service.wait_run(run_id)
    assert {m["run_summary"]["run_id"] for m in msgs} == {a1_run_id}


@pytest.mark.asyncio
async def test_window_no_index():
    with closing(start(drop_index=True)) as inst:
        url = f"ws://localhost:{inst.port}/api/v1/summary?init&time=now"
        async with websockets.client.connect(url) as conn:
            with pytest.raises(websockets.ConnectionClosedError):
                await conn.recv()
            assert conn.close_code == 4503
