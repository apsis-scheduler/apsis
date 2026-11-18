import asyncio
from contextlib import closing
from pathlib import Path

import ora
import pytest
import ujson

from apsis.service.client import get_ws_msgs
from instance import ApsisService


@pytest.mark.asyncio
async def test_summary_init_sends_every_run():
    """Summary init over a real websocket sends each run once across more than one DB page."""
    with closing(ApsisService(job_dir=Path(__file__).parent / "jobs")) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()

        runs = inst.client.schedule(
            "job1", {"name": "x", "value": 1}, time=ora.now() + 86400, count=300
        )
        run_ids = {r["run_id"] for r in runs}

        async def read_summaries() -> list[str]:
            seen = []
            url = f"ws://localhost:{inst.port}/api/v1/summary?init"
            async with get_ws_msgs(url) as frames:
                async for frame in frames:
                    msgs = ujson.loads(frame)
                    assert isinstance(msgs, list)
                    seen += [m["run_summary"]["run_id"] for m in msgs if m["type"] == "run_summary"]
                    if len(seen) >= len(run_ids):
                        return seen

        seen = await asyncio.wait_for(read_summaries(), 10)
        assert sorted(seen) == sorted(run_ids)
