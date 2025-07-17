"""
Tests thread actions.
"""

import asyncio
from contextlib import closing
import json
import logging
from pathlib import Path
import pytest
import time
import websockets.client

from instance import ApsisService

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


@pytest.fixture
def client(inst, scope="function"):
    return inst.client


def test_slow_thread_action(inst):
    """
    Tests that a slow, blocking thread action doesn't block Apsis.
    """
    client = inst.client

    start = time.perf_counter()
    # Run sleeps for 0.5 s.
    r1 = client.schedule("sleep", {"duration": 0.5})["run_id"]
    # Run finishes immediately, then starts 2 s blocking thread action.
    r2 = client.schedule("action", {})["run_id"]
    inst.wait_run(r2)
    # Run sleeps for 0.5 s.
    r3 = client.schedule("sleep", {"duration": 0.5})["run_id"]
    r4 = client.schedule("stats", {})["run_id"]
    inst.wait_run(r1)
    inst.wait_run(r3)
    inst.wait_run(r4)
    elapsed = time.perf_counter() - start

    # This should have taken long enough for the 0.5 s sleep runs;
    # the longer action should still be running.
    assert 0.5 < elapsed < 1

    # There should be one action running now.
    stats = json.loads(client.get_output(r4, "output"))
    assert stats["tasks"]["num_action"] == 1

    # Logs should show that the action started but did not finish.
    with inst.get_log() as log:
        log = list(log)
    assert any("sleeping action for" in l for l in log)
    assert not any("sleeping action done" in l for l in log)

    # Sleep long enough for the action to complete.
    time.sleep(2.5 - elapsed)

    # These should be no action running.
    r5 = client.schedule("stats", {})["run_id"]
    inst.wait_run(r5)
    stats = json.loads(client.get_output(r5, "output"))
    assert stats["tasks"]["num_action"] == 0

    # Logs should show that the action started and finished.
    with inst.get_log() as log:
        log = list(log)
    assert any("sleeping action for" in l for l in log)
    assert any("sleeping action done" in l for l in log)


def test_error_thread_action(inst):
    """
    Tests a thread action that raises an exception.
    """
    client = inst.client

    # Run sleeps for 0.5 s.
    r1 = client.schedule("error", {})["run_id"]
    inst.wait_run(r1)
    time.sleep(0.2)

    r2 = client.schedule("stats", {})["run_id"]
    inst.wait_run(r2)

    # There should be no action running.
    stats = json.loads(client.get_output(r2, "output"))
    assert stats["tasks"]["num_action"] == 0

    # Logs should show that the action started and raised.
    with inst.get_log() as log:
        log = list(log)
    assert any("error action" in l for l in log)
    assert any("RuntimeError: something went wrong" in l for l in log)
