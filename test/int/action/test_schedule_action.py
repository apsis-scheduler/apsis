from contextlib import closing
from pathlib import Path
import pytest

from instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "schedule-jobs"


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def test_schedule_action(inst):
    client = inst.client

    r1 = client.schedule("first", {"label": "foo"})["run_id"]
    # There should be only one one right now.
    assert {r["run_id"] for r in client.get_runs().values()} == {r1}

    inst.wait_run(r1)

    # first(label=foo) should have scheduled second(label=on-success).
    (run,) = tuple(client.get_runs(job_id="second").values())
    assert run["args"] == {"label": "on-success"}

    # first(label=foo) should not have scheduled third.
    runs = tuple(client.get_runs(job_id="third").values())
    assert len(runs) == 0

    # second(label=on-success) should have scheduled fourth().
    runs = tuple(client.get_runs(job_id="fourth").values())
    assert len(runs) == 1

    # first(label=foo) should have scheduled fifth(label=foo) with automatic
    # label.
    (run,) = tuple(client.get_runs(job_id="fifth").values())
    assert run["args"] == {"label": "foo"}
