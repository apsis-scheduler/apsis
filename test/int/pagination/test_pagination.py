"""
Integration tests for pagination of the runs endpoints (iac#2129).
"""

from contextlib import closing
from pathlib import Path

import pytest
import requests

from instance import ApsisService

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


def _schedule_runs(inst, n):
    """Schedule n runs of the parametrized job and wait for them to finish."""
    client = inst.client
    ids = set()
    for i in range(n):
        run_id = client.schedule("paginated", {"i": str(i)})["run_id"]
        ids.add(run_id)
    for run_id in ids:
        inst.wait_run(run_id)
    return ids


def _raw_runs(inst, **params):
    """Raw GET /api/v1/runs, returning the parsed JSON (envelope + paging)."""
    url = f"http://localhost:{inst.port}/api/v1/runs"
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()


def test_raw_pages_forward_limit_and_cursor(inst):
    """The HTTP endpoint applies limit/cursor and returns the next cursor."""
    ids = sorted(_schedule_runs(inst, n=3), key=lambda r: int(r[1:]), reverse=True)
    first = _raw_runs(inst, job_id="paginated", limit=2)
    assert list(first["runs"]) == ids[:2]
    assert first["paging"]["next"] == ids[1]

    last = _raw_runs(inst, job_id="paginated", limit=2, cursor=first["paging"]["next"])
    assert list(last["runs"]) == ids[2:]
    assert last["paging"]["next"] is None


def test_run_id_filter_returns_all_in_one_page(inst):
    """Explicit run_id filters return every requested run with no further page."""
    ids = sorted(_schedule_runs(inst, n=3))
    jso = _raw_runs(inst, run_id=ids, limit=2)  # limit is ignored for run_id
    assert set(jso["runs"]) == set(ids)
    assert jso["paging"]["next"] is None


def test_invalid_paging_params_rejected(inst):
    """Parser ValueError becomes HTTP 400; input cases are covered in unit tests."""
    url = f"http://localhost:{inst.port}/api/v1/runs"
    assert requests.get(url, params={"job_id": "paginated", "cursor": "bad"}).status_code == 400
