"""
Integration tests for pagination of the runs endpoints (iac#2129).

The page size is fixed at api.PAGE_SIZE, so a handful of runs fit in one page.
Multi-page cursor walking is covered by the unit tests (test_run_store,
test_run_db_query, test_client_paging).
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


def test_raw_page_envelope_newest_first(inst):
    """One page returns the matching runs newest first with a paging envelope."""
    ids = _schedule_runs(inst, n=5)
    jso = _raw_runs(inst, job_id="paginated")
    page = list(jso["runs"])
    assert set(page) == ids
    nums = [int(r[1:]) for r in page]
    assert nums == sorted(nums, reverse=True)  # descending
    assert jso["paging"]["next"] is None  # all fit on one page


def test_client_get_runs_returns_all(inst):
    """client.get_runs merges the page walk and returns every run."""
    ids = _schedule_runs(inst, n=5)
    got = inst.client.get_runs(job_id="paginated")
    assert set(got) == ids


def test_client_get_runs_limit_returns_newest_n(inst):
    """get_runs(limit=n) returns the newest n runs, not the whole history."""
    ids = _schedule_runs(inst, n=5)
    newest = sorted(ids, key=lambda r: int(r[1:]), reverse=True)[:2]
    got = inst.client.get_runs(job_id="paginated", limit=2)
    assert set(got) == set(newest)


def test_run_id_filter_returns_all(inst):
    """Explicit run_id filters return every requested run with no further page."""
    ids = sorted(_schedule_runs(inst, n=3))
    jso = _raw_runs(inst, run_id=ids)
    assert set(jso["runs"]) == set(ids)
    assert jso["paging"]["next"] is None


@pytest.mark.parametrize("param", ["cursor", "state", "since"])
def test_invalid_param_rejected(inst, param):
    """Parser ValueError becomes HTTP 400; cursor cases are covered in unit tests."""
    url = f"http://localhost:{inst.port}/api/v1/runs"
    assert requests.get(url, params={"job_id": "paginated", param: "bad"}).status_code == 400
