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

N = 25


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def _schedule_runs(inst, n=N):
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


def test_get_runs_follows_cursor(inst):
    """client.get_runs follows paging.next internally and returns every run."""
    ids = _schedule_runs(inst)
    got = inst.client.get_runs(job_id="paginated")
    assert set(got) == ids
    assert len(got) == N


def test_raw_page_is_bounded_with_cursor(inst):
    """A single page honors the limit and reports a next cursor when more remain."""
    _schedule_runs(inst)
    jso = _raw_runs(inst, job_id="paginated", limit=10)
    assert len(jso["runs"]) == 10
    assert "paging" in jso
    assert jso["paging"]["next"] is not None


def test_raw_pages_scroll_complete_no_dupes(inst):
    """Walking raw pages by cursor yields every run once, newest first."""
    ids = _schedule_runs(inst)
    seen = []
    cursor = None
    while True:
        params = {"job_id": "paginated", "limit": 7}
        if cursor is not None:
            params["cursor"] = cursor
        jso = _raw_runs(inst, **params)
        page = list(jso["runs"])
        assert len(page) <= 7
        seen.extend(page)
        cursor = jso["paging"]["next"]
        if cursor is None:
            break

    assert len(seen) == len(set(seen))  # no duplicates
    assert set(seen) == ids  # no skips
    # descending by run number
    nums = [int(r[1:]) for r in seen]
    assert nums == sorted(nums, reverse=True)


def test_last_page_next_is_none(inst):
    """The final page reports paging.next == null."""
    _schedule_runs(inst)
    jso = _raw_runs(inst, job_id="paginated", limit=N)
    assert len(jso["runs"]) == N
    assert jso["paging"]["next"] is None


def test_invalid_cursor_and_limit_rejected(inst):
    """Malformed cursor/limit produce 400s, not 500s."""
    _schedule_runs(inst, n=1)
    url = f"http://localhost:{inst.port}/api/v1/runs"
    assert requests.get(url, params={"job_id": "paginated", "cursor": "bad"}).status_code == 400
    assert requests.get(url, params={"job_id": "paginated", "limit": "nope"}).status_code == 400
    assert requests.get(url, params={"job_id": "paginated", "limit": "0"}).status_code == 400
