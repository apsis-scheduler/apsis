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
    """client.get_runs walks several real pages by cursor and merges them all."""
    ids = _schedule_runs(inst, n=5)
    client = inst.client

    # force a tiny page so the client has to follow the cursor across many real
    # http requests, and record the cursor each request actually carried
    cursors = []
    original = client._Client__get

    def recording_get(*path, **query):
        cursors.append(query.get("cursor"))
        return original(*path, limit=2, **query)

    client._Client__get = recording_get

    got = client.get_runs(job_id="paginated")
    assert set(got) == ids
    # more than one page, first request has no cursor, later ones carry the prior next
    assert len(cursors) > 1
    assert cursors[0] is None
    assert all(c is not None for c in cursors[1:])


def test_raw_pages_scroll_complete_no_dupes(inst):
    """
    Walking raw pages by cursor yields every run once, newest first.  A non-final
    page is bounded by the limit and carries a next cursor; the final page reports
    paging.next null.
    """
    ids = _schedule_runs(inst)
    limit = 7
    seen = []
    cursor = None
    for _ in range(len(ids) // limit + 2):  # bounded so a broken cursor fails, not hangs
        params = {"job_id": "paginated", "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        jso = _raw_runs(inst, **params)
        page = list(jso["runs"])
        assert len(page) <= limit
        seen.extend(page)
        cursor = jso["paging"]["next"]
        if cursor is None:
            break
        # every non-final page is exactly full and reports more to come
        assert len(page) == limit
    else:
        raise AssertionError("scroll did not terminate")

    assert cursor is None  # final page ends the scroll
    assert len(seen) == len(set(seen))  # no duplicates
    assert set(seen) == ids  # no skips
    nums = [int(r[1:]) for r in seen]
    assert nums == sorted(nums, reverse=True)  # descending by run number


def test_run_id_filter_returns_all_in_one_page(inst):
    """Explicit run_id filters return every requested run with no further page."""
    ids = sorted(_schedule_runs(inst, n=8))
    jso = _raw_runs(inst, run_id=ids, limit=2)  # limit is ignored for run_id
    assert set(jso["runs"]) == set(ids)
    assert jso["paging"]["next"] is None


def test_invalid_paging_params_rejected(inst):
    """Malformed, oversized, or repeated cursor/limit produce 400s, not 500s."""
    url = f"http://localhost:{inst.port}/api/v1/runs"
    bad = [
        {"cursor": "bad"},
        {"cursor": "r-5"},
        {"cursor": f"r{2**63}"},  # past the signed 64 bit sqlite max
        {"limit": "nope"},
        {"limit": "0"},
    ]
    for extra in bad:
        assert requests.get(url, params={"job_id": "paginated", **extra}).status_code == 400
    # a repeated pagination param is a clean 400 too
    repeated = [("job_id", "paginated"), ("cursor", "r1"), ("cursor", "r2")]
    assert requests.get(url, params=repeated).status_code == 400
