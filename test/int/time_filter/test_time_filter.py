"""
Integration tests for schedule time filtering of GET /runs, iac#2130.
"""

from contextlib import closing
import os
from pathlib import Path
import subprocess
import sys

import ora
import pytest
import requests
import ujson

from instance import ApsisService

# -------------------------------------------------------------------------------

job_dir = Path(__file__).absolute().parent / "jobs"

# nominal times, all in the past so the runs run right away
DAYS = [ora.Time(f"2026-01-{d:02d}T09:00:00Z") for d in range(1, 8)]


@pytest.fixture(scope="function")
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def _schedule(inst, time, i):
    """schedule one finished run of `timed` at nominal `time`, return its run_id"""
    run_id = inst.client.schedule("timed", {"i": str(i)}, time)["run_id"]
    inst.wait_run(run_id)
    return run_id


def _get(inst, **params):
    resp = requests.get(f"http://localhost:{inst.port}/api/v1/runs", params=params)
    return resp.status_code, resp.json()


def _cli(inst, *argv):
    proc = subprocess.run(
        [sys.executable, "-m", "apsis.cli", "--port", str(inst.port), "runs", "-j", "timed", *argv],
        capture_output=True,
        text=True,
        env=os.environ,
    )
    return proc.returncode, proc.stdout, proc.stderr


def test_schedule_span_query(inst):
    """bounds, the returned schedule field, a blank bound, and the state filter over http"""
    by_time = {t: _schedule(inst, t, i) for i, t in enumerate(DAYS)}
    d = DAYS

    # lower inclusive
    _, jso = _get(inst, job_id="timed", schedule_since=str(d[2]))
    assert set(jso["runs"]) == {by_time[t] for t in d[2:]}
    assert all(ora.Time(r["times"]["schedule"]) >= d[2] for r in jso["runs"].values())
    # upper exclusive
    _, jso = _get(inst, job_id="timed", schedule_until=str(d[5]))
    assert set(jso["runs"]) == {by_time[t] for t in d[:5]}
    # span composes with a job-argument filter
    _, jso = _get(inst, job_id="timed", i="1", schedule_until=str(d[2]))
    assert set(jso["runs"]) == {by_time[d[1]]}
    # empty span
    _, jso = _get(
        inst,
        job_id="timed",
        schedule_since="2030-01-01T00:00:00Z",
        schedule_until="2030-01-02T00:00:00Z",
    )
    assert jso["runs"] == {} and jso["paging"]["next"] is None

    # add a failed run in the span, then check state composes and a blank bound is unbounded
    failed = _schedule(inst, d[3], "fail")
    inst.client.mark(failed, "failure")
    status, jso = _get(inst, job_id="timed", schedule_until="")  # sanic drops the blank value
    assert status == 200 and failed in jso["runs"]
    _, jso = _get(
        inst, job_id="timed", state="success", schedule_since=str(d[2]), schedule_until=str(d[5])
    )
    assert failed not in jso["runs"]  # in the span but not success
    assert set(jso["runs"]) == {by_time[d[2]], by_time[d[3]], by_time[d[4]]}


def test_schedule_span_paging(inst):
    """
    scroll a span over http with below-lower and above-upper runs interleaved, so
    a dropped bound on a later page would surface an excluded run.  ids are
    explicit and the cursor must strictly advance.
    """
    since, until = "2026-01-05T00:00:00Z", "2026-01-06T00:00:00Z"
    expected = []  # creation order by run number, excluded runs sit between matches
    expected.append(_schedule(inst, "2026-01-05T01:00:00Z", 0))
    _schedule(inst, "2026-01-04T00:00:00Z", 1)  # below lower
    expected.append(_schedule(inst, "2026-01-05T02:00:00Z", 2))
    _schedule(inst, "2026-01-06T12:00:00Z", 3)  # above upper
    expected.append(_schedule(inst, "2026-01-05T03:00:00Z", 4))
    expected.append(_schedule(inst, "2026-01-05T04:00:00Z", 5))
    want = list(reversed(expected))

    seen, cursor, last = [], None, None
    for _ in range(len(want) + 5):  # bounded, can't hang
        params = dict(job_id="timed", schedule_since=since, schedule_until=until, limit=2)
        if cursor is not None:
            params["cursor"] = cursor
        _, jso = _get(inst, **params)
        assert len(jso["runs"]) <= 2
        seen.extend(jso["runs"])
        cursor = jso["paging"]["next"]
        if cursor is None:
            break
        assert last is None or int(cursor[1:]) < int(last[1:])  # strictly advancing
        last = cursor
    assert seen == want


def test_schedule_span_invalid_rejected(inst):
    """malformed, reversed and repeated bounds are 400s (the job is loaded, no runs needed)"""
    url = f"http://localhost:{inst.port}/api/v1/runs"
    for params in (
        [("job_id", "timed"), ("schedule_since", "bad")],
        [("job_id", "timed"), ("schedule_until", "2026-01-01")],  # a bare date is not a time
        [("job_id", "timed"), ("schedule_since", str(DAYS[3])), ("schedule_until", str(DAYS[1]))],
        [("job_id", "timed"), ("schedule_since", str(DAYS[1])), ("schedule_since", str(DAYS[2]))],
    ):
        assert requests.get(url, params=params).status_code == 400, params


def test_cli_runs_times_end_to_end(inst):
    """the real apsis runs -t filters through the client; no --times is unchanged"""
    by_time = {t: _schedule(inst, t, i) for i, t in enumerate(DAYS)}
    since, until = DAYS[2], DAYS[5]

    rc, out, err = _cli(inst, "-t", f"{since}..{until}", "--format", "json")
    assert rc == 0, err
    assert set(ujson.loads(out)) == {by_time[DAYS[2]], by_time[DAYS[3]], by_time[DAYS[4]]}

    rc, out, _ = _cli(inst, "--format", "json")
    assert rc == 0 and set(ujson.loads(out)) == set(by_time.values())
