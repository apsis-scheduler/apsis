"""Schedule filtering through the real HTTP service and CLI."""

from contextlib import closing
from pathlib import Path
import subprocess
import sys

import ora
import pytest
import requests
import ujson

from instance import ApsisService
from apsis.service.api import PAGE_SIZE

job_dir = Path(__file__).absolute().parent / "jobs"
DAYS = [ora.Time(f"2026-01-{d:02d}T09:00:00Z") for d in range(1, 8)]


@pytest.fixture
def inst():
    with closing(ApsisService(job_dir=job_dir)) as inst:
        inst.create_db()
        inst.write_cfg()
        inst.start_serve()
        inst.wait_for_serve()
        yield inst


def _schedule(inst, time, i):
    run_id = inst.client.schedule("timed", {"i": str(i)}, time)["run_id"]
    inst.wait_run(run_id)
    return run_id


def _get(inst, **params):
    return requests.get(f"http://localhost:{inst.port}/api/v1/runs", params=params)


def _cli(inst, *args):
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "apsis.cli",
            "--port",
            str(inst.port),
            "runs",
            "-j",
            "timed",
            "--format",
            "json",
            *args,
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return ujson.loads(proc.stdout)


def test_schedule_span_query_and_cli(inst):
    ids = [_schedule(inst, t, i) for i, t in enumerate(DAYS)]
    failed = _schedule(inst, DAYS[3], "fail")
    inst.client.mark(failed, "failure")
    for bound, expected in [
        ({"schedule_since": str(DAYS[2])}, {*ids[2:], failed}),
        ({"schedule_until": str(DAYS[5])}, {*ids[:5], failed}),
    ]:
        resp = _get(inst, job_id="timed", **bound)
        assert resp.status_code == 200, resp.text
        assert set(resp.json()["runs"]) == expected
    span = dict(schedule_since=str(DAYS[2]), schedule_until=str(DAYS[5]))
    resp = _get(inst, job_id="timed", state="success", **span)
    assert resp.status_code == 200
    runs = resp.json()["runs"]
    assert list(runs) == list(reversed(ids[2:5]))  # excludes the otherwise matching failed run
    assert [ora.Time(r["times"]["schedule"]) for r in runs.values()] == list(reversed(DAYS[2:5]))
    assert set(inst.client.get_runs(job_id="timed", args={"i": "1"}, schedule_until=DAYS[2])) == {
        ids[1]
    }
    resp = _get(inst, job_id="timed", i="", state="", cursor="", _schedule_since="")
    assert resp.status_code == 200 and set(resp.json()["runs"]) == {*ids, failed}
    for name in ("schedule_since", "schedule_until"):
        for value in (
            "",
            [str(DAYS[2]), str(DAYS[3])],
            ["", str(DAYS[2])],
            [str(DAYS[2]), ""],
            ["", ""],
            "2026-01-01T00:00:00+99:99",
            "2026-01-01T00:00:00+99:99\0",
            "0001-01-01T00:00:00+23:59",
        ):
            resp = _get(inst, job_id="timed", **{name: value})
            assert resp.status_code == 400
            message = "may be given at most once" if isinstance(value, list) else "invalid"
            assert name in resp.json()["error"] and message in resp.json()["error"]

    args = ("-t", f"{DAYS[2]}..{DAYS[5]}", "-s", "success")
    assert list(_cli(inst, *args)) == list(reversed(ids[2:5]))
    assert list(_cli(inst, *args, "--limit", "2")) == [ids[4], ids[3]]
    assert set(_cli(inst)) == {*ids, failed}
    assert _get(inst, job_id="timed", schedule_since="bad").status_code == 400


def test_schedule_span_paging(inst, monkeypatch):
    """Excluded rows fall beyond the first HTTP page's cursor."""
    expected = []
    for i, (time, matches) in enumerate(
        [
            ("2026-01-05T01:00:00Z", True),
            ("2026-01-04T00:00:00Z", False),  # below lower
            ("2026-01-05T02:00:00Z", True),
            ("2026-01-06T12:00:00Z", False),  # above upper
            ("2026-01-05T03:00:00Z", True),
            ("2026-01-05T04:00:00Z", True),
        ]
    ):
        run_id = _schedule(inst, time, i)
        if matches:
            expected.append(run_id)
    padding = inst.client.schedule(
        "timed", {"i": "page-fill"}, "2026-01-05T05:00:00Z", count=PAGE_SIZE
    )
    for run in padding:
        inst.wait_run(run["run_id"])
        expected.append(run["run_id"])
    want, seen = list(reversed(expected)), []
    request = requests.request

    def check_page(*args, **kwargs):
        resp = request(*args, **kwargs)
        resp.raise_for_status()
        page = resp.json()["runs"]
        assert len(page) <= PAGE_SIZE
        seen.extend(page)
        assert seen == want[: len(seen)]  # catches duplicates before the client merges them
        return resp

    monkeypatch.setattr(requests, "request", check_page)
    runs = inst.client.get_runs(
        job_id="timed", schedule_since="2026-01-05T00:00:00Z", schedule_until="2026-01-06T00:00:00Z"
    )
    assert seen == want
    assert list(runs) == want
