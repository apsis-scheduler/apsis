"""
Tests that Client.get_runs forwards the schedule time span to GET /runs and
keeps run args from shadowing the query params.
"""

import ora
import pytest

from apsis.service.client import Client

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"
ONE_PAGE = {"runs": {}, "paging": {"next": None}}


@pytest.fixture
def client_calls(monkeypatch):
    """a Client whose __get is stubbed to record queries and return one empty page"""
    client = Client(("localhost", 1))
    calls = []

    def fake_get(*path, **query):
        calls.append(query)
        return ONE_PAGE

    monkeypatch.setattr(client, "_Client__get", fake_get)
    return client, calls


@pytest.mark.parametrize(
    "since, until, exp_since, exp_until",
    [
        (None, None, None, None),  # none is dropped from the url by the client
        (ora.Time(T1), ora.Time(T2), str(ora.Time(T1)), str(ora.Time(T2))),
        ("2026-01-05T14:00:00+05:00", None, str(ora.Time(T2)), None),  # string in another zone
    ],
)
def test_span_forwarded_as_iso_utc(client_calls, since, until, exp_since, exp_until):
    client, calls = client_calls
    client.get_runs(job_id="job", schedule_since=since, schedule_until=until)
    (query,) = calls
    assert query["schedule_since"] == exp_since
    assert query["schedule_until"] == exp_until


def test_span_sent_on_every_page(monkeypatch):
    """the bounds are repeated on each cursor-following request"""
    client = Client(("localhost", 1))
    pages = iter(
        [
            {"runs": {"r2": {}}, "paging": {"next": "r2"}},
            {"runs": {"r1": {}}, "paging": {"next": None}},
        ]
    )
    calls = []

    def fake_get(*path, **query):
        calls.append(query)
        return next(pages)

    monkeypatch.setattr(client, "_Client__get", fake_get)

    runs = client.get_runs(job_id="job", schedule_since=T1, schedule_until=T2)
    assert set(runs) == {"r1", "r2"}
    assert [q["cursor"] for q in calls] == [None, "r2"]
    assert all(q["schedule_since"] == str(ora.Time(T1)) for q in calls)
    assert all(q["schedule_until"] == str(ora.Time(T2)) for q in calls)


def test_run_args_do_not_shadow_query_params(client_calls):
    """
    a run arg literally named like a query param is sent underscore-prefixed, so
    it can't be mistaken for the filter (or paging) params
    """
    client, calls = client_calls
    reserved = (
        "job_id",
        "run_id",
        "state",
        "since",
        "summary",
        "cursor",
        "limit",
        "schedule_since",
        "schedule_until",
    )
    client.get_runs(job_id="job", args={n: "v" for n in reserved}, schedule_since=T1)
    (query,) = calls
    for n in reserved:
        assert query["_" + n] == "v"
    assert query["job_id"] == "job"  # the real filter, untouched
    assert query["schedule_since"] == str(ora.Time(T1))
