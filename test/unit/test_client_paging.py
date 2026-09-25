"""
Tests that Client.get_runs follows the /runs paging.next cursor and returns the
full merged {run_id: run} dict.
"""

from unittest.mock import Mock

import pytest

from apsis.service.client import Client


def _client_returning(monkeypatch, pages):
    """A Client whose __get yields the given pages in order, recording cursors."""
    client = Client(("localhost", 1))
    it = iter(pages)
    calls = []

    def fake_get(*path, query):
        calls.append(query.get("cursor"))
        return next(it)

    monkeypatch.setattr(client, "_Client__get", fake_get)
    return client, calls


def test_get_runs_follows_cursor(monkeypatch):
    client, calls = _client_returning(
        monkeypatch,
        [
            {"runs": {"r5": {}, "r4": {}}, "paging": {"next": "r4"}},
            {"runs": {"r3": {}, "r2": {}}, "paging": {"next": "r2"}},
            {"runs": {"r1": {}}, "paging": {"next": None}},
        ],
    )
    result = client.get_runs(job_id="job")

    assert set(result) == {"r1", "r2", "r3", "r4", "r5"}
    assert calls == [None, "r4", "r2"]


def test_get_runs_limit_stops_early(monkeypatch):
    # limit is a total, the walk stops once it has that many and never fetches page 3
    client, calls = _client_returning(
        monkeypatch,
        [
            {"runs": {"r5": {}, "r4": {}}, "paging": {"next": "r4"}},
            {"runs": {"r3": {}, "r2": {}}, "paging": {"next": "r2"}},
            {"runs": {"r1": {}}, "paging": {"next": None}},
        ],
    )
    result = client.get_runs(job_id="job", limit=3)

    assert list(result) == ["r5", "r4", "r3"]  # newest three, in order
    assert calls == [None, "r4"]  # stopped after two pages


@pytest.mark.parametrize("limit", [0, -1])
def test_get_runs_rejects_limit_below_one(monkeypatch, limit):
    client, calls = _client_returning(monkeypatch, [])
    with pytest.raises(ValueError, match="limit must be at least 1"):
        client.get_runs(job_id="job", limit=limit)
    assert calls == []


def test_get_job_runs_walks_cursor(monkeypatch):
    # get_job_runs paginates the same way, following paging.next across pages
    client, calls = _client_returning(
        monkeypatch,
        [
            {"runs": {"r5": {}, "r4": {}}, "paging": {"next": "r4"}},
            {"runs": {"r3": {}}, "paging": {"next": None}},
        ],
    )
    result = client.get_job_runs("job")

    assert set(result) == {"r5", "r4", "r3"}
    assert calls == [None, "r4"]


@pytest.mark.parametrize("next_cursor", ["r5", "r6"])
def test_get_runs_raises_on_non_decreasing_cursor(monkeypatch, next_cursor):
    # a repeated or increasing next cursor must fail loud, not loop
    client, _ = _client_returning(
        monkeypatch,
        [
            {"runs": {"r5": {}}, "paging": {"next": "r5"}},
            {"runs": {"r5": {}}, "paging": {"next": next_cursor}},
        ],
    )
    with pytest.raises(RuntimeError, match="cursor did not decrease"):
        client.get_runs(job_id="job")


def test_get_runs_sends_colliding_args_as_filters(monkeypatch):
    # job args named like client keywords or starting with an underscore still reach the server as filters
    requests = []

    def fake_request(method, url, json, timeout):
        requests.append((url, json, timeout))
        return Mock(status_code=200, json=Mock(return_value={"runs": {}}))

    monkeypatch.setattr("apsis.service.client.requests.request", fake_request)
    Client(("localhost", 1)).get_runs(
        job_id="job", args={"data": "a", "timeout": "5", "summary": "true", "_x": "1"}
    )

    ((url, json, timeout),) = requests
    assert url.endswith("?job_id=job&data=a&timeout=5&_summary=true&__x=1")
    assert json is None and timeout is None
