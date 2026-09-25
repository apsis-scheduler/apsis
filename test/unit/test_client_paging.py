"""
Tests that Client.get_runs follows the /runs paging.next cursor and returns the
full merged {run_id: run} dict.
"""

import pytest

from apsis.service.client import Client


def _client_returning(monkeypatch, pages):
    """A Client whose __get yields the given pages in order, recording cursors."""
    client = Client(("localhost", 1))
    it = iter(pages)
    calls = []

    def fake_get(*path, **query):
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
