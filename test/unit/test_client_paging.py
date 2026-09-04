"""
Tests that the Client follows the runs endpoints' paging.next cursor and
returns the full merged {run_id: run} dict.
"""

from apsis.service.client import Client


def _fake_pages(pages):
    """Return a fake __get that yields canned paged responses in order."""
    it = iter(pages)
    calls = []

    def fake_get(*path, **query):
        calls.append(query.get("cursor"))
        return next(it)

    return fake_get, calls


def test_get_runs_follows_cursor(monkeypatch):
    client = Client(("localhost", 1))
    pages = [
        {"runs": {"r5": {}, "r4": {}}, "paging": {"next": "r4"}},
        {"runs": {"r3": {}, "r2": {}}, "paging": {"next": "r2"}},
        {"runs": {"r1": {}}, "paging": {"next": None}},
    ]
    fake_get, calls = _fake_pages(pages)
    monkeypatch.setattr(client, "_Client__get", fake_get)

    result = client.get_runs(job_id="job")

    assert set(result) == {"r1", "r2", "r3", "r4", "r5"}
    # first request has no cursor, then each subsequent uses the prior next.
    assert calls == [None, "r4", "r2"]


def test_get_runs_single_page(monkeypatch):
    client = Client(("localhost", 1))
    fake_get, calls = _fake_pages([{"runs": {"r1": {}}, "paging": {"next": None}}])
    monkeypatch.setattr(client, "_Client__get", fake_get)

    assert set(client.get_runs(job_id="job")) == {"r1"}
    assert calls == [None]
