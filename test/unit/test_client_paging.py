"""
Tests that Client.get_runs follows the /runs paging.next cursor and returns the
full merged {run_id: run} dict.
"""

from apsis.service.client import Client


def test_get_runs_follows_cursor(monkeypatch):
    client = Client(("localhost", 1))
    pages = [
        {"runs": {"r5": {}, "r4": {}}, "paging": {"next": "r4"}},
        {"runs": {"r3": {}, "r2": {}}, "paging": {"next": "r2"}},
        {"runs": {"r1": {}}, "paging": {"next": None}},
    ]
    it = iter(pages)
    calls = []

    def fake_get(*path, **query):
        calls.append(query.get("cursor"))
        return next(it)

    monkeypatch.setattr(client, "_Client__get", fake_get)

    result = client.get_runs(job_id="job")

    assert set(result) == {"r1", "r2", "r3", "r4", "r5"}
    assert calls == [None, "r4", "r2"]
