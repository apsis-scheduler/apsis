"""Client run queries: pagination, limits, schedule bounds, and job arguments."""

from unittest.mock import Mock

import ora
import pytest

from apsis.service.client import Client

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"


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


def test_get_runs_raises_on_non_advancing_cursor(monkeypatch):
    # a server bug that repeats the same next cursor must fail loud, not loop
    client, _ = _client_returning(
        monkeypatch,
        [
            {"runs": {"r5": {}}, "paging": {"next": "r5"}},
            {"runs": {"r5": {}}, "paging": {"next": "r5"}},
        ],
    )
    with pytest.raises(RuntimeError, match="cursor did not advance"):
        client.get_runs(job_id="job")


@pytest.mark.parametrize(
    "since, until, expected",
    [
        (ora.Time(T1), ora.Time(T2), (str(ora.Time(T1)), str(ora.Time(T2)))),
        ("2026-01-05T14:00:00+05:00", None, (str(ora.Time(T2)), None)),
    ],
)
def test_span_and_run_args_survive_paging(monkeypatch, since, until, expected):
    client = Client(("localhost", 1))
    get = Mock(
        side_effect=[
            {"runs": {"r2": {}}, "paging": {"next": "r2"}},
            {"runs": {"r1": {}}, "paging": {"next": None}},
        ]
    )
    monkeypatch.setattr(client, "_Client__get", get)
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
    runs = client.get_runs(
        job_id="job", args={n: "v" for n in reserved}, schedule_since=since, schedule_until=until
    )
    assert list(runs) == ["r2", "r1"]
    queries = [call.kwargs for call in get.call_args_list]
    assert [q["cursor"] for q in queries] == [None, "r2"]
    for query in queries:
        assert (query["schedule_since"], query["schedule_until"]) == expected
        assert query["job_id"] == "job"
        assert all(query["_" + name] == "v" for name in reserved)


@pytest.mark.parametrize("name", ["schedule_since", "schedule_until"])
def test_invalid_span_is_rejected_before_request(monkeypatch, name):
    client = Client(("localhost", 1))
    get = Mock()
    monkeypatch.setattr(client, "_Client__get", get)
    for value, error in [("2026-01-01T00:00:00+99:99", ValueError), (0, TypeError)]:
        with pytest.raises(error):
            client.get_runs(job_id="job", **{name: value})
        get.assert_not_called()
