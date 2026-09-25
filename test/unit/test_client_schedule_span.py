"""Client schedule bounds and colliding job arguments survive pagination."""

from unittest.mock import Mock

import ora
import pytest

from apsis.service.client import Client

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"


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
        "schedule_since",
        "schedule_until",
    )
    ordinary = {name: "v" for name in ("limit", "max_runs", "data", "timeout", "query", "scheme")}
    job_args = {name: "v" for name in reserved} | ordinary | {"_schedule_since": "literal"}
    runs = client.get_runs(job_id="job", args=job_args, schedule_since=since, schedule_until=until)
    assert list(runs) == ["r2", "r1"]
    queries = [call.kwargs["query"] for call in get.call_args_list]
    assert [q["cursor"] for q in queries] == [None, "r2"]
    for query in queries:
        assert (query["schedule_since"], query["schedule_until"]) == expected
        assert query["job_id"] == "job"
        assert all(query["_" + name] == "v" for name in reserved)
        assert {name: query[name] for name in ordinary} == ordinary
        assert query["__schedule_since"] == "literal"


@pytest.mark.parametrize("name", ["schedule_since", "schedule_until"])
def test_invalid_span_is_rejected_before_request(monkeypatch, name):
    client = Client(("localhost", 1))
    get = Mock()
    monkeypatch.setattr(client, "_Client__get", get)
    for value, error in [("2026-01-01T00:00:00+99:99", ValueError), (0, TypeError)]:
        with pytest.raises(error):
            client.get_runs(job_id="job", **{name: value})
        get.assert_not_called()
