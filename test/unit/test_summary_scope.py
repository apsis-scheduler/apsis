from types import SimpleNamespace

import pytest
import ujson

from apsis.runs import Instance, Run
from apsis.service import api, messages
from apsis.service.summary import (
    ALL_TYPES,
    MAX_JOB_IDS,
    SummaryScope,
    make_summary_predicate,
    parse_summary_scope,
)


class FakeJobs:
    def get_dependents(self, job_id):
        return frozenset({f"{job_id}/down"})


def parse(query_string):
    return parse_summary_scope(query_string, FakeJobs())


def test_parse_defaults():
    assert parse("init") == SummaryScope(init=True)
    assert parse("") == SummaryScope(init=False)
    assert not parse("init").scoped


def test_parse_types():
    scope = parse("init&types=job,agent_conn")
    assert scope.types == {"job", "agent_conn"}
    assert scope.job_ids is None
    assert scope.scoped


def test_parse_job_ids():
    scope = parse("job_id=a&job_id=b%2Fc&dependents_of=a")
    assert scope.job_ids == {"a", "b/c", "a/down"}
    assert scope.types == ALL_TYPES
    assert parse("dependents_of=a").job_ids == {"a/down"}


@pytest.mark.parametrize(
    "query_string",
    [
        "types=job,bogus",
        "&".join(f"job_id={i}" for i in range(MAX_JOB_IDS + 1)),
        "&".join(f"job_id={i}" for i in range(MAX_JOB_IDS)) + "&dependents_of=a",
    ],
)
def test_parse_invalid(query_string):
    with pytest.raises(ValueError):
        parse(query_string)


MSGS = [
    {"type": "job", "job": {"job_id": "a"}},
    {"type": "job_add", "job": {"job_id": "b"}},
    {"type": "job_delete", "job_id": "a"},
    {"type": "agent_conn", "conn": {}},
    {"type": "agent_conn_delete", "conn_id": "c"},
    {"type": "run_summary", "run_summary": {"job_id": "a"}},
    {"type": "run_transition", "run_summary": {"job_id": "b"}},
    {"type": "run_delete", "run_id": "r1", "job_id": "a"},
    {"type": "run_log", "run_id": "r1"},
]


@pytest.mark.parametrize(
    "scope, passed",
    [
        (SummaryScope(init=True), [0, 1, 2, 3, 4, 5, 6, 7]),
        (SummaryScope(init=True, types=frozenset({"job", "agent_conn"})), [0, 1, 2, 3, 4]),
        (SummaryScope(init=True, job_ids=frozenset({"a"})), [0, 2, 3, 4, 5, 7]),
    ],
)
def test_predicate(scope, passed):
    predicate = make_summary_predicate(scope)
    assert [i for i, m in enumerate(MSGS) if predicate(m)] == passed


def test_run_delete_job_id():
    assert messages.make_run_delete(Run(Instance("a", {})))["job_id"] == "a"


@pytest.mark.asyncio
async def test_scope_failure_sends_error(monkeypatch):
    """A scope that fails to build for any reason gets a summary_error and a close."""

    def fail(query_string: str, jobs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(api, "parse_summary_scope", fail)
    sent, closed = [], []

    async def send(data: str) -> None:
        sent.append(ujson.loads(data))

    async def close(code: int, reason: str) -> None:
        closed.append(code)

    request = SimpleNamespace(
        app=SimpleNamespace(apsis=SimpleNamespace(jobs=None)),
        socket=("127.0.0.1", 1),
        query_string="init&dependents_of=a",
    )
    await api.websocket_summary(request, SimpleNamespace(send=send, close=close))
    assert [m["type"] for msgs in sent for m in msgs] == ["summary_error"]
    assert closed == [1011]
