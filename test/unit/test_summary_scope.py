import ora
import pytest

from apsis.run_window import JobFilter, WindowSpec
from apsis.runs import Instance, Run
from apsis.service import messages
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


def test_parse_window():
    scope = parse(
        "init&time=2026-10-14T12:00:00Z&limit=10&grouping=0&path=a&keyword=X&label=l"
        "&state=failure&arg=x=1&arg=y"
    )
    assert scope == SummaryScope(
        init=True,
        types=frozenset({"run"}),
        window=WindowSpec(
            anchor=ora.Time("2026-10-14T12:00:00Z"),
            limit=10,
            grouping=False,
            states=frozenset({"failure"}),
            args=(("x", "1"), ("y", None)),
            job_filter=JobFilter(path="a", keywords=("X",), labels=("l",)),
        ),
    )

    assert not parse("time=now&grouping=false").window.grouping
    assert parse("time=now&grouping=True").window.grouping

    scope = parse("time=now&types=job,run&job_id=a/x")
    assert scope.window == WindowSpec(job_ids=frozenset({"a/x"}))
    assert scope.types == {"job", "run"}


@pytest.mark.parametrize(
    "query_string",
    [
        "types=job,bogus",
        "&".join(f"job_id={i}" for i in range(MAX_JOB_IDS + 1)),
        "&".join(f"job_id={i}" for i in range(MAX_JOB_IDS)) + "&dependents_of=a",
        "time=bogus",
        "time=now&limit=0",
        "time=now&limit=1001",
        "time=now&state=bogus",
        "time=now&limit=1&limit=2",
        "time=now&grouping=maybe",
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


def test_window_predicate():
    predicate = make_summary_predicate(parse("time=now&path=a&label=l&arg=x=1"))

    def run(job_id, args, labels=("l",)):
        return {
            "type": "run_transition",
            "run_summary": {"job_id": job_id, "args": args, "labels": list(labels)},
        }

    # runs match on their own job id and labels, even for jobs that are gone
    assert predicate(run("a/gone", {"x": "1"}))
    assert not predicate(run("a/x", {"x": "2"}))
    assert not predicate(run("b/x", {"x": "1"}))
    assert not predicate(run("ab/x", {"x": "1"}))
    assert not predicate(run("a/x", {"x": "1"}, labels=()))
    assert predicate({"type": "run_delete", "run_id": "r1", "job_id": "a/x"})
    assert not predicate({"type": "job_add", "job": {"job_id": "a/z", "metadata": {}}})


def test_run_delete_job_id():
    assert messages.make_run_delete(Run(Instance("a", {})))["job_id"] == "a"
