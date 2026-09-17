"""Query argument parsing for GET /runs."""

import ora
import pytest
from sanic.request import RequestParameters

from apsis.service.api import MAX_CURSOR, _parse_cursor, _parse_schedule_span_args

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"


def _args(**query):
    """Build sanic-style request args, where every value is a list."""
    return RequestParameters({k: v if isinstance(v, list) else [v] for k, v in query.items()})


def test_cursor_absent_is_none():
    args = _args(job_id="job")
    assert _parse_cursor(args) is None
    # other params are left in place
    assert args["job_id"] == ["job"]


def test_cursor_parsed_and_popped():
    args = _args(cursor="r7", job_id="job")
    assert _parse_cursor(args) == "r7"
    assert "cursor" not in args


# the last case is one past the signed 64 bit sqlite max, which would 500 in the worker
@pytest.mark.parametrize("bad", ["bad", "r-5", "r", "5", "r1.0", f"r{MAX_CURSOR + 1}"])
def test_bad_cursor_raises(bad):
    with pytest.raises(ValueError):
        _parse_cursor(_args(cursor=bad))


def test_cursor_at_sqlite_max_is_accepted():
    # the largest signed 64 bit integer still binds as a sqlite param
    assert _parse_cursor(_args(cursor=f"r{MAX_CURSOR}")) == f"r{MAX_CURSOR}"


def test_repeated_cursor_raises():
    with pytest.raises(ValueError, match="cursor may be given at most once"):
        _parse_cursor(_args(cursor=["r1", "r2"]))


def test_valid_inputs_and_arg_preservation():
    preserved = _args(job_id="job", cursor="r7", _schedule_since="run-arg-value")
    for query, expected in [
        ({}, (None, None)),
        ({"schedule_since": T1}, (ora.Time(T1), None)),
        ({"schedule_until": T2}, (None, ora.Time(T2))),
        ({"schedule_since": T1, "schedule_until": T2}, (ora.Time(T1), ora.Time(T2))),
        ({"schedule_since": "2026-01-05T14:00:00+05:00"}, (ora.Time(T2), None)),
    ]:
        args = _args(**query, **preserved)
        assert _parse_schedule_span_args(args) == expected
        assert args == preserved  # bounds consumed; unrelated and escaped arguments retained


@pytest.mark.parametrize("name", ["schedule_since", "schedule_until"])
@pytest.mark.parametrize(
    "bad", ["abc", "", "2026-01-01", "now", "+1h", "2026-01-01T00:00:00+99:99"]
)
def test_invalid_endpoint_is_rejected(name, bad):
    with pytest.raises(ValueError, match=f"invalid {name}"):
        _parse_schedule_span_args(_args(**{name: bad}))


@pytest.mark.parametrize("name", ["schedule_since", "schedule_until"])
def test_repeated_param_raises(name):
    for values in ([T1, T2], ["", T1], [T1, ""], ["", ""]):
        with pytest.raises(ValueError, match=f"{name} may be given at most once"):
            _parse_schedule_span_args(_args(**{name: values}))


@pytest.mark.parametrize("since, until", [(T2, T1), (T1, T1)])  # reversed, then equal
def test_bad_ordering_raises(since, until):
    with pytest.raises(ValueError, match="schedule_since must be before schedule_until"):
        _parse_schedule_span_args(_args(schedule_since=since, schedule_until=until))
