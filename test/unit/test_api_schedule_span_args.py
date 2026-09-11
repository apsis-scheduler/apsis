"""
Tests for the schedule_since / schedule_until query param parsing on GET /runs.
"""

import ora
import pytest
from sanic.request import RequestParameters

from apsis.service.api import _parse_schedule_span_args

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"


def _args(**query):
    """sanic-style request args, where every value is a list"""
    return RequestParameters({k: v if isinstance(v, list) else [v] for k, v in query.items()})


def test_valid_inputs_and_arg_preservation():
    # absent means no bounds, and unrelated params are left in place
    args = _args(job_id="job", cursor="r7", _schedule_since="run-arg-value")
    assert _parse_schedule_span_args(args) == (None, None)
    assert args["job_id"] == ["job"] and args["cursor"] == ["r7"]
    assert args["_schedule_since"] == ["run-arg-value"]  # a shadowing run arg is untouched

    # since only, until only, both, and an offset-bearing bound normalized to utc
    assert _parse_schedule_span_args(_args(schedule_since=T1)) == (ora.Time(T1), None)
    assert _parse_schedule_span_args(_args(schedule_until=T2)) == (None, ora.Time(T2))
    both = _args(schedule_since=T1, schedule_until=T2)
    assert _parse_schedule_span_args(both) == (ora.Time(T1), ora.Time(T2))
    assert "schedule_since" not in both and "schedule_until" not in both  # consumed
    assert _parse_schedule_span_args(_args(schedule_since="2026-01-05T14:00:00+05:00")) == (
        ora.Time(T2),
        None,
    )


@pytest.mark.parametrize("name", ["schedule_since", "schedule_until"])
@pytest.mark.parametrize("bad", ["abc", "", "2026-01-01", "now", "+1h"])
def test_invalid_endpoint_is_rejected(name, bad):
    with pytest.raises(ValueError, match=f"invalid {name}"):
        _parse_schedule_span_args(_args(**{name: bad}))


@pytest.mark.parametrize("name", ["schedule_since", "schedule_until"])
def test_repeated_param_raises(name):
    with pytest.raises(ValueError, match=f"{name} may be given at most once"):
        _parse_schedule_span_args(_args(**{name: [T1, T2]}))


@pytest.mark.parametrize("since, until", [(T2, T1), (T1, T1)])  # reversed, then equal
def test_bad_ordering_raises(since, until):
    with pytest.raises(ValueError, match="schedule_since must be before schedule_until"):
        _parse_schedule_span_args(_args(schedule_since=since, schedule_until=until))
