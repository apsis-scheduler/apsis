"""Schedule-bound argument parsing for GET /runs."""

import ora
import pytest
from sanic.request import RequestParameters

from apsis.service.api import _parse_schedule_span_args

T1 = "2026-01-01T00:00:00Z"
T2 = "2026-01-05T09:00:00Z"


def _args(**query):
    """Build sanic-style request args, where every value is a list."""
    return RequestParameters({k: v if isinstance(v, list) else [v] for k, v in query.items()})


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
@pytest.mark.parametrize("bad", ["2026-01-01", "now", "+1h"])
def test_invalid_endpoint_is_rejected(name, bad):
    with pytest.raises(ValueError, match=f"invalid {name}"):
        _parse_schedule_span_args(_args(**{name: bad}))


@pytest.mark.parametrize("since, until", [(T2, T1), (T1, T1)])  # reversed, then equal
def test_bad_ordering_raises(since, until):
    with pytest.raises(ValueError, match="schedule_since must be before schedule_until"):
        _parse_schedule_span_args(_args(schedule_since=since, schedule_until=until))
