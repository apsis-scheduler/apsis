"""
Tests for the cursor query param parsing on GET /runs.
"""

import pytest
from sanic.request import RequestParameters

from apsis.service.api import MAX_CURSOR, _parse_cursor


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
