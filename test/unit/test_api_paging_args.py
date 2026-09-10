"""
Tests for the cursor and limit query param parsing on GET /runs.
"""

import pytest
from sanic.request import RequestParameters

from apsis.service.api import DEFAULT_PAGE_SIZE, MAX_CURSOR, MAX_PAGE_SIZE, _parse_paging_args


def _args(**query):
    """Build sanic-style request args, where every value is a list."""
    return RequestParameters({k: v if isinstance(v, list) else [v] for k, v in query.items()})


def test_defaults_when_absent():
    args = _args(job_id="job")
    assert _parse_paging_args(args) == (None, DEFAULT_PAGE_SIZE)
    # other params are left in place
    assert args["job_id"] == ["job"]


def test_limit_and_cursor_are_parsed_and_popped():
    args = _args(limit="25", cursor="r7", job_id="job")
    assert _parse_paging_args(args) == ("r7", 25)
    assert "limit" not in args and "cursor" not in args


def test_limit_is_clamped_to_max():
    assert _parse_paging_args(_args(limit=str(MAX_PAGE_SIZE * 10))) == (None, MAX_PAGE_SIZE)
    assert _parse_paging_args(_args(limit=str(MAX_PAGE_SIZE))) == (None, MAX_PAGE_SIZE)


@pytest.mark.parametrize("bad", ["abc", "0", "-3", "1.5", ""])
def test_bad_limit_raises(bad):
    with pytest.raises(ValueError):
        _parse_paging_args(_args(limit=bad))


# the last case is one past the signed 64 bit sqlite max, which would 500 in the worker
@pytest.mark.parametrize("bad", ["bad", "r-5", "r", "5", "r1.0", f"r{MAX_CURSOR + 1}"])
def test_bad_cursor_raises(bad):
    with pytest.raises(ValueError):
        _parse_paging_args(_args(cursor=bad))


def test_cursor_at_sqlite_max_is_accepted():
    # the largest signed 64 bit integer still binds as a sqlite param
    assert _parse_paging_args(_args(cursor=f"r{MAX_CURSOR}")) == (
        f"r{MAX_CURSOR}",
        DEFAULT_PAGE_SIZE,
    )


def test_repeated_param_raises():
    with pytest.raises(ValueError, match="cursor may be given at most once"):
        _parse_paging_args(_args(cursor=["r1", "r2"]))
    with pytest.raises(ValueError, match="limit may be given at most once"):
        _parse_paging_args(_args(limit=["1", "2"]))
