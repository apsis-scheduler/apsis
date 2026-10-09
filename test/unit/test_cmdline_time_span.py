"""Time-span parsing for apsis runs --times."""

import ora
import pytest

import apsis.cmdline
from apsis.cmdline import parse_time_span

T1 = ora.Time("2026-01-01T00:00:00Z")
T2 = ora.Time("2026-01-05T09:00:00Z")
NOW = ora.Time("2026-03-01T12:00:00Z")


@pytest.fixture
def fixed_now(monkeypatch):
    """pin the clock and display zone so relative and daytime bounds are deterministic"""
    monkeypatch.setattr(apsis.cmdline, "now", lambda: NOW)
    monkeypatch.setattr(apsis.cmdline, "get_display_time_zone", lambda: ora.UTC)
    return NOW


@pytest.mark.parametrize(
    "text, expected",
    [
        (f"{T1}..", (T1, None)),
        (f"..{T2}", (None, T2)),
        (f"  {T1} .. {T2} ", (T1, T2)),  # whitespace stripped
        (f" {T1} ", (T1, None)),
        ("2026-01-05T14:00:00+05:00..", (T2, None)),  # offset normalized to utc
    ],
)
def test_absolute_span(text, expected):
    result = parse_time_span(text)
    assert result == expected
    assert all(t is None or isinstance(t, ora.Time) for t in result)


def test_now_and_relative_bounds(fixed_now):
    assert parse_time_span("now") == (fixed_now, None)
    assert parse_time_span("..now") == (None, fixed_now)
    assert parse_time_span("+0") == (fixed_now, None)
    assert parse_time_span("+1h") == (fixed_now + 3600, None)
    assert parse_time_span("+1h..+2h") == (fixed_now + 3600, fixed_now + 7200)


def test_daytime_bound_is_today_not_next_occurrence(fixed_now):
    nine, thirteen = ora.Time("2026-03-01T09:00:00Z"), ora.Time("2026-03-01T13:00:00Z")
    assert parse_time_span("09:00:00") == (nine, None)  # earlier today, not tomorrow
    assert parse_time_span("09:00:00..13:00:00") == (nine, thirteen)


@pytest.fixture
def new_york(monkeypatch):
    monkeypatch.setattr(
        apsis.cmdline, "get_display_time_zone", lambda: ora.TimeZone("America/New_York")
    )


def test_daytime_bound_uses_display_time_zone(new_york, monkeypatch):
    # 02:00Z is still Feb 28 in New York.
    monkeypatch.setattr(apsis.cmdline, "now", lambda: ora.Time("2026-03-01T02:00:00Z"))
    assert parse_time_span("09:00:00") == (ora.Time("2026-02-28T14:00:00Z"), None)


def test_daytime_in_dst_gap_is_a_value_error(new_york, monkeypatch):
    monkeypatch.setattr(apsis.cmdline, "now", lambda: ora.Time("2026-03-08T12:00:00Z"))
    with pytest.raises(ValueError, match="does not exist today"):
        parse_time_span("02:30:00..03:30:00")
    since, until = parse_time_span("01:30:00..03:30:00")
    assert since < until


def test_now_is_read_once(monkeypatch):
    """both endpoints share one clock read, so now..now is empty even if the clock advances"""
    ticks = iter([ora.Time("2026-03-01T12:00:00Z"), ora.Time("2026-03-01T12:00:05Z")])
    monkeypatch.setattr(apsis.cmdline, "now", lambda: next(ticks))
    # a second clock read would make start < end and pass, one read makes them equal
    with pytest.raises(ValueError, match="start must be before end"):
        parse_time_span("now..now")


@pytest.mark.parametrize(
    "message, inputs",
    [
        ("empty time span", ["", "..", " .. "]),
        ("start must be before end", [f"{T2}..{T1}", f"{T1}..{T1}"]),
        (
            "cannot interpret as time",
            [
                "garbage",
                f"garbage..{T2}",
                f"{T1}..garbage",
                f"{T1}..{T2}..{T2}",
                "2026-01-01",
                "09:00",
                "25:00:00",
            ],
        ),
        ("duration", ["+abc", "+1x", "+", "+1e100", "+nan", "+inf"]),
    ],
)
def test_invalid_span_raises(message, inputs):
    for text in inputs:
        with pytest.raises(ValueError, match=message):
            parse_time_span(text)
