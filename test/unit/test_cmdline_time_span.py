"""
Tests for apsis.cmdline.parse_time_span, the parser behind `apsis runs --times`.
"""

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
        (f"{T1}..{T2}", (T1, T2)),
        (f"{T1}..", (T1, None)),
        (str(T1), (T1, None)),  # bare, no separator
        (f"..{T2}", (None, T2)),
        (f"  {T1} .. {T2} ", (T1, T2)),  # whitespace stripped
        (f" {T1} ", (T1, None)),
        ("2026-01-05T14:00:00+05:00..", (T2, None)),  # offset normalized to utc
    ],
)
def test_absolute_span(text, expected):
    since, until = parse_time_span(text)
    assert (since, until) == expected
    assert since is None or isinstance(since, ora.Time)
    assert until is None or isinstance(until, ora.Time)


def test_now_and_relative_bounds(fixed_now):
    assert parse_time_span("now") == (fixed_now, None)
    assert parse_time_span("..now") == (None, fixed_now)
    assert parse_time_span("+1h") == (fixed_now + 3600, None)
    assert parse_time_span("+1h..+2h") == (fixed_now + 3600, fixed_now + 7200)


def test_daytime_bound_is_today_not_next_occurrence(fixed_now):
    """
    a daytime means today in the display zone, even one earlier than now, so a
    span around the current time isn't inverted (parse_at_time would roll it to
    tomorrow, which is wrong for a filter over past runs)
    """
    today = ora.Date(2026, 3, 1)
    nine = (today, ora.Daytime(9, 0, 0)) @ ora.UTC
    thirteen = (today, ora.Daytime(13, 0, 0)) @ ora.UTC
    assert parse_time_span("09:00:00") == (nine, None)  # before pinned noon, still today
    assert parse_time_span("09:00:00..13:00:00") == (nine, thirteen)  # today..today, in order


def test_daytime_bound_uses_display_time_zone(monkeypatch):
    """the date comes from the display zone, not utc, so it can differ near midnight"""
    monkeypatch.setattr(
        apsis.cmdline, "get_display_time_zone", lambda: ora.TimeZone("America/New_York")
    )
    # 02:00Z is still Feb 28 in New York, so 09:00 there is 2026-02-28T14:00Z
    monkeypatch.setattr(apsis.cmdline, "now", lambda: ora.Time("2026-03-01T02:00:00Z"))
    since, _ = parse_time_span("09:00:00")
    assert since == ora.Time("2026-02-28T14:00:00Z")


def test_daytime_in_dst_gap_is_a_value_error(monkeypatch):
    """a daytime that doesn't exist today (spring-forward gap) is a parse error, not a crash"""
    monkeypatch.setattr(
        apsis.cmdline, "get_display_time_zone", lambda: ora.TimeZone("America/New_York")
    )
    monkeypatch.setattr(apsis.cmdline, "now", lambda: ora.Time("2026-03-08T12:00:00Z"))
    with pytest.raises(ValueError, match="does not exist today"):
        parse_time_span("02:30:00..03:30:00")  # 02:00-03:00 local doesn't exist that day
    since, until = parse_time_span("01:30:00..03:30:00")  # neighbors are fine
    assert since < until


def test_now_is_read_once(monkeypatch):
    """both endpoints share one clock read, so now..now is empty even if the clock advances"""
    ticks = iter([ora.Time("2026-03-01T12:00:00Z"), ora.Time("2026-03-01T12:00:05Z")])
    monkeypatch.setattr(apsis.cmdline, "now", lambda: next(ticks))
    # a second clock read would make start < end and pass, one read makes them equal
    with pytest.raises(ValueError, match="start must be before end"):
        parse_time_span("now..now")


@pytest.mark.parametrize("bad", ["", "..", " .. "])
def test_empty_span_raises(bad):
    with pytest.raises(ValueError, match="empty time span"):
        parse_time_span(bad)


@pytest.mark.parametrize("bad", [f"{T2}..{T1}", f"{T1}..{T1}"])  # reversed, then equal
def test_bad_ordering_raises(bad):
    with pytest.raises(ValueError, match="start must be before end"):
        parse_time_span(bad)


@pytest.mark.parametrize(
    "bad",
    [
        "garbage",
        f"garbage..{T2}",
        f"{T1}..garbage",
        f"{T1}..{T2}..{T2}",
        "2026-01-01",
        "09:00",
        "25:00:00",
    ],
)
def test_unparseable_bound_raises(bad):
    with pytest.raises(ValueError, match="cannot interpret as time"):
        parse_time_span(bad)


@pytest.mark.parametrize("bad", ["+abc", "+1x", "+", "+1e100", "+nan", "+inf"])
def test_bad_duration_raises(bad):
    # includes non-finite/overflowing durations that ora can't add to a time
    with pytest.raises(ValueError, match="duration"):
        parse_time_span(bad)
