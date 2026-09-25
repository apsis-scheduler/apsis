import ora
import pytest

from apsis.lib.parse import parse_time

# -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        ora.Time("2026-01-05T09:00:00.00000101Z"),
        "2026-01-05T09:00:00.00000101Z",
        "2026-01-06T08:59:00.00000101+23:59",
        "2026-01-04T09:01:00.00000101-23:59",
    ],
)
def test_parse_time_preserves_instant_and_precision(value):
    assert parse_time(value) == ora.Time("2026-01-05T09:00:00.00000101Z")


@pytest.mark.parametrize("offset", ["+99:99", "+24:00", "-24:00", "+00:60", "-00:60", "+23:60"])
def test_parse_time_rejects_invalid_offset(offset):
    with pytest.raises(ValueError, match="invalid UTC offset"):
        parse_time("2026-01-05T09:00:00" + offset)


@pytest.mark.parametrize("suffix", ["Z\0junk", "+99:99\0", "+00:60\0junk"])
def test_parse_time_rejects_nul(suffix):
    with pytest.raises(ValueError, match="embedded NUL"):
        parse_time("2026-01-05T09:00:00" + suffix)


@pytest.mark.parametrize("value", [ora.Time.INVALID, ora.Time.MISSING])
def test_parse_time_rejects_invalid_time(value):
    with pytest.raises(ValueError, match="invalid time"):
        parse_time(value)


@pytest.mark.parametrize("value", ["0001-01-01T00:00:00+23:59", "9999-12-31T23:59:59-23:59"])
def test_parse_time_rejects_out_of_range_instant(value):
    with pytest.raises(ValueError, match="time out of range"):
        parse_time(value)


def test_parse_duration_err():
    from apsis.lib.parse import parse_duration as p

    with pytest.raises(ValueError):
        p("")
    with pytest.raises(ValueError):
        p("foo")
    with pytest.raises(ValueError):
        p("1-2")
    with pytest.raises(ValueError):
        p("3.4.5 s")
    with pytest.raises(ValueError):
        p("10 meters")
    with pytest.raises(ValueError):
        p("forever")
    with pytest.raises(ValueError):
        p(None)
    with pytest.raises(ValueError):
        p("2 eras")


def test_parse_duration():
    from apsis.lib.parse import parse_duration as p

    assert p(1) == 1
    assert p(1.5) == 1.5
    assert p(-10) == -10

    assert p("1") == 1
    assert p("1.") == 1
    assert p("1.0") == 1
    assert p("1.5") == 1.5
    assert p("-10") == -10

    assert p("1s") == 1
    assert p("1.s") == 1
    assert p("1.0s") == 1
    assert p("1.5s") == 1.5
    assert p("-10s") == -10

    assert p("1 s") == 1
    assert p("1. s") == 1
    assert p("1.0 s") == 1
    assert p("1.5 s") == 1.5
    assert p("-10 s") == -10

    assert p("1 sec") == 1
    assert p("1. second") == 1
    assert p("1.0 sec") == 1
    assert p("1.5 seconds") == 1.5
    assert p("-10 sec") == -10

    assert p("1m") == 60
    assert p("1. m") == 60
    assert p("1.0h") == 3600
    assert p("1.5 h") == 5400
    assert p("-10 d") == -864000
