import pytest

# -------------------------------------------------------------------------------


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
