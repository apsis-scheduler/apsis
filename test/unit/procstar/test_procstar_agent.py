import pytest

from   apsis.program.procstar.agent import _combine_fd_data
from   procstar.agent.proc import FdData, Interval

#-------------------------------------------------------------------------------

FD = "output"
ENC = "utf-8"

def test_combine_fd_data_normal():
    data = _combine_fd_data(
        FdData(FD, Interval(   0, 2048), ENC, 2048 * b"x"),
        FdData(FD, Interval(2048, 3072), ENC, 1024 * b"y")
    )
    assert data.interval.start == 0
    assert data.interval.stop == 3072
    assert data.data == 2048 * b"x" + 1024 * b"y"


def test_combine_fd_data_dup():
    data = _combine_fd_data(
        FdData(FD, Interval(0, 2048), ENC, 2048 * b"x"),
        FdData(FD, Interval(0, 2048), ENC, 2048 * b"x")
    )
    assert data.interval.start == 0
    assert data.interval.stop == 2048
    assert data.data == 2048 * b"x"


def test_combine_fd_data_overlap():
    data = _combine_fd_data(
        FdData(FD, Interval(   0, 2048), ENC, 2048 * b"x"),
        FdData(FD, Interval(   0, 3072), ENC, 3072 * b"y")
    )
    assert data.interval.start == 0
    assert data.interval.stop == 3072
    assert data.data == 2048 * b"x" + 1024 * b"y"

    data = _combine_fd_data(
        FdData(FD, Interval(   0, 2048), ENC, 2048 * b"x"),
        FdData(FD, Interval(1024, 3072), ENC, 2048 * b"x")
    )
    assert data.interval.start == 0
    assert data.interval.stop == 3072
    assert data.data == 3072 * b"x"


def test_combine_fd_data_gap():
    with pytest.raises(RuntimeError):
        _combine_fd_data(
            FdData(FD, Interval(   0, 2048), ENC, 2048 * b"x"),
            FdData(FD, Interval(3072, 4096), ENC, 1024 * b"y")
        )


