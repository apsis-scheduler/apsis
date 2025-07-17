import os
from pathlib import Path

from apsis.lib.pidfile import PidFile

# -------------------------------------------------------------------------------


def test_lock_data(tmpdir):
    path = Path(tmpdir) / "pidfile"
    token = "hello, world!\n"

    pf0 = PidFile(path)
    pf1 = PidFile(path)

    assert pf0.lock() is None
    pf0.file.write(token)
    pf0.file.flush()

    assert pf1.lock() == os.getpid()
    data = pf1.file.read()
    assert data == token
    # FIXME: This is unfortunate.
    pf1.unlock()

    pf0.unlock()
    assert pf1.lock() is None
    pf1.unlock()
