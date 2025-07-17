import errno
import fcntl
import os
from pathlib import Path
import time

# -------------------------------------------------------------------------------


class PidFile:
    """
    Pid file.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.file = None

    def lock(self):
        """
        Locks the pid file.

        Whether or not the lock succeeds, the `file` attribute contains a file
        object open to the pid file, with its position set to right after the
        pid itself.  The caller may write (on success) or read (on failure)
        additional data from this point.

        :return:
          None on success.  If another process has the file locked, the pid
          of the other process.
        """
        assert self.file is None

        # Open for append.  The file may exist, or not.
        self.file = open(self.path, "a+")

        try:
            # Lock it.
            fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        except IOError as exc:
            if exc.errno == errno.EWOULDBLOCK:
                # Someone other process has locked it.  Try to read its info.
                # But first give it a moment, to reduces races.  FIXME: Yak.
                time.sleep(0.001)
                self.file.seek(0)
                pid_str = self.file.readline()
                pid = int(pid_str.strip())
                return pid

            else:
                # Something else went wrong.
                raise

        else:
            # Our lock.  Write immediately.
            pid_str = str(os.getpid()) + "\n"
            self.file.seek(0)
            self.file.truncate()
            self.file.write(pid_str)
            self.file.flush()

            return None

    def unlock(self):
        if self.file is not None:
            self.file.close()
            self.file = None
