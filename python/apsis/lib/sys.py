from contextlib import suppress
import os
import pwd
import signal

# -------------------------------------------------------------------------------


def get_username():
    """
    Returns the username of the process owner.
    """
    return pwd.getpwuid(os.getuid()).pw_name


def to_signal(sig) -> signal.Signals:
    """
    Parses a signal number or named signal.

      >>> to_signal(15)
      <Signals.SIGTERM: 15>
      >>> to_signal("15")
      <Signals.SIGTERM: 15>
      >>> to_signal("SIGTERM")
      <Signals.SIGTERM: 15>
      >>> to_signal("TERM")
      <Signals.SIGTERM: 15>

    """
    with suppress(TypeError, ValueError):
        return signal.Signals(int(sig))

    with suppress(KeyError):
        return signal.Signals[str(sig)]

    with suppress(KeyError):
        return signal.Signals["SIG" + str(sig)]

    raise ValueError(f"not a signal: {sig}")
