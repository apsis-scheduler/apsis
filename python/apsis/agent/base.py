import os
import pathlib
import pwd
import tempfile

# -------------------------------------------------------------------------------


def get_default_state_dir():
    uid = os.getuid()
    try:
        user = pwd.getpwuid(os.getuid()).pw_name
    except KeyError:
        user = f"#{uid}"
    return pathlib.Path(tempfile.gettempdir()) / f"apsis-agent-{user}"
