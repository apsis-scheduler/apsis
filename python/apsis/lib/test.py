import os

# -------------------------------------------------------------------------------


def in_test():
    """
    True if currently running automated tests.
    """
    return "PYTEST_CURRENT_TEST" in os.environ
