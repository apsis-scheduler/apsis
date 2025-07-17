# Required for pytest to set CWD properly for imports from test scripts.

import os
import pytest


@pytest.fixture(autouse=True)
def set_apsis_host(monkeypatch):
    """Set APSIS_HOST=localhost for all integration tests."""
    monkeypatch.setenv("APSIS_HOST", "localhost")
