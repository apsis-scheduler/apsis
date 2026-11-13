# Required for pytest to set CWD properly for imports from test scripts.

import os
import pytest


@pytest.fixture(autouse=True)
def set_apsis_host(monkeypatch):
    """Set APSIS_HOST=localhost and clear APSIS_URL and APSIS_KRB5CCNAME for integration tests."""
    monkeypatch.setenv("APSIS_HOST", "localhost")
    monkeypatch.delenv("APSIS_URL", raising=False)
    monkeypatch.delenv("APSIS_KRB5CCNAME", raising=False)
