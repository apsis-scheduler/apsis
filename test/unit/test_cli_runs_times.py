"""
Tests that `apsis runs --times TIMESPAN` reaches Client.get_runs as a schedule
time span, and that a bad span is a clean usage error (the parser tests own the
full syntax matrix).
"""

import ora
import pytest

import apsis.cli
import apsis.service.client

T1 = ora.Time("2026-01-01T00:00:00Z")
T2 = ora.Time("2026-01-05T09:00:00Z")


class FakeClient:
    calls = []

    def __init__(self, address):
        self.address = address

    def get_runs(self, **kwargs):
        FakeClient.calls.append(kwargs)
        return {}


@pytest.fixture
def cli(monkeypatch):
    """run `apsis ARGS...` against a recording FakeClient, return the recorded calls"""
    FakeClient.calls = []
    monkeypatch.setattr(apsis.service.client, "Client", FakeClient)

    def run(*argv):
        monkeypatch.setattr("sys.argv", ["apsis", *argv])
        apsis.cli.main()
        return FakeClient.calls

    return run


def test_runs_without_times(cli):
    (call,) = cli("runs", "-j", "job", "--format", "json")
    assert call["job_id"] == "job"
    assert call["schedule_since"] is None and call["schedule_until"] is None


def test_runs_times_forwarded_with_state(cli):
    # representative wiring: --times reaches the client as since and until with the other filters
    (call,) = cli(
        "runs", "-j", "job", "-s", "success", "--times", f"{T1}..{T2}", "--format", "json"
    )
    assert call["job_id"] == "job" and call["state"] == "success"
    assert call["schedule_since"] == T1 and call["schedule_until"] == T2


@pytest.mark.parametrize(
    "bad, msg",
    [
        ("garbage", "cannot interpret as time: garbage"),
        ("+1e100", "duration out of range"),  # ora overflow, translated to a usage error
        (f"{T2}..{T1}", "start must be before end"),
    ],
)
def test_runs_times_bad_span_is_usage_error(cli, capsys, bad, msg):
    with pytest.raises(SystemExit) as exc:
        cli("runs", "-j", "job", "--times", bad)
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "--times" in err and msg in err
    assert FakeClient.calls == []  # never reached the client
