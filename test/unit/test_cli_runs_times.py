"""CLI defaults and usage errors for apsis runs --times."""

from unittest.mock import Mock

import pytest

import apsis.cli


def _run(monkeypatch, *args):
    monkeypatch.setattr("sys.argv", ["apsis", "runs", "-j", "job", "--format", "json", *args])
    apsis.cli.main()


def test_runs_defaults_and_bad_span(monkeypatch, capsys):
    client = Mock(get_runs=Mock(return_value={}))
    monkeypatch.setattr("apsis.service.client.Client", Mock(return_value=client))
    _run(monkeypatch, "-s", "success", "--limit", "2")
    client.get_runs.assert_called_once_with(
        job_id="job",
        state="success",
        limit=2,
        schedule_since=None,
        schedule_until=None,
    )
    client.get_runs.reset_mock()
    for bad in ("2026-01-01T00:00:00+99:99", "0001-01-01T00:00:00+23:59"):
        with pytest.raises(SystemExit) as exc:
            _run(monkeypatch, "--times", bad)
        assert exc.value.code == 2
        err = capsys.readouterr().err
        assert "--times" in err and f"cannot interpret as time: {bad}" in err
        client.get_runs.assert_not_called()
