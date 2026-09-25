import pytest

import apsis.cli


@pytest.mark.parametrize("limit", ["0", "-1"])
def test_runs_rejects_limit_below_one(monkeypatch, capsys, limit):
    monkeypatch.setattr("sys.argv", ["apsis", "runs", "--job", "job", "--limit", limit])
    with pytest.raises(SystemExit) as exc:
        apsis.cli.main()
    assert exc.value.code == 1
    assert "--limit must be at least 1" in capsys.readouterr().err
