from contextlib import closing
import sqlite3

import pytest

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB


def test_rename_job_rolls_back_runs(tmp_path):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path=path)
    db = SqliteDB.open(path)
    run = Run(Instance("old", {}))
    RunStore(db, min_timestamp=None).add(run)
    with closing(sqlite3.connect(path)) as con, con:
        con.execute(
            "CREATE TRIGGER fail BEFORE UPDATE ON run_summary BEGIN SELECT RAISE(ABORT, 'boom'); END"
        )

    with pytest.raises(sqlite3.DatabaseError, match="boom"):
        db.run_db.rename_job("old", "new", 100, None)
    # the runs update goes back with the failed summary update
    assert db.run_db.get(run.run_id).inst.job_id == "old"
