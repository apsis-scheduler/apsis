import sqlite3

import pytest

from apsis.runs import Instance, Run, RunStore
from apsis.sqlite import SqliteDB


def test_rename_job_rolls_back_summaries(tmp_path):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path=path)
    db = SqliteDB.open(path)
    run = Run(Instance("old", {}))
    RunStore(db, min_timestamp=None).add(run)
    with sqlite3.connect(path) as con:
        con.execute(
            "CREATE TRIGGER fail BEFORE UPDATE ON runs BEGIN SELECT RAISE(ABORT, 'boom'); END"
        )

    with pytest.raises(sqlite3.DatabaseError, match="boom"):
        db.run_db.rename_job("old", "new", 100)
    # the summary update goes back with the failed runs update
    (summary,) = db.run_summary_db.get([run.run_id], None)
    assert summary["run_summary"]["job_id"] == "old"
