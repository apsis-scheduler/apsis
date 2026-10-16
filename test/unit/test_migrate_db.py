import importlib.util
from pathlib import Path

import pytest

from apsis.sqlite import SqliteDB

SCRIPT = Path(__file__).parents[2] / "scripts" / "migrate-db.py"


@pytest.fixture(scope="module")
def migrate_db():
    spec = importlib.util.spec_from_file_location("migrate_db", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "replace_sql",
    [
        None,
        "CREATE INDEX idx_runs_schedule ON runs (json_extract(times, '$.running'))",
    ],
)
def test_migrate_2_7_0(tmp_path, migrate_db, replace_sql):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    conn = SqliteDB.open(path).conn
    conn.execute("DROP INDEX idx_runs_schedule")
    if replace_sql is not None:
        conn.execute(replace_sql)
    conn.commit()

    db = SqliteDB.open(path)
    assert not db.run_db.has_schedule_index
    migrate_db.migrate_2_7_0(db)
    db = SqliteDB.open(path)
    assert db.run_db.has_schedule_index

    # running again leaves the index alone
    migrate_db.migrate_2_7_0(db)
    assert SqliteDB.open(path).run_db.has_schedule_index
