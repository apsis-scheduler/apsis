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


def test_migrate_2_7_0(tmp_path, migrate_db):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    conn = SqliteDB.open(path).conn
    conn.execute("DROP INDEX idx_runs_schedule")
    conn.commit()

    db = SqliteDB.open(path)
    assert not db.run_db.has_schedule_index
    migrate_db.migrate_2_7_0(db)
    db = SqliteDB.open(path)
    assert db.run_db.has_schedule_index

    # running again leaves the index alone
    migrate_db.migrate_2_7_0(db)
    assert SqliteDB.open(path).run_db.has_schedule_index


def test_main(tmp_path, migrate_db, monkeypatch):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    conn = SqliteDB.open(path).conn
    conn.execute("DROP INDEX idx_runs_schedule")
    conn.commit()

    monkeypatch.setattr("sys.argv", ["migrate-db.py", str(path)])
    migrate_db.main()
    assert SqliteDB.open(path).run_db.has_schedule_index
