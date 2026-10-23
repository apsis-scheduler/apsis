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


def indexes(path: Path) -> set[str]:
    rows = SqliteDB.open(path).conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
    return {name for (name,) in rows}


def test_migrate_2_7_0(tmp_path, migrate_db):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    assert {"idx_runs_schedule", "idx_runs_job_cover"} <= indexes(path)
    conn = SqliteDB.open(path).conn
    conn.execute("DROP INDEX idx_runs_schedule")
    conn.execute("DROP INDEX idx_runs_job_cover")
    conn.commit()

    db = SqliteDB.open(path)
    assert not db.run_db.has_schedule_index
    migrate_db.migrate_2_7_0(db)
    assert {"idx_runs_schedule", "idx_runs_job_cover"} <= indexes(path)

    # running again leaves the indexes alone
    migrate_db.migrate_2_7_0(db)
    assert {"idx_runs_schedule", "idx_runs_job_cover"} <= indexes(path)


def test_main(tmp_path, migrate_db, monkeypatch):
    path = tmp_path / "apsis.db"
    SqliteDB.create(path)
    conn = SqliteDB.open(path).conn
    conn.execute("DROP INDEX idx_runs_schedule")
    conn.commit()

    monkeypatch.setattr("sys.argv", ["migrate-db.py", str(path)])
    migrate_db.main()
    assert SqliteDB.open(path).run_db.has_schedule_index
