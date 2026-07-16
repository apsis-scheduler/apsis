"""
Makes schema changes to an existing Apsis database file.

All changes are applied only if necessary, and thus this script is idempotent.
"""

from argparse import ArgumentParser
from contextlib import closing
import logging
from pathlib import Path

from apsis.sqlite import SqliteDB


log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def has_column(table_name, col_name, *, conn):
    ((count,),) = conn.execute(
        """
        SELECT COUNT(*)
        FROM pragma_table_info(?)
        WHERE name = ?
        """,
        (table_name, col_name),
    )
    return count > 0


def has_table(table_name, *, conn):
    ((count,),) = conn.execute(
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
        AND name = ?
        """,
        (table_name,),
    )
    return count > 0


def migrate_0_33_7(db: SqliteDB):
    conn = db.conn
    for table_name, col_name, col_def in (
        ("runs", "conds", "VARCHAR NULL"),
        ("runs", "actions", "VARCHAR NULL"),
    ):
        if not has_column(table_name, col_name, conn=conn):
            log.info(f"creating column: {table_name}.{col_name}")
            conn.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMN {col_name} {col_def}
                """
            )

    conn.execute("CREATE INDEX IF NOT EXISTS index_runs_job_id ON runs (job_id)")


def migrate_2_3_0(db: SqliteDB):
    """
    Create run_summary table and backfill it from runs table.
    """
    conn = db.conn
    if has_table("run_summary", conn=conn):
        log.info("already has run summary")
        return

    log.info("creating table: run_summary")
    conn.execute(
        """
        CREATE TABLE run_summary (
            run_id   VARCHAR NOT NULL UNIQUE,
            timestamp FLOAT NOT NULL,
            payload  VARCHAR NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX idx_timestamp ON run_summary (timestamp)")

    log.info("backfilling run_summary from runs table")
    runs = db.run_db.query()

    for idx, run in enumerate(runs):
        db.run_summary_db.upsert(run)

        if idx % 10000 == 0:
            log.info(f"  backfilled {idx} rows")

    log.info(f"backfill complete: {len(runs)} rows")


def migrate_2_4_0(db: SqliteDB):
    """
    Replace single-column job_id index with compound (job_id, args) index
    for faster condition queries (max_running, skip_duplicate, dependency).

    Args are rewritten with sorted keys so equality on the raw JSON string is
    a direct index lookup.
    """
    import ujson

    from apsis.sqlite import canonical_args_json

    conn = db.conn
    conn.execute("DROP INDEX IF EXISTS index_runs_job_id")

    log.info("rewriting runs.args to canonical form")
    cursor = conn.execute("SELECT rowid, args FROM runs")
    batch = []
    total = 0
    for rowid, args_json in cursor:
        canonical = canonical_args_json(ujson.loads(args_json))
        if canonical != args_json:
            batch.append((canonical, rowid))
        if len(batch) >= 10000:
            conn.executemany("UPDATE runs SET args = ? WHERE rowid = ?", batch)
            total += len(batch)
            log.info(f"  rewrote {total} rows")
            batch = []
    if batch:
        conn.executemany("UPDATE runs SET args = ? WHERE rowid = ?", batch)
        total += len(batch)
    log.info(f"rewrote {total} rows")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_job_args ON runs (job_id, args)")
    log.info("created idx_runs_job_args")


def main():
    parser = ArgumentParser()
    parser.add_argument("path", metavar="PATH", type=Path, help="migrate db file PATH")
    args = parser.parse_args()

    with closing(SqliteDB.open(args.path, timeout=120)) as db:
        migrate_0_33_7(db)
        migrate_2_3_0(db)
        migrate_2_4_0(db)

        db.conn.commit()


if __name__ == "__main__":
    main()
