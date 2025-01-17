"""
Makes schema changes to an existing Apsis database file.

All changes are applied only if necessary, and thus this script is idempotent.
"""

from   argparse import ArgumentParser
from   contextlib import closing
import logging
from   pathlib import Path
import sqlite3

logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

parser = ArgumentParser()
parser.add_argument(
    "path", metavar="PATH", type=Path,
    help="migrate db file PATH")
args = parser.parse_args()

with closing(sqlite3.connect(args.path)) as conn:
    def has_table(table_name):
        (count, ), = conn.execute(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table'
            AND name = ?
            """,
            (table_name, )
        )
        return count > 0

    def has_column(table_name, col_name):
        (count, ), = conn.execute(
            """
            SELECT COUNT(*)
            FROM pragma_table_info(?)
            WHERE name = ?
            """,
            (table_name, col_name)
        )
        return count > 0

    for table_name, col_name, col_def in (
            ("runs", "conds", "VARCHAR NULL"),
            ("runs", "actions", "VARCHAR NULL"),
            ("runs", "stop_time", "FLOAT NULL"),
    ):
        if not has_column(table_name, col_name):
            log.info(f"creating column: {table_name}.{col_name}")
            conn.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMN {col_name} {col_def}
                """
            )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS index_runs_job_id ON runs (job_id)")

    conn.commit()


