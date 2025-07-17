"""
Persistent state stored in a sqlite file.
"""

import contextlib
import logging
import ora
from pathlib import Path
import sqlalchemy as sa
import ujson

from .actions.base import Action
from .cond.base import Condition
from .jobs import jso_to_job, job_to_jso
from .lib import itr
from .lib.timing import Timer
from .runs import Instance, Run
from .states import State
from .program import Program, Output, OutputMetadata

log = logging.getLogger(__name__)

# FIXME: For next schema migration:
# - remove runs.rerun column
# - remove runs.message column
# - remove runs.expected column
# - move runs.meta into its own table
# - rename run_history to run_log

# -------------------------------------------------------------------------------

# We store times as float seconds from the epoch.


def dump_time(time):
    return time - ora.UNIX_EPOCH


def load_time(time):
    return ora.UNIX_EPOCH + time


@contextlib.contextmanager
def disposing(engine):
    """
    Context manager for `Engine.dispose()`.
    """
    try:
        yield engine
    finally:
        engine.dispose()


def _make_run_id(rowid):
    return f"r{rowid}"


def _parse_run_id(run_id):
    assert run_id[0] == "r"
    return int(run_id[1:])


# SQLite implicitly includes a 'rowid' column in each table, which SA doesn't
# automatically include in the schema.
COL_ROWID = sa.literal_column("rowid")

METADATA = sa.MetaData()

# -------------------------------------------------------------------------------

TBL_CLOCK = sa.Table(
    "clock",
    METADATA,
    sa.Column("time", sa.Float(), nullable=False),
)


class ClockDB:
    """
    Stores the most recent application time.
    """

    # We use a DB-API connection and SQL statements because it's faster than
    # the SQLAlchemy ORM.

    def __init__(self, engine):
        self.__connection = engine.connect().connection

        ((length,),) = self.__connection.execute("SELECT COUNT(*) FROM clock")
        if length == 0:
            time = ora.now() - ora.UNIX_EPOCH
            self.__connection.execute("INSERT INTO clock VALUES (?)", (time,))
            self.__connection.commit()
        else:
            assert length == 1

    def get_time(self):
        ((time,),) = self.__connection.execute("SELECT time FROM clock")
        return time + ora.UNIX_EPOCH

    def set_time(self, time):
        time -= ora.UNIX_EPOCH
        self.__connection.execute("UPDATE clock SET time = ?", (time,))
        self.__connection.commit()


# -------------------------------------------------------------------------------


def _get_max_run_id_number(engine):
    """
    Returns the number of the largest run ID in any table that refers to runs,
    or 0 if there are no rows.
    """
    rows = tuple(
        engine.execute(
            """
        SELECT MAX(run_id)
          FROM (
                SELECT MAX(CAST(SUBSTRING(run_id, 2) AS INTEGER)) AS run_id
                  FROM runs
                 UNION
                SELECT MAX(CAST(SUBSTRING(run_id, 2) AS INTEGER)) AS run_id
                  FROM run_history
                 UNION
                SELECT MAX(CAST(SUBSTRING(run_id, 2) AS INTEGER)) AS run_id
                  FROM output
               )
        """
        )
    )
    if len(rows) == 0:
        log.warning
        return 0
    else:
        ((run_id_number,),) = rows
        return run_id_number


class RunIDDB:
    """
    Stores the next available run ID.

    A run ID is of the form `r#`, where the integer # increments sequentially.
    """

    TABLE = sa.Table(
        "next_run_id",
        METADATA,
        sa.Column("number", sa.Integer(), nullable=False),
    )

    INTERVAL = 64

    @staticmethod
    def initialize(engine):
        with engine.connect() as conn:
            conn.connection.execute("INSERT INTO next_run_id VALUES (1)")
            conn.connection.commit()

    def __init__(self, engine):
        self.__engine = engine
        self.TABLE.create(engine, checkfirst=True)

        with self.__engine.connect() as conn:
            rows = tuple(conn.execute(sa.text("SELECT number FROM next_run_id")))
        if len(rows) == 0:
            log.warning("no next_run_id; migrating")
            # For backward compatibility, determine the largest run ID used.
            # FIXME: Remove this after migration.
            self.__db_next = _get_max_run_id_number(engine) + 1
            # Store it.
            with self.__engine.connect() as conn:
                conn.connection.execute("INSERT INTO next_run_id VALUES (?)", (self.__db_next,))
                conn.connection.commit()
            log.info(f"next run ID: r{self.__db_next}")

        else:
            ((self.__db_next,),) = rows
            log.info(f"next run ID: r{self.__db_next}")

        # Start generating run IDs from the next available.
        self.__next = self.__db_next

    def get_next_run_id(self):
        run_id = _make_run_id(self.__next)
        self.__next += 1
        if self.__db_next < self.__next:
            # Increment by more than one, to decrease database writes.  If we
            # restart now, we'll skip over some run IDs, but that's OK; we're
            # not going to run out of them.
            self.__db_next += self.INTERVAL
            with self.__engine.connect() as conn:
                res = conn.connection.execute(
                    "UPDATE next_run_id SET number = ?", (self.__db_next,)
                )
                assert res.rowcount == 1
                conn.connection.commit()

        return run_id


# -------------------------------------------------------------------------------

TBL_JOBS = sa.Table(
    "jobs",
    METADATA,
    sa.Column("job_id", sa.String(), nullable=False),
    sa.Column("job", sa.String(), nullable=False),
)


class JobDB:
    """
    Deprecated.

    Ad hoc runs serialize the entire job in the run; this table isn't needed.
    """

    def __init__(self, engine):
        self.__engine = engine

    def insert(self, job):
        # FIXME: Check that the job ID doesn't exist already
        with self.__engine.begin() as conn:
            conn.execute(
                TBL_JOBS.insert().values(
                    job_id=job.job_id,
                    job=ujson.dumps(job_to_jso(job)),
                )
            )

    def get(self, job_id):
        with self.__engine.begin() as conn:
            query = sa.select([TBL_JOBS]).where(TBL_JOBS.c.job_id == job_id)
            rows = list(conn.execute(query))
            assert len(rows) <= 1

            if len(rows) == 0:
                raise LookupError(job_id)
            else:
                ((_, job),) = rows
                return jso_to_job(ujson.loads(job), job_id)

    def query(self, *, ad_hoc=None):
        query = sa.select([TBL_JOBS])
        with self.__engine.begin() as conn:
            for job_id, job in conn.execute(query):
                # FIXME: Filter ad hoc jobs in the query.
                try:
                    job = jso_to_job(ujson.loads(job), job_id)
                except Exception as exc:
                    logging.error(f"failed to load job from DB: {exc}")
                    continue
                if ad_hoc is None or job.ad_hoc == ad_hoc:
                    yield job


# -------------------------------------------------------------------------------

TBL_RUNS = sa.Table(
    "runs",
    METADATA,
    sa.Column("rowid", sa.Integer(), primary_key=True),
    sa.Column("run_id", sa.String(), nullable=False),
    sa.Column("timestamp", sa.Float(), nullable=False),
    sa.Column("job_id", sa.String(), nullable=False),
    sa.Column("args", sa.String(), nullable=False),
    sa.Column("state", sa.String(), nullable=False),
    sa.Column("program", sa.String(), nullable=True),
    sa.Column("times", sa.String(), nullable=False),
    sa.Column("meta", sa.String(), nullable=False),
    sa.Column("message", sa.String(), nullable=True),
    sa.Column("run_state", sa.String(), nullable=True),
    sa.Column("rerun", sa.String(), nullable=True),  # FIXME: Unused.
    sa.Column("expected", sa.Boolean(), nullable=True),
    sa.Column("conds", sa.String(), nullable=True),
    sa.Column("actions", sa.String(), nullable=True),
)

# This index is used to speed up removal of orphaned jobs during archiving, i.e.
# (ad hoc) jobs in the `jobs` table that no longer have an associated run.  If
# we ever remove the `jobs` table, we can remove this.
sa.Index("index_runs_job_id", TBL_RUNS.c.job_id)

TBL_RUNS_SELECT = sa.select(
    [
        TBL_RUNS.columns[n]
        for n in (
            "rowid",
            "run_id",
            "timestamp",
            "job_id",
            "args",
            "state",
            "program",
            "conds",
            "actions",
            "times",
            "meta",
            "run_state",
        )
    ]
)


class RunDB:
    # For runs in the database (either inserted into or loaded from), we stash
    # the sqlite rowid in the Run._rowid attribute.

    def __init__(self, engine):
        self.__engine = engine
        self.__connection = engine.raw_connection()
        # FIXME: Do we need to clean this up?

    @staticmethod
    def __query_runs(conn, expr):
        query = TBL_RUNS_SELECT.where(expr)
        cursor = conn.execute(query)
        for (
            rowid,
            run_id,
            timestamp,
            job_id,
            args,
            state,
            program,
            conds,
            actions,
            times,
            meta,
            run_state,
        ) in cursor:
            program = None if program is None else Program.from_jso(ujson.loads(program))
            conds = None if conds is None else [Condition.from_jso(c) for c in ujson.loads(conds)]
            actions = (
                None if actions is None else [Action.from_jso(a) for a in ujson.loads(actions)]
            )

            times = ujson.loads(times)
            times = {n: ora.Time(t) for n, t in times.items()}

            args = ujson.loads(args)
            inst = Instance(job_id, args)
            run = Run(inst)

            run.run_id = run_id
            run.timestamp = load_time(timestamp)
            run.state = State[state]
            run.program = program
            run.conds = conds
            run.actions = actions
            run.times = times
            run.meta = ujson.loads(meta)
            run.run_state = ujson.loads(run_state)
            run._rowid = rowid
            yield run

    def upsert(self, run):
        assert not run.expected
        rowid = _parse_run_id(run.run_id)

        program = None if run.program is None else ujson.dumps(run.program.to_jso())
        conds = None if run.conds is None else ujson.dumps([c.to_jso() for c in run.conds])
        actions = None if run.actions is None else ujson.dumps([a.to_jso() for a in run.actions])

        times = {n: str(t) for n, t in run.times.items()}

        # We use SQL instead of SQLAlchemy for performance.
        con = self.__connection.connection
        values = (
            run.run_id,
            dump_time(run.timestamp),
            run.inst.job_id,
            ujson.dumps(run.inst.args),
            run.state.name,
            program,
            conds,
            actions,
            ujson.dumps(times),
            ujson.dumps(run.meta),
            ujson.dumps(run.run_state),
            rowid,
        )

        # Get the rowid; if it's missing, this run isn't in the table.
        try:
            run._rowid

        except AttributeError:
            # This run isn't in the database yet.
            con.execute(
                """
                INSERT INTO runs (
                    run_id,
                    timestamp,
                    job_id,
                    args,
                    state,
                    program,
                    conds,
                    actions,
                    times,
                    meta,
                    run_state,
                    expected,
                    rowid
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
                values,
            )
            con.commit()
            run._rowid = values[-1]

        else:
            assert run._rowid == rowid
            # Update the existing row.
            changes = con.total_changes
            con.execute(
                """
                UPDATE runs SET
                    run_id      = ?,
                    timestamp   = ?,
                    job_id      = ?,
                    args        = ?,
                    state       = ?,
                    program     = ?,
                    conds       = ?,
                    actions     = ?,
                    times       = ?,
                    meta        = ?,
                    run_state   = ?
                WHERE rowid = ?
            """,
                values,
            )
            # We should have updated one row.
            assert con.total_changes == changes + 1
            con.commit()

    def get(self, run_id):
        with self.__engine.begin() as conn:
            (run,) = self.__query_runs(conn, TBL_RUNS.c.run_id == run_id)
        return run

    def query(self, *, job_id=None, since=None, min_timestamp=None):
        """
        :param min_timestamp:
          If not none, limits to runs with timestamp not less than this.
        """
        where = []
        if job_id is not None:
            where.append(TBL_RUNS.c.job_id == job_id)
        if since is not None:
            where.append(TBL_RUNS.c.rowid >= int(since))
        if min_timestamp is not None:
            where.append(TBL_RUNS.c.timestamp >= dump_time(min_timestamp))

        runs = list(self.__query_runs(self.__engine, sa.and_(*where)))

        log.debug(
            f"query job_id={job_id} since={since} min_timestamp={min_timestamp} → {len(runs)} runs"
        )
        return runs


# -------------------------------------------------------------------------------


class RunLogDB:
    # FIXME: Rename table to run_log.

    TABLE = sa.Table(
        "run_history",
        METADATA,
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("timestamp", sa.Float, nullable=False),
        sa.Column("message", sa.String(), nullable=False),
        sa.Index("idx_run_id", "run_id"),
    )

    def __init__(self, engine):
        self.__engine = engine
        self.__cache = {}

    def cache(self, run_id: str, timestamp: ora.Time, message: str):
        values = {
            "run_id": run_id,
            "timestamp": timestamp,
            "message": str(message),
        }
        self.__cache.setdefault(run_id, []).append(values)

    def insert(self, run_id: str, timestamp: ora.Time, message: str):
        values = {
            "run_id": run_id,
            "timestamp": dump_time(timestamp),
            "message": str(message),
        }
        with self.__engine.begin() as conn:
            conn.execute(self.TABLE.insert().values(**values))

    def flush(self, run_id):
        """
        Flushes cached run log to the database.
        """
        cache = self.__cache.pop(run_id, ())
        if len(cache) > 0:
            for item in cache:
                item["timestamp"] = dump_time(item["timestamp"])
            with self.__engine.begin() as conn:
                conn.execute(self.TABLE.insert().values(cache))

    def query(self, *, run_id: str):
        where = self.TABLE.c.run_id == run_id

        # Respond with cached values.
        yield from self.__cache.get(run_id, ())

        # Now query the database.
        with self.__engine.begin() as conn:
            rows = list(conn.execute(sa.select([self.TABLE]).where(where)))

        for run_id, timestamp, message in rows:
            yield {
                "run_id": run_id,
                "timestamp": load_time(timestamp),
                "message": message,
            }


# -------------------------------------------------------------------------------


class OutputDB:
    """
    We store even large outputs in the SQLite database, which should generally
    be efficient.  See https://www.sqlite.org/intern-v-extern-blob.html.
    """

    TABLE = sa.Table(
        "output",
        METADATA,
        sa.Column("run_id", sa.String(), nullable=False),
        sa.Column("output_id", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=False),
        sa.Column("length", sa.Integer(), nullable=False),
        sa.Column("compression", sa.String(), nullable=True),
        sa.Column("data", sa.BINARY(), nullable=False),
        sa.PrimaryKeyConstraint("run_id", "output_id"),
    )

    def __init__(self, engine):
        self.__engine = engine
        self.__connection = engine.connect().connection

    def upsert(self, run_id: str, output_id: str, output: Output):
        self.__connection.connection.execute(
            """
            INSERT INTO output (
                run_id,
                output_id,
                name,
                content_type,
                length,
                compression,
                data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, output_id)
            DO UPDATE SET
                name            = excluded.name,
                content_type    = excluded.content_type,
                length          = excluded.length,
                compression     = excluded.compression,
                data            = excluded.data
            """,
            (
                run_id,
                output_id,
                output.metadata.name,
                output.metadata.content_type,
                output.metadata.length,
                output.compression,
                output.data,
            ),
        )
        self.__connection.connection.commit()

    def get_metadata(self, run_id):
        """
        Returns all output metadata for run `run_id`.

        :return:
          A mapping from output ID to `OutputMetadata` instances.  If no output
          is stored for `run_id`, returns an empty dict.
        """
        cols = self.TABLE.c
        columns = [cols.output_id, cols.name, cols.content_type, cols.length]
        query = sa.select(*columns).where(cols.run_id == run_id)
        with self.__engine.connect() as conn:
            return {
                r[0]: OutputMetadata(name=r[1], length=r[3], content_type=r[2])
                for r in conn.execute(query)
            }

    def get_output(self, run_id, output_id) -> Output:
        """
        Returns an output.

        :raise LookupError:
          No output for `run_id, output_id`.
        """
        cols = self.TABLE.c
        query = sa.select(
            cols.name,
            cols.length,
            cols.content_type,
            cols.data,
            cols.compression,
        ).where((cols.run_id == run_id) & (cols.output_id == output_id))
        with self.__engine.connect() as conn:
            rows = list(conn.execute(query))
        if len(rows) == 0:
            raise LookupError(f"no output {output_id} for {run_id}")
        else:
            (r,) = rows
            return Output(
                OutputMetadata(r[0], r[1], content_type=r[2]),
                data=r[3],
                compression=r[4],
            )


# -------------------------------------------------------------------------------

# Tables other than "runs" that need to be archived.
RUN_TABLES = (RunLogDB.TABLE, OutputDB.TABLE)
ARCHIVE_TABLES = (*RUN_TABLES, TBL_RUNS)


class SqliteDB:
    """
    A SQLite3 file containing persistent state.
    """

    def __init__(self, engine):
        """
        :param path:
          Path to SQLite file.  If `None`, use a memory DB (for testing).
        """
        self.__engine = engine
        self.clock_db = ClockDB(engine)
        self.next_run_id_db = RunIDDB(engine)
        self.job_db = JobDB(engine)
        self.run_db = RunDB(engine)
        self.run_log_db = RunLogDB(engine)
        self.output_db = OutputDB(engine)

    @classmethod
    def __get_engine(cls, path, *, timeout=None):
        url = "sqlite://" if path is None else f"sqlite:///{path}"
        connect_args = {}
        if timeout is not None:
            connect_args["timeout"] = timeout

        # Use a static pool-- exactly one persistent connection-- since we are a
        # single-threaded async application, and sqlite doesn't support
        # concurrent access.
        engine = sa.create_engine(
            url,
            poolclass=sa.pool.StaticPool,
            connect_args=connect_args,
        )

        # Adjust defaults, for performance.
        for pragma, value in (
            ("journal_mode", "WAL"),
            ("synchronous", "NORMAL"),
            ("cache_size", "-32768"),  # 32 MB
            ("optimize", "0x10002"),
            ("mmap_size", "2147483648"),  # 2 GB
            ("page_size", "8192"),
        ):
            engine.execute(f"PRAGMA {pragma} = {value}")

        return engine

    def close(self):
        self.__engine.dispose()
        del self.__engine

    @classmethod
    def create(cls, path, *, clock=None):
        """
        Creates a new database.

        :param path:
          The database path, which must not already exist.
        """
        if path is not None:
            path = Path(path).absolute()
            if path.exists():
                raise FileExistsError(path)

        log.info(f"creating database: {path}")
        engine = cls.__get_engine(path)
        log.info("creating tables")
        METADATA.create_all(engine)
        log.info("initializing next run ID")
        RunIDDB.initialize(engine)
        if clock is not None:
            ClockDB(engine).set_time(clock)

    @classmethod
    def open(cls, path, *, timeout=None):
        if path is not None:
            path = Path(path).absolute()
            if not path.exists():
                raise FileNotFoundError(path)

        engine = cls.__get_engine(path, timeout=timeout)
        # FIXME: Check that tables exist.
        return cls(engine)

    def check(self):
        """
        Checks `db` for consistency.
        """
        ok = True

        def error(msg):
            nonlocal ok
            logging.error(msg)
            ok = False

        engine = self.__engine

        # Check run tables for valid run ID (referential integrity).
        for tbl in RUN_TABLES:
            sel = (
                sa.select([tbl.c.run_id])
                .distinct(tbl.c.run_id)
                .select_from(tbl.outerjoin(TBL_RUNS, tbl.c.run_id == TBL_RUNS.c.run_id))
                .where(TBL_RUNS.c.run_id is None)
            )
            log.debug(f"query:\n{sel}")
            res = engine.execute(sel)
            run_ids = [r for (r,) in res]
            if len(run_ids) > 0:
                error(f"unknown run IDs in {tbl}: {itr.join_truncated(8, run_ids)}")

    def get_archive_run_ids(self, *, before, count):
        """
        Determines run IDs eligible for archive.

        :param before:
          Time before which to archive runs.
        :param count:
          Maximum count of runs to archive.
        :return:
          A sequence of run IDs.
        """
        # Only finished runs are eligible for archiving.
        FINISHED_STATES = [s.name for s in State if s.finished]

        with (
            Timer() as timer,
            self.__engine.begin() as tx,
        ):
            res = list(
                tx.execute(
                    sa.select([TBL_RUNS.c.run_id, COL_ROWID])
                    .where(TBL_RUNS.c.timestamp < dump_time(before))
                    .where(TBL_RUNS.c.state.in_(FINISHED_STATES))
                    .limit(count)
                )
            )

        # Make sure rowids and run_ids correspond.
        assert all(rowid == _parse_run_id(run_id) for run_id, rowid in res)
        run_ids = [r for r, _ in res]

        log.info(f"obtained {len(run_ids)} runs to archive in {timer.elapsed:.3f} s")
        return run_ids

    def archive(self, path, run_ids):
        """
        Archives data for `run_ids` to sqlite archive file `path`.

        :param path:
          Path to archive file.  If the path doesn't exist, creates the archive.
          If the archive exists, appends after first checking that no data
          already is present for any of `run_ids`.
        :param run_ids:
          Sequence of run IDs to archive.
        """
        rowids = [_parse_run_id(i) for i in run_ids]

        # Open the archive file, creating if necessary.
        archive_engine = self.__get_engine(path)
        # Create tables if necessary.
        METADATA.create_all(archive_engine, tables=ARCHIVE_TABLES)

        row_counts = {}

        with (
            disposing(archive_engine),
            Timer() as timer,
            self.__engine.begin() as src_tx,
            archive_engine.begin() as archive_tx,
        ):
            log.info(f"archiving {len(run_ids)} runs")

            if len(run_ids) > 0:
                # Process all row-related tables.

                # First, make sure there are no records already in the archive
                # that match the run IDs.
                for table in ARCHIVE_TABLES:
                    res = archive_tx.execute(
                        sa.select([table.c.run_id])
                        .distinct()
                        .select_from(table)
                        .where(table.c.run_id.in_(run_ids))
                    )
                    dup_run_ids = sorted(r for (r,) in res)
                    if len(dup_run_ids) > 0:
                        raise RuntimeError(
                            f"run IDs already in archive {table}: " + " ".join(dup_run_ids)
                        )

                for table in ARCHIVE_TABLES:
                    # Predicate for rows to select.  In the runs table, query
                    # by rowid instead of run_id, for performance.
                    row_pred = (
                        COL_ROWID.in_(rowids) if table is TBL_RUNS else table.c.run_id.in_(run_ids)
                    )

                    # Extract the rows to archive.
                    res = src_tx.execute(sa.select(table).where(row_pred))
                    rows = tuple(res.mappings())

                    # Write the rows to the archive.
                    if len(rows) > 0:
                        res = archive_tx.execute(sa.insert(table), rows)
                        assert res.rowcount == len(rows)

                    # Confirm the number of rows.
                    ((count,),) = archive_tx.execute(
                        sa.select(sa.func.count())
                        .select_from(table)
                        .where(table.c.run_id.in_(run_ids))
                    )
                    assert count == len(rows), f"archive {table} contains {count} rows"

                    # Remove the rows from the source table.
                    res = src_tx.execute(sa.delete(table).where(row_pred))
                    assert res.rowcount == len(rows)

                    # Keep count of how many rows we archived from each table.
                    row_counts[table.name] = len(rows)

                # Clean up any jobs that no longer have associated runs.
                src_tx.execute(
                    """
                    DELETE FROM jobs
                    WHERE job_id IN (
                        SELECT jobs.job_id
                        FROM jobs
                        LEFT OUTER JOIN runs
                        ON runs.job_id = jobs.job_id
                        WHERE runs.run_id IS NULL
                    )
                    """
                )

        log.info(f"archived in {timer.elapsed:.3f} s")
        return row_counts

    def vacuum(self):
        log.info("vacuuming database")
        with Timer() as timer:
            self.__engine.execute("VACUUM")
        log.info(f"vacuumed in {timer.elapsed:.3f} s")
