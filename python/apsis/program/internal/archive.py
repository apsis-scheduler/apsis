import asyncio
import logging

import ora

from ..base import Program, RunningProgram, ProgramRunning, ProgramSuccess, memo
from apsis.lib.json import check_schema, nkey
from apsis.lib.parse import parse_duration
from apsis.lib.timing import Timer
from apsis.runs import template_expand

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class ArchiveProgram(Program):
    """
    A program that archives old runs from the Apsis database to an archive
    database.

    This program runs within the Apsis process, and blocks all other activities
    while it runs.  Avoid archiving too many runs in a single invocation.

    A run must be retired before it is archived.  If it cannot be retired, it is
    skipped for archiving.
    """

    def __init__(self, *, age, path, count, chunk_size=None, chunk_sleep=None):
        """
        If this archive file doesn't exist, it is created automatically on
        first use; the contianing directory must exist.

        :param age:
          Minimum age in sec for a run to be archived.
        :param path:
          Path to the archive file, a SQLite database in a format similar to the
          Apsis database file.
        :param count:
          Maximum number of runs to archive per run of this program.
        :param chunk_size:
          Number of runs to archive in one chunk.  Each chunk is blocking.
        :param chunk_sleep:
          Time in seconds to wait between chunks.
        """
        self.__age = age
        self.__path = path
        self.__count = count
        self.__chunk_size = chunk_size
        self.__chunk_sleep = chunk_sleep

    def __str__(self):
        return f"archive age {self.__age} → {self.__path}"

    def bind(self, args):
        return type(self)(
            age=parse_duration(template_expand(self.__age, args)),
            path=template_expand(self.__path, args),
            count=int(template_expand(self.__count, args)),
            chunk_size=None
            if self.__chunk_size is None
            else int(template_expand(self.__chunk_size, args)),
            chunk_sleep=None
            if self.__chunk_sleep is None
            else float(template_expand(self.__chunk_sleep, args)),
        )

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            age = pop("age")
            path = pop("path", str)
            count = pop("count", int)
            chunk_size = pop("chunk_size", int, None)
            chunk_sleep = pop("chunk_sleep", float, None)
        return cls(
            age=age,
            path=path,
            count=count,
            chunk_size=chunk_size,
            chunk_sleep=chunk_sleep,
        )

    def to_jso(self):
        return {
            **super().to_jso(),
            "age": self.__age,
            "path": self.__path,
            "count": self.__count,
            **nkey("chunk_size", self.__chunk_size),
            **nkey("chunk_sleep", self.__chunk_sleep),
        }

    def run(self, run_id, cfg):
        """Start a new archive operation."""
        # For internal programs, cfg is the apsis instance
        return RunningArchiveProgram(run_id, self, cfg, run_state=None)

    def connect(self, run_id, run_state, cfg):
        """Reconnect to an existing archive operation."""
        # For internal programs, cfg is the apsis instance
        return RunningArchiveProgram(run_id, self, cfg, run_state=run_state)


class RunningArchiveProgram(RunningProgram):
    """A running instance of the archive program."""

    def __init__(self, run_id, program, cfg, run_state):
        super().__init__(run_id)
        self.program = program
        self.run_state = run_state
        # For internal programs, cfg IS the apsis instance
        self.apsis = cfg

    @memo.property
    async def updates(self):
        """Async generator that yields program state updates."""
        if self.run_state is None:
            # Starting fresh
            self.run_state = {}
            yield ProgramRunning(self.run_state)
        # If reconnecting, just restart from beginning (same as legacy behavior)

        # Access private attributes from program
        age = self.program._ArchiveProgram__age
        path = self.program._ArchiveProgram__path
        count = self.program._ArchiveProgram__count
        chunk_size = self.program._ArchiveProgram__chunk_size
        chunk_sleep = self.program._ArchiveProgram__chunk_sleep

        # FIXME: Private attributes.
        db = self.apsis._Apsis__db

        if not (chunk_size is None or 0 < chunk_size):
            raise ValueError("nonpositive chunk size")

        row_counts = {}
        meta = {
            "run count": 0,
            "run_ids": [],
            "row counts": row_counts,
            "time": {
                "get runs": 0,
                "archive runs": 0,
            },
        }

        archive_cutoff = ora.now() - age
        remaining_count = count

        while remaining_count > 0:
            chunk = remaining_count if chunk_size is None else min(remaining_count, chunk_size)

            with Timer() as timer:
                run_ids = db.get_archive_run_ids(
                    before=archive_cutoff,
                    count=chunk,
                )
            meta["time"]["get runs"] += timer.elapsed

            if not run_ids:
                # All eligible runs have been archived
                break

            remaining_count -= chunk

            # Make sure all runs are retired; else skip them.
            run_ids = [r for r in run_ids if self.apsis.run_store.retire(r)]

            if len(run_ids) > 0:
                # Archive these runs.
                with Timer() as timer:
                    chunk_row_counts = db.archive(path, run_ids)
                # Accumulate metadata.
                meta["run count"] += len(run_ids)
                meta["run_ids"].append(run_ids)
                for key, value in chunk_row_counts.items():
                    row_counts[key] = row_counts.get(key, 0) + value
                meta["time"]["archive runs"] += timer.elapsed

            if remaining_count > 0 and chunk_sleep is not None:
                # Yield to the event loop.
                await asyncio.sleep(chunk_sleep)

        yield ProgramSuccess(meta=meta)

    async def stop(self):
        """Archive operation cannot be stopped once started."""
        # Could potentially implement graceful stop between chunks
        pass

    async def signal(self, signal):
        """Archive operation ignores signals."""
        pass
