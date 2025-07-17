import aiofiles
import asyncio
import logging
import os
from pathlib import Path
import random
import string
from ruamel.yaml import YAML
from ruamel.yaml.constructor import DuplicateKeyError

from .actions import Action
from .actions.schedule import successor_from_jso
from .cond import Condition
from .exc import JobError, JobsDirErrors, SchemaError
from .lib.json import to_array, to_narray, check_schema
from .lib.py import tupleize, format_ctor
from .program import Program, NoOpProgram
from .schedule import schedule_to_jso, schedule_from_jso

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class Job:
    def __init__(
        self,
        job_id,
        params=[],
        schedules=[],
        program=NoOpProgram(),
        conds=[],
        actions=[],
        *,
        meta={},
        ad_hoc=False,
    ):
        """
        :param schedules:
          A sequence of `Schedule, args` pairs, where `args` is an arguments
          dict.
        :param ad_hoc:
          True if this is an ad hoc job.
        :param meta:
          Dict of metadata.  Must be JSON-serlializable.
        """
        self.job_id = None if job_id is None else str(job_id)
        self.params = frozenset(str(p) for p in tupleize(params))
        self.schedules = tupleize(schedules)
        self.program = program
        self.conds = tupleize(conds)
        self.actions = actions
        self.meta = meta
        self.ad_hoc = bool(ad_hoc)

    def __repr__(self):
        return format_ctor(
            self,
            self.job_id,
            tuple(self.params),
            schedules=self.schedules,
            program=self.program,
            conds=self.conds,
            actions=self.actions,
            meta=self.meta,
            ad_hoc=self.ad_hoc,
        )

    def __eq__(self, other):
        return (
            not self.ad_hoc
            and not other.ad_hoc
            and other.params == self.params
            and other.schedules == self.schedules
            and other.program == self.program
            and other.conds == self.conds
            and other.actions == self.actions
            and other.meta == self.meta
        )

    def to_jso(self):
        return {
            "job_id": self.job_id,
            "params": list(sorted(self.params)),
            "schedule": [schedule_to_jso(s) for s in self.schedules],
            "program": self.program.to_jso(),
            "condition": [c.to_jso() for c in self.conds],
            "action": [a.to_jso() for a in self.actions],
            "metadata": self.meta,
            "ad_hoc": self.ad_hoc,
        }

    @classmethod
    def from_jso(cls, jso, job_id):
        with check_schema(jso) as pop:
            assert pop("job_id", default=job_id) == job_id, f"JSON job_id mismatch {job_id}"

            params = pop("params", default=[])
            params = [params] if isinstance(params, str) else params

            schedules = pop("schedule", to_narray, default=())
            schedules = [schedule_from_jso(s) for s in schedules]

            program = pop("program", Program.from_jso)

            conds = pop("condition", to_array, default=[])
            conds = [Condition.from_jso(c) for c in conds]

            acts = pop("action", to_array, default=[])
            acts = [Action.from_jso(a) for a in acts]

            # Successors are syntactic sugar for actions.
            sucs = pop("successors", to_array, default=[])
            acts.extend([successor_from_jso(s) for s in sucs])

            metadata = pop("metadata", default={})
            metadata["labels"] = [str(l) for l in tupleize(metadata.get("labels", []))]

            ad_hoc = pop("ad_hoc", bool, default=False)

        return cls(
            job_id,
            params,
            schedules,
            program,
            conds=conds,
            actions=acts,
            meta=metadata,
            ad_hoc=ad_hoc,
        )


# -------------------------------------------------------------------------------


def job_to_jso(job):
    """
    :deprecated:
      Use `Job.to_jso()`.
    """
    return job.to_jso()


def jso_to_job(jso, job_id):
    """
    :deprecated:
      Use `job.to_jso()`.
    """
    return Job.from_jso(jso, job_id)


def dump_yaml(file, job):
    YAML().dump(job_to_jso(job), file)


def list_yaml_files(dir_path):
    dir_path = Path(dir_path)
    for dir, _, names in os.walk(dir_path):
        dir = Path(dir)
        paths = (dir / n for n in names if not n.startswith("."))
        paths = (p for p in paths if p.suffix == ".yaml")
        for path in paths:
            job_id = str(path.with_suffix("").relative_to(dir_path))
            yield path, job_id


# -------------------------------------------------------------------------------

# FIXME: Use mapping API for jobs.


class InMemoryJobs:
    """
    In-memory set of jobs.  Used for testing.
    """

    def __init__(self, jobs):
        self.__jobs = {j.job_id: j for j in jobs}

    def get_job(self, job_id) -> Job:
        try:
            return self.__jobs[job_id]
        except KeyError:
            raise LookupError(f"no job {job_id}")

    def get_jobs(self, *, ad_hoc=None):
        jobs = self.__jobs.values()
        if ad_hoc is not None:
            jobs = (j for j in jobs if j.ad_hoc == ad_hoc)
        return jobs


class JobsDir:
    def __init__(self, path, jobs):
        self.__path = path
        self.__jobs = jobs

    def __repr__(self):
        return format_ctor(self, str(self.__path))

    @property
    def path(self):
        return self.__path

    def get_job(self, job_id) -> Job:
        """
        :raise LookupError:
          Can't find `job_id`.
        """
        try:
            return self.__jobs[job_id]
        except KeyError:
            raise LookupError(f"no job {job_id}")

    def get_jobs(self, *, ad_hoc=None):
        jobs = self.__jobs.values()
        if ad_hoc is not None:
            jobs = (j for j in jobs if j.ad_hoc == ad_hoc)
        return jobs


async def load_jobs_dir(path):
    """
    Attempts to loads jobs from a jobs dir.

    :return:
      The successfully loaded `JobsDir`.
    :raise NotADirectoryError:
      `path` is not a directory.
    :raise JobsDirErrors:
      One or more errors while loading jobs.  The exception's `errors` attribute
      contains the errors; each has a `job_id` attribute.
    """
    from .check import check_job

    jobs_path = Path(path)
    if not jobs_path.is_dir():
        raise NotADirectoryError(f"not a directory: {jobs_path}")

    jobs = {}
    errors = []

    async def load_job(path, job_id):
        log.debug(f"loading: {path}")
        try:
            async with aiofiles.open(path, mode="r") as file:
                content = await file.read()
            job_jso = YAML().load(content)
            job = Job.from_jso(job_jso, job_id)
            return job_id, job, None
        except DuplicateKeyError as exc:
            err_msg = exc.problem if exc.problem else str(exc)
            schema_err = SchemaError(err_msg)
            schema_err.job_id = job_id
            return job_id, None, schema_err
        except SchemaError as exc:
            log.debug(f"error: {path}: {exc}", exc_info=True)
            exc.job_id = job_id
            return job_id, None, exc

    load_coros = [load_job(path, job_id) for path, job_id in list_yaml_files(jobs_path)]
    results = await asyncio.gather(*load_coros)
    for job_id, job, exc in results:
        if job is not None:
            jobs[job_id] = job
        if exc is not None:
            errors.append(exc)

    jobs_dir = JobsDir(jobs_path, jobs)

    for job in jobs_dir.get_jobs():
        log.info(f"checking: {job.job_id}")
        for err in check_job(jobs_dir, job):
            errors.append(JobError(job.job_id, str(err)))

    if len(errors) > 0:
        raise JobsDirErrors(f"errors loading jobs in {jobs_path}", errors)

    return jobs_dir


def dump_job(jobs_dir_path, job):
    path = (jobs_dir_path / job.job_id).with_suffix(".yaml")
    with path.open("w") as file:
        dump_yaml(file, job)


# -------------------------------------------------------------------------------

# FIXME: This feels so awkward.  Is there a better design?


class Jobs:
    """
    Combines a job dir and a job DB.
    """

    def __init__(self, jobs_dir, job_db):
        self.__jobs_dir = jobs_dir
        self.__job_db = job_db

    def get_job(self, job_id) -> Job:
        try:
            return self.__jobs_dir.get_job(job_id)
        except LookupError:
            try:
                return self.__job_db.get(job_id)
            except LookupError:
                raise LookupError(f"unknown job ID: {job_id}") from None

    __getitem__ = get_job

    def get_jobs(self, *, ad_hoc=None):
        """
        :param ad_hoc:
          If true, return ad hoc jobs only; if false, return normal jobs only;
          if none, return all jobs.
        """
        if ad_hoc is None or not ad_hoc:
            yield from self.__jobs_dir.get_jobs()
        # FIXME: Yield only job ids we haven't seen.
        yield from self.__job_db.query(ad_hoc=ad_hoc)

    def __get_job_id(self):
        # FIXME: Something better.
        return "adhoc-" + "".join(random.choice(string.ascii_letters) for _ in range(12))

    def add(self, job):
        assert job.job_id is None
        job.job_id = self.__get_job_id()

        self.__job_db.insert(job)


def diff_jobs_dirs(jobs_dir0, jobs_dir1):
    """
    Finds differences between job dirs.

    :return:
      Job IDs that have been removed, job IDs that have been added, and job IDs
      that have changed.
    """
    jobs0 = {j.job_id: j for j in jobs_dir0.get_jobs(ad_hoc=False)}
    jobs1 = {j.job_id: j for j in jobs_dir1.get_jobs(ad_hoc=False)}
    job_ids0 = frozenset(jobs0)
    job_ids1 = frozenset(jobs1)
    ids = job_ids0 & job_ids1
    return (
        job_ids0 - job_ids1,
        job_ids1 - job_ids0,
        frozenset(i for i in ids if jobs1[i] != jobs0[i]),
    )
