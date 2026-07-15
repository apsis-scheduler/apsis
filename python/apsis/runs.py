from collections import namedtuple
import itertools
import jinja2
import logging
import ora
from ora import now, Time
import shlex
from typing import Iterator

import ujson

from .states import State, TRANSITIONS, to_state
from .lib.asyn import Publisher
from .lib.calendar import get_calendar
from .lib.memo import memoize
from .lib.py import format_ctor, iterize

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class TransitionError(RuntimeError):
    def __init__(self, from_state, to_state):
        super().__init__(f"cannot transition from {from_state} to {to_state}")


class RunError(RuntimeError):
    pass


class MissingArgumentError(RunError):
    def __init__(self, run, *args):
        super().__init__(f"missing args ({', '.join(args)}) for job {run.inst.job_id}")


class ExtraArgumentError(RunError):
    def __init__(self, run, *args):
        super().__init__(f"extra args ({', '.join(args)}) for job {run.inst.job_id}")


# -------------------------------------------------------------------------------


class Instance:
    """
    A job with bound parameters.  Not user-visible.
    """

    __slots__ = (
        "job_id",
        "args",
    )

    def __init__(self, job_id, args):
        self.job_id = job_id
        self.args = dict(sorted((str(k), str(v)) for k, v in args.items()))

    def __repr__(self):
        return format_ctor(self, self.job_id, self.args)

    def __str__(self):
        return "{}({})".format(
            self.job_id, " ".join("{}={}".format(k, v) for k, v in self.args.items())
        )

    def __hash__(self):
        return hash(self.job_id) ^ hash(tuple(sorted(self.args.items())))

    def __eq__(self, other):
        return (
            (self.job_id == other.job_id and self.args == other.args)
            if isinstance(other, Instance)
            else NotImplemented
        )

    def __lt__(self, other):
        return (
            (
                self.job_id < other.job_id
                or (
                    self.job_id == other.job_id
                    and sorted(self.args.items()) < sorted(other.args.items())
                )
            )
            if isinstance(other, Instance)
            else NotImplemented
        )

    def to_jso(self):
        return [self.job_id, self.args]

    @classmethod
    def from_jso(cls, jso):
        job_id, args = jso
        return cls(job_id, args)


# -------------------------------------------------------------------------------


class _Undefined(jinja2.StrictUndefined):
    """
    Custom Jinja2 undefined type that throws `NameError`.
    """

    def __init__(self, *args, name, **kw_args):
        raise NameError(f"name '{name}' is not defined")


_JINJA_ENV = jinja2.Environment(
    undefined=_Undefined,
)


@memoize
def _get_template(template):
    try:
        return _JINJA_ENV.from_string(template)
    except jinja2.TemplateSyntaxError as exc:
        raise SyntaxError(str(exc))


def is_template(string):
    """
    Returns true if `string` contains Jinja2 template syntax.
    """
    s = str(string)
    return "{{" in s or "{%" in s


def template_expand(template, args):
    """
    Expands Jinja2-style `template` with names from `args`.

    :raise SyntaxError:
      The template doesn't conform to Jinja2 syntax.
    :raise NameError:
      The template references a name not in `args`.
    """
    template = _get_template(str(template))
    return template.render(args)


def arg_to_bool(arg):
    if arg in ("true", "True"):
        return True
    elif arg in ("false", "False"):
        return False
    else:
        return bool(arg)


def eval_enabled(enabled, args):
    """
    Evaluates an `enabled` field value.

    :param enabled:
      `None` (always active), a `bool`, or a Jinja2 template string.
    :param args:
      Template expansion context.
    :return:
      True if active, False if skipped.
    """
    if enabled is None:
        return True
    if isinstance(enabled, bool):
        return enabled
    result = template_expand(enabled, args)
    return arg_to_bool(result)


def join_args(argv):
    return " ".join(shlex.quote(a) for a in argv)


def propagate_args(old_args, job, new_args):
    """
    Propagates args from `old_args` to `new_args` if needed for `job`.

    Returns an arg dict, containing `new_args` plus any args that are missing
    for `job` but available in `old_args`.
    """
    args = {p: old_args[p] for p in job.params if p in old_args}
    args.update(new_args)
    return args


# -------------------------------------------------------------------------------


class Run:
    # FIXME: Make the attributes read-only.
    __slots__ = (
        "inst",
        "run_id",
        "timestamp",
        "state",
        "expected",
        "conds",
        "actions",
        "program",
        "times",
        "meta",
        "message",
        "run_state",
        "_rowid",
    )

    def __init__(self, inst, *, expected=False):
        """
        :param expected:
          True if this run was scheduled from a job schedule.  A run scheduled
          from a job schedule is subject to change, as the job's schedules may
          change.
        """
        self.inst = inst

        self.run_id = None
        self.timestamp = None

        self.state = State.new
        self.expected = bool(expected)
        self.conds = None
        self.actions = None
        self.program = None
        # Timestamps for state transitions and other events.
        self.times = {}
        # Additional run metadata.
        self.meta = {}
        # User message explaining the state.
        self.message = None
        # State information specific to the program, for a running run.
        self.run_state = None

    def __hash__(self):
        return hash(self.run_id)

    def __eq__(self, other):
        return other.run_id == self.run_id

    def __repr__(self):
        return format_ctor(self, self.run_id, self.inst, state=self.state)

    def __str__(self):
        return f"{self.run_id} {self.state.name} {self.inst}"

    def _transition(
        self,
        timestamp,
        state,
        *,
        meta={},
        times={},
        message=None,
        run_state=None,
        force=False,
    ):
        """
        :param force:
          Transition outside of the state model.
        :param meta:
          Metadata updates.  Sets or replaces run metadata keys from this
          mapping.
        """
        # Check that this is a valid transition.
        if not force and self.state not in TRANSITIONS[state]:
            raise TransitionError(self.state, state)

        assert all(isinstance(t, Time) and t.valid for t in times.values())

        # Update attributes.
        self.timestamp = timestamp
        self.message = None if message is None else str(message)
        self.meta.update(meta)
        self.times[state.name] = self.timestamp
        self.times.update(times)
        self.run_state = run_state

        # Compute and add elapsed time.
        start = self.times.get("running")
        end = self.times.get("success", self.times.get("failure"))
        if start is not None and end is not None:
            elapsed = end - start
            self.meta["elapsed"] = elapsed

        # Transition to the new state.
        self.state = state


def validate_args(run, params):
    """
    Checks that a run's args match job params.
    """
    args = frozenset(run.inst.args)
    missing, extra = params - args, args - params
    if missing:
        raise MissingArgumentError(run, *missing)
    if extra:
        raise ExtraArgumentError(run, *extra)


# -------------------------------------------------------------------------------
# Binding

# Matches run args to job params to fully instantiate the run's components.

BIND_ARGS = {
    **{
        n: getattr(ora, n)
        for n in (
            "Date",
            "Daytime",
            "Time",
            "TimeZone",
            "to_local",
            "from_local",
        )
    },
    "format": format,
    "get_calendar": get_calendar,
}


def get_bind_args(run):
    """
    Returns args available to template expansion for `run`.
    """
    return {
        **BIND_ARGS,
        "run_id": run.run_id,
        "job_id": run.inst.job_id,
        **run.inst.args,
    }


def bind(run, job, jobs):
    if run.actions is None:
        # FIXME: Actions aren't bound, but may be in the future.
        run.actions = list(job.actions)
    if run.conds is None:
        run.conds = [bc for c in job.conds if (bc := c.bind(run, jobs)) is not None]
    if run.program is None:
        run.program = job.program.bind(get_bind_args(run))


# -------------------------------------------------------------------------------


class RunStore:
    """
    Storage API that stitches together cached in-memory runs and data from the DB. Additionally it keeps cached run
    summary data in the DB up-to-date.

    - Stores runs in all states.
    - Satisfies run queries.
    """

    Message = namedtuple("Message", ("run_id", "job_id", "args", "state"))

    def __init__(self, db, *, min_timestamp):
        self.__run_db = db.run_db
        self.__summary_db = db.run_summary_db
        self.__next_run_id_db = db.next_run_id_db

        self.__min_timestamp = min_timestamp

        # in-memory storage of scheduled runs that came from a schedule ie not reruns or adhoc runs
        # all runs are not expected unless they came from the scheduler
        self.__expected_runs: dict[str, Run] = {}

        # Publisher for run transitions.  Messages are `Message` objects;
        # `state` is none if the run is removed.
        self.publisher = Publisher()

    def __query_run_db(self, *args, min_timestamp=None, **kwargs) -> list[Run]:
        """
        Query the run DB.

        :param min_timestamp: Timestamp filter to pass to DB query.
        """
        return self.__run_db.query(*args, min_timestamp=min_timestamp, **kwargs)

    def add(self, run):
        assert run.state == State.new

        timestamp = now()
        run_id = self.__next_run_id_db.get_next_run_id()

        run.run_id = run_id
        run.timestamp = timestamp

        log.debug(f"new run: {run}")
        if run.expected:
            self.__expected_runs[run.run_id] = run
        self.update(run, timestamp)
        self.publisher.publish(self.Message(run.run_id, run.inst.job_id, run.inst.args, run.state))

    # FIXME: Remove timestamp.
    def update(self, run, timestamp):
        """
        Called when `run` is changed.

        Persists the run if necessary.
        """
        # Persist the changes, but not for expected runs.
        if not run.expected:
            self.__run_db.upsert(run)
            self.__summary_db.upsert(run)
            # run is only in expected dict when it was a regularly scheduled run in the in the "scheduled" state
            self.__expected_runs.pop(run.run_id, None)

        # FIXME: Separate transition() so we don't send this on updates.
        self.publisher.publish(self.Message(run.run_id, run.inst.job_id, run.inst.args, run.state))

    def remove(self, run_id, *, expected=True):
        """
        Removes run with `run_id`.

        :param expected:
          If true, only an expected run may be removed.
        """
        if not expected:
            raise ValueError("removing unexpected runs not supported")

        run = self.__expected_runs[run_id]
        del self.__expected_runs[run_id]
        self.publisher.publish(self.Message(run.run_id, run.inst.job_id, run.inst.args, None))
        return run

    def archivable(self, run_id) -> bool:
        """
        :return:
          True if `run_id` is not in the working set, either because it finished, or because it wasn't there to begin
          with.
        """
        try:
            _, run = self.get(run_id)
        except LookupError:
            return True
        else:
            # we still need to report if the run is finished for the internal archive program, which shouldn't archive
            # unfinished runs
            return run.state.finished

    def __contains__(self, run_id):
        if run_id in self.__expected_runs:
            return True
        try:
            self.__run_db.get(run_id)
        except LookupError:
            return False
        else:
            return True

    def get(self, run_id):
        if run_id in self.__expected_runs:
            return now(), self.__expected_runs[run_id]
        else:
            run = self.__run_db.get(run_id)
        return now(), run

    # FIXME: Remove `when` from the result; I think we don't use it.
    # FIXME: Remove `since`?
    def query(
        self,
        run_ids=None,
        job_id=None,
        state=None,
        since=None,
        args=None,
        with_args=None,
        limit_lookback=True,
    ):
        """
        :param state:
          Limits results to runs in the specified state(s).
        :param since:
          Return only runs with timestamp >= since. Bounded to RunStore min_timestamp if limit_lookback=True.
        :param args:
          Limits results to runs with exactly the specified args.
        :param with_args:
          Limits results to runs with the specified args.  Runs may include
          other args not explicitly given.
        :param limit_lookback:
          If True (default), applies lookback window. If False, queries all runs.
          Set to False for condition checks that need to see all active runs.
        """
        expected = self.__expected_runs.values()

        if state is not None:
            state = set(to_state(s) for s in iterize(state))
            expected = (r for r in expected if r.state in state)

        if since is not None:
            since = ora.Time(since)
            expected = (r for r in expected if r.timestamp >= since)

        # args takes precedence over with_args
        if args is not None:
            args = {str(k): str(v) for k, v in args.items()}
            with_args = None
            expected = (r for r in expected if r.inst.args == args)

        elif with_args is not None:
            with_args = {str(k): str(v) for k, v in with_args.items()}
            expected = (
                r for r in expected if all(r.inst.args.get(k) == v for k, v in with_args.items())
            )

        if run_ids is not None:
            run_ids = set(iterize(run_ids))
            expected = (r for r in expected if r.run_id in run_ids)

        if job_id is not None:
            expected = (r for r in expected if r.inst.job_id == job_id)

        # compute effective min_timestamp for DB query
        if limit_lookback and self.__min_timestamp is not None:
            if since is not None:
                # use the more restrictive of user's since or lookback window
                min_ts = max(self.__min_timestamp, ora.Time(since))
            else:
                # just use lookback window
                min_ts = self.__min_timestamp
        else:
            # no lookback, but respect user's since if provided
            min_ts = ora.Time(since) if since is not None else None

        runs = itertools.chain(
            expected,
            self.__query_run_db(
                run_ids=run_ids,
                job_id=job_id,
                state=state,
                args=args,
                with_args=with_args,
                min_timestamp=min_ts,
            ),
        )

        return now(), list(runs)

    def summaries(self) -> Iterator[str]:
        from .lib.api import run_to_summary_jso

        for run in self.__expected_runs.values():
            yield ujson.dumps({"type": "run_summary", "run_summary": run_to_summary_jso(run)})

        yield from self.__summary_db.query(min_timestamp=self.__min_timestamp)

    def count_runs(
        self, *, job_id=None, state=None, args=None, with_args=None, limit_lookback=True
    ):
        """
        Counts runs matching the given filters without deserializing Run objects.

        :param limit_lookback:
          If True (default), applies lookback window. If False, counts all runs.
          Conditions (MaxRunning, SkipDuplicate) should pass limit_lookback=False
          to see every active run, including any that has been running longer than lookback.
        """
        # count matching expected (in-memory) runs
        expected = self.__expected_runs.values()
        if job_id is not None:
            expected = (r for r in expected if r.inst.job_id == job_id)
        if state is not None:
            states = set(to_state(s) for s in iterize(state))
            expected = (r for r in expected if r.state in states)
        if args is not None:
            args = {str(k): str(v) for k, v in args.items()}
            expected = (r for r in expected if r.inst.args == args)
        elif with_args is not None:
            with_args = {str(k): str(v) for k, v in with_args.items()}
            expected = (
                r for r in expected if all(r.inst.args.get(k) == v for k, v in with_args.items())
            )

        min_ts = self.__min_timestamp if limit_lookback else None
        return len(list(expected)) + self.__run_db.count_runs(
            job_id=job_id,
            state=state,
            args=args,
            with_args=with_args,
            min_timestamp=min_ts,
        )

    def get_stats(self):
        return {
            "num_runs": len(self.__expected_runs)
            + self.__run_db.count_runs(min_timestamp=self.__min_timestamp),
            "publisher": self.publisher.get_stats(),
        }
