import asyncio
import concurrent.futures
from dataclasses import dataclass
import logging

from apsis.lib import py
from apsis.lib.json import TypedJso, check_schema
from apsis.lib.timing import LogSlow
from apsis.runs import template_expand
from apsis.states import State

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class Condition(TypedJso):
    """
    A boolean condition that blocks a run from starting.  The run waits until
    the condition evaluates true.
    """

    TYPE_NAMES = TypedJso.TypeNames()

    def bind(self, run, jobs):
        """
        Binds the condition to `inst`.

        :param run:
          The run to bind to.
        :param jobs:
          The jobs DB.
        :return:
          An instance of the same type, bound to the instances.
        """

    @dataclass
    class Transition:
        """
        The run should transition to `state`.
        """

        state: State
        reason: str = None


# -------------------------------------------------------------------------------


class PolledCondition(Condition):
    # Poll inteval in sec.
    poll_interval = 1

    async def check(self):
        """
        Checks if conditions have been met and the run is ready to start.

        :return:
          `True` if the run is ready, `False` otherwise, or `Transition` to
          cause the run to transition to a new state.
        """
        return True

    async def wait(self):
        """
        Waits for the condition to complete.

        :return:
          `True` if the run is ready or `Transition` to cause the run to
          transition to a new state.
        """
        while True:
            with LogSlow(f"checking cond: {self}", 0):
                result = await self.check()
            if result is False:
                await asyncio.sleep(self.poll_interval)
            else:
                return result


class ThreadPolledCondition(PolledCondition):
    """
    Abstract base condition that polls a check function in a thread.

    An implementation should provide `check()`, which may perform blocking
    activities.  The implementation must take care not to access any global
    resources that aren't properly threadsafe, including all resources used by
    Apsis.  Logging is threadsafe, however.

    Each invocation of `check()` may block for a period of time, but the maximum
    duration should be bounded and as short as possible.  A condition cannot be
    cancelled while `check()` is running, so a run cannot be skipped or started
    until the current check completes.
    """

    def check(self):
        return True

    async def wait(self):
        loop = asyncio.get_event_loop()
        # Use a single executor for all check invocations.
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as exe:
            # The poll loop needs to be outside the thread and async, so that it
            # can be canelled via async cancellation.  This will occur only
            # between checks; a single check runs in a thread executor and
            # cannot be cancelled.
            while True:
                with LogSlow(f"thread cond check: {self}", 0, level=logging.DEBUG):
                    result = await loop.run_in_executor(exe, self.check)
                if result is False:
                    await asyncio.sleep(self.poll_interval)
                else:
                    return result


# -------------------------------------------------------------------------------


class RunStoreCondition(Condition):
    async def wait(self, run_store):
        """
        Checks if run-based conditions have been met and the run is ready to
        start.

        :return:
          `True` if the run is ready, or `Transition` to cause the run to
          transition to a new state.
        """
        return True


class NonmonotonicRunStoreCondition(RunStoreCondition):
    """
    A `RunStoreCondition` where the condition is not monotonic, i.e. once
    satisfied, it may later no longer be satisfied.

    The waiting logic invokes condition's (synchronous) `check()` method after
    `wait()` has succeeded, to confirm that condition is still met before
    immediatley continuing to the next condition.
    """

    def check(self, run_store):
        """
        Checks synchronously if run-based conditions have been met.  The
        semantics should match those of `wait()`.

        :return:
          `True` if the run is ready, `False` otherwise, or `Transition` to
          cause the run to transition to a new state.
        """
        return True


# -------------------------------------------------------------------------------


def _bind(job, obj_args, inst_args, bind_args):
    """
    Binds args to `job.params`.

    Binds `obj_args` and `inst_args` to params by name.  `obj_args` take
    precedence, and are template-expanded with `bind_args`; `inst_args` are
    not expanded.
    """

    def get(name):
        try:
            template = obj_args[name]
        except KeyError:
            pass
        else:
            return template_expand(template, bind_args)
        try:
            return inst_args[name]
        except KeyError:
            pass
        raise LookupError(f"no value for param {name} of job {job.job_id}")

    return {n: get(n) for n in job.params}


# -------------------------------------------------------------------------------


class ConstantCondition(PolledCondition):
    """
    Condition with a constant value, either true or false.
    """

    def __init__(self, value):
        self.__value = bool(value)

    def __repr__(self):
        return py.format_ctor(self, self.__value)

    def bind(self, run, jobs):
        return self

    def to_jso(self):
        return super().to_jso() | {
            "value": self.__value,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            return cls(
                value=pop("value", bool),
            )

    async def check(self):
        return self.__value
