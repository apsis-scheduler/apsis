import logging

from apsis.lib.py import format_ctor
from apsis.runs import Instance, get_bind_args, template_expand
from apsis.states import State
from .base import Condition, NonmonotonicRunStoreCondition

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class MaxRunning(Condition):
    """
    Limits simultaneous running jobs.

    The condition is true if the number of runs matching `job_id` and `args` in
    the starting or running states is less than `count`.
    """

    def __init__(self, count, job_id=None, args=None, *, enabled=None):
        """
        :param job_id:
          Job ID of runs to count.  If none, bound to the job ID of the
          owning instance.
        :param args:
          Args to match.  If none, the bound to the args of the owning instance.
        :param enabled:
          If not none, a Jinja2 expression or bool controlling whether this
          condition is active for a given run.
        """
        self.__count = count
        self.__job_id = job_id
        self.__args = args
        self.enabled = enabled

    def __repr__(self):
        return format_ctor(
            self,
            self.__count,
            job_id=self.__job_id,
            args=self.__args,
            enabled=self.enabled,
        )

    def __str__(self):
        return f"fewer than {self.__count} runs running{self._enabled_str()}"

    def to_jso(self):
        return {
            **super().to_jso(),
            "count": self.__count,
            "job_id": self.__job_id,
            "args": self.__args,
        }

    @classmethod
    def from_jso(cls, jso):
        enabled = jso.pop("enabled", None)
        cls._validate_enabled(enabled)
        return cls(
            jso.pop("count", "1"),
            jso.pop("job_id", None),
            jso.pop("args", None),
            enabled=enabled,
        )

    def bind(self, run, jobs):
        if not self._eval_enabled(run):
            return None
        bind_args = get_bind_args(run)
        count = template_expand(self.__count, bind_args)
        job_id = run.inst.job_id if self.__job_id is None else self.__job_id
        # FIXME: Support self.__args not none.  Template-expand them, add in
        # inst.args, and bind to job args.
        if self.__args is not None:
            raise NotImplementedError()
        return BoundMaxRunning(count, job_id, run.inst.args)


# -------------------------------------------------------------------------------


class BoundMaxRunning(NonmonotonicRunStoreCondition):
    def __init__(self, count, job_id, args):
        self.__count = int(count)
        self.__job_id = job_id
        self.__args = args

    def __repr__(self):
        return format_ctor(self, self.__count, job_id=self.__job_id, args=self.__args)

    def __str__(self):
        inst = Instance(self.__job_id, self.__args)
        return f"fewer than {self.__count} runs of {inst} running"

    def to_jso(self):
        return {
            **super().to_jso(),
            "count": self.__count,
            "job_id": self.__job_id,
            "args": self.__args,
        }

    @classmethod
    def from_jso(cls, jso):
        return cls(
            jso.pop("count"),
            jso.pop("job_id"),
            jso.pop("args"),
        )

    def check(self, run_store):
        # Count running jobs.
        _, running = run_store.query(
            job_id=self.__job_id,
            args=self.__args,
            state=(State.starting, State.running),
        )
        count = len(list(running))
        log.debug(f"found {count} running")
        return count < self.__count

    async def wait(self, run_store):
        # Set up a subscription to run transitions matching the job ID and args.
        with run_store.publisher.subscription(
            predicate=lambda m: m.job_id == self.__job_id and m.args == self.__args
        ) as sub:
            while (result := self.check(run_store)) is False:
                # Wait until a relevant run transitions, then check again.
                await anext(sub)
        return result
