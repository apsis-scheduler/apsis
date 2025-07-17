import asyncio
import concurrent.futures
import logging

from .condition import Condition
from apsis.lib import py
from apsis.lib.json import TypedJso, check_schema

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class Action(TypedJso):
    """
    An action to perform when a run has transitioned to a new state.
    """

    TYPE_NAMES = TypedJso.TypeNames()

    async def __call__(self, apsis, run):
        """
        Performs the action on `run`.

        An implementation must be properly async, i.e. should not block the
        event loop for substantial periods of time.
        """
        raise NotImplementedError("Action.__call__")

    @property
    def condition(self):
        raise NotImplementedError("Action.condition")


# -------------------------------------------------------------------------------


class BaseAction(Action):
    """
    Base class for action types.
    """

    def __init__(self, *, condition=None):
        self.__condition = condition

    def __repr__(self):
        return py.format_ctor(self, condition=self.__condition)

    @property
    def condition(self):
        return self.__condition

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            condition = pop("if", Condition.from_jso, None)
        return cls(condition=condition)

    def to_jso(self):
        jso = super().to_jso()
        if self.__condition is not None:
            jso["if"] = self.__condition.to_jso()
        return jso


# -------------------------------------------------------------------------------


class ThreadAction(BaseAction):
    """
    Abstract base action that is invoked in a thread.

    An implementation should provide `run()`, which may perform blocking
    activities.  The implementation must take care not to access any global
    resources that aren't properly threadsafe, including all resources used by
    Apsis.  Logging is threadsafe, however.  The Apsis instance is not available
    to `run()`.
    """

    def run(self, run):
        raise NotImplementedError("ThreadAction.run")

    async def __call__(self, apsis, run):
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as exe:
            log.debug(f"thread action start: {self}")
            await loop.run_in_executor(exe, self.run, run)
            log.debug(f"thread action done: {self}")
