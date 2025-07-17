import logging

from apsis.lib import py
from apsis.lib.json import check_schema
from apsis.runs import Instance
from apsis.states import to_state, to_states
from .base import Condition, NonmonotonicRunStoreCondition

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class SkipDuplicate(Condition):
    """
    Transitions a run if another run with the same job_id and args exists.

    This condition will never hold a run in the waiting state; it either
    transitions it immediately or evaluates to true.
    """

    DEFAULT_CHECK_STATES = ("waiting", "starting", "running")

    def __init__(
        self,
        *,
        check_states=DEFAULT_CHECK_STATES,
        target_state="skipped",
    ):
        """
        :param check_states:
          Run states to consider when looking for duplicates.
        :param target_state:
          The state to transition this run to; must be a finished state.
        """
        self.__check_states = [to_state(s) for s in py.iterize(check_states)]
        self.__target_state = to_state(target_state)
        if not self.__target_state.finished:
            raise ValueError(f"invalid targat state: {self.__target_state.name}")

    def __repr__(self):
        return py.format_ctor(
            self, check_states=self.__check_states, target_state=self.__target_state
        )

    def __str__(self):
        target = self.__target_state.name
        states = "|".join(s.name for s in self.__check_states)
        return f"transition to {target} if another run is {states}"

    @property
    def check_states(self):
        return frozenset(self.__check_states)

    @property
    def target_state(self):
        return self.__target_state

    def to_jso(self):
        return {
            **super().to_jso(),
            "check_states": [s.name for s in self.__check_states],
            "target_state": self.__target_state.name,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            return cls(
                check_states=pop("check_states", default=cls.DEFAULT_CHECK_STATES),
                target_state=pop("target_state", default="skipped"),
            )

    def bind(self, run, jobs):
        return BoundSkipDuplicate(
            check_states=self.__check_states,
            target_state=self.__target_state,
            inst=run.inst,
            run_id=run.run_id,
        )


# -------------------------------------------------------------------------------


class BoundSkipDuplicate(NonmonotonicRunStoreCondition):
    def __init__(self, check_states, target_state, inst, run_id):
        self.__check_states = check_states
        self.__target_state = target_state
        self.__inst = inst
        self.__run_id = run_id

    def __repr__(self):
        return py.format_ctor(
            self,
            check_states=self.__check_states,
            target_state=self.__target_state,
            job_id=self.__job_id,
            inst=self.__inst,
        )

    def __str__(self):
        target = self.__target_state.name
        states = "|".join(s.name for s in self.__check_states)
        return f"transition to {target} if another {self.__inst} is {states}"

    @property
    def check_states(self):
        return frozenset(self.__check_states)

    @property
    def target_state(self):
        return self.__target_state

    @property
    def inst(self):
        return self.__inst

    @property
    def run_id(self):
        return self.__run_id

    def to_jso(self):
        return {
            **super().to_jso(),
            "check_states": [s.name for s in self.__check_states],
            "target_state": self.__target_state.name,
            "instance": self.__inst.to_jso(),
            "run_id": self.__run_id,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            return cls(
                check_states=pop("check_states", to_states),
                target_state=pop("target_state", to_state),
                inst=pop("instance", Instance.from_jso),
                run_id=pop("run_id", str),
            )

    # Note: Because check() never returns false, there's no poll loop between
    # wait() and check().

    def check(self, run_store):
        # Query runs with the same job_id and args as this one.
        _, runs = run_store.query(
            job_id=self.__inst.job_id,
            args=self.__inst.args,
            state=self.__check_states,
        )
        # Exclude this run itself.
        runs = [r for r in runs if r.run_id != self.__run_id]

        if len(runs) > 0:
            # Found a match.  Transition this run.
            return self.Transition(
                self.__target_state,
                f"transitioning to {self.__target_state.name} because "
                f"{runs[0].run_id} {runs[0].state.name}",
            )
        else:
            # No match; we're good.
            return True

    async def wait(self, run_store):
        return True
