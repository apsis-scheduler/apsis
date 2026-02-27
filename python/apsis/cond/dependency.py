import logging

from apsis.lib.json import check_schema
from apsis.lib.py import format_ctor, iterize
from apsis.runs import Instance, get_bind_args, template_expand
from apsis.states import State, reachable
from .base import RunStoreCondition, _bind

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


def join_states(states):
    return "|".join(s.name for s in sorted(states, key=lambda s: s.value))


class Dependency(RunStoreCondition):
    """
    Waits for a run in a given state or states.
    """

    DEFAULT_STATE = State.success

    def __init__(
        self,
        job_id,
        args={},
        *,
        states=DEFAULT_STATE,
        exist=None,
        enabled=None,
    ):
        args = {str(k): str(v) for k, v in args.items()}
        states = frozenset(iterize(states))
        assert all(isinstance(s, State) for s in states)
        if exist is not None:
            exist = frozenset(iterize(exist))
            assert all(isinstance(s, State) for s in exist)
            assert len(states - exist) == 0

        self.job_id = job_id
        self.args = args
        self.states = states
        self.exist = exist
        self.enabled = enabled

    def __repr__(self):
        return format_ctor(
            self,
            self.job_id,
            self.args,
            states=self.states,
            exist=self.exist,
            enabled=self.enabled,
        )

    def __str__(self):
        inst = Instance(self.job_id, self.args)
        states = join_states(self.states)
        exist = "" if self.exist is None else ", must exist as " + join_states(self.exist)
        return f"dependency {inst} is {states}{exist}{self._enabled_str()}"

    def to_jso(self):
        jso = {
            **super().to_jso(),
            "job_id": self.job_id,
            "args": self.args,
        }
        if self.states != {self.DEFAULT_STATE}:
            jso["states"] = [s.name for s in self.states]
        if self.exist is not None:
            jso["exist"] = [s.name for s in self.exist]
        return jso

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            states = pop("states", default=cls.DEFAULT_STATE.name)
            states = {State[s] for s in iterize(states)}
            exist = pop("exist", default=None)
            if exist is True:
                # Require the existence of a run in any state from which one of
                # the target states is reachable.
                exist = {s for s in State if reachable(s) & states}
            elif exist is not None:
                exist = {State[s] for s in iterize(exist)}
            enabled = pop("enabled", default=None)
            cls._validate_enabled(enabled)
            return cls(
                pop("job_id"),
                pop("args", default={}),
                states=states,
                exist=exist,
                enabled=enabled,
            )

    def bind(self, run, jobs):
        if not self._eval_enabled(run):
            return None

        bind_args = get_bind_args(run)
        job_id = template_expand(self.job_id, bind_args)
        job = jobs[job_id]
        args = _bind(job, self.args, run.inst.args, bind_args)
        return type(self)(
            job_id,
            args,
            states=self.states,
            exist=self.exist,
        )

    async def wait(self, run_store):
        # Wait for a matching run to transition into a matching state.
        with run_store.publisher.subscription(
            predicate=lambda m: m.job_id == self.job_id and m.args == self.args
        ) as sub:
            while True:
                # Look for matching runs.
                _, runs = run_store.query(job_id=self.job_id, args=self.args)

                # Is the dependency satisfied?
                if any(r.state in self.states for r in runs):
                    return True

                # Check for runs in exist states, if indicated.
                if self.exist is not None and not any(r.state in self.exist for r in runs):
                    # No dependency run exists in a valid starting state.
                    inst = Instance(self.job_id, self.args)
                    return self.Transition(
                        State.error,
                        f"no dependency {inst} exists in {join_states(self.exist)}",
                    )

                # Wait for a matching run to transition.
                await anext(sub)
