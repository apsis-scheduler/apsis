from apsis.lib.json import check_schema
from apsis.lib import py
from apsis.states import State, states_from_jso, states_to_jso

# -------------------------------------------------------------------------------


class Condition:
    def __init__(self, *, states=None):
        self.states = frozenset(states)

    def __repr__(self):
        return py.format_ctor(self, states=self.states)

    def __call__(self, run):
        return run.state in self.states

    @classmethod
    def from_jso(cls, jso):
        if jso is None:
            return None
        with check_schema(jso) as pop:
            states = pop("states", states_from_jso, default=list(State))
        return cls(states=states)

    def to_jso(self):
        return (
            None
            if self is None
            else {
                "states": states_to_jso(self.states),
            }
        )


Condition.DEFAULT = Condition(states=list(s for s in State if s.finished))
