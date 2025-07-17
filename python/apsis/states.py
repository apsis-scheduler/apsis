import enum

from apsis.lib import py

# -------------------------------------------------------------------------------


class State(enum.Enum):
    new = enum.auto()
    scheduled = enum.auto()
    waiting = enum.auto()
    starting = enum.auto()
    running = enum.auto()
    stopping = enum.auto()
    success = enum.auto()
    failure = enum.auto()
    error = enum.auto()
    skipped = enum.auto()

    @property
    def finished(self):
        return self in {State.success, State.failure, State.error, State.skipped}


def to_state(state):
    if isinstance(state, State):
        return state
    try:
        return State[state]
    except KeyError:
        pass
    raise ValueError(f"not a state: {state!r}")


def to_states(states):
    """
    Convers `states` to an iterator of States instances.

      >>> tuple(to_states("running"))
      (<State.running: 5>,)
      >>> tuple(to_states(["failure", State.error]))
      (<State.failure: 7>, <State.error: 8>)

    """
    return (to_state(s) for s in py.iterize(states))


# State model.  Allowed transitions _to_ each state.
TRANSITIONS = {
    State.new: set(),
    State.scheduled: {State.new},
    State.waiting: {State.scheduled},
    State.starting: {State.scheduled, State.waiting},
    State.running: {State.starting},
    State.stopping: {State.running},
    State.error: {
        State.new,
        State.scheduled,
        State.waiting,
        State.starting,
        State.running,
        State.stopping,
        State.skipped,
    },
    State.success: {State.running, State.stopping},
    State.failure: {State.running, State.stopping},
    State.skipped: {State.new, State.scheduled, State.waiting},
}


def states_from_jso(jso):
    return frozenset(State[s] for s in py.tupleize(jso))


def states_to_jso(states):
    states = frozenset(states)
    return None if states == frozenset(State) else [s.name for s in states]


def reachable(state):
    """
    Returns the set of states reachable from `state` by zero or more
    transitions.
    """
    reachable = {state}
    while True:
        # Find immediate successors.
        succ = {s for s, pred in TRANSITIONS.items() if any(r in pred for r in reachable)}
        # If there are no new successors, stop.
        if len(succ - reachable) == 0:
            return reachable
        else:
            reachable |= succ
