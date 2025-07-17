from apsis.states import State, reachable

# -------------------------------------------------------------------------------


def test_reachable():
    states = set(State)
    assert reachable(State.new) == states
    states -= {State.new}

    assert reachable(State.scheduled) == states
    states -= {State.scheduled}

    assert reachable(State.waiting) == states
    states -= {State.waiting, State.skipped}

    assert reachable(State.starting) == states
    states -= {State.starting}

    assert reachable(State.running) == states

    assert reachable(State.success) == {State.success}
    assert reachable(State.failure) == {State.failure}
    assert reachable(State.skipped) == {State.skipped, State.error}
    assert reachable(State.error) == {State.error}
