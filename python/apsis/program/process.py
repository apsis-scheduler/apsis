from dataclasses import dataclass
from signal import Signals

from apsis.lib.json import check_schema, ifkey
from apsis.lib.parse import nparse_duration
from apsis.lib.py import or_none
from apsis.lib.sys import to_signal
from apsis.runs import template_expand

ntemplate_expand = or_none(template_expand)

# -------------------------------------------------------------------------------


@dataclass
class Stop:
    """
    Specification for how to stop a running process.

    1. Send `signal` to the process.
    2. Wait up to `grace_period` sec.
    3. If the process has not terminated, send SIGKILL.
    """

    signal: str = "SIGTERM"
    grace_period: str = "60"

    def to_jso(self):
        cls = type(self)
        return ifkey("signal", self.signal, cls.signal) | ifkey(
            "grace_period", self.grace_period, cls.grace_period
        )

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso or {}) as pop:
            signal = pop("signal", str, default=cls.signal)
            grace_period = pop("grace_period", default=cls.grace_period)
        return cls(signal, grace_period)

    def bind(self, args):
        return BoundStop(
            to_signal(template_expand(self.signal, args)),
            nparse_duration(ntemplate_expand(self.grace_period, args)),
        )


@dataclass
class BoundStop:
    signal: Signals = Signals.SIGTERM
    grace_period: float = 60

    def to_jso(self):
        cls = type(self)
        return ifkey("signal", self.signal, cls.signal) | ifkey(
            "grace_period", self.grace_period, cls.grace_period
        )

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso or {}) as pop:
            signal = pop("signal", to_signal, cls.signal)
            grace_period = pop("grace_period", int, cls.grace_period)
        return cls(signal, grace_period)

