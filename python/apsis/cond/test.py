"""
Conditions for integration testing.
"""

import logging
import time

from .base import ThreadPolledCondition
from apsis.lib.json import check_schema
from apsis.lib.py import format_ctor
from apsis.runs import template_expand, get_bind_args

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class TestThreadPolledCondition(ThreadPolledCondition):
    """
    A polled condition which returns false (not ready) `count - 1` times,
    and then true.  Each check has a sleep of `delay` s.  The poll interval is
    small.
    """

    poll_interval = 0.1

    def __init__(self, delay, count):
        self.__delay = delay
        self.__count = count
        self.__counted = 0

    def __repr__(self):
        return format_ctor(self, self.__delay, self.__count)

    def bind(self, run, jobs):
        bind_args = get_bind_args(run)
        delay = float(template_expand(self.__delay, bind_args))
        count = int(template_expand(self.__count, bind_args))
        return type(self)(delay, count)

    def to_jso(self):
        return super().to_jso() | {
            "delay": self.__delay,
            "count": self.__count,
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            return cls(
                delay=pop("delay"),
                count=pop("count"),
            )

    def check(self):
        log.debug(f"checking condition: {id(self)} {self} {self.__counted}")
        time.sleep(self.__delay)
        self.__counted += 1
        return self.__counted == self.__count
