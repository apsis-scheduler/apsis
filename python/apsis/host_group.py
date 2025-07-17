"""
Designation of host(s) to run on.
"""

import random

from .lib.json import TypedJso
from .runs import template_expand

# -------------------------------------------------------------------------------


class HostGroup(TypedJso):
    TYPE_NAMES = TypedJso.TypeNames()

    def __init__(self, hosts):
        self.__hosts = tuple(hosts)
        assert all(isinstance(h, str) for h in self.__hosts)

    @classmethod
    def from_jso(cls, jso):
        # Accept a single host or a list of hosts.
        return (
            SingleHost(jso)
            if isinstance(jso, str)
            else RandomHostGroup(jso)
            if isinstance(jso, list)
            else TypedJso.from_jso.__func__(cls, jso)
        )

    @property
    def hosts(self):
        return self.__hosts

    def bind(self, args):
        hosts = tuple(template_expand(a, args) for a in self.__hosts)
        return type(self)(hosts)


class SingleHost(HostGroup):
    """
    Single fixed host.
    """

    def __init__(self, host):
        super().__init__([host])

    @property
    def host(self):
        return self.hosts[0]

    @classmethod
    def from_jso(cls, jso):
        # Special case: just use the host.
        return cls(jso.pop("host"))

    def to_jso(self):
        return self.host

    def choose(self):
        return self.host


class RoundRobinHostGroup(HostGroup):
    """
    A round-robin cycle of hosts.
    """

    def __init__(self, hosts):
        super().__init__(hosts)
        self.__index = 0

    @classmethod
    def from_jso(cls, jso):
        return cls(jso.pop("hosts"))

    def to_jso(self):
        return {
            **super().to_jso(),
            "hosts": self.hosts,
        }

    def choose(self):
        host = self.hosts[self.__index]
        self.__index = (self.__index + 1) % len(self.hosts)
        return host


class RandomHostGroup(HostGroup):
    """
    A list of hosts to choose from at random with replacement.
    """

    @classmethod
    def from_jso(cls, jso):
        return cls(jso.pop("hosts"))

    def to_jso(self):
        return {
            **super().to_jso(),
            "hosts": self.hosts,
        }

    def choose(self):
        return random.choice(self.hosts)


# Aliases.
HostGroup.TYPE_NAMES.set(SingleHost, "single")
HostGroup.TYPE_NAMES.set(RoundRobinHostGroup, "round-robin")
HostGroup.TYPE_NAMES.set(RandomHostGroup, "random")

# -------------------------------------------------------------------------------


def config_host_groups(cfg):
    cfg["host_groups"] = {n: HostGroup.from_jso(g) for n, g in cfg.get("host_groups", {}).items()}


def expand_host(host, cfg):
    host_groups = cfg.get("host_groups", {})
    try:
        host_group = host_groups[host]
    except KeyError:
        return host
    else:
        return host_group.choose()
