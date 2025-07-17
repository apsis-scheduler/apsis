import apsis.host_group as hg
from apsis.lib import itr

# -------------------------------------------------------------------------------


def test_single_from_jso():
    g = hg.HostGroup.from_jso("foobar")
    assert isinstance(g, hg.SingleHost)
    assert g.host == "foobar"
    assert g.choose() == "foobar"


def test_random_from_jso():
    g = hg.HostGroup.from_jso(
        {
            "type": "apsis.host_group.RandomHostGroup",
            "hosts": ["foo", "bar", "baz"],
        }
    )
    assert isinstance(g, hg.RandomHostGroup)
    assert g.hosts == ("foo", "bar", "baz")
    assert g.choose() in {"foo", "bar", "baz"}


def test_round_robin_from_jso():
    HOSTS = ["foo", "bar", "baz"]

    g = hg.HostGroup.from_jso(
        {
            "type": "round-robin",
            "hosts": HOSTS,
        }
    )
    assert isinstance(g, hg.RoundRobinHostGroup)
    assert g.hosts == ("foo", "bar", "baz")
    hosts = [g.choose() for _ in range(20)]
    while hosts[0] != "foo":
        hosts.pop(0)
    assert hosts == list(itr.take(len(hosts), itr.cycle(HOSTS)))
