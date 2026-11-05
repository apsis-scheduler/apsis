import pytest

from apsis.runs import Instance, _JINJA_ENV, _get_template, template_expand

# -------------------------------------------------------------------------------


def test_instance_args_sort():
    i = Instance("test_job_id", {"foo": 42, "bar": 17, "baz": 0})
    assert tuple(i.args) == ("bar", "baz", "foo")
    assert tuple(i.args.values()) == ("17", "0", "42")


@pytest.mark.parametrize(
    "template, literal",
    [
        ("", True),
        ("some/job 2026-09-28", True),
        (42, True),
        ("café", True),
        ("{literal}", True),
        ("{ {value} }", True),
        ("before{# comment #}after", True),
        ("{# only a comment #}", True),
        ("before {#- comment -#} after", True),
        ("trailing\n", True),
        ("trailing\n\n", True),
        ("first\r\nsecond\r\n", True),
        ("first\rsecond\r", True),
        ("{% raw %}{{ value }}{% endraw %}", True),
        ("{{ value }}", False),
        ("{{ 'const' }}", False),
        ("{% if value %}enabled{% endif %}\n", False),
        ("{% set x = 1 %}", False),
    ],
)
def test_template_expand_matches_jinja(template, literal):
    args = {"value": "expanded"}
    assert isinstance(_get_template(str(template)), str) == literal
    assert template_expand(template, args) == _JINJA_ENV.from_string(str(template)).render(args)
