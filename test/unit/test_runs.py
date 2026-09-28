import pytest

from apsis.runs import Instance, template_expand

# -------------------------------------------------------------------------------


def test_instance_args_sort():
    i = Instance("test_job_id", {"foo": 42, "bar": 17, "baz": 0})
    assert tuple(i.args) == ("bar", "baz", "foo")
    assert tuple(i.args.values()) == ("17", "0", "42")


@pytest.mark.parametrize(
    "template, expected",
    [
        ("", ""),
        ("some/job 2026-09-28", "some/job 2026-09-28"),
        (42, "42"),
        ("café", "café"),
        ("{literal}", "{literal}"),
        ("{{ value }}", "expanded"),
        ("{% if value %}enabled{% endif %}", "enabled"),
        ("before{# comment #}after", "beforeafter"),
        ("trailing\n", "trailing"),
        ("trailing\n\n", "trailing\n"),
        ("first\r\nsecond\r\n", "first\nsecond"),
        ("first\rsecond\r", "first\nsecond"),
    ],
)
def test_template_expand(template, expected):
    assert template_expand(template, {"value": "expanded"}) == expected
