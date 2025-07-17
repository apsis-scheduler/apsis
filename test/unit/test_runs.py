from apsis.runs import Instance

# -------------------------------------------------------------------------------


def test_instance_args_sort():
    i = Instance("test_job_id", {"foo": 42, "bar": 17, "baz": 0})
    assert tuple(i.args) == ("bar", "baz", "foo")
    assert tuple(i.args.values()) == ("17", "0", "42")
