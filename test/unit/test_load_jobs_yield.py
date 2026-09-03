"""
Tests that `load_jobs_dir` stays cooperative: it walks and parses the jobs dir
without blocking the event loop for the whole (potentially slow, NFS-backed)
traversal.
"""

import asyncio
import datetime
import math
import os
import time

import pytest
import yaml

import apsis.exc
import apsis.jobs
from apsis.jobs import DupCheckSafeLoader, DuplicateKeyError

# -------------------------------------------------------------------------------


def _write_job(path):
    path.write_text("params: []\nprogram:\n  type: no-op\n")


def _make_jobs_tree(root, n_dirs, per_dir=1):
    for d in range(n_dirs):
        sub = root / f"d{d}"
        sub.mkdir()
        for f in range(per_dir):
            _write_job(sub / f"job{f}.yaml")


@pytest.mark.asyncio
async def test_load_jobs_dir_interleaves_slow_walk(tmp_path, monkeypatch):
    """
    A slow directory walk must not block the event loop for its whole duration.

    We inject a synchronous per-directory delay (simulating slow NFS metadata
    reads) and assert that a concurrent task still runs *between* batches, so the
    worst stall is about one batch's worth of directories -- not the sum over all
    directories (which is what eagerly consuming the whole walk would cost).
    """
    # Many more dirs than the load batch (16) so the worst stall -- about one
    # batch of walking -- stays well under the whole-walk time.
    n_dirs = 64
    _make_jobs_tree(tmp_path, n_dirs)

    delay = 0.005
    real_walk = os.walk

    def slow_walk(*args, **kwargs):
        for entry in real_walk(*args, **kwargs):
            # Simulate a slow scandir for each directory.
            time.sleep(delay)
            yield entry

    monkeypatch.setattr(apsis.jobs.os, "walk", slow_walk)

    gaps = []
    stop = False

    async def ticker():
        last = time.perf_counter()
        while not stop:
            await asyncio.sleep(0)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now

    task = asyncio.create_task(ticker())
    # Let the ticker start and establish its baseline before the load begins,
    # so it can observe a stall that happens at the very start of the walk.
    await asyncio.sleep(0.01)
    jobs_dir = await apsis.jobs.load_jobs_dir(tmp_path)
    stop = True
    await task

    # All jobs loaded correctly through the sequential loop.
    assert len(list(jobs_dir.get_jobs())) == n_dirs

    # The whole walk costs ~n_dirs * delay.  If the loop were blocked for the
    # entire walk (e.g. by consuming it eagerly into a list), the worst stall
    # would approach that total.  Batched iteration bounds it to about one
    # batch's worth of directories.
    total_walk = delay * (n_dirs + 1)
    assert max(gaps) < total_walk / 2, (
        f"event loop blocked {max(gaps) * 1000:.0f}ms; whole walk is {total_walk * 1000:.0f}ms"
    )


@pytest.mark.asyncio
async def test_load_jobs_dir_loads_all_jobs(tmp_path):
    """The refactored loop still loads every job, with correct job ids."""
    _make_jobs_tree(tmp_path, n_dirs=3, per_dir=4)
    jobs_dir = await apsis.jobs.load_jobs_dir(tmp_path)
    job_ids = {j.job_id for j in jobs_dir.get_jobs()}
    assert job_ids == {f"d{d}/job{f}" for d in range(3) for f in range(4)}


@pytest.mark.asyncio
async def test_load_jobs_dir_skips_hidden_dirs(tmp_path):
    """Hidden dirs (e.g. `.git`) are not descended into."""
    _write_job(tmp_path / "real.yaml")
    git = tmp_path / ".git" / "objects"
    git.mkdir(parents=True)
    _write_job(git / "nope.yaml")
    jobs_dir = await apsis.jobs.load_jobs_dir(tmp_path)
    assert {j.job_id for j in jobs_dir.get_jobs()} == {"real"}


@pytest.mark.asyncio
async def test_load_jobs_dir_reports_bad_yaml(tmp_path):
    """A malformed job still surfaces as an error through the sequential loop."""
    _write_job(tmp_path / "good.yaml")
    # Valid YAML but missing the required `program`: raises SchemaError,
    # which the loader collects rather than propagating.
    (tmp_path / "bad.yaml").write_text("params: [x]\n")
    # Malformed YAML syntax -> collected, not raised out of the whole reload.
    (tmp_path / "broken.yaml").write_text("params: [x\nprogram\n")
    with pytest.raises(apsis.exc.JobsDirErrors) as exc_info:
        await apsis.jobs.load_jobs_dir(tmp_path)
    assert {e.job_id for e in exc_info.value.errors} == {"bad", "broken"}


# -------------------------------------------------------------------------------
# DupCheckSafeLoader


def test_dup_check_loader_rejects_duplicate_keys():
    with pytest.raises(DuplicateKeyError):
        yaml.load("command: one\ncommand: two\n", Loader=DupCheckSafeLoader)


def test_dup_check_loader_parses_normal_mapping():
    assert yaml.load("a: 1\nb: two\n", Loader=DupCheckSafeLoader) == {"a": 1, "b": "two"}


def test_dup_check_loader_empty_scalar_is_null():
    assert yaml.load("a:\n", Loader=DupCheckSafeLoader) == {"a": None}


@pytest.mark.parametrize(
    "text, expected",
    [
        # YAML 1.1 would parse these as an int (43200) and a bool; the YAML 1.2
        # core schema (like ruamel) keeps them as strings.
        ("12:00:00", "12:00:00"),
        ("NO", "NO"),
        ("no", "no"),
        ("on", "on"),
        ("off", "off"),
        ("yes", "yes"),
        # ...while these still resolve as scalars.
        ("true", True),
        ("false", False),
        ("null", None),
        ("42", 42),
        ("1.5", 1.5),
        # Leading-zero ints are decimal (YAML 1.2), not YAML 1.1 octal; `09`
        # isn't even valid octal and would raise under the inherited constructor.
        ("010", 10),
        ("0123", 123),
        ("09", 9),
        ("0x1A", 26),
        ("0o17", 15),
        ("0b101", 5),
        # Signed hex/octal/binary/decimal.
        ("-0x1A", -26),
        ("+0x1A", 26),
        ("-0o17", -15),
        ("+0o17", 15),
        ("-0b101", -5),
        ("+0b101", 5),
        ("-42", -42),
        ("+42", 42),
        # Invalid numeric literals fall back to strings.
        ("0b102", "0b102"),
        ("0o99", "0o99"),
        ("0x", "0x"),
        ("0o", "0o"),
        ("0b", "0b"),
        # Null spellings.
        ("~", None),
        ("Null", None),
        ("NULL", None),
        # Floats: scientific notation, leading/trailing dot, infinity.
        ("1e3", 1000.0),
        ("1.5E-3", 0.0015),
        ("6.022e23", 6.022e23),
        ("-1.2e-5", -1.2e-5),
        (".5", 0.5),
        ("5.", 5.0),
        ("-.5", -0.5),
        (".inf", float("inf")),
        ("-.inf", float("-inf")),
        (".Inf", float("inf")),
        # Underscore digit separators, like ruamel.
        ("1_000", 1000),
        ("12_34", 1234),
        ("0xFF_FF", 65535),
        ("1_000.5", 1000.5),
    ],
)
def test_dup_check_loader_uses_yaml_1_2_scalars(text, expected):
    result = yaml.load(f"x: {text}\n", Loader=DupCheckSafeLoader)["x"]
    assert result == expected
    assert type(result) is type(expected)


def test_dup_check_loader_resolves_nan():
    result = yaml.load("x: .nan\n", Loader=DupCheckSafeLoader)["x"]
    assert isinstance(result, float) and math.isnan(result)


def test_dup_check_loader_quoted_scalars_stay_strings():
    # Quoting forces a string, even for things that would otherwise resolve.
    for text in ('"12:00:00"', '"true"', '"42"', "'null'", '"~"'):
        result = yaml.load(f"x: {text}\n", Loader=DupCheckSafeLoader)["x"]
        assert result == text.strip("\"'")
        assert type(result) is str


def test_dup_check_loader_resolves_timestamps():
    assert yaml.load("d: 2027-03-03\n", Loader=DupCheckSafeLoader)["d"] == datetime.date(2027, 3, 3)
    assert yaml.load("t: 2027-03-03 12:00:00\n", Loader=DupCheckSafeLoader)[
        "t"
    ] == datetime.datetime(2027, 3, 3, 12, 0, 0)
    assert yaml.load("t: 2027-03-03 12:00:00.5\n", Loader=DupCheckSafeLoader)[
        "t"
    ] == datetime.datetime(2027, 3, 3, 12, 0, 0, 500000)
    assert yaml.load("t: 2027-03-03T12:00:00Z\n", Loader=DupCheckSafeLoader)[
        "t"
    ] == datetime.datetime(2027, 3, 3, 12, 0, tzinfo=datetime.timezone.utc)
    assert yaml.load("t: 2027-03-03T12:00:00+05:00\n", Loader=DupCheckSafeLoader)[
        "t"
    ] == datetime.datetime(2027, 3, 3, 12, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5)))


def test_dup_check_loader_honors_merge_keys():
    doc = "base: &b {a: 1, b: 2}\nchild:\n  <<: *b\n  c: 3\n"
    assert yaml.load(doc, Loader=DupCheckSafeLoader)["child"] == {"a": 1, "b": 2, "c": 3}


def test_dup_check_loader_merge_override_is_not_a_duplicate():
    # An explicit key overriding one from `<<` must win, not raise.
    doc = "base: &b {a: 1, p: 9}\nchild:\n  <<: *b\n  p: 3\n"
    assert yaml.load(doc, Loader=DupCheckSafeLoader)["child"] == {"a": 1, "p": 3}


def test_dup_check_loader_still_rejects_real_duplicate_with_merge():
    # A genuine duplicate among explicit keys still raises, merge present or not.
    doc = "base: &b {a: 1}\nchild:\n  <<: *b\n  p: 1\n  p: 2\n"
    with pytest.raises(DuplicateKeyError):
        yaml.load(doc, Loader=DupCheckSafeLoader)


def test_dup_check_loader_rejects_duplicate_merge_keys():
    doc = "a: &a {x: 1}\nb: &b {y: 2}\nchild:\n  <<: *a\n  <<: *b\n"
    with pytest.raises(DuplicateKeyError):
        yaml.load(doc, Loader=DupCheckSafeLoader)


@pytest.mark.parametrize(
    "doc",
    [
        "1: a\n1: b\n",  # int key
        "true: a\ntrue: b\n",  # bool key
        "null: a\n~: b\n",  # both spellings resolve to the same None key
    ],
)
def test_dup_check_loader_rejects_duplicate_nonstring_keys(doc):
    with pytest.raises(DuplicateKeyError):
        yaml.load(doc, Loader=DupCheckSafeLoader)


def test_dup_check_loader_rejects_duplicate_in_flow_mapping():
    with pytest.raises(DuplicateKeyError):
        yaml.load("m: {a: 1, a: 2}\n", Loader=DupCheckSafeLoader)


def test_dup_check_loader_rejects_duplicate_in_nested_mapping():
    with pytest.raises(DuplicateKeyError):
        yaml.load("outer:\n  inner:\n    k: 1\n    k: 2\n", Loader=DupCheckSafeLoader)


def test_dup_check_loader_merges_list_of_anchors():
    # The first anchor wins for a key present in more than one merged mapping.
    doc = "a: &a {x: 1, y: 1}\nb: &b {y: 2}\nc:\n  <<: [*a, *b]\n"
    assert yaml.load(doc, Loader=DupCheckSafeLoader)["c"] == {"x": 1, "y": 1}


def test_dup_check_loader_unhashable_key_is_yaml_error():
    # A complex (unhashable) key must raise a catchable yaml.YAMLError, not a
    # bare TypeError that would escape load_jobs_dir and abort the whole reload.
    with pytest.raises(yaml.YAMLError):
        yaml.load("? [a, b]\n: 1\n", Loader=DupCheckSafeLoader)


@pytest.mark.parametrize("doc", ['x: !!int ""\n', 'x: !!int "+"\n'])
def test_dup_check_loader_malformed_explicit_int_is_yaml_error(doc):
    # Malformed explicit !!int must raise a catchable yaml.YAMLError, not a bare
    # IndexError/ValueError.
    with pytest.raises(yaml.YAMLError):
        yaml.load(doc, Loader=DupCheckSafeLoader)


def test_dup_check_loader_does_not_mutate_base_loaders():
    # The resolver/constructor swaps live on the subclass only; stock PyYAML
    # loaders in the same process must keep their YAML 1.1 behavior and still
    # allow duplicate keys.
    for loader in (yaml.SafeLoader, yaml.CSafeLoader):
        assert yaml.load("x: NO\n", Loader=loader)["x"] is False
        assert yaml.load("x: 12:00:00\n", Loader=loader)["x"] == 43200
        assert yaml.load("a: 1\na: 2\n", Loader=loader) == {"a": 2}
