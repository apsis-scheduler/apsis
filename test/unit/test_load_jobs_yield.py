"""Tests that `load_jobs_dir` doesn't block the event loop while loading."""

import asyncio
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
    """A slow walk shouldn't block the loop for its whole duration."""
    # Lots of dirs so the cooperative stall (~1 dir) is well under the threshold.
    n_dirs = 20
    _make_jobs_tree(tmp_path, n_dirs)

    delay = 0.02
    real_walk = os.walk

    def slow_walk(*args, **kwargs):
        # Simulate slow NFS metadata reads, one blocking step per directory.
        for entry in real_walk(*args, **kwargs):
            time.sleep(delay)
            yield entry

    monkeypatch.setattr(apsis.jobs.os, "walk", slow_walk)

    gaps = []
    stop = False

    # Ticker records the gap between its wakeups; a blocked loop -> big gap.
    async def ticker():
        last = time.perf_counter()
        while not stop:
            await asyncio.sleep(0)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now

    task = asyncio.create_task(ticker())
    await asyncio.sleep(0.01)  # let the ticker establish a baseline
    jobs_dir = await apsis.jobs.load_jobs_dir(tmp_path)
    stop = True
    await task

    assert len(list(jobs_dir.get_jobs())) == n_dirs
    # Eager loading would block for the whole walk; cooperative bounds the stall
    # to a file-bearing dir plus any yaml-less dirs preceding it (here, 1 each).
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
    # Valid YAML, but missing the required `program` -> collected SchemaError.
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
    ],
)
def test_dup_check_loader_uses_yaml_1_2_scalars(text, expected):
    result = yaml.load(f"x: {text}\n", Loader=DupCheckSafeLoader)["x"]
    assert result == expected
    assert type(result) is type(expected)


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
