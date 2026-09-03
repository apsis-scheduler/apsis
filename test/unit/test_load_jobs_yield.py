"""
Tests that `load_jobs_dir` stays cooperative: it walks and parses the jobs dir
without blocking the event loop for the whole (potentially slow, NFS-backed)
traversal.
"""

import asyncio
import os
import time

import pytest

import apsis.exc
import apsis.jobs

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
async def test_load_jobs_dir_reports_bad_yaml(tmp_path):
    """A malformed job still surfaces as an error through the sequential loop."""
    _write_job(tmp_path / "good.yaml")
    # Valid YAML but missing the required `program`: raises SchemaError,
    # which the loader collects rather than propagating.
    (tmp_path / "bad.yaml").write_text("params: [x]\n")
    with pytest.raises(apsis.exc.JobsDirErrors) as exc_info:
        await apsis.jobs.load_jobs_dir(tmp_path)
    assert len(exc_info.value.errors) == 1
    assert exc_info.value.errors[0].job_id == "bad"
