import subprocess
import sys

import pytest

from apsis.exc import JobsDirErrors
from apsis.jobs import load_jobs_dir


def write_jobs(job_dir, *job_ids):
    for job_id in job_ids:
        path = job_dir / f"{job_id}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("program:\n  type: no-op\n")


def check_jobs(job_dir, *args):
    return subprocess.run(
        [sys.executable, "-m", "apsis.ctl", "check-jobs", str(job_dir), *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize("args", [(), ("--check-dependencies-scheduled",)])
def test_check_jobs_rejects_runs_suffix(tmp_path, args):
    job_ids = ("group/runs", "deep/group/runs", "runs/runs")
    write_jobs(tmp_path, *job_ids)

    result = check_jobs(tmp_path, *args)

    assert result.returncode == 1, result.stdout + result.stderr
    for job_id in job_ids:
        assert f"{job_id}:" in result.stdout
    assert result.stdout.count("must not end in '/runs'") == len(job_ids)


def test_check_jobs_allows_other_runs_names(tmp_path):
    write_jobs(
        tmp_path,
        "runs",
        "dryruns",
        "group/test-runs",
        "group/archive apsis runs",
        "group/runs/step",
        "group/RUNS",
        "literal%2Fruns",
    )

    result = check_jobs(tmp_path)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == ""


def test_check_jobs_reports_name_and_other_validation_errors(tmp_path):
    write_jobs(tmp_path, "group/runs")
    (tmp_path / "group/runs.yaml").write_text("params: [bad-name]\nprogram:\n  type: no-op\n")

    result = check_jobs(tmp_path)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "must not end in '/runs'" in result.stdout
    assert "invalid param names" in result.stdout


@pytest.mark.asyncio
async def test_loading_rejects_runs_suffix_job(tmp_path):
    # The loader is shared by check-jobs, service startup, and reload, so the
    # naming rule applies to all of them.
    write_jobs(tmp_path, "group/runs", "group/ok")
    with pytest.raises(JobsDirErrors) as exc_info:
        await load_jobs_dir(tmp_path)
    (err,) = exc_info.value.errors
    assert err.job_id == "group/runs"
    assert "must not end in '/runs'" in str(err)
