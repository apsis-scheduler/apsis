from types import SimpleNamespace

from apsis.run_snapshot import snapshot_run
from apsis.runs import Instance, Run


def test_snapshot_of_run_whose_job_is_gone():
    def get_job(job_id):
        raise LookupError(f"unknown job ID: {job_id}")

    apsis = SimpleNamespace(
        jobs=SimpleNamespace(get_job=get_job),
        outputs=SimpleNamespace(get_metadata=lambda run_id: {}),
    )
    run = Run(Instance("deleted job", {}))

    assert snapshot_run(apsis, run).job is None
