from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .cond import Condition
from .jobs import Job
from .program import Program, Output
from .runs import Instance
from .states import State
from apsis.lib import py

# -------------------------------------------------------------------------------


@dataclass
class RunSnapshot:
    """
    A snapshot of the state of a run; not updated on transitions.
    """

    run_id: str
    inst: Instance
    state: State
    job: Job
    conds: Sequence[Condition]
    program: Program
    meta: Mapping[str, object]
    outputs: Mapping[str, Output]

    def __repr__(self):
        return py.format_ctor(self, self.run_id, self.inst, state=self.state)

    @property
    def labels(self):
        return self.meta.get("job", {}).get("labels", [])


def snapshot_run(apsis, run):
    # Get the job, if available.
    try:
        job = apsis.jobs.get_job(run.inst.job_id)
    except KeyError:
        job = None

    # Load all outputs.
    outputs = {
        oi: apsis.outputs.get_output(run.run_id, oi)
        for oi in apsis.outputs.get_metadata(run.run_id)
    }

    snapshot = RunSnapshot(
        run_id=run.run_id,
        inst=run.inst,
        state=run.state,
        job=job,
        conds=run.conds,
        program=run.program,
        meta=run.meta.copy(),
        outputs=outputs,
    )
    # FIXME: expected isn't part of the API, but we need it for now so that the
    # run log can log messages from the snapshot.
    snapshot.expected = run.expected
    return snapshot
