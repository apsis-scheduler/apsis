import logging

from .base import BaseAction
from .condition import Condition
from apsis.lib.json import check_schema
from apsis.runs import template_expand, Instance
from apsis.states import State

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class ScheduleAction(BaseAction):
    """
    Schedules a new run.
    """

    def __init__(self, instance, *, condition=None):
        super().__init__(condition=condition)
        self.job_id = instance.job_id
        self.args = instance.args

    async def __call__(self, apsis, run):
        if self.condition is not None and not self.condition(run):
            return

        # Expand args templates.
        args = {n: template_expand(v, run.inst.args) for n, v in self.args.items()}
        inst = Instance(self.job_id, args)
        # Propagate missing args.
        inst = apsis._propagate_args(run.inst.args, inst)

        log.info(f"action for {run.run_id}: scheduling {inst}")
        new_run = await apsis.schedule(None, inst)
        log.info(f"action for {run.run_id}: scheduled {new_run.run_id}")

        apsis.run_log.info(run, f"action scheduling {inst} as {new_run.run_id}")
        apsis.run_log.info(new_run, f"scheduled by action of {run.run_id}")

    def to_jso(self):
        return {
            **super().to_jso(),
            "job_id": self.job_id,
            "args": self.args,
            "if": self.condition.to_jso(),
        }

    @classmethod
    def from_jso(cls, jso):
        with check_schema(jso) as pop:
            condition = pop("if", Condition.from_jso, None)
            job_id = pop("job_id")
            args = pop("args", default={})
        return cls(Instance(job_id, args), condition=condition)


def successor_from_jso(jso):
    """
    Convert successors from JSO.

    Successors are syntactic sugar for and are converted into `ScheduleAction`
    records.
    """
    if isinstance(jso, str):
        jso = {"job_id": jso}

    with check_schema(jso) as pop:
        job_id = pop("job_id")
        args = pop("args", default={})
    return ScheduleAction(Instance(job_id, args), condition=Condition(states=[State.success]))
