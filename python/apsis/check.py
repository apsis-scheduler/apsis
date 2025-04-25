import ora

from   apsis.cond.dependency import Dependency
from   apsis.jobs import Jobs
from   apsis.runs import Instance, Run, validate_args, bind
from   apsis.scheduler import get_insts_to_schedule

#-------------------------------------------------------------------------------

# FIXME: Use normal protocols for this, not random APIs that need mocks.
class MockJobDb:

    def get(self, job_id):
        raise LookupError(job_id)



def check_job(jobs_dir, job):
    """
    Performs consistency checks on `job` in `jobs_dir`.

    :return:
      Generator of errors.
    """
    # Try scheduling a run for each schedule of each job.  This tests that
    # template expansions work, that all names and params are bound, and that
    # actions and conditions refer to valid jobs with correct args.
    now = ora.now()
    jobs = Jobs(jobs_dir, MockJobDb())
    checked_run = False
    for schedule in job.schedules:
        try:
            _, args = next(schedule(now))
        except StopIteration:
            continue
        args = { a: str(v) for a, v in args.items() if a in job.params }
        run = Run(Instance(job.job_id, args))
        checked_run = True
        try:
            validate_args(run, job.params)
            bind(run, job, jobs)
        except Exception as exc:
            yield str(exc)
            continue

    if not checked_run:
        def check_associated_run(obj, context):
            try:
                associated_job_id = obj.job_id
            except AttributeError:
                # No associated job; that's OK.
                return

            # Find the associated job.
            try:
                associated_job = jobs_dir.get_job(associated_job_id)
            except LookupError:
                yield f"unknown job ID in {context}: {associated_job_id}"
                return

            # Look up additional args, if any.
            try:
                args = set(obj.args)
            except AttributeError:
                args = set()

            params = set(associated_job.params)
            # Check for missing args.  The params of the associated job can be bound
            # either to the args of this job or to explicit args.
            for missing in params - set(job.params) - args:
                yield f"missing arg in {context}: param {missing} of job {associated_job_id}"
            # Check for extraneous explicit args.
            for extra in args - params:
                yield f"extraneous arg in {context}: param {extra} of job {associated_job_id}"

        # We couldn't construct a run to check.  At least confirm that any
        # action or condition that refers to another job is valid.
        for action in job.actions:
            yield from check_associated_run(action, "action")
        for cond in job.conds:
            yield from check_associated_run(cond, "condition")


def check_job_dependencies_scheduled(
        jobs_dir,
        job,
        *,
        sched_times=None,
        dep_times=None,
):
    """
    Attempts to check that dependencies are scheduled.

    For each run of `job` scheduled within `sched_times`, looks for
    dependencies.  For each dependency, checks that there is a matching run of
    that job scheduled in `dep_times`.

    :param sched_times:
      (start, stop) time interval for scheduled runs of `job` to consider.  If
      none, uses (now, now + 1 day).
    :param dep_times:
      (start, stop) time interval in which to look for scheduled runs of
      dependencies.  If none, uses (now - 1 day, now + 1 day).
    :return:
      Generator of errors indicating apparently unscheduled dependencies.
    """
    deps = [ c for c in job.conds if isinstance(c, Dependency) ]
    if len(deps) == 0:
        return

    jobs = Jobs(jobs_dir, MockJobDb())
    time = ora.now()

    sched_start, sched_stop = (
        sched_times if sched_times is not None
        else (time, time + 86400)
    )
    dep_start, dep_stop = (
        dep_times if dep_times is not None
        else (time - 86400, time + 86400)
    )
    dep_ivl = f"[{dep_start}, {dep_stop})"

    # Cache for dependent job schedules: job_id -> set of frozenset(argdict.items())
    dep_insts = {}
    def get_dep_insts(job):
        try:
            return dep_insts[job.job_id]
        except KeyError:
            insts = list(get_insts_to_schedule(job, dep_start, dep_stop))
            dep_insts[job.job_id] = {
                frozenset(inst.args.items()) for _, _, inst in insts
            }
            return dep_insts[job.job_id]

    # Construct all instances that will be scheduled soon.
    insts = get_insts_to_schedule(job, sched_start, sched_stop)

    for _, _, inst in insts:
        run = Run(inst)
        # Check each dependency.
        for dep in deps:
            # Bind the dependency to get the precise args that match.
            dep = dep.bind(run, jobs)

            # Look at scheduled runs of the dependency job.  Check if any
            # matches the dependency args.
            dep_job = jobs_dir.get_job(dep.job_id)
            if frozenset(dep.args.items()) not in get_dep_insts(dep_job):
                # No matches.
                dep_inst = Instance(dep.job_id, dep.args)
                yield (
                    f"scheduled run {inst}: "
                    f"dependency {dep_inst} not scheduled in {dep_ivl}"
                )


