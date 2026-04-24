from collections import defaultdict, deque

import ora

from apsis.cond.dependency import Dependency
from apsis.jobs import Jobs
from apsis.runs import Instance, Run, is_template, validate_args, bind
from apsis.scheduler import get_insts_to_schedule

# -------------------------------------------------------------------------------


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
        args = {a: str(v) for a, v in args.items() if a in job.params}
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

            # Template job_ids can't be validated without schedule args.
            if is_template(associated_job_id):
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


def args_key(args):
    """Stable, hashable key for args."""
    return tuple(sorted(args.items()))


def _format_time(t):
    return ora.format_time("%Y-%m-%dT%H:%M:%SZ", t, "UTC")


def _build_instances(jobs, start, stop):
    """Build all job instances with their earliest schedule time.

    Returns:
        dict of (job_id, args_key) -> earliest sched_time
    """
    inst_times = {}
    for job in jobs:
        for sched_time, _, inst in get_insts_to_schedule(job, start, stop):
            key = (job.job_id, args_key(inst.args))
            if key not in inst_times or sched_time < inst_times[key]:
                inst_times[key] = sched_time
    return inst_times


def _build_dep_graph(jobs_dir, jobs_obj, inst_times):
    """Build forward and reverse dependency edges for known instances.

    Returns:
        (graph, deps_of) where:
        - graph: dep_node -> [dependent_nodes]  (forward edges)
        - deps_of: node -> [its dependency nodes]  (reverse edges)
    """
    graph = defaultdict(list)
    deps_of = defaultdict(list)

    for job_id, args_k in inst_times:
        job = jobs_dir.get_job(job_id)
        inst = Instance(job_id, dict(args_k))
        run = Run(inst)

        for cond in job.conds:
            if not isinstance(cond, Dependency):
                continue
            bound = cond.bind(run, jobs_obj)
            if not bound:
                continue
            dep_node = (bound.job_id, args_key(bound.args))
            if dep_node in inst_times:
                graph[dep_node].append((job_id, args_k))
                deps_of[(job_id, args_k)].append(dep_node)

    return graph, deps_of


def _propagate_expected_starts(inst_times, graph):
    """Propagate expected start times via topological traversal (Kahn’s algorithm).

    The expected start of a node is the max of its own schedule time and
    those of its transitive dependencies.  Does not account for run durations.

    Returns:
        (exp_start, cycle_nodes) where cycle_nodes is empty if no cycles.
    """
    indegree = {node: 0 for node in inst_times}
    for dependents in graph.values():
        for node in dependents:
            indegree[node] += 1

    exp_start = dict(inst_times)
    queue = deque(node for node in inst_times if indegree[node] == 0)

    while queue:
        node = queue.popleft()
        for nxt in graph[node]:
            if exp_start[node] > exp_start[nxt]:
                exp_start[nxt] = exp_start[node]
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)

    cycle_nodes = {node for node in inst_times if indegree[node] > 0}
    return exp_start, cycle_nodes


def _find_cycle(cycle_nodes, deps_of):
    """Extract one representative cycle path from the remaining nodes.

    Walks deps_of from an arbitrary node until revisiting one, then returns
    the cycle as "A → B → C → A".  Not guaranteed to find the shortest cycle.
    """
    start = next(iter(cycle_nodes))
    visited = {}
    node = start
    path = []

    while node not in visited:
        visited[node] = len(path)
        path.append(node)
        node = next(
            (d for d in deps_of.get(node, []) if d in cycle_nodes),
            None,
        )
        if node is None:
            break

    if node is not None and node in visited:
        cycle = path[visited[node] :] + [node]
        return " → ".join(job_id for job_id, _ in cycle)
    return ", ".join(job_id for job_id, _ in list(cycle_nodes)[:5])


def _build_delay_tree(node, deps_of, exp_start, inst_times, indent=0, seen=None):
    """Build indented lines explaining why `node` is delayed.

    Only includes dependencies whose expected start actually pushes this
    node later than its own schedule time.
    """
    if seen is None:
        seen = set()
    if node in seen:
        return []
    seen.add(node)

    blocking = [d for d in deps_of.get(node, []) if exp_start[d] > inst_times[node]]
    pad = "  " * indent

    if not blocking:
        time_str = _format_time(inst_times[node])
        return [f"{pad}- It’s scheduled to start at {time_str}."]

    lines = []
    for dep in blocking:
        dep_inst = Instance(dep[0], dict(dep[1]))
        time_str = _format_time(exp_start[dep])
        lines.append(
            f"{pad}- dependency: {dep_inst} not expected to"
            f" start until {time_str}."
            f" This is because:"
        )
        lines.extend(_build_delay_tree(dep, deps_of, exp_start, inst_times, indent + 1, seen))
    return lines


def check_job_dependencies_scheduled(
    jobs_dir,
    jobs,
    *,
    sched_times=None,
    dep_times=None,
    max_wait_time=None,
):
    """
    Validate that job dependencies are scheduled and respect timing constraints.

    For each scheduled run of the given jobs within `sched_times`, this checks:
    - A matching scheduled run exists for each dependency within `dep_times`
    - The dependency is not scheduled too late relative to the job run

    If `max_wait_time` is provided, emits an error if dependency delay exceeds it.

    Yields:
        (job, message)
    """

    now = ora.now()
    sched_start, sched_stop = sched_times or (now, now + 86400)
    dep_start, dep_stop = dep_times or (now - 86400, now + 86400)

    jobs_obj = Jobs(jobs_dir, MockJobDb())

    # Build the dependency graph and propagate expected start times.
    inst_times = _build_instances(jobs, dep_start, dep_stop)
    graph, deps_of = _build_dep_graph(jobs_dir, jobs_obj, inst_times)
    exp_start, cycle_nodes = _propagate_expected_starts(inst_times, graph)

    # Report cycles.
    if cycle_nodes:
        cycle_str = _find_cycle(cycle_nodes, deps_of)
        for job_id in sorted({jid for jid, _ in cycle_nodes}):
            yield (
                jobs_dir.get_job(job_id),
                f"dependency cycle detected: {cycle_str}",
            )

    # Validate each job’s instances.
    for job in jobs:
        dep_conds = [c for c in job.conds if isinstance(c, Dependency)]
        if not dep_conds:
            continue

        timing_reported = False

        for sched_time, _, inst in get_insts_to_schedule(job, sched_start, sched_stop):
            run = Run(inst)
            inst_node = (job.job_id, args_key(inst.args))

            # Check for unscheduled dependencies.
            for dep in dep_conds:
                bound = dep.bind(run, jobs_obj)
                if not bound:
                    continue
                dep_node = (bound.job_id, args_key(bound.args))
                if dep_node not in inst_times:
                    dep_inst = Instance(bound.job_id, bound.args)
                    yield (
                        job,
                        f"scheduled run {inst}: dependency {dep_inst} not scheduled",
                    )

            # Timing check: only report once per job (first violating instance).
            # Skip cycle nodes — their exp_start is invalid.
            if (
                max_wait_time is None
                or timing_reported
                or inst_node not in exp_start
                or inst_node in cycle_nodes
            ):
                continue

            wait = exp_start[inst_node] - sched_time
            if wait <= max_wait_time:
                continue

            exp_str = _format_time(exp_start[inst_node])
            sched_str = _format_time(sched_time)
            wait_h = wait / 3600
            max_h = max_wait_time / 3600
            header = (
                f"{inst} not expected to start until {exp_str},"
                f" {wait_h:.0f}h after scheduled time {sched_str}"
                f" (max_wait={max_h:.0f}h)."
                f" This is because:"
            )
            tree_lines = _build_delay_tree(
                inst_node,
                deps_of,
                exp_start,
                inst_times,
            )
            yield (job, header + "\n" + "\n".join(tree_lines))
            timing_reported = True
