from collections import defaultdict, deque
from collections.abc import Iterable
import re

import ora

from apsis.cond.dependency import Dependency
from apsis.jobs import JOB_RUNS_SUFFIX, Job, Jobs, JobsDir
from apsis.runs import (
    BIND_ARGS,
    Instance,
    Run,
    is_template,
    validate_args,
    bind,
    template_expand,
)
from apsis.scheduler import get_insts_to_schedule

# -------------------------------------------------------------------------------

# A param name is a Jinja2 variable in a template expansion, part of an environment
# variable name, and `NAME=VALUE` on the `apsis schedule` command line, so it has to
# be an identifier.
PARAM_NAME_REGEX = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
PARAM_NAME_DESCRIPTION = "a letter or underscore followed by letters, digits, and underscores"

# Identifiers a template expansion doesn't read as the param's arg: Jinja2 constants
# and an operator, plus `self` and `loop`, which its runtime binds.  `{{ None }}`
# expands to "None" whatever the arg says.
PARAM_NAME_NOT_EXPANDABLE = frozenset(
    {"False", "None", "True", "false", "loop", "none", "not", "self", "true"}
)

# Names Apsis provides to template expansion, which a param's arg would shadow; see
# `runs.get_bind_args()`.  A param named `format` makes `{{ format(...) }}` elsewhere
# in the job fail, and one named `run_id` silently displaces the run's ID.
PARAM_NAME_PROVIDED = frozenset({"job_id", "run_id", *BIND_ARGS})

# job id and args key of an instance
Node = tuple[str, tuple[tuple[str, str], ...]]


def check_param_names(params):
    """
    Checks that `params` are all usable as param names.

    :return:
      Generator of errors.
    """

    def show(names):
        return ", ".join(repr(n) for n in sorted(names))

    invalid = [p for p in params if PARAM_NAME_REGEX.fullmatch(p) is None]
    if len(invalid) > 0:
        yield f"invalid param names ({show(invalid)}): must be {PARAM_NAME_DESCRIPTION}"

    reserved = [p for p in params if p in PARAM_NAME_NOT_EXPANDABLE]
    if len(reserved) > 0:
        yield f"invalid param names ({show(reserved)}): reserved by template expansion"

    provided = [p for p in params if p in PARAM_NAME_PROVIDED]
    if len(provided) > 0:
        yield f"invalid param names ({show(provided)}): provided to template expansion by Apsis"


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
    # `GET /jobs/<job_id>/runs` is the run history URL, so a job ID ending in
    # that suffix would be indistinguishable from it; see `api.job()`.
    if job.job_id.endswith(JOB_RUNS_SUFFIX):
        yield f"job ID must not end in {JOB_RUNS_SUFFIX!r} (reserved for run history URLs)"

    yield from check_param_names(job.params)

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


def _has_dependencies(job: Job) -> bool:
    return any(isinstance(c, Dependency) for c in job.conds)


def _node_inst(node: Node) -> Instance:
    job_id, args_k = node
    return Instance(job_id, dict(args_k))


def _get_dependency_job_ids(job: Job) -> set[str] | None:
    """Returns the ids of jobs that dependency conditions of `job` name.

    Ids are expanded as when binding a run, with the job id and the args of
    each schedule that are params other than `schedule_time`, or with no args
    if the job has no schedules.  Returns `None` if one of them uses an arg
    that only a run has, like `date`.
    """
    job_ids = set()
    for schedule_args in [s.args for s in job.schedules] or [{}]:
        args = {
            a: str(v) for a, v in schedule_args.items() if a in job.params and a != "schedule_time"
        }
        bind_args = {**BIND_ARGS, "job_id": job.job_id, **args}
        for cond in job.conds:
            if not isinstance(cond, Dependency):
                continue
            try:
                job_ids.add(template_expand(cond.job_id, bind_args))
            except NameError:
                return None
    return job_ids


def _get_job_graph(jobs_dir: JobsDir) -> dict[str, set[str] | None]:
    """Returns the ids of jobs each job in `jobs_dir` depends on.

    A job maps to `None` if it may depend on any job.
    """
    return {job.job_id: _get_dependency_job_ids(job) for job in jobs_dir.get_jobs()}


def _get_upstream_job_ids(
    job_graph: dict[str, set[str] | None], job_ids: Iterable[str]
) -> set[str] | None:
    """Returns `job_ids` and the ids of jobs they transitively depend on.

    Returns `None` if that may be any job.
    """
    upstream = set()
    queue = list(job_ids)
    while queue:
        job_id = queue.pop()
        if job_id in upstream:
            continue
        upstream.add(job_id)
        dep_ids = job_graph.get(job_id, set())
        if dep_ids is None:
            return None
        queue.extend(dep_ids)
    return upstream


def get_downstream_job_ids(jobs_dir: JobsDir, job_ids: Iterable[str]) -> set[str]:
    """
    Returns ids of jobs in `jobs_dir` that are in `job_ids` or transitively
    depend on any of them.

    `job_ids` may include ids of jobs not in `jobs_dir`, such as deleted jobs,
    whose dependents are still included.  A job whose templated dependency ids
    fail to expand is treated as depending on every job.
    """
    job_graph = _get_job_graph(jobs_dir)
    dependents = defaultdict(set)
    any_dependents = []
    for job_id, dep_ids in job_graph.items():
        if dep_ids is None:
            any_dependents.append(job_id)
        else:
            for dep_id in dep_ids:
                dependents[dep_id].add(job_id)

    downstream = set()
    queue = list(job_ids)
    if queue:
        queue.extend(any_dependents)
    while queue:
        job_id = queue.pop()
        if job_id not in downstream:
            downstream.add(job_id)
            queue.extend(dependents[job_id])
    return downstream & job_graph.keys()


def _build_instances(jobs, start, stop):
    """Build all job instances with their earliest schedule time.

    Returns:
        (inst_times, job_insts) where:
        - inst_times: (job_id, args_key) -> earliest sched_time
        - job_insts: job_id -> ([sched_time], [(job_id, args_key)]) in
          schedule order, for jobs with dependencies
    """
    inst_times = {}
    job_insts = {}
    for job in jobs:
        has_deps = _has_dependencies(job)
        if has_deps:
            times, nodes = job_insts[job.job_id] = ([], [])
        for sched_time, _, inst in get_insts_to_schedule(job, start, stop):
            key = (job.job_id, args_key(inst.args))
            if key not in inst_times or sched_time < inst_times[key]:
                inst_times[key] = sched_time
            if has_deps:
                times.append(sched_time)
                nodes.append(key)
    return inst_times, job_insts


def _bind_dependencies(job: Job, node: Node, jobs_obj: Jobs) -> list[Node]:
    """Bind the enabled dependencies of `job` for the run of `node`.

    Returns:
        dependency nodes, in condition order
    """
    run = Run(_node_inst(node))
    dep_nodes = []
    for cond in job.conds:
        if not isinstance(cond, Dependency):
            continue
        bound = cond.bind(run, jobs_obj)
        if bound:
            dep_nodes.append((bound.job_id, args_key(bound.args)))
    return dep_nodes


def _build_dep_graph(jobs_dir, jobs_obj, inst_times):
    """Build forward and reverse dependency edges for known instances.

    Returns:
        (graph, deps_of, missing) where:
        - graph: dep_node -> [dependent_nodes]  (forward edges)
        - deps_of: node -> [its dependency nodes]  (reverse edges)
        - missing: node -> [its dependency nodes not in inst_times]
    """
    graph = defaultdict(list)
    deps_of = defaultdict(list)
    missing = defaultdict(list)

    for node in inst_times:
        for dep_node in _bind_dependencies(jobs_dir.get_job(node[0]), node, jobs_obj):
            if dep_node in inst_times:
                graph[dep_node].append(node)
                deps_of[node].append(dep_node)
            else:
                missing[node].append(dep_node)

    return graph, deps_of, missing


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
        time_str = _format_time(exp_start[dep])
        lines.append(
            f"{pad}- dependency: {_node_inst(dep)} not expected to"
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

    Runs are listed only for the given jobs with dependencies and the jobs
    upstream of them.  Upstream jobs that aren't given aren't checked, apart
    from cycle reporting.

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

    # list runs for the given jobs with dependencies and every job upstream of them
    job_graph = _get_job_graph(jobs_dir)
    list_ids = _get_upstream_job_ids(job_graph, (j.job_id for j in jobs if _has_dependencies(j)))
    if list_ids is None:
        list_ids = job_graph.keys()
    upstream_ids = list_ids - {j.job_id for j in jobs}
    list_jobs = [j for j in jobs if j.job_id in list_ids]
    list_jobs += [j for j in jobs_dir.get_jobs() if j.job_id in upstream_ids]

    # Build the dependency graph and propagate expected start times.
    inst_times, job_insts = _build_instances(list_jobs, dep_start, dep_stop)
    graph, deps_of, missing = _build_dep_graph(jobs_dir, jobs_obj, inst_times)
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
    sched_in_dep = dep_start <= sched_start and sched_stop <= dep_stop
    for job in jobs:
        if not _has_dependencies(job):
            continue

        if sched_in_dep:
            insts = (
                (sched_time, node)
                for sched_time, node in zip(*job_insts[job.job_id])
                if sched_start <= sched_time < sched_stop
            )
        else:
            insts = (
                (sched_time, (job.job_id, args_key(inst.args)))
                for sched_time, _, inst in get_insts_to_schedule(job, sched_start, sched_stop)
            )

        timing_reported = False

        for sched_time, inst_node in insts:
            # check for unscheduled dependencies, binding runs outside the dep window
            if inst_node in inst_times:
                dep_nodes = missing.get(inst_node, ())
            else:
                dep_nodes = [
                    d for d in _bind_dependencies(job, inst_node, jobs_obj) if d not in inst_times
                ]
            for dep_node in dep_nodes:
                yield (
                    job,
                    f"scheduled run {_node_inst(inst_node)}:"
                    f" dependency {_node_inst(dep_node)} not scheduled",
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
                f"{_node_inst(inst_node)} not expected to start until {exp_str},"
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
