import brotli
import gzip
import logging
import sanic
import zlib

from apsis.cond.dependency import Dependency
from apsis.runs import arg_to_bool, template_expand
from apsis.schedule import schedule_to_jso

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


def response_json(jso, status=200):
    return sanic.response.json(
        jso,
        status=status,
        indent=0,
        escape_forward_slashes=False,
    )


def error(message, status=400, **kw_args):
    return response_json({"error": str(message), **kw_args}, status=status)


def time_to_jso(time):
    return format(time, "%.3i")


def to_bool(string):
    if string in {"True", "true", "T", "t"}:
        return True
    elif string in {"False", "false", "F", "f"}:
        return False
    else:
        raise ValueError(f"unknown bool: {string}")


def decompress(data, compression) -> bytes:
    """
    Decompresses `data` assuming it is compressed with `compression`.
    """
    match compression:
        case "br":
            data = brotli.decompress(data)
        case "deflate":
            data = zlib.decompress(data)
        case "gzip":
            data = gzip.decompress(data)
        case None:
            pass
        case _:
            raise RuntimeError(f"can't decompress: {compression}")
    return data


def encode_response(headers, data, compression):
    """
    Encodes data for a response.

    :param headers:
      Request headers.
    :param data:
      Response data bytes.
    :param compression:
      Current compression of data.
    :return:
      Header dict for the response, and the payload data.
    """
    accept = headers.get("Accept-Encoding", "*")
    # Split fields, and drop quality values.
    accept = {p.strip().split(";")[0] for p in accept.split(",")}

    if "*" in accept or compression in accept:
        # The current compression is accepted.
        encoding = compression

    else:
        # Use identity, which is always implicitly acceptable.
        data = decompress(data, compression)
        encoding = "identity"

    return {"Content-Encoding": encoding}, data


# -------------------------------------------------------------------------------


def _to_jso(obj):
    return (
        None
        if obj is None
        else {
            **obj.to_jso(),
            "str": str(obj),
        }
    )


def _to_jsos(objs):
    return [] if objs is None else [_to_jso(o) for o in objs]


def job_to_jso(job, jobs=None):
    def sched_to_jso(s):
        jso = schedule_to_jso(s)
        jso["str"] = str(s) if s.stop_schedule is None else f"{s}, {s.stop_schedule}"
        jso["enabled"] = s.enabled
        return jso

    def _expand_value(val, sched_args):
        """Try Jinja2 expansion; on failure return the original template."""
        try:
            return template_expand(val, sched_args)
        except Exception:
            return str(val)

    def _resolve_dep_args(dep, resolved_job_id, sched_args):
        """Build a resolved args dict for a dependency.

        If the target job can be looked up, iterate over all its params:
        - Explicit args from the dependency are expanded with sched_args.
        - Inherited params (not in dep.args) use the sched_args value if
          available, otherwise show ``{{ param }}``.

        If the target job can't be looked up, fall back to expanding
        just the dependency's explicit args.
        """
        target_params = None
        if jobs is not None and resolved_job_id is not None:
            try:
                target_params = jobs.get_job(resolved_job_id).params
            except LookupError:
                pass

        resolved = {}
        if target_params is not None:
            for param in sorted(target_params):
                if param in dep.args:
                    resolved[param] = _expand_value(dep.args[param], sched_args)
                elif param in sched_args:
                    resolved[param] = sched_args[param]
                else:
                    resolved[param] = "{{ " + param + " }}"
        else:
            # Fallback: just expand the explicit args.
            for key, val in sorted(dep.args.items()):
                resolved[key] = _expand_value(val, sched_args)
        # Sort: concrete values first, then templates.
        resolved = dict(
            sorted(resolved.items(), key=lambda kv: (1 if "{{" in str(kv[1]) else 0, kv[0]))
        )
        return resolved

    # Collect unique schedule arg sets.
    sched_arg_sets = []
    for sched in job.schedules:
        if sched.args not in sched_arg_sets:
            sched_arg_sets.append(sched.args)

    def _eval_enabled(cond, sched_args):
        """Evaluate enabled for a condition given schedule args."""
        if cond.enabled is None:
            return True
        if isinstance(cond.enabled, bool):
            return cond.enabled
        try:
            result = _expand_value(cond.enabled, sched_args)
            return arg_to_bool(result)
        except Exception:
            return True  # If we can't evaluate, assume enabled.

    def _is_variable_dep(dep):
        """Does this dependency resolve differently across schedule arg sets?"""
        if "{{" in dep.job_id:
            return True
        if dep.enabled is not None and len(sched_arg_sets) > 1:
            # Check if enabled evaluates differently across arg sets.
            first = _eval_enabled(dep, sched_arg_sets[0])
            if any(_eval_enabled(dep, sa) != first for sa in sched_arg_sets[1:]):
                return True
        if len(sched_arg_sets) <= 1:
            return False
        # Check if the resolved args differ across schedule arg sets.
        first = _resolve_dep_args(dep, dep.job_id, sched_arg_sets[0])
        return any(_resolve_dep_args(dep, dep.job_id, sa) != first for sa in sched_arg_sets[1:])

    def _is_variable_non_dep(cond):
        """Does this non-dependency condition vary across schedule arg sets?"""
        if cond.enabled is None or isinstance(cond.enabled, bool):
            return False
        if len(sched_arg_sets) <= 1:
            return False
        first = _eval_enabled(cond, sched_arg_sets[0])
        return any(_eval_enabled(cond, sa) != first for sa in sched_arg_sets[1:])

    # Separate conditions into common (same for all arg sets) and variable.
    # Conditions unconditionally disabled (enabled=False) are omitted from
    # the schedule display — they appear only in the definitions list.
    common_conds = []
    variable_deps = []
    variable_non_deps = []
    for cond in job.conds:
        if cond.enabled is False:
            pass  # Unconditionally disabled; show only in Definitions.
        elif isinstance(cond, Dependency):
            if _is_variable_dep(cond):
                variable_deps.append(cond)
            else:
                common_conds.append(cond)
        else:
            if _is_variable_non_dep(cond):
                variable_non_deps.append(cond)
            else:
                common_conds.append(cond)

    # Resolve variable conditions, grouped by unique schedule args set.
    resolved_groups = []
    if variable_deps or variable_non_deps:
        for sched_args in sched_arg_sets:
            group_conds = []
            for dep in variable_deps:
                if not _eval_enabled(dep, sched_args):
                    continue
                entry = {**_to_jso(dep)}
                if "{{" in dep.job_id:
                    try:
                        resolved_job_id = template_expand(dep.job_id, sched_args)
                        entry["resolved_job_id"] = resolved_job_id
                    except NameError:
                        resolved_job_id = None
                        entry["resolved_job_id"] = None
                else:
                    resolved_job_id = dep.job_id
                    entry["resolved_job_id"] = dep.job_id
                entry["resolved_args"] = _resolve_dep_args(dep, resolved_job_id, sched_args)
                group_conds.append(entry)
            for cond in variable_non_deps:
                if not _eval_enabled(cond, sched_args):
                    continue
                entry = {**_to_jso(cond)}
                # Strip the [if ...] suffix — enabled is already evaluated.
                if cond.enabled is not None:
                    suffix = f" [if {cond.enabled}]"
                    if entry.get("str", "").endswith(suffix):
                        entry["str"] = entry["str"][: -len(suffix)]
                group_conds.append(entry)
            resolved_groups.append(
                {
                    "schedule_args": sched_args,
                    "conditions": group_conds,
                }
            )

    # Build common conditions JSO (using first sched arg set for resolving
    # dependency args — they're guaranteed to be the same across all sets).
    first_sched_args = sched_arg_sets[0] if sched_arg_sets else {}
    common_conds_jso = []
    for cond in common_conds:
        entry = {**_to_jso(cond)}
        if isinstance(cond, Dependency):
            resolved = _resolve_dep_args(cond, cond.job_id, first_sched_args)
            if resolved:
                entry["resolved_args"] = resolved
        common_conds_jso.append(entry)

    return {
        "job_id": job.job_id,
        "params": list(sorted(job.params)),
        "schedule": [sched_to_jso(s) for s in job.schedules],
        "program": _to_jso(job.program),
        "condition": [_to_jso(c) for c in job.conds],
        "common_conditions": common_conds_jso,
        "resolved_conditions": resolved_groups,
        "action": [_to_jso(a) for a in job.actions],
        "metadata": job.meta,
        "ad_hoc": job.ad_hoc,
    }


def run_to_summary_jso(run):
    jso = run._summary_jso_cache
    if jso is not None:
        # Use the cached JSO.
        return jso

    jso = {
        "job_id": run.inst.job_id,
        "args": run.inst.args,
        "run_id": run.run_id,
        "state": run.state.name,
        "times": {n: time_to_jso(t) for n, t in run.times.items()},
        "labels": run.meta.get("job", {}).get("labels", []),
    }
    if run.expected:
        jso["expected"] = run.expected

    if run.conds is not None:
        deps = [[c.job_id, c.args] for c in run.conds if isinstance(c, Dependency)]
        if len(deps) > 0:
            jso["dependencies"] = deps

    run._summary_jso_cache = jso
    return jso


def run_to_jso(app, run, summary=False):
    if run.state is None:
        # This run is being deleted.
        # FIXME: Hack.
        return {"run_id": run.run_id, "state": None}

    jso = run_to_summary_jso(run)

    if not summary:
        jso = {
            **jso,
            "conds": _to_jsos(run.conds),
            "actions": _to_jsos(run.actions),
            # FIXME: Rename to metadata.
            "meta": run.meta,
            "program": _to_jso(run.program),
        }

    return jso


# FIXME: Remove when.
def runs_to_jso(app, when, runs, summary=False):
    return {
        "when": time_to_jso(when),
        "runs": {r.run_id: run_to_jso(app, r, summary) for r in runs},
    }


def run_log_record_to_jso(rec):
    return {
        "timestamp": time_to_jso(rec["timestamp"]),
        "message": rec["message"],
    }


def run_log_to_jso(recs):
    return [run_log_record_to_jso(r) for r in recs]


# FIXME: Get rid of this and the whole endpoint, which is silly.
def output_metadata_to_jso(app, run_id, outputs):
    return [
        {
            "output_id": output_id,
            "output_len": output.length,
        }
        for output_id, output in outputs.items()
    ]


def output_to_http_message(output, *, interval=(0, None)) -> bytes:
    length = output.metadata.length
    start, stop = interval
    if stop is None:
        stop = length
    if not (start <= stop):
        raise ValueError("stop before start")
    if output.compression is not None:
        raise ValueError("output is compressed")

    return (
        "\r\n".join(
            [
                f"Content-Type: {output.metadata.content_type}",
                # f"Content-Encoding: {output.compression}",
                f"Content-Range: bytes={start}-{stop - 1}/{length}",
                f"Content-Length: {str(stop - start)}",
                "",
                "",
            ]
        ).encode("ascii")
        + output.data[start:stop]
    )
