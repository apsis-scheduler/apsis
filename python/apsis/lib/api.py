import brotli
import gzip
import logging
import sanic
import zlib

from apsis.cond.dependency import Dependency
from apsis.runs import BIND_ARGS, eval_enabled, is_template, template_expand
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


def job_to_jso(job, jobs=None) -> dict:
    """
    Serializes a job for the API / web UI.

    In addition to the raw job fields (returned under `condition`),
    resolves each condition against each schedule's args to classify it:

    - `common_conditions`: conditions whose resolution is identical across
      all schedules (shown once in the UI).
    - `resolved_conditions`: one group per schedule, containing conditions
      that vary (e.g. a templated `job_id` that resolves differently per
      schedule, or an `enabled` template that is true for some schedules
      and false for others).

    Conditions with `enabled=False` are omitted from both lists (they
    remain in the raw `condition` definitions).

    When *jobs* is provided, dependency target jobs are looked up via
    `jobs.get_job()` (O(1) dict lookup) to discover their params, so
    that inherited args (not explicitly listed in the dependency but
    present on the target job) can be filled in.

    Complexity per job: O(C * S * K * log K), where C = conditions,
    S = schedules, K = number of resolved dependency args (typically
    the number of params on the target job).  The log K factor comes
    from sorting resolved args.  In practice all three are small
    (single digits), so the per-job cost is negligible.
    """

    def sched_to_jso(s):
        jso = schedule_to_jso(s)
        jso["str"] = str(s) if s.stop_schedule is None else f"{s}, {s.stop_schedule}"
        jso["enabled"] = s.enabled
        return jso

    def _expand_value(val, sched_args):
        """Try Jinja2 expansion; on failure return the original template."""
        try:
            return template_expand(val, {**BIND_ARGS, **sched_args})
        except (NameError, SyntaxError):
            return str(val)

    def _resolve_dep(dep, sched_args):
        """Resolve a dependency's job_id and args for a given schedule arg set."""
        if is_template(dep.job_id):
            try:
                resolved_job_id = template_expand(dep.job_id, {**BIND_ARGS, **sched_args})
            except (NameError, SyntaxError):
                resolved_job_id = None
        else:
            resolved_job_id = dep.job_id

        target_params = None
        if jobs is not None and resolved_job_id is not None:
            try:
                target_params = jobs.get_job(resolved_job_id).params
            except LookupError:
                pass

        if target_params is not None:
            resolved_args = {}
            for param in target_params:
                if param in dep.args:
                    resolved_args[param] = _expand_value(dep.args[param], sched_args)
                elif param in sched_args:
                    resolved_args[param] = sched_args[param]
                else:
                    resolved_args[param] = "{{ " + param + " }}"
        else:
            resolved_args = {k: _expand_value(v, sched_args) for k, v in dep.args.items()}

        resolved_args = dict(
            sorted(resolved_args.items(), key=lambda kv: (is_template(kv[1]), kv[0]))
        )
        return {"resolved_job_id": resolved_job_id, "resolved_args": resolved_args}

    def _resolve_cond(cond, sched_args):
        """Build a resolved JSO entry for a condition."""
        entry = {**_to_jso(cond)}
        if isinstance(cond, Dependency):
            entry.update(_resolve_dep(cond, sched_args))
        return entry

    # One resolved group per schedule; "common" means identical across all.
    schedules = job.schedules

    # Conditions unconditionally disabled (enabled=False) are omitted from
    # the schedule display — they appear only in the definitions list.
    common_conds_jso = []
    resolved_groups = [{"schedule_args": s.args, "conditions": []} for s in schedules]

    for cond in job.conds:
        if cond.enabled is False:
            continue

        if not schedules:
            # No schedules: resolve non-template conditions with empty args;
            # skip template deps that can't be resolved.
            if isinstance(cond, Dependency) and is_template(cond.job_id):
                continue
            common_conds_jso.append(_resolve_cond(cond, {}))
            continue

        # Resolve for every schedule, tracking enabled status.
        entries = []
        enabled_flags = []
        for s in schedules:
            try:
                enabled = eval_enabled(cond.enabled, {**BIND_ARGS, **s.args})
            except (NameError, SyntaxError):
                # Template references an arg not in schedule args (e.g.
                # a date param generated at scheduling time) — can't
                # determine, so treat as enabled.
                enabled = True
            enabled_flags.append(enabled)
            entries.append(_resolve_cond(cond, s.args) if enabled else None)

        # If all entries are identical and all enabled → common.
        if all(enabled_flags) and all(e == entries[0] for e in entries[1:]):
            common_conds_jso.append(entries[0])
        else:
            for i, entry in enumerate(entries):
                if entry is not None:
                    resolved_groups[i]["conditions"].append(entry)

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
