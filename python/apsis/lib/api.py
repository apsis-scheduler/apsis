import brotli
import gzip
import logging
import sanic
import zlib
import jinja2 as _j2

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

    def _display_expand(template, args):
        """Best-effort Jinja2 expansion for display purposes.

        Substitutes known variables while rendering unknown ones as
        ``{{ name }}`` placeholders.  Supports attribute access, function
        calls, and arithmetic on unknown values so that complex expressions
        like ``{{ get_calendar("strat/" ~ strat).before(Date(date) - 1) }}``
        partially resolve (e.g. to
        ``{{ get_calendar("strat/eu").before(Date(date) - 1) }}``).
        """

        class _Passthrough(_j2.Undefined):
            """An Undefined that is chainable and renders as {{ name }}."""

            def __str__(self):
                return "{{ " + self._undefined_name + " }}"

            def __getattr__(self, name):
                if name.startswith("_"):
                    raise AttributeError(name)
                return type(self)(name=f"{self._undefined_name}.{name}")

            def __call__(self, *a, **kw):
                parts = []
                for v in a:
                    if isinstance(v, _j2.Undefined):
                        parts.append(v._undefined_name)
                    else:
                        parts.append(repr(v))
                for k, v in kw.items():
                    rhs = v._undefined_name if isinstance(v, _j2.Undefined) else repr(v)
                    parts.append(f"{k}={rhs}")
                return type(self)(
                    name=f"{self._undefined_name}({', '.join(parts)})"
                )

            # Arithmetic — keep as symbolic expression.
            def __sub__(self, other):
                return type(self)(name=f"{self._undefined_name} - {other}")

            def __rsub__(self, other):
                return type(self)(name=f"{other} - {self._undefined_name}")

            def __add__(self, other):
                return type(self)(name=f"{self._undefined_name} + {other}")

            def __radd__(self, other):
                return type(self)(name=f"{other} + {self._undefined_name}")

        env = _j2.Environment(undefined=_Passthrough)
        try:
            tmpl = env.from_string(str(template))
            return tmpl.render(args)
        except Exception:
            return str(template)

    def _expand_value(val, sched_args):
        """Try a full Jinja2 expansion; on failure fall back to display."""
        try:
            return template_expand(val, sched_args)
        except Exception:
            return _display_expand(val, sched_args)

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
        return resolved

    # Collect unique schedule arg sets.
    sched_arg_sets = []
    for sched in job.schedules:
        if sched.args not in sched_arg_sets:
            sched_arg_sets.append(sched.args)

    def _eval_enabled(dep, sched_args):
        """Evaluate enabled for a dependency given schedule args."""
        if dep.enabled is None:
            return True
        if isinstance(dep.enabled, bool):
            return dep.enabled
        try:
            result = _expand_value(dep.enabled, sched_args)
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
        return any(
            _resolve_dep_args(dep, dep.job_id, sa) != first
            for sa in sched_arg_sets[1:]
        )

    # Separate dependency conditions into common (same for all arg sets) and
    # variable.  Non-dependency conditions are omitted here — they appear only
    # in the "condition" (Definitions) list.  Dependencies unconditionally
    # disabled (enabled=False) are also omitted.
    common_conds = []
    variable_deps = []
    for cond in job.conds:
        if isinstance(cond, Dependency):
            if cond.enabled is False:
                pass  # Unconditionally disabled; show only in Definitions.
            elif _is_variable_dep(cond):
                variable_deps.append(cond)
            else:
                common_conds.append(cond)

    # Resolve variable dependencies, grouped by unique schedule args set.
    resolved_groups = []
    if variable_deps:
        for sched_args in sched_arg_sets:
            group_conds = []
            for dep in variable_deps:
                if not _eval_enabled(dep, sched_args):
                    continue
                entry = {**_to_jso(dep)}
                if "{{" in dep.job_id:
                    try:
                        resolved_job_id = template_expand(
                            dep.job_id, sched_args
                        )
                        entry["resolved_job_id"] = resolved_job_id
                    except NameError:
                        resolved_job_id = None
                        entry["resolved_job_id"] = None
                else:
                    resolved_job_id = dep.job_id
                    entry["resolved_job_id"] = dep.job_id
                entry["resolved_args"] = _resolve_dep_args(
                    dep, resolved_job_id, sched_args
                )
                group_conds.append(entry)
            resolved_groups.append({
                "schedule_args": sched_args,
                "conditions": group_conds,
            })

    # Build common conditions JSO with resolved args (using first sched
    # arg set — they're guaranteed to be the same across all sets).
    first_sched_args = sched_arg_sets[0] if sched_arg_sets else {}
    common_conds_jso = []
    for cond in common_conds:
        if isinstance(cond, Dependency):
            entry = {**_to_jso(cond)}
            resolved = _resolve_dep_args(
                cond, cond.job_id, first_sched_args
            )
            if resolved:
                entry["resolved_args"] = resolved
            common_conds_jso.append(entry)
        else:
            common_conds_jso.append(_to_jso(cond))

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
