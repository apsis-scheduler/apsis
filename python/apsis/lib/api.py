import brotli
import gzip
import logging
import sanic
import zlib

from apsis.cond.dependency import Dependency
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


def job_to_jso(job):
    def sched_to_jso(s):
        jso = schedule_to_jso(s)
        jso["str"] = str(s) if s.stop_schedule is None else f"{s}, {s.stop_schedule}"
        jso["enabled"] = s.enabled
        return jso

    return {
        "job_id": job.job_id,
        "params": list(sorted(job.params)),
        "schedule": [sched_to_jso(s) for s in job.schedules],
        "program": _to_jso(job.program),
        "condition": [_to_jso(c) for c in job.conds],
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
