import asyncio
import itertools
import logging
import ora
import re
import sanic
import time
import ujson
from urllib.parse import unquote, parse_qs
import websockets

from . import messages
from apsis import procstar
from apsis.lib import asyn
from apsis.lib.api import (
    response_json,
    error,
    time_to_jso,
    to_bool,
    encode_response,
    runs_to_jso,
    run_to_summary_jso,
    job_to_jso,
    output_metadata_to_jso,
    run_log_to_jso,
    output_to_http_message,
)
import apsis.lib.itr
from apsis.lib.parse import parse_duration
from apsis.lib.sys import to_signal
from apsis.states import to_state
from ..jobs import jso_to_job
from ..runs import Instance, RunError

log = logging.getLogger(__name__)

# Time to wait for new items to arrive when bundling a websocket message.
WS_DRAIN_TIME = 0.5
# Max number of items to send in one websocket message.
WS_CHUNK = 4096
# Time to sleep between websocket messages.
WS_CHUNK_SLEEP = 0.001

# -------------------------------------------------------------------------------


def parse_query(query):
    parts = parse_qs(query, keep_blank_values=True)
    return {n: v[0] for n, v in parts.items()}


API = sanic.Blueprint("v1")


@API.exception(RunError)
def no_such_process_error(request, exception):
    return error(exception, status=400)


@API.route("/alive")
async def live(request):
    return response_json({})


@API.route("/running")
async def running(request):
    # Block until running.
    apsis = request.app.apsis
    await apsis.running_flag.wait()
    return response_json({})


@API.route("/stats")
async def stats(request):
    stats = request.app.apsis.get_stats()
    return response_json(stats)


# -------------------------------------------------------------------------------
# Jobs


class JobLookupError(LookupError):
    pass


@API.exception(JobLookupError)
def job_lookup_error(request, exception):
    return error(exception, status=400)


class AmbiguousJobError(ValueError):
    pass


@API.exception(AmbiguousJobError)
def ambiguous_job_error(request, exception):
    return error(exception, status=400)


def match(choices, target):
    """
    Matches `target` to one of `choices`.

    Splits the target and each choice into words.  Selects a choice such that
    each word in the target appears as a word in the choice, at least as a
    prefix.

    :return:
      The matching choice.
    """
    REGEX = re.compile(r"[^A-Za-z0-9]")

    def words(target):
        return set(REGEX.split(target))

    target_words = words(target)

    def match(choice):
        choice_words = words(choice)
        return all(any(cw.startswith(sw) for cw in choice_words) for sw in target_words)

    choices = {c for c in choices if match(c)}

    if len(choices) == 0:
        raise JobLookupError("no job id match: " + target)
    elif len(choices) == 1:
        return next(iter(choices))
    else:
        if len(choices) > 8:
            choices = ", ".join(list(choices)[:8]) + " …"
        else:
            choices = ", ".join(choices)
        raise AmbiguousJobError("ambiguous job id: " + choices)


def match_job_id(jobs, job_id):
    """
    Matches `job_id` as an exact or fuzzy match.
    """
    # Try for an exact match first.a
    try:
        jobs.get_job(job_id)
    except LookupError:
        pass
    else:
        return job_id

    # FIXME: Cache job ids (or word split job ids) to make this efficient.
    job_ids = [j.job_id for j in jobs.get_jobs(ad_hoc=False)]
    return match(job_ids, job_id)


@API.route("/jobs/<job_id:path>")
async def job(request, job_id):
    jobs = request.app.apsis.jobs
    try:
        job_id = match_job_id(jobs, unquote(job_id))
    except LookupError:
        return error(f"no job_id {job_id}", status=404)
    job = jobs.get_job(job_id)
    return response_json(job_to_jso(job))


@API.route("/jobs/<job_id:path>/runs")
async def job_runs(request, job_id):
    job_id = match_job_id(request.app.apsis.jobs, unquote(job_id))
    when, runs = request.app.apsis.run_store.query(job_id=job_id)
    jso = runs_to_jso(request.app, when, runs)
    return response_json(jso)


@API.route("/jobs")
async def jobs(request):
    """
    Returns (non ad-hoc) jobs.
    """
    args = request.args
    try:
        (label,) = args["label"]
    except KeyError:
        label = None

    jso = [
        job_to_jso(j)
        for j in request.app.apsis.jobs.get_jobs(ad_hoc=False)
        if label is None or label in j.meta.get("labels")
    ]
    return response_json(jso)


# -------------------------------------------------------------------------------
# Runs


@API.route("/runs/<run_id>", methods={"GET"})
async def run(request, run_id):
    try:
        when, run = request.app.apsis.run_store.get(run_id)
    except KeyError:
        return error(f"unknown run {run_id}", 404)

    jso = runs_to_jso(request.app, when, [run])
    return response_json(jso)


@API.route("/runs/<run_id>/log", methods={"GET"})
async def run_log(request, run_id):
    try:
        run_log = request.app.apsis.get_run_log(run_id)
    except KeyError:
        return error(f"unknown run {run_id}", 404)

    return response_json(
        {
            "run_log": [
                {
                    "run_id": r["run_id"],
                    "timestamp": time_to_jso(r["timestamp"]),
                    "message": r["message"],
                }
                for r in sorted(run_log, key=lambda r: r["timestamp"])
            ]
        }
    )


@API.websocket("/runs/<run_id>/updates")
async def websocket_run_updates(request, ws, run_id):
    # request.query_args doesn't work correctly for ws endpoints?
    init = "init" in request.query_string.split("&")

    # Make sure the run exists.
    try:
        _ = request.app.apsis.run_store.get(run_id)
    except LookupError:
        await ws.close()
        return

    apsis = request.app.apsis
    with apsis.run_update_publisher.subscription(run_id) as subscription:
        if init:
            # Initialize run metadata.
            try:
                _, run = apsis.run_store.get(run_id)
            except KeyError:
                return error(f"unknown run {run_id}", 404)

            # Initialize run log.
            try:
                run_log = apsis.get_run_log(run_id)
            except KeyError:
                run_log = []

            # Initialize output metadata.
            try:
                outputs = apsis.outputs.get_metadata(run_id)
            except KeyError:
                outputs = {}
            await ws.send(
                ujson.dumps(
                    {
                        "run": run_to_summary_jso(run),
                        "meta": run.meta,
                        "run_log": run_log_to_jso(run_log),
                        "outputs": {n: o.to_jso() for n, o in outputs.items()},
                    }
                )
            )

        async for msg in subscription:
            await ws.send(ujson.dumps(msg))


@API.route("/runs/<run_id>/outputs", methods={"GET"})
async def run_output_meta(request, run_id):
    try:
        outputs = request.app.apsis.outputs.get_metadata(run_id)
    except KeyError:
        log.error(f"unknown run {run_id}", exc_info=True)
        return error(f"unknown run {run_id}", 404)

    jso = output_metadata_to_jso(request.app, run_id, outputs)
    return response_json(jso)


@API.route("/runs/<run_id>/output/<output_id>", methods={"GET"})
async def run_output(request, run_id, output_id):
    try:
        output = request.app.apsis.outputs.get_output(run_id, output_id)
    except LookupError as exc:
        return error(exc, 404)
    else:
        headers, data = encode_response(request.headers, output.data, output.compression)
        headers["Content-Type"] = output.metadata.content_type
        return sanic.response.raw(data, headers=headers)


@API.websocket("/runs/<run_id>/output/<output_id>/updates")
async def websocket_output_updates(request, ws, run_id, output_id):
    query = parse_query(request.query_string)
    try:
        start = int(query["start"])
    except (KeyError, ValueError):
        start = None

    apsis = request.app.apsis
    # Make sure the run exists.
    try:
        _, run = request.app.apsis.run_store.get(run_id)
    except LookupError:
        await ws.close()
        return

    if run.state.finished:
        # The run is finished; there won't be any future output.
        if start is not None:
            # Send existing outputs.
            try:
                output = apsis.outputs.get_output(run_id, output_id)
            except LookupError:
                log.warning(f"no output: {run_id} {output_id}")
            else:
                msg = output_to_http_message(output, interval=(start, None))
                await ws.send(msg)

    else:
        # The run is not finished, so subscribe for live updates.
        with apsis.output_update_publisher.subscription(run_id) as sub:
            try:
                output = apsis.outputs.get_output(run_id, output_id)
                if start is not None and output.compression is not None:
                    # Send the output data up to now.
                    msg = output_to_http_message(output, interval=(start, None))
                    await ws.send(msg)
                cur = output.metadata.length
            except LookupError:
                # No output yet.
                cur = 0

            async for output in sub:
                if output.compression is not None:
                    # Compressed output means the run is finished.
                    break

                msg = output_to_http_message(output, interval=(cur, None))
                await ws.send(msg)
                cur = output.metadata.length

    await ws.close()


@API.route("/runs/<run_id>/state", methods={"GET"})
async def run_state_get(request, run_id):
    _, run = request.app.apsis.run_store.get(run_id)
    return response_json({"state": run.state})


@API.route("/runs/<run_id>/skip", methods={"POST"})
async def run_skip(request, run_id):
    state = request.app.apsis
    _, run = state.run_store.get(run_id)
    await state.skip(run)
    return response_json({})


@API.route("/runs/<run_id>/start", methods={"POST"})
async def run_start(request, run_id):
    state = request.app.apsis
    _, run = state.run_store.get(run_id)
    await state.start(run)
    return response_json({})


@API.route("/runs/<run_id>/rerun", methods={"POST"})
async def run_rerun(request, run_id):
    state = request.app.apsis
    _, run = state.run_store.get(run_id)
    new_run = await state.rerun(run)
    jso = runs_to_jso(request.app, ora.now(), [new_run])
    # Let UIs know to show the new run.
    jso["show_run_id"] = new_run.run_id
    return response_json(jso)


# PUT is probably right, but run actions currently are POST only.
@API.route("/runs/<run_id>/stop", methods={"PUT", "POST"})
async def run_stop(request, run_id):
    apsis = request.app.apsis
    _, run = apsis.run_store.get(run_id)

    try:
        await apsis.stop_run(run)
    except RuntimeError as exc:
        return error(str(exc), 400)
    else:
        jso = runs_to_jso(request.app, ora.now(), [run])
        return response_json(jso)


# PUT is probably right, but run actions currently are POST only.
@API.route("/runs/<run_id>/signal/<signal>", methods={"PUT", "POST"})
async def run_signal(request, run_id, signal):
    apsis = request.app.apsis
    _, run = apsis.run_store.get(run_id)

    try:
        signal = to_signal(signal)
    except ValueError:
        return error(f"invalid signal: {signal}", 400)

    try:
        await apsis.send_signal(run, signal)
    except RuntimeError as exc:
        return error(str(exc), 400)
    else:
        return response_json({})


@API.route("/runs/<run_id>/mark/<state>", methods={"PUT", "POST"})
async def run_mark(request, run_id, state):
    try:
        apsis = request.app.apsis
        _, run = apsis.run_store.get(run_id)
        try:
            state = to_state(state)
        except ValueError as err:
            return error(str(err), status=400)

        await apsis.mark(run, state)
    except Exception:
        log.error("error", exc_info=True)
        raise
    return response_json({})


@API.route("/runs/<run_id>/dependencies")
async def run_dependencies(request, run_id):
    apsis = request.app.apsis
    _, run = apsis.run_store.get(run_id)

    from apsis.cond.dependency import Dependency

    # Collect dependency job instances.
    dep_instances = [(c.job_id, c.args) for c in (run.conds or []) if isinstance(c, Dependency)]

    def get_run_ids(job_id, args):
        _, runs = apsis.run_store.query(job_id=job_id, args=args)
        return [r.run_id for r in runs]

    # Query matching runs and package up.
    deps = [
        {
            "job_id": j,
            "args": a,
            "run_ids": get_run_ids(j, a),
        }
        for j, a in dep_instances
    ]

    return response_json({run_id: deps})


@API.route("/runs")
async def runs(request):
    apsis = request.app.apsis

    # Get runs from the selected interval.
    args = request.args
    (summary,) = args.pop("summary", ("False",))
    summary = to_bool(summary)
    run_id = args.pop("run_id", None)
    (job_id,) = args.pop("job_id", (None,))
    if job_id is not None:
        job_id = match_job_id(apsis.jobs, job_id)
    (state,) = args.pop("state", (None,))
    (since,) = args.pop("since", (None,))

    # Remainders are args to match, though strip off leading underscores, where
    # were added to avoid collision with fixed args.
    args = {n[1:] if n.startswith("_") else n: a[-1] for n, a in args.items()}

    when, runs = apsis.run_store.query(
        run_ids=run_id,
        job_id=job_id,
        state=None if state is None else to_state(state),
        since=since,
        with_args=args,
    )

    return response_json(runs_to_jso(request.app, when, runs, summary=summary))


async def _send_chunked(msgs, ws, prefix):
    # Break large sets into chunks, to avoid block for too long.
    for chunk in apsis.lib.itr.chunks(msgs, WS_CHUNK):
        json = ujson.dumps(chunk, escape_forward_slashes=False)
        log.debug(f"{prefix} sending {len(chunk)} msgs, {len(json)} bytes")
        await ws.send(json)
        # Take a break, let others go.
        await asyncio.sleep(WS_CHUNK_SLEEP)


# Message types (see apsis.api.messages) to include in summary.
SUMMARY_MSG_TYPES = {
    "agent_conn",
    "agent_conn_delete",
    "job",
    "job_add",
    "job_delete",
    "run_delete",
    "run_summary",
    "run_transition",
}


@API.websocket("/summary")
async def websocket_summary(request, ws):
    connect_time = time.monotonic()

    # request.query_args doesn't work correctly for ws endpoints?
    init = "init" in request.query_string.split("&")

    apsis = request.app.apsis

    addr, port = request.socket
    prefix = f"/summary {addr}:{port}:"
    log.debug(f"{prefix} connected init={init}")

    predicate = lambda msg: msg["type"] in SUMMARY_MSG_TYPES
    with apsis.summary_publisher.subscription(predicate=predicate) as sub:
        try:
            if init:
                # Full initialization.

                # Send all jobs.
                jobs = apsis.jobs.get_jobs(ad_hoc=False)
                job_msgs = (messages.make_job(j) for j in jobs)

                # Send all procstar agent conns.
                try:
                    agent_server = procstar.get_agent_server()
                except procstar.NoServerError:
                    conn_msgs = iter([])
                else:
                    conn_msgs = (
                        messages.make_agent_conn(c) for c in agent_server.connections.values()
                    )

                # Send summaries of all runs.
                _, runs = apsis.run_store.query()
                # Reverse runs to have the latest runs first.
                # This depends on 'apsis.run_store.query()' returning runs in ascending order.
                runs = runs[::-1]
                run_msgs = (messages.make_run_summary(r) for r in runs)

                msgs = itertools.chain(job_msgs, conn_msgs, run_msgs)
                await _send_chunked(msgs, ws, prefix)

            while not sub.closed:
                # Wait for the next msg, then grab all that show up in a short time.
                # This avoids sending lots of short websocket traffic.
                msgs = await asyn.anext_and_drain(sub, WS_DRAIN_TIME)
                await _send_chunked(msgs, ws, prefix)

            await ws.close()

        except websockets.ConnectionClosed:
            log.debug(f"{prefix} closed")

        except asyncio.CancelledError:
            # task is cancelled in the event of abnormal websocket closure, usually if
            # websocket close due to a ping timeout can't complete successfully
            log.warning(f"{prefix} task cancelled after {time.monotonic() - connect_time:.2f}s")
            raise

    log.debug(f"{prefix} done")


@API.route("/runs", methods={"POST"})
async def run_post(request):
    # FIXME: Add a way to specify the stop time.
    query = parse_query(request.query_string)
    try:
        count = int(query["count"])
    except KeyError:
        count = 1
    apsis = request.app.apsis

    # The run may either contain a job ID, or a complete job.
    jso = request.json
    if "job" in jso:
        # A complete job.
        job = jso_to_job(jso["job"], None)
        job.ad_hoc = True
        request.app.apsis.jobs.add(job)
        job_id = job.job_id

    elif "job_id" in jso:
        # Just a job ID.
        job_id = match_job_id(apsis.jobs, jso["job_id"])

    else:
        return error("missing job_id or job")

    args = jso.get("args", {})
    inst = Instance(job_id, args)

    times = jso.get("times", {})
    time = times.get("schedule", "now")
    time = "now" if time == "now" else ora.Time(time)

    stop_time = times.get("stop", None)
    if stop_time is not None:
        # Either an absolute time or a duration ahead of schedule time.
        try:
            stop_time = ora.Time(stop_time)
        except ValueError:
            try:
                duration = parse_duration(stop_time)
            except ValueError:
                raise ValueError(f"invalid stop time: {stop_time}")
            else:
                stop_time = (ora.now() if time == "now" else time) + duration

    runs = (apsis.schedule(time, inst, stop_time=stop_time) for _ in range(count))
    runs = await asyncio.gather(*runs)
    jso = runs_to_jso(request.app, ora.now(), runs)
    return response_json(jso)
