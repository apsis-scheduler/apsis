import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import ora
from urllib.parse import parse_qs
import ujson

from . import messages
from apsis import procstar
from apsis.jobs import Jobs
from apsis.lib.itr import chunks
from apsis.run_window import MAX_LIMIT, JobFilter, WindowSpec, args_match
from apsis.states import to_state

# message family for each summary message type
MSG_FAMILIES = {
    "job": "job",
    "job_add": "job",
    "job_delete": "job",
    "agent_conn": "agent_conn",
    "agent_conn_delete": "agent_conn",
    "run_summary": "run",
    "run_transition": "run",
    "run_delete": "run",
}
ALL_TYPES = frozenset(MSG_FAMILIES.values())

MAX_JOB_IDS = 500
# runs read per db page and jobs per websocket send
PAGE_ROWS = 500
JOB_CHUNK = 100
# newest runs sent per job
JOB_RUN_CAP = 10000
# window walks that run at once
_WINDOW_WALKS = asyncio.Semaphore(2)


@dataclass(frozen=True)
class SummaryScope:
    init: bool
    # message families to send
    types: frozenset[str] = ALL_TYPES
    # jobs whose job and run messages to send, or none for all
    job_ids: frozenset[str] | None = None
    # window of runs to send on init instead of all runs of the jobs
    window: WindowSpec | None = None

    @property
    def scoped(self) -> bool:
        return self.types != ALL_TYPES or self.job_ids is not None


def parse_summary_scope(query_string: str, jobs: Jobs) -> SummaryScope:
    """
    Parses summary websocket query params.

    `init` is a flag, `types` a comma separated list of message families,
    `job_id` a repeatable job ID, and `dependents_of` a repeatable job ID
    whose dependent jobs are added to the job IDs.

    With `time`, an ISO time or `now`, init sends a window of runs around it
    as the web UI runs list shows them, and types default to `run`.  The
    window takes `limit`, `grouping` of 0 or 1, `path`, and repeatable
    `keyword`, `label`, `state`, and `arg` of `param=value` or `param`.

    :raise ValueError:
      An invalid param.
    """
    query = parse_qs(query_string, keep_blank_values=True)

    def one(name: str, default: str | None) -> str | None:
        values = query.get(name, (default,))
        if len(values) > 1:
            raise ValueError(f"more than one {name}")
        return values[0]

    window = "time" in query
    types = frozenset({"run"}) if window else ALL_TYPES
    if "types" in query:
        types = frozenset(t for v in query["types"] for t in v.split(","))
        if unknown := types - ALL_TYPES:
            raise ValueError(f"unknown types: {', '.join(sorted(unknown))}")

    job_ids = None
    if "job_id" in query or "dependents_of" in query:
        job_ids = frozenset(query.get("job_id", ()))
        for job_id in query.get("dependents_of", ()):
            job_ids |= jobs.get_dependents(job_id)
        if len(job_ids) > MAX_JOB_IDS:
            raise ValueError(f"more than {MAX_JOB_IDS} job IDs")

    if not window:
        return SummaryScope(init="init" in query, types=types, job_ids=job_ids)

    time = one("time", None)
    limit = int(one("limit", "50"))
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be 1 to {MAX_LIMIT}")
    states = query.get("state")
    spec = WindowSpec(
        anchor=None if time == "now" else ora.Time(time),
        limit=limit,
        grouping=_parse_bool(one("grouping", "1")),
        job_ids=job_ids,
        states=None if states is None else frozenset(to_state(s).name for s in states),
        args=tuple(map(_parse_arg, query.get("arg", ()))),
        job_filter=JobFilter(
            path=one("path", None),
            keywords=tuple(query.get("keyword", ())),
            labels=tuple(query.get("label", ())),
        ),
    )
    return SummaryScope(init="init" in query, types=types, job_ids=job_ids, window=spec)


def _parse_bool(value: str) -> bool:
    try:
        return {"1": True, "true": True, "0": False, "false": False}[value.lower()]
    except KeyError:
        raise ValueError(f"not a bool: {value}") from None


def _parse_arg(arg: str) -> tuple[str, str | None]:
    param, eq, value = arg.partition("=")
    return param, value if eq else None


def _get_job_id(msg: dict) -> str | None:
    if "job" in msg:
        return msg["job"]["job_id"]
    if "run_summary" in msg:
        return msg["run_summary"]["job_id"]
    return msg.get("job_id")


def make_summary_predicate(scope: SummaryScope) -> Callable[[dict], bool]:
    """
    Returns a predicate for summary messages in `scope`.

    Messages without a job ID pass the job ID filter, and in a window run
    messages pass its job filter and args but not its states, so the client
    sees runs leave them.
    """
    window = scope.window

    def predicate(msg: dict) -> bool:
        if MSG_FAMILIES.get(msg["type"]) not in scope.types:
            return False
        job_id = _get_job_id(msg)
        if scope.job_ids is not None and job_id is not None and job_id not in scope.job_ids:
            return False
        if window is not None and "run_summary" in msg:
            run = msg["run_summary"]
            return window.job_filter.matches(run["job_id"], run["labels"]) and args_match(
                run["args"], window.args
            )
        return True

    return predicate


async def _send(ws, msgs: list[str]) -> None:
    if msgs:
        await ws.send("[" + ",".join(msgs) + "]")
    await asyncio.sleep(0)


async def _send_jobs_and_conns(apsis, ws, scope: SummaryScope) -> None:
    if "job" in scope.types:
        if scope.job_ids is None:
            jobs = list(apsis.jobs.get_jobs(ad_hoc=False))
        else:
            jobs = []
            for job_id in sorted(scope.job_ids):
                try:
                    jobs.append(apsis.jobs.get_job(job_id))
                except LookupError:
                    pass
        for chunk in chunks(jobs, JOB_CHUNK):
            await _send(ws, [ujson.dumps(messages.make_job(j, jobs=apsis.jobs)) for j in chunk])

    if "agent_conn" in scope.types:
        try:
            conns = procstar.get_agent_server().connections.values()
        except procstar.NoServerError:
            conns = ()
        await _send(ws, [ujson.dumps(messages.make_agent_conn(c)) for c in conns])


async def send_scoped_init(apsis, ws, scope: SummaryScope) -> None:
    """
    Sends the jobs, agent conns, and runs in `scope`, then a
    `summary_init_done` message whose `truncated` maps each job with runs
    beyond the newest `JOB_RUN_CAP` to the number sent.
    """
    await _send_jobs_and_conns(apsis, ws, scope)
    truncated = {}
    if "run" in scope.types:
        if scope.job_ids is None:
            for page in apsis.run_store.summaries():
                await _send(ws, page)
        else:
            await _send(ws, apsis.run_store.expected_summaries(scope.job_ids))
            for job_id in sorted(scope.job_ids):
                count = 0
                for page in apsis.run_store.job_summaries(job_id, page_size=PAGE_ROWS):
                    if count + len(page) > JOB_RUN_CAP:
                        page = page[: JOB_RUN_CAP - count]
                        truncated[job_id] = JOB_RUN_CAP
                    await _send(ws, page)
                    count += len(page)
                    if job_id in truncated:
                        break

    await ws.send(ujson.dumps([{"type": "summary_init_done", "truncated": truncated}]))


async def send_window_init(apsis, ws, scope: SummaryScope) -> None:
    """
    Sends the jobs and agent conns in `scope` and the runs in its window,
    then a `run_window` message describing the window.

    :raise WindowUnavailable:
      The window needs the runs schedule index.
    """
    await _send_jobs_and_conns(apsis, ws, scope)
    spec = scope.window
    async with _WINDOW_WALKS:
        window = await apsis.run_store.query_window(spec)
    for chunk in chunks(window.run_ids, PAGE_ROWS):
        await _send(ws, apsis.run_store.get_summaries(chunk))
    msg = {
        "type": "run_window",
        "anchor": str(window.anchor),
        "limit": spec.limit,
        "grouping": spec.grouping,
        "groups": window.counts if spec.grouping else {},
        "earlier": None if window.earlier is None else {"time": window.earlier},
        "later": None if window.later is None else {"time": window.later},
        "complete": window.complete,
    }
    await ws.send(ujson.dumps([msg]))
