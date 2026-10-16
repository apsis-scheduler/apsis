import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import parse_qs
import ujson

from . import messages
from apsis import procstar
from apsis.jobs import Jobs
from apsis.lib.itr import chunks

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


@dataclass(frozen=True)
class SummaryScope:
    init: bool
    # message families to send
    types: frozenset[str] = ALL_TYPES
    # jobs whose job and run messages to send, or none for all
    job_ids: frozenset[str] | None = None

    @property
    def scoped(self) -> bool:
        return self.types != ALL_TYPES or self.job_ids is not None


def parse_summary_scope(query_string: str, jobs: Jobs) -> SummaryScope:
    """
    Parses summary websocket query params.

    `init` is a flag, `types` a comma separated list of message families,
    `job_id` a repeatable job ID, and `dependents_of` a repeatable job ID
    whose dependent jobs are added to the job IDs.

    :raise ValueError:
      An unknown type or too many job IDs.
    """
    query = parse_qs(query_string, keep_blank_values=True)

    types = ALL_TYPES
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

    return SummaryScope(init="init" in query, types=types, job_ids=job_ids)


def _get_job_id(msg: dict) -> str | None:
    if "job" in msg:
        return msg["job"]["job_id"]
    if "run_summary" in msg:
        return msg["run_summary"]["job_id"]
    return msg.get("job_id")


def make_summary_predicate(scope: SummaryScope) -> Callable[[dict], bool]:
    """
    Returns a predicate for summary messages in `scope`.

    Messages without a job ID pass the job ID filter.
    """

    def predicate(msg: dict) -> bool:
        if MSG_FAMILIES.get(msg["type"]) not in scope.types:
            return False
        if scope.job_ids is None:
            return True
        job_id = _get_job_id(msg)
        return job_id is None or job_id in scope.job_ids

    return predicate


async def _send(ws, msgs: list[str]) -> None:
    if msgs:
        await ws.send("[" + ",".join(msgs) + "]")
    await asyncio.sleep(0)


async def send_scoped_init(apsis, ws, scope: SummaryScope) -> None:
    """
    Sends the jobs, agent conns, and runs in `scope`, then a
    `summary_init_done` message whose `truncated` maps each job with runs
    beyond the newest `JOB_RUN_CAP` to the number sent.
    """
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
