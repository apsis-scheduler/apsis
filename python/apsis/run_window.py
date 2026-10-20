"""
Windows of runs around a schedule time, as shown in the web UI runs list.
"""

import asyncio
import bisect
from collections import defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable, Container, Iterable, Iterator
from dataclasses import dataclass
from typing import NamedTuple

import ora
import ujson

from .lib.itr import chunks
from .runs import Run, run_number
from .sqlite import RunDB, canonical_args_json, dump_time

# state group of each run state, as in the web UI
STATE_GROUPS = {
    "new": "S",
    "scheduled": "S",
    "waiting": "R",
    "starting": "R",
    "running": "R",
    "stopping": "R",
    "success": "C",
    "failure": "C",
    "error": "C",
    "skipped": "C",
}
GROUP_STATES = {g: frozenset(s for s, x in STATE_GROUPS.items() if x == g) for g in "SC"}

MAX_LIMIT = 1000
# db rows per page
PAGE_ROWS = 2000
# most db rows walked on each side of the anchor
SIDE_BUDGET = 200_000
# job filtered windows with at most this many runs load them all through the job index
JOB_PATH_ROWS = 20_000


class Entry(NamedTuple):
    rowid: int
    run_id: str
    sched: str | None
    job_id: str
    # canonical args json
    args: str
    state: str
    timestamp: float
    # run labels json array, if read
    labels: str | None


def group_key(entry: Entry) -> str:
    """
    Returns the key of the group `entry` belongs to, starting with its state group.

    Scheduled and completed runs group by instance, and others are never grouped.
    """
    sgrp = STATE_GROUPS[entry.state]
    # keys are plain strings with a null byte between job ID and args
    return sgrp + entry.run_id if sgrp == "R" else f"{sgrp}{entry.job_id}\0{entry.args}"


@dataclass(frozen=True)
class JobFilter:
    """
    Job filters of the web UI runs list, on the job ID and labels of each run.
    """

    # job ID or job ID prefix ending before a slash
    path: str | None = None
    # case insensitive job ID substrings
    keywords: tuple[str, ...] = ()
    labels: tuple[str, ...] = ()

    def matches_job_id(self, job_id: str) -> bool:
        if self.path is not None and job_id != self.path and not job_id.startswith(self.path + "/"):
            return False
        lower = job_id.lower()
        return all(k.lower() in lower for k in self.keywords)

    def matches(self, job_id: str, labels: Iterable[str]) -> bool:
        return self.matches_job_id(job_id) and set(self.labels) <= set(labels)


@dataclass(frozen=True)
class WindowSpec:
    # center time, or none for now
    anchor: ora.Time | None = None
    limit: int = 50
    grouping: bool = True
    # jobs to include, or none for all
    job_ids: frozenset[str] | None = None
    # run state names to include, or none for all
    states: frozenset[str] | None = None
    # param to required arg value, or none for any value
    args: tuple[tuple[str, str | None], ...] = ()
    job_filter: JobFilter = JobFilter()


@dataclass
class RunWindow:
    anchor: ora.Time
    # shown runs, by schedule time
    run_ids: list[str]
    # group size of each shown run
    counts: dict[str, int]
    # anchors that move the window past each edge, or none if no group is past it
    earlier: str | None
    later: str | None


class WindowUnavailable(RuntimeError):
    def __init__(self) -> None:
        super().__init__("runs schedule index missing")


def select_window(before: int, after: int, limit: int) -> tuple[int, int]:
    """
    Returns how many of `before` and `after` runs to show.

    The window shows `limit // 2` runs before the anchor and the rest after,
    and a side with fewer runs lends its slots to the other.
    """
    nb = limit // 2
    na = limit - nb
    if before < nb:
        return before, min(after, limit - before)
    if after < na:
        return min(before, limit - after), after
    return nb, na


class _Side:
    """
    Walks matching entries on one side of the anchor, nearest first.
    """

    def __init__(
        self,
        pages: Iterator[list[Entry]],
        keep: Callable[[Entry], bool],
        desc: bool,
        extra: list[Entry],
        live: Container[str] = (),
    ):
        """
        :param pages:
          Nonempty pages of entries, nearest first.
        :param extra:
          Matching entries to merge in, nearest first.
        :param live:
          Run IDs of `extra` entries to merge in, checked as each is reached.
        """
        # true once every entry is walked
        self.done = False
        # schedule time of the last db row walked
        self.last = None
        self.entries = self.__walk(pages, keep, desc, extra, live)

    async def __walk(
        self,
        pages: Iterator[list[Entry]],
        keep: Callable[[Entry], bool],
        desc: bool,
        extra: list[Entry],
        live: Container[str],
    ) -> AsyncIterator[Entry]:
        def nearer(a: Entry, b: Entry) -> bool:
            return (
                (a.sched, a.rowid) > (b.sched, b.rowid)
                if desc
                else (a.sched, a.rowid) < (b.sched, b.rowid)
            )

        extra = iter(extra)
        nxt = next(extra, None)
        scanned = 0
        for page in pages:
            for entry in page:
                while nxt is not None and nearer(nxt, entry):
                    if nxt.run_id in live:
                        yield nxt
                    nxt = next(extra, None)
                if keep(entry):
                    yield entry
            scanned += len(page)
            self.last = page[-1].sched
            if scanned >= SIDE_BUDGET:
                return
            await asyncio.sleep(0)
        while nxt is not None:
            if nxt.run_id in live:
                yield nxt
            nxt = next(extra, None)
        self.done = True


def _schedule_pages(run_db: RunDB, anchor: str, desc: bool, labels: bool) -> Iterator[list[Entry]]:
    time, rowid = anchor, None
    while True:
        rows = run_db.schedule_page(
            desc=desc, time=time, rowid=rowid, limit=PAGE_ROWS, labels=labels
        )
        if not rows:
            break
        yield [Entry._make(r) for r in rows]
        rowid, time = rows[-1][0], rows[-1][2]


def _expected_entry(run: Run) -> Entry:
    sched = run.times.get("schedule")
    return Entry(
        run_number(run.run_id),
        run.run_id,
        None if sched is None else str(sched),
        run.inst.job_id,
        canonical_args_json(run.inst.args),
        run.state.name,
        dump_time(run.timestamp),
        ujson.dumps(run.meta.get("job", {}).get("labels", [])),
    )


def _all(entry: Entry) -> bool:
    return True


def args_match(args: dict[str, str], spec_args: tuple[tuple[str, str | None], ...]) -> bool:
    """
    Returns true if `args` has each param in `spec_args` with its value, or
    any value for none.
    """
    return all(k in args and (v is None or args[k] == v) for k, v in spec_args)


def _make_keep(
    spec: WindowSpec, job_ids: frozenset[str] | None, min_ts: float
) -> Callable[[Entry], bool]:
    labels = set(spec.job_filter.labels)

    def keep(entry: Entry) -> bool:
        if entry.sched is None or entry.timestamp < min_ts:
            return False
        if job_ids is not None and entry.job_id not in job_ids:
            return False
        if spec.states is not None and entry.state not in spec.states:
            return False
        if labels and not labels <= set(ujson.loads(entry.labels or "[]")):
            return False
        return not spec.args or args_match(ujson.loads(entry.args), spec.args)

    return keep


async def _collect(
    side: _Side,
    limit: int,
    key_of: Callable[[Entry], str],
    principal: Callable[[Entry, str], Awaitable[int | None]],
) -> list[tuple[Entry, int]]:
    """
    Walks `side` for up to `limit` group principals, with their group sizes.
    """
    found = []
    async for entry in side.entries:
        count = await principal(entry, key_of(entry))
        if count is not None:
            found.append((entry, count))
            if len(found) == limit:
                break
    return found


def _edge(side: _Side, found: list[tuple[Entry, int]], shown: int) -> str | None:
    """
    Returns an anchor that moves the window past its edge on `side`, or none
    if no group is past it.
    """
    if len(found) == shown and side.done:
        return None
    if shown > 0:
        # anchoring at the outermost shown run brings in the groups past it
        return found[shown - 1][0].sched
    if found:
        # no slots on this side so anchor at the nearest group past the edge
        return found[0][0].sched
    # nothing found before the budget ran out so continue where it stopped
    return side.last


async def _later_edge(
    side: _Side,
    found: list[tuple[Entry, int]],
    shown: int,
    key_of: Callable[[Entry], str],
    principal: Callable[[Entry, str], Awaitable[int | None]],
) -> str | None:
    """
    Returns an anchor that moves the window past its later edge, or none if
    no group is past it.

    The later side includes runs at the anchor, so this skips groups tied with
    the outermost shown run and anchors at the next group after them, or just
    after them if the rest of the walk finds none.
    """
    edge = _edge(side, found, shown)
    if edge is None or shown == 0:
        return edge
    for entry, _ in found[shown:]:
        if entry.sched > edge:
            return entry.sched
    async for entry in side.entries:
        if entry.sched > edge and await principal(entry, key_of(entry)) is not None:
            return entry.sched
    return str(ora.Time(edge) + 1)


async def query_window(
    run_db: RunDB,
    expected_runs: dict[str, Run],
    spec: WindowSpec,
    *,
    min_timestamp: ora.Time | None,
) -> RunWindow:
    """
    Returns the window of runs that the web UI runs list would show for
    `spec`, for runs with a schedule time and timestamp at least `min_timestamp`.

    :param expected_runs:
      Runs that are only in memory by run ID, which runs leave as they move
      to the db or are removed while the window is built.
    :raise WindowUnavailable:
      The window needs the schedule index and the db doesn't have it.
    """
    anchor = ora.now() if spec.anchor is None else spec.anchor
    t = str(anchor)
    job_filter = spec.job_filter
    labels = bool(job_filter.labels)
    runs = list(expected_runs.values())
    # narrow job ID filters to the job IDs of runs they match
    job_ids = spec.job_ids
    if job_ids is None and (job_filter.path is not None or job_filter.keywords):
        job_ids = run_db.get_job_ids() + [r.inst.job_id for r in runs]
        await asyncio.sleep(0)
    if job_ids is not None:
        job_ids = frozenset(j for j in job_ids if job_filter.matches_job_id(j))
    keep = _make_keep(
        spec, job_ids, float("-inf") if min_timestamp is None else dump_time(min_timestamp)
    )
    expected = []
    for chunk in chunks(runs, PAGE_ROWS):
        expected.extend(e for e in map(_expected_entry, chunk) if keep(e))
        await asyncio.sleep(0)
    expected.sort(key=lambda e: (e.sched, e.rowid))
    key_of = group_key if spec.grouping else lambda e: "R" + e.run_id

    if job_ids is not None and run_db.count_job_rows(job_ids, JOB_PATH_ROWS + 1) <= JOB_PATH_ROWS:
        # load every run of the jobs and group them in memory
        entries = []
        for job_id in job_ids:
            max_rowid = 2**63 - 1
            while rows := run_db.job_page(
                job_id, max_rowid=max_rowid, limit=PAGE_ROWS, labels=labels
            ):
                entries.extend(e for e in map(Entry._make, rows) if keep(e))
                max_rowid = rows[-1][0]
                await asyncio.sleep(0)
        entries.extend(e for e in expected if e.run_id in expected_runs)
        entries.sort(key=lambda e: (e.sched, e.rowid))
        i = bisect.bisect_left([e.sched for e in entries], t)
        before = _Side(chunks(entries[i - 1 :: -1] if i else [], PAGE_ROWS), _all, True, [])
        after = _Side(chunks(entries[i:], PAGE_ROWS), _all, False, [])

        groups = {}
        for i, e in enumerate(entries, 1):
            if i % PAGE_ROWS == 0:
                await asyncio.sleep(0)
            key = key_of(e)
            run_id, count = groups.get(key, (None, 0))
            # scheduled groups show their earliest run and others their latest
            if run_id is None or key[0] != "S":
                run_id = e.run_id
            groups[key] = run_id, count + 1

        async def stats(entry: Entry, key: str) -> tuple[str | None, int]:
            return groups[key]

    else:
        if not run_db.has_schedule_index:
            raise WindowUnavailable()
        i = bisect.bisect_left([e.sched for e in expected], t)
        before = _Side(
            _schedule_pages(run_db, t, True, labels),
            keep,
            True,
            expected[i - 1 :: -1] if i else [],
            expected_runs,
        )
        after = _Side(
            _schedule_pages(run_db, t, False, labels), keep, False, expected[i:], expected_runs
        )

        expected_groups = defaultdict(list)
        for e in expected:
            expected_groups[key_of(e)].append(e)

        async def stats(entry: Entry, key: str) -> tuple[str | None, int]:
            sgrp = key[0]
            states = GROUP_STATES[sgrp] if spec.states is None else GROUP_STATES[sgrp] & spec.states
            earliest = sgrp == "S"
            rowid, run_id, sched, count = run_db.group_stats(
                entry.job_id,
                entry.args,
                states,
                min_timestamp,
                earliest=earliest,
                labels=job_filter.labels,
            )
            for e in expected_groups.get(key, ()):
                if e.run_id not in expected_runs:
                    continue
                pos = e.sched, e.rowid
                if sched is None or (pos < (sched, rowid) if earliest else pos > (sched, rowid)):
                    rowid, run_id, sched = e.rowid, e.run_id, e.sched
                count += 1
            await asyncio.sleep(0)
            return run_id, count

    cache = {}

    async def principal(entry: Entry, key: str) -> int | None:
        if key[0] == "R":
            return 1
        if key not in cache:
            cache[key] = await stats(entry, key)
        run_id, count = cache[key]
        return count if run_id == entry.run_id else None

    # one more group than fits on a side shows whether any is past its edge
    found_b = await _collect(before, spec.limit + 1, key_of, principal)
    found_a = await _collect(after, spec.limit + 1, key_of, principal)
    nb, na = select_window(len(found_b), len(found_a), spec.limit)
    shown = [f[0] for f in reversed(found_b[:nb])] + [f[0] for f in found_a[:na]]

    return RunWindow(
        anchor=anchor,
        run_ids=[e.run_id for e in shown],
        counts={f[0].run_id: f[1] for f in found_b[:nb] + found_a[:na]},
        earlier=_edge(before, found_b, nb),
        later=await _later_edge(after, found_a, na, key_of, principal),
    )
