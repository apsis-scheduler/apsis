import asyncio
import heapq
import logging
from ora import now, Time

from .runs import Run

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


async def sleep_until(time):
    """
    Sleep until `time`, or do our best at least.
    """
    delay = time - now()

    # FIXME: These thresholds are rather ad hoc.

    if delay <= 0:
        # Nothing to do.
        pass

    else:
        await asyncio.sleep(delay)
        late = now() - time
        if late < -0.005:
            log.error(f"woke up early: {-late:.3f} s")
        elif late > 1:
            log.error(f"woke up late: {late:.1f} s")


class ScheduledRuns:
    """
    Scheduled runs waiting to start.
    """

    # We maintain an explicit run schedule, rather than using the event loop, to
    # guarantee that we are scheduling to the real time clock, rather than the
    # event loop's clock.

    # Max delay time.  Generally, a run is started very close to its scheduled
    # time, but in some cases the run may be delayed as much as this time.

    LOOP_TIME = 1

    # Entry is the data structure stored in __heap.  It represents a scheduled
    # run.  We also maintain __scheduled, a map from Run to Entry, to find an
    # entry of an already-scheduled job.
    #
    # Since an entry cannot easily be removed from the middle of a heap, we
    # unschedule a job by setting scheduled=False.  It stays in the heap, but
    # we ignore it when it comes to to top.  However, __scheduled only includes
    # entries for which scheduled==True.

    class Entry:
        def __init__(self, time, run):
            self.time = time
            self.run = run
            self.scheduled = True

        def __hash__(self):
            return hash(self.time)

        def __eq__(self, other):
            return self.time == other.time

        def __lt__(self, other):
            return self.time < other.time

    def __init__(self, clock_db, get_scheduler_time, start_run):
        """
        :param clock_db:
          Persistence for most recent scheduled time.
        :param start_run:
          Async function that starts a run.
        """
        self.__clock_db = clock_db
        self.__get_scheduler_time = get_scheduler_time
        self.__start_run = start_run

        # Heap of Entry, ordered by schedule time.  The top entry is the next
        # scheduled run.
        self.__heap = []

        # Mapping from Run to Entry.  Values satisfy entry.scheduled==True.
        self.__scheduled = {}

    def __len__(self):
        return len(self.__heap)

    def get_stats(self):
        return {
            "num_heap": len(self.__heap),
            "num_entries": len(self.__scheduled),
        }

    def get_scheduled_time(self):
        """
        Returns the time through which scheduled runs have been started.
        """
        return self.__clock_db.get_time()

    async def loop(self):
        # The start loop sleeps until the time to start the next scheduled job,
        # or for LOOP_TIME, whichever comes first.  LOOP_TIME comes in to play
        # when the event loop clock wanders from the real time clock, or if a
        # run is scheduled in the very near future after the start loop has
        # already gone to sleep.
        try:
            while True:
                time = now()

                # Make sure we don't get ahead of the scheduler.  This is may
                # occur:
                # - if the scheduling horizon is very short, or
                # - on startup, when there are a lot of past runs to schedule
                scheduler_time = self.__get_scheduler_time()
                if time > scheduler_time:
                    log.warning(f"scheduled time > scheduler {scheduler_time}")
                    await asyncio.sleep(1)
                    continue

                ready = set()
                while len(self.__heap) > 0 and self.__heap[0].time <= time:
                    # The next run is ready.
                    entry = heapq.heappop(self.__heap)
                    if entry.scheduled:
                        # Take it out of the entries dict.
                        assert self.__scheduled.pop(entry.run) is entry
                        ready.add(entry.run)
                self.__clock_db.set_time(time)

                if len(ready) > 0:
                    log.debug(f"{len(ready)} runs ready")
                    # Start the runs.
                    for run in ready:
                        self.__start_run(run)

                next_time = time + self.LOOP_TIME
                if len(self.__heap) > 0:
                    next_time = min(next_time, self.__heap[0].time)

                await sleep_until(next_time)

        except asyncio.CancelledError:
            # Let this through.
            raise

        except Exception:
            # FIXME: Do this in Apsis.
            log.critical("scheduled loop failed", exc_info=True)
            raise SystemExit(1)

    def schedule_at(self, time: Time, run: Run):
        """
        Schedules `run` to start at `time`.
        """
        # Put it onto the schedule heap.
        entry = self.Entry(time, run)
        heapq.heappush(self.__heap, entry)
        self.__scheduled[run] = entry

    async def schedule(self, time: Time, run: Run):
        """
        Schedules `run` to start at `time`.

        If `time` is not in the future, starts the run now.
        """
        wait = time - now()
        if wait <= 0:
            # Job is current; start it now.
            log.debug(f"run immediately: {time} {run.run_id}")
            self.__start_run(run)
        else:
            self.schedule_at(time, run)

    def unschedule(self, run: Run) -> bool:
        """
        Unschedules `run`.

        :return:
          Whether the run was unscheduled: true iff it was scheduled, hasn't
          started yet, and hasn't already been unscheduled.
        """
        log.info(f"unschedule: {run}")
        try:
            # Remove it from the scheduled dict.
            entry = self.__scheduled.pop(run)
        except KeyError:
            # Wasn't scheduled.
            return False
        else:
            # Mark it as unscheduled, so the start loop will ignore it.  Note
            # that we don't remove it from the heap; there's no constant-time
            # way to do this.
            assert entry.scheduled
            entry.scheduled = False
            return True
