import asyncio
import itertools
import logging
from ora import Time, now

from .runs import Instance
from .sqlite import canonical_args_json
from apsis.lib.parse import parse_duration

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


def get_insts_to_schedule(job, start, stop):
    """
    Builds runs to schedule for `job` between `start` and `stop`.

    :return:
      Iterable of (sched_time, stop_time, inst).
    """
    for schedule in job.schedules:
        if schedule.enabled:
            times = itertools.takewhile(lambda t: t[0] < stop, schedule(start))
            for sched_time, args in times:
                args = {**args, "schedule_time": sched_time}
                args = {a: str(v) for a, v in args.items() if a in job.params}
                stop_time = (
                    None if schedule.stop_schedule is None else schedule.stop_schedule(sched_time)
                )
                # FIXME: Store additional args for later expansion.
                yield sched_time, stop_time, Instance(job.job_id, args)


class Scheduler:
    """
    Agent that creates and schedules new runs according to job schedules, up
    to a future time (the "scheduler time").

    Does not own any runs.
    """

    def __init__(self, cfg, jobs, schedule, stop, *, get_schedule_times=None):
        """
        :param jobs:
          Jobs object.
        :param schedule:
          Function of `time, run` that schedules a run.
        :param get_schedule_times:
          Function of no args returning a mapping from `(job_id, args JSON)` to
          the set of nominal schedule times for which a run already exists.
          Used to avoid recreating runs after a restart.  If none, no such
          check is made.
        """
        cfg = cfg.get("schedule", {})

        horizon = parse_duration(cfg.get("horizon", 86400))
        assert horizon > 0

        max_age = cfg.get("max_age")
        if max_age is not None:
            max_age = parse_duration(max_age)
            assert max_age > 0

        since = cfg.get("since")
        if since is not None:
            since = now() if since == "now" else Time(since)
            stop = max(stop, since)

        log.info(f"scheduler starts from {stop}")

        self.__jobs = jobs
        self.__stop = stop
        self.__schedule = schedule
        self.__horizon = horizon
        self.__max_age = max_age
        self.__get_schedule_times = get_schedule_times

    def set_jobs(self, jobs):
        """
        Replaces the jobs object.
        """
        self.__jobs = jobs

    def get_scheduler_time(self):
        """
        Returns the time up to which runs have been scheduled.
        """
        return self.__stop

    async def schedule(self, stop):
        """
        Advances scheduler time to `stop` by scheduling runs.
        """
        if stop <= self.__stop:
            # Nothing to do.
            return

        log.debug(f"scheduling runs until {stop}")

        # Counts of runs that already exist, by job, args, and nominal schedule
        # time, so that we don't create a second run for a schedule time we
        # already handled.  Only needed when scheduling into the past, i.e.
        # catching up after downtime; in steady state the window is in the
        # future, where no run exists yet.
        if self.__get_schedule_times is not None and self.__stop < now():
            existing = self.__get_schedule_times()
            log.info(f"scheduling from {self.__stop}: {len(existing)} existing run keys")
        else:
            existing = None

        n = 0
        skipped = 0
        for job in self.__jobs.get_jobs():
            items = get_insts_to_schedule(job, self.__stop, stop)
            for sched_time, stop_time, inst in items:
                if existing is not None:
                    # Account for each existing run against one schedule that
                    # would produce it.  A job may have several schedules that
                    # produce the same schedule time and args, in which case
                    # each is a run of its own, so match them up one for one
                    # rather than skipping every schedule that collides.
                    times = existing.get((inst.job_id, canonical_args_json(inst.args)))
                    if times is not None and times.get(sched_time, 0) > 0:
                        times[sched_time] -= 1
                        skipped += 1
                        continue

                await self.__schedule(sched_time, inst, stop_time=stop_time)
                # using modulo instead of batching a generator because reducing allocations actually matters here for
                # because of GC pressure
                n += 1
                if n % 5 == 0:
                    # Performance optimization for cases where apsis is recovering after extended downtime and needs to
                    # launch thousands of runs at once. Scheduling all the runs to run immediately without yielding can
                    # make it a long wait until the scheduler will respond to network requests. The number 5 was chosen
                    # after measuring startup times.
                    await asyncio.sleep(0)

        if skipped > 0:
            log.info(f"skipped {skipped} runs that already exist")

        self.__stop = stop

    async def loop(self):
        """
        Infinite loop that periodically schedules runs.
        """
        try:
            while True:
                # Make sure we're not too old.
                time = now()
                log.debug(f"scheduler loop: {time}")

                if self.__max_age is not None and self.__max_age < time - self.stop:
                    raise RuntimeError(f"last scheduled more than {self.__max_age} s ago")

                await self.schedule(time + self.__horizon)
                await asyncio.sleep(60)

        except asyncio.CancelledError:
            pass

        except Exception:
            log.critical("scheduler loop failed", exc_info=True)
            raise SystemExit(1)
