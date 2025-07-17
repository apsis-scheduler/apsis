import asyncio
import itertools
import logging
from ora import Time, now

from .runs import Instance
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

    def __init__(self, cfg, jobs, schedule, stop):
        """
        :param jobs:
          Jobs object.
        :param schedule:
          Function of `time, run` that schedules a run.
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
        for job in self.__jobs.get_jobs():
            items = get_insts_to_schedule(job, self.__stop, stop)
            for sched_time, stop_time, inst in items:
                await self.__schedule(sched_time, inst, stop_time=stop_time)

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
