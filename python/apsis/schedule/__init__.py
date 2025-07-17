from .base import Schedule, DaytimeSpec, schedule_to_jso, schedule_from_jso
from .daily import DailySchedule
from .daily_interval import DailyIntervalSchedule
from .explicit import ExplicitSchedule
from .interval import IntervalSchedule

# -------------------------------------------------------------------------------

Schedule.TYPE_NAMES.set(DailyIntervalSchedule, "daily-interval")
Schedule.TYPE_NAMES.set(DailySchedule, "daily")
Schedule.TYPE_NAMES.set(ExplicitSchedule, "explicit")
Schedule.TYPE_NAMES.set(IntervalSchedule, "interval")
