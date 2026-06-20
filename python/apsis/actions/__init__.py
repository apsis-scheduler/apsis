import logging

from apsis import runs
from apsis.lib.json import check_schema

from .base import Action
from .condition import Condition
from .schedule import ScheduleAction

log = logging.getLogger(__name__)

Action.TYPE_NAMES.set(ScheduleAction, "schedule")
