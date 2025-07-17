import logging

from .base import Action
from .condition import Condition
from .schedule import ScheduleAction
from apsis import runs
from apsis.lib.json import check_schema

log = logging.getLogger(__name__)

Action.TYPE_NAMES.set(ScheduleAction, "schedule")
