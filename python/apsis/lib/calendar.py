import functools
import ora.calendar

# -------------------------------------------------------------------------------


# Cache calendars.  We assume on-disk calendars don't change during the
# scheduler's lifetime.
@functools.cache
def get_calendar(name):
    try:
        return ora.calendar.get_calendar(name)
    except LookupError:
        raise LookupError(f"no such calendar: {name}") from None
