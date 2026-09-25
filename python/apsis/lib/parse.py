import re
from ora import Time

from .py import or_none

# -------------------------------------------------------------------------------


def parse_time(value: Time | str) -> Time:
    """Parse a timestamp with strict offset and range validation.

    :param value: An absolute timestamp or Ora time.
    :return: A valid Time.
    :raise ValueError: The timestamp, offset, or resulting time is invalid.
    """
    if isinstance(value, str):
        if "\0" in value:
            raise ValueError("invalid time: embedded NUL")
        offset = re.search(r"[+-]([0-9]{2}):([0-9]{2})\Z", value)
        if offset is not None and (int(offset[1]) > 23 or int(offset[2]) > 59):
            raise ValueError(f"invalid UTC offset: {offset[0]}")
    try:
        time = Time(value)
    except OverflowError as exc:
        raise ValueError(f"time out of range: {value}") from exc
    if not time.valid:
        raise ValueError("invalid time")
    return time


_DURATION_RE = re.compile(
    r"""
    (
      [-+]?
      \d+
      (?: \. \d*)?
    )
    \s*
    (
      \w+
    )
    $
    """,
    re.VERBOSE,
)

_DURATION_UNITS = {
    unit: mult
    for units, mult in [
        (("s", "sec", "second", "seconds"), 1),
        (("m", "min", "minute", "minutes"), 60),
        (("h", "hour", "hours"), 3600),
        (("d", "day", "days"), 86400),
    ]
    for unit in units
}


def parse_duration(string) -> float:
    """
    Parses a duration to seconds.

    :raise ValueError:
      Can't parse `string` as a duration.
    """
    string = str(string)

    try:
        return float(string)
    except (TypeError, ValueError):
        pass

    match = _DURATION_RE.match(str(string))
    if match is None:
        raise ValueError(f"can't parse as duration: {string}")
    res = float(match.group(1))
    unit = match.group(2)
    try:
        res *= _DURATION_UNITS[unit]
    except KeyError:
        raise ValueError(f"can't parse as duration: {string}: unknown unit {unit}") from None
    return res


nparse_duration = or_none(parse_duration)
