import re

from .py import or_none

# -------------------------------------------------------------------------------

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
