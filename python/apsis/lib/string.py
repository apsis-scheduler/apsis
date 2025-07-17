def prefix_match(choices, string, *, key=str):
    """
    Returns the element of `choices` of which `string` is an unambiguous prefix.

    :param choices:
      Iterable of strings.
    :raise ValueError:
      `string` is a prefix of no `choices` or of more than one.
    """
    matches = {c for c in choices if key(c).startswith(string)}
    if len(matches) == 0:
        raise ValueError(f"no match: {string}")
    elif len(matches) == 1:
        (match,) = matches
        return match
    else:
        raise ValueError(f"multiple matches: {' '.join(matches)}")
