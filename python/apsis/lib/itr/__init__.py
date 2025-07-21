"""
Tools for working with iterators.
"""

# -------------------------------------------------------------------------------

from collections import deque

from .recipes import *  # noqa: F403  # also imports * from itertools

# -------------------------------------------------------------------------------


def ntimes(value, times):
    """
    Generates `times` copies of `value`.
    """
    for _ in range(times):
        yield value


def first(iterable):
    """
    Generates `(first, item)` for each item in `iterable`, where `first` is
    true for the first time and false for subsequent items.
    """
    i = iter(iterable)
    yield True, next(i)
    while True:
        yield False, next(i)


def last(iterable):
    """
    Generates `(last, item)` for each item in `iterable`, where `last` is
    false except for the last item.
    """
    i = iter(iterable)
    item = next(i)
    while True:
        try:
            next_item = next(i)
        except StopIteration:
            yield True, item
            break
        else:
            yield False, item
            item = next_item


def find_groups(items, group=lambda x: x):
    """
    Detects consecutive groups in an iterable.

      >>> list(find_groups([1, 1, 2, 3, 3, 3, 4]))
      [('f', 1), ('l', 1), ('o', 2), ('f', 3), ('i', 3), ('l', 3), ('o', 4)]

    Yields `code, item` for each element in `items`, where `code` is:
    - "f" if this is the first element of a group
    - "i" if this is an interior element of a group
    - "l" if this is the last element in of group
    - "o" if this is the lone element of a group with one element
    """
    last = None
    for i in items:
        g = group(i)
        if last is None:
            # The very first item.
            first = True
        else:
            if g == last[1]:
                # Same group as the previous.
                yield "f" if last[2] else "i", last[0]
                first = False
            else:
                # New group.
                yield "o" if last[2] else "l", last[0]
                first = True
        last = i, g, first
    if last is not None:
        yield "o" if last[2] else "l", last[0]


def take_last(iterable):
    """
    Returns the last element.
    """
    i = iter(iterable)
    e = next(i)
    for e in i:
        pass
    return e


# -------------------------------------------------------------------------------

# FIXME: Elsewhere


def ensure_incl(obj):
    if obj is None:
        return (True, False)
    if obj in (True, False):
        return (obj, obj)
    begin, end = obj
    return bool(begin), bool(end)


def range(start, end, step=1, *, incl=None):
    """
    Generates values starting at `start` that are less than `end`, incrementing
    by `step` for each.
    """
    incl_start, incl_end = ensure_incl(incl)
    val = start
    if not incl_start:
        val += step
    while val < end or (incl_end and val == end):
        yield val
        val += step


def chunks(items, size):
    """
    Returns `items` in chunks of `size`.

      >>> list(chunks("foobar", 2))
      [['f', 'o'], ['o', 'b'], ['a', 'r']]

    Unlike `grouper`, no padding.

      >>> list(chunks([1, 2, 3, 4, 5, 6, 7], 3))
      [[1, 2, 3], [4, 5, 6], [7]]

    """
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


def join_truncated(max, items, sep=" ", ellipsis="…"):
    """
    Joins up to `max` of `items` with `sep`.
    """
    items = list(items)
    result = sep.join(items[:max])
    if len(items) > max:
        result += ellipsis + f" ({len(items)} items)"
    return result


# -------------------------------------------------------------------------------


class PeekIter:
    """
    Iterator wrapper that supports arbitrary push back and peek ahead.
    """

    def __init__(self, iterable):
        self.__iter = iter(iterable)
        self.__items = deque()

    def __iter__(self):
        # FIXME: Sloppy.
        return self

    def __next__(self):
        try:
            return self.__items.popleft()
        except IndexError:
            return next(self.__iter)

    @property
    def is_done(self):
        """
        True if the iterator is exhausted.
        """
        if len(self.__items) > 0:
            return False
        else:
            try:
                self.__items.append(next(self._iter))
            except StopIteration:
                return True
            else:
                return False

    def push(self, item):
        """
        Pushes an `item` to the front of the iterator so that it is next.
        """
        self.__items.appendleft(item)

    def peek(self, ahead=0):
        """
        Returns a future item from the iterator, without advancing.

        @param ahead
          The number of items to peek ahead; 0 for the next item.
        """
        while len(self.__items) <= ahead:
            self.__items.append(next(self.__iter))
        return self.__items[ahead]
