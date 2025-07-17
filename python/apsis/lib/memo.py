import functools

# -------------------------------------------------------------------------------


def memoize_with(memo):
    def memoize(fn):
        @functools.wraps(fn)
        def memoized(*args, **kw_args):
            # FIXME: It would be better to bind to the signature first, to pick
            # up default arguments.
            key = args + tuple(sorted(kw_args.items()))
            try:
                return memo[key]
            except KeyError:
                value = memo[key] = fn(*args, **kw_args)
                return value

        memoized.__memo__ = memo
        return memoized

    return memoize


def memoize(fn):
    """
    Memoizes with a new empty `dict`.
    """
    return memoize_with({})(fn)


class property:
    """
    Simplified version of Python 3.8 `functools.cached_property`.
    """

    def __init__(self, fn):
        self.fn = fn
        self.name = None
        self.__doc__ = fn.__doc__

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
        assert self.name == name

    def __get__(self, self_, _):
        if self_ is None:
            return self
        assert self.name is not None

        try:
            return self_.__dict__[self.name]
        except KeyError:
            val = self.fn(self_)
            self_.__dict__[self.name] = val
            return val
