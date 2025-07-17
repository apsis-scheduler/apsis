import contextlib
from typing import Mapping

from apsis.exc import SchemaError
from .imp import import_fqname, get_type_fqname

# ------------------------------------------------------------------------------

NO_DEFAULT = object()


def to_array(obj):
    return obj if isinstance(obj, list) else [obj]


def to_narray(obj):
    return [] if obj is None else to_array(obj)


@contextlib.contextmanager
def check_schema(jso):
    """
    Wraps a JSO dict in a `pop` function.  On exit, checks for no remaining
    keys.
    """
    copy = dict(jso)

    def pop(key, type=None, default=NO_DEFAULT):
        try:
            value = copy.pop(key)
        except KeyError:
            if default is NO_DEFAULT:
                raise SchemaError(f"missing key: {key}") from None
            else:
                value = default
        else:
            if type is not None:
                value = type(value)
        return value

    try:
        yield pop
    except Exception as exc:
        raise SchemaError(str(exc))

    if len(copy) > 0:
        raise SchemaError(f"unexpected keys: {' '.join(copy)}")


def get_dotted(mapping, key, default=NO_DEFAULT):
    """
    Returns the value for a dotted `key`.

      >>> m = {"foo": {"bif": 10}}
      >>> get_dotted(m, "foo.bif")
      10
      >>> get_dotted(m, "bar")
      Traceback (most recent call last):
      ...
      KeyError: 'bar'
      >>> get_dotted(m, "foo.bof")
      Traceback (most recent call last):
      ...
      KeyError: 'bof'

    """
    m = mapping
    try:
        for part in key.split("."):
            m = m[part]
        return m
    except KeyError:
        if default is NO_DEFAULT:
            raise
        else:
            return default


def set_dotted(mapping, key, value):
    """
    Sets dotted `key` to `value` into a hierarchical `mapping`, creating
    nested mappings as needed.

      >>> m = {"foo": {"bif": 10}}
      >>> set_dotted(m, "foo.bar.baz", 42)
      >>> m
      {'foo': {'bif': 10, 'bar': {'baz': 42}}}

    """
    m = mapping
    *parts, last = key.split(".")
    for part in parts:
        try:
            m = m[part]
        except KeyError:
            m[part] = m = type(mapping)()
    m[last] = value


def expand_dotted_keys(mapping):
    """
    Expands dotted keys recursively.

      >>> expand_dotted_keys({
      ...     "foo": [10, 11],
      ...     "bar.baz": {
      ...         "hip.hop": True,
      ...     },
      ...     "bar.bif.bof": "hello",
      ... })
      {'foo': [10, 11], 'bar': {'baz': {'hip': {'hop': True}}, 'bif': {'bof': 'hello'}}}

    """
    result = {}
    for key, value in mapping.items():
        if isinstance(value, Mapping):
            value = expand_dotted_keys(value)
        set_dotted(result, key, value)
    return type(mapping)(result)


def nkey(name, value):
    return {} if value is None else {name: value}


def ifkey(name, value, default):
    return {} if value == default else {name: value}


# -------------------------------------------------------------------------------


class TypedJso:
    """
    Support for JSON de/serialization of subtypes.

    Mix in to the base class:

        class Base(TypedJso):

            TYPE_NAMES = TypedJso.TypeNames()


    In each concrete subtype:

        class Subtype(Base):

            @classmethod
            def from_jso(cls, jso):
                # deserialize...


            def to_jso(self):
                return {
                    **super().to_jso(),
                    # serialize...
                }

    The concrete subtype is encoded as a "type" field in the JSO, using the
    fully-qualified type name.  Provide shorter type aliases with,

        Base.TYPE_NAMES.set(Subtype, "short_name")

    """

    class TypeNames:
        def __init__(self):
            self.__by_name = {}
            self.__by_type = {}

        def set(self, type, name):
            self.__by_name[name] = type
            self.__by_type[type] = name

        def get_type(self, name):
            try:
                return self.__by_name[name]
            except KeyError:
                try:
                    return import_fqname(name)
                except ImportError as exc:
                    raise LookupError(exc)

        def get_name(self, type):
            try:
                return self.__by_type[type]
            except KeyError:
                return get_type_fqname(type)

    @classmethod
    def from_jso(cls, jso):
        try:
            name = jso.pop("type")

        except KeyError:
            # No type field specified.
            raise SchemaError("missing type")

        else:
            try:
                type = cls.TYPE_NAMES.get_type(name)
            except LookupError as exc:
                raise SchemaError(f"bad type: {exc}")
            if not issubclass(type, cls):
                raise SchemaError(f"type {type} not a {cls}")

        return type.from_jso(jso)

    def to_jso(self):
        return {
            "type": self.TYPE_NAMES.get_name(type(self)),
        }

    def __eq__(self, other):
        return type(other) is type(self) and other.to_jso() == self.to_jso()
