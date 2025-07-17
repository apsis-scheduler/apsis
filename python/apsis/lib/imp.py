"""
Import-related functions.
"""

import sys

# -------------------------------------------------------------------------------


def join(part0, part1):
    return part0 if not part1 else part1 if not part0 else part0 + "." + part1


def import_module(name):
    """
    Imports a module.

    :param name:
      The fully-qualified module name.
    :raise ImportError:
      `name` could not be imported.
    """
    try:
        __import__(name)
    except ImportError:
        raise
    except Exception:
        raise ImportError(name)
    return sys.modules[name]


def getattr_qualname(obj, qualname):
    """
    Looks up a qualified name in (nested) attributes of `obj`.

    Splits `name` at dots, and successively looks up attributes in `obj`.
    """
    if qualname == "":
        return obj
    result = obj
    for part in qualname.split("."):
        result = getattr(result, part)
    return result


def import_fqname(name):
    """
    Looks up a fully qualified name, importing modules as needed.

    :param name:
      A fully-qualified name.
    """
    modname = name
    qualname = ""

    while True:
        try:
            module = import_module(modname)
        except ImportError as exc:
            # Failed to import.  Try removing the last component.
            try:
                modname, part = modname.rsplit(".", 1)
            except ValueError:
                # No module name left.
                raise ImportError(exc)
            else:
                # The last component becomes part of the qualname.
                qualname = join(part, qualname)
        else:
            # Look up the rest of the name in the module.
            return getattr_qualname(module, qualname)


def get_type_fqname(type):
    """
    Returns the fully qualified name of a type.
    """
    return join(type.__module__, type.__qualname__)
