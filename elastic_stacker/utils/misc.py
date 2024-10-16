from typing import Iterable
from collections import defaultdict


def without_keys(d: dict, keys: Iterable[str]):
    """
    Returns the given dict d without the specified keys, which may be nested
    keys separated by dots (e.g. foo.bar.baz). This thin wrapper splits the keys
    at the dots before passing them down to _without_keys where the real work
    happens.
    """
    keys = { tuple(key.split(".")) for key in keys }
    return _without_keys(d, keys)

def _without_keys(d: dict, keys: set[tuple[str]] ):
    """
    Given a nested dict and a set of keys to delete, selectively walk down the
    structure, deleting keys as as they're found. Only walks the parts of the
    tree that are candidates for deletion.

    This is frankly over-engineered, but is about as fast as you can do this;
    the efficiency comes from never walking a part of the tree more than once.
    For example; if I want to delete a.b.c.d.e.f and a.b.c.g.h.i, I still walk
    the tree under a.b.c only once, instead of twice, by first building up a
    list of keys to delete at each next level of the tree.
    """

    delete_map = defaultdict(set)
    for k in keys:
        if len(k) == 1:
            # None means this key should be deleted at this level
            delete_map[k[0]] = None
        elif delete_map[k[0]] != None:
            # if k[0] is present at this level, delete k[1:] at the next level
            delete_map[k[0]].add(k[1:])

    keys = list(d.keys())

    for k in keys:
        if delete_map[k] is None:
            del(d[k])
        elif delete_map[k] and isinstance(d[k], dict):
            d[k] = _without_keys(d[k], delete_map[k])

    return d



