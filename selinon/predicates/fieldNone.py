#!/bin/env python3
# pragma: no cover

from functools import reduce


def fieldNone(message, key):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], message)
        return val is None
    except:
        return False
