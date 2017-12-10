#!/bin/env python3
# pragma: no cover

from functools import reduce


def fieldInt(message, key):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], message)
        return isinstance(val, int)
    except:
        return False
