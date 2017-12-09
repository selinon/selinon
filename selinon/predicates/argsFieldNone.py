#!/bin/env python3
# pragma: no cover

from functools import reduce


def argsFieldNone(node_args, key):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], node_args)
        return val is None
    except:
        return False
