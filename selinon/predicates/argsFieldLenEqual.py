#!/usr/bin/env python3
# pragma: no cover

from functools import reduce


def argsFieldLenEqual(node_args, key, length):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], node_args)
        return len(val) == length
    except:
        return False
