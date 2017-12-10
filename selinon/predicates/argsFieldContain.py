#!/bin/env python3
# pragma: no cover

from functools import reduce


def argsFieldContain(node_args, key, value):
    try:
        val = reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], node_args)
        return value in val
    except:
        return False
