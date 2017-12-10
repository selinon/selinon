#!/bin/env python3
# pragma: no cover

from functools import reduce


def fieldExist(message, key):
    try:
        reduce(lambda m, k: m[k], key if isinstance(key, list) else [key], message)
        return True
    except:
        return False
