#!/usr/bin/env python3
# pragma: no cover

import os


def envEqual(env, value):
    if env not in os.environ:
        return False
    return os.environ[env] == value
