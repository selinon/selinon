#!/usr/bin/env python3
# pragma: no cover

import os


def envExist(env):
    return env in os.environ
