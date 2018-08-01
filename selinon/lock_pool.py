#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Global lock pool for shared locks."""

from multiprocessing import Lock


class LockPool:  # pylint: disable=too-few-public-methods
    """Lock pool for shared locks."""

    def __init__(self):
        """Initialize lock-pool."""
        self._locks = {}
        # Instance global lock to defend operations on stored locks
        self._global_lock = Lock()

    def get_lock(self, lock_id):
        """Get lock for resource, exclusively.

        :param lock_id: lock_id for uniquely identify lock
        :return: lock, can be acquired if already taken, if new, always released
        """
        # TODO: we could make it more optimal by storing only locks that are used and release them once unused
        with self._global_lock:
            if not self._locks.get(lock_id):
                ret = Lock()
                self._locks[lock_id] = ret
            else:
                ret = self._locks[lock_id]

        return ret
