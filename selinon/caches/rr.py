#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Random replacement cache implementation."""

import random

from selinon import Cache
from selinon.errors import CacheMissError


class RR(Cache):
    """Random replacement cache."""

    def __init__(self, max_cache_size):
        """Initialize cache.

        :param max_cache_size: maximum cache size
        """
        assert max_cache_size >= 0  # nosec

        self.max_cache_size = max_cache_size
        self._cache = {}

    @property
    def current_cache_size(self):
        """Get current cache size.

        :return: current cache size
        """
        return len(list(self._cache.keys()))

    def __repr__(self):
        """Representation of cache for logs/debug.

        :return: a string representation of this cache
        """
        return "%s(%s)" % (self.__class__.__name__, list(self._cache.keys()))

    def add(self, item_id, item, task_name=None, flow_name=None):
        """Add item to cache.

        :param item_id: item id under which item should be referenced
        :param item: item itself
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        """
        if item_id in self._cache:
            return

        while self.current_cache_size + 1 > self.max_cache_size and self.current_cache_size > 0:
            to_remove = random.choice(list(self._cache.keys()))  # nosec
            del self._cache[to_remove]

        if self.max_cache_size > 0:
            self._cache[item_id] = item

    def get(self, item_id, task_name=None, flow_name=None):
        """Get item from cache.

        :param item_id: item id under which the item is stored
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        :return: item itself
        """
        if item_id not in self._cache:
            raise CacheMissError()

        return self._cache[item_id]
