#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""First-In-First-Out cache implementation."""

from collections import deque

from selinon import Cache
from selinon.errors import CacheMissError


class FIFO(Cache):
    """First-In-First-Out cache."""

    def __init__(self, max_cache_size):
        """Instantiate cache.

        :param max_cache_size: maximum number of items in the cache
        """
        assert max_cache_size >= 0  # nosec

        self.max_cache_size = max_cache_size
        self._cache = {}
        # Use deque as we want to do popleft() in O(1)
        self._cache_usage = deque()

    @property
    def current_cache_size(self):
        """Get current cache size.

        :return: length of the current cache
        """
        return len(self._cache_usage)

    def __repr__(self):
        """Representation of the cache for logs/debug.

        :return: a string representation
        """
        return "%s(%s)" % (self.__class__.__name__, list(self._cache_usage))

    def _clean_cache(self):
        """Trim cache."""
        while self.current_cache_size + 1 > self.max_cache_size and self.current_cache_size > 0:
            latest = self._cache_usage.popleft()
            del self._cache[latest]

    def add(self, item_id, item, task_name=None, flow_name=None):
        """Add item to cache.

        :param item_id: item id under which item should be referenced
        :param item: item itself
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        """
        if item_id in self._cache:
            return

        self._clean_cache()

        if self.max_cache_size > 0:
            self._cache[item_id] = item
            self._cache_usage.append(item_id)

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
