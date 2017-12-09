#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Least-Recently-Used cache implementation."""

from selinon import Cache
from selinon.errors import CacheMissError


class _Record(object):
    """Record that is used in a double-linked list in order to track usage."""

    def __init__(self, item_id, item):  # noqa
        self.item_id = item_id
        self.item = item
        self.previous = None
        self.next = None

    def __repr__(self):  # noqa
        return "<%s>" % self.item_id


class LRU(Cache):
    """Least-Recently-Used cache."""

    def __init__(self, max_cache_size):
        """Initialize cache.

        :param max_cache_size: maximum number of items stored in the cache
        """
        # let's allow zero size
        assert max_cache_size >= 0  # nosec

        self.max_cache_size = max_cache_size
        self._cache = {}

        self._record_head = None
        self._record_tail = None
        self.current_cache_size = 0

    def __repr__(self):
        """Cache representation for logs/debug.

        :return: string representation of cache
        """
        records = []

        record = self._record_head
        while record:
            records.append(record.item_id)
            record = record.previous

        return "%s(%s)" % (self.__class__.__name__, records)

    def _add_record(self, record):
        """Add record to cache, record shouldn't be present in the cache.

        :param record: record to add to cache
        """
        self._cache[record.item_id] = record
        self.current_cache_size += 1

        if not self._record_head:
            self._record_head = record

        if self._record_tail:
            record.next = self._record_tail
            self._record_tail.previous = record

        record.next = self._record_tail
        self._record_tail = record

    def _remove_record(self, record):
        """Remove record from cache, record should be present in the cache.

        :param record: record to be deleted
        """
        del self._cache[record.item_id]
        self.current_cache_size -= 1

        if record.next:
            record.next.previous = record.previous

        if record.previous:
            record.previous.next = record.next

        if record == self._record_tail:
            self._record_tail = record.next

        if record == self._record_head:
            self._record_head = record.previous

        record.next = None
        record.previous = None

    def _clean_cache(self):
        """Trim cache."""
        while self.current_cache_size + 1 > self.max_cache_size and self.current_cache_size > 0:
            self._remove_record(self._record_head)

    def add(self, item_id, item, task_name=None, flow_name=None):
        """Add item to cache.

        :param item_id: item id under which item should be referenced
        :param item: item itself
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        """
        if item_id in self._cache:
            # we mark usage only in get()
            return

        self._clean_cache()

        if self.max_cache_size > 0:
            record = _Record(item_id, item)
            self._add_record(record)

    def get(self, item_id, task_name=None, flow_name=None):
        """Get item from cache.

        :param item_id: item id under which the item is stored
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        :return: item itself
        """
        record = self._cache.get(item_id)

        # this if is safe as we store tuple - we handle even None results
        if not record:
            raise CacheMissError()

        # mark record usage
        self._remove_record(record)
        self._add_record(record)

        return record.item
