#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from selinon.errors import CacheMissError
from selinon.caches import (FIFO, LIFO, LRU, MRU, RR)
from selinon_test_case import SelinonTestCase

# Available caches that should be tested
_CACHE_TYPES = [
    FIFO,
    LIFO,
    LRU,
    MRU,
    RR
]


@pytest.mark.parametrize("cache_cls", _CACHE_TYPES)
class TestCache(SelinonTestCase):
    """
    Generic cache test cases
    """
    @staticmethod
    def _item_id2item(i):
        return "x%d" % i

    def test_empty_miss(self, cache_cls):
        cache = cache_cls(max_cache_size=3)

        with pytest.raises(CacheMissError):
            cache.get("item_id", "Task1", "flow1")

    def test_zero_items(self, cache_cls):
        cache = cache_cls(max_cache_size=0)

        with pytest.raises(CacheMissError):
            cache.get("item_id1", "Task1", "flow1")

        cache.add("item_id1", "item", "Task1", "flow1")

        with pytest.raises(CacheMissError):
            assert cache.get("item_id1", "Task1", "flow1") == "item"

    def test_one_item(self, cache_cls):
        cache = cache_cls(max_cache_size=1)

        cache.add("item_id1", "item", "Task1", "flow1")
        assert cache.get("item_id1", "Task1", "flow1") == "item"

    def test_two_items(self, cache_cls):
        cache = cache_cls(max_cache_size=2)

        cache.add("item_id1", 1, "Task1", "flow1")
        assert cache.get("item_id1", "Task1", "flow1") == 1

        cache.add("item_id2", 2, "Task1", "flow1")
        assert cache.get("item_id2", "Task1", "flow1") == 2

    def test_multiple_items(self, cache_cls):
        item_count = 16
        cache = cache_cls(max_cache_size=item_count)

        for item_id in range(item_count):
            cache.add(item_id, self._item_id2item(item_id), "Task1", "flow1")
            assert cache.get(item_id, "Task1", "flow1") == self._item_id2item(item_id)

        for item_id in range(item_count - 1, -1, -1):
            assert cache.get(item_id, "Task1", "flow1") == self._item_id2item(item_id)
