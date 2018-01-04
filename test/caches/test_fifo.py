#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from selinon.errors import CacheMissError
from selinon.caches import FIFO
from selinon_test_case import SelinonTestCase


class TestFIFO(SelinonTestCase):
    """
    Test First-In-First-Out cache
    """
    @staticmethod
    def _item_id2item(i):
        return "x%s" % i

    def test_one_item_miss(self):
        cache = FIFO(max_cache_size=1)

        cache.add("item_id1", "item1", "Task1", "flow1")
        cache.add("item_id2", "item2", "Task1", "flow1")

        with pytest.raises(CacheMissError):
            cache.get("item_id1", "Task1", "flow1")

    def test_two_items(self):
        cache = FIFO(max_cache_size=2)

        cache.add("item_id1", "item1", "Task1", "flow1")
        cache.add("item_id2", "item2", "Task1", "flow1")
        cache.add("item_id3", "item3", "Task1", "flow1")

        with pytest.raises(CacheMissError):
            cache.get("item_id1", "Task1", "flow1")

    def test_multiple_items(self):
        item_count = 16
        cache = FIFO(max_cache_size=item_count)

        for item_id in range(item_count):
            cache.add(item_id, self._item_id2item(item_id), "Task1", "flow1")

        cache.add(item_count, self._item_id2item(item_count), "Task1", "flow1")

        with pytest.raises(CacheMissError):
            # first out
            cache.get(0, "Task1", "flow2")

    def test_multiple_items2(self):
        item_count = 16
        cache = FIFO(max_cache_size=item_count)

        for item_id in range(item_count):
            cache.add(item_id, self._item_id2item(item_id), "Task1", "flow1")

        for item_id in range(item_count - 1, -1, -1):
            assert cache.get(item_id, "Task1", "flow1") == self._item_id2item(item_id)
