#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from selinon.errors import CacheMissError
from selinon.caches import RR
from selinon_test_case import SelinonTestCase


class TestRR(SelinonTestCase):
    """
    Test Random-Replacement Cache
    """
    def test_remove(self):
        cache = RR(max_cache_size=1)

        cache.add("item_id1", "item1", "Task1", "flow1")
        assert cache.get("item_id1", "Task1", "flow1") == "item1"

        cache.add("item_id2", "item2", "Task1", "flow1")
        assert cache.get("item_id2", "Task1", "flow1") == "item2"

        with pytest.raises(CacheMissError):
            cache.get("item_id1", "Task1", "flow1")

    def test_random_remove(self):
        cache = RR(max_cache_size=2)

        cache.add("item_id1", "item1", "Task1", "flow1")
        cache.add("item_id2", "item2", "Task1", "flow1")
        cache.add("item_id3", "item3", "Task1", "flow1")

        with pytest.raises(CacheMissError):
            # at least one should fail
            cache.get("item_id1", "Task1", "flow1")
            cache.get("item_id2", "Task1", "flow1")

        assert cache.get("item_id3", "Task1", "flow1") == "item3"
        assert cache.current_cache_size == 2
