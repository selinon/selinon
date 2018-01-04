#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Most-Recently-Used cache implementation."""

from .lru import LRU


class MRU(LRU):
    """Most-Recently-Used - implementation based on LRU."""

    def _clean_cache(self):
        """Trim cache."""
        while self.current_cache_size + 1 > self.max_cache_size and self.current_cache_size > 0:
            self._remove_record(self._record_tail)
