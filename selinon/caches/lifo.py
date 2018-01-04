#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Last-In-First-Out cache implementation."""

from .fifo import FIFO


class LIFO(FIFO):
    """Last-In-First-Out cache - based on FIFO implementation."""

    def _clean_cache(self):
        """Trim cache."""
        while self.current_cache_size + 1 > self.max_cache_size and self.current_cache_size > 0:
            latest = self._cache_usage.pop()
            del self._cache[latest]
