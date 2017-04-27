#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################
"""Base class for Cache interface."""

import abc


class Cache(metaclass=abc.ABCMeta):
    """Base class for Cache classes."""

    @abc.abstractmethod
    def add(self, item_id, item, task_name=None, flow_name=None):
        """Add item to cache.

        :param item_id: item id under which item should be referenced
        :param item: item itself
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        """
        pass

    @abc.abstractmethod
    def get(self, item_id, task_name=None, flow_name=None):
        """Get item from cache.

        :param item_id: item id under which the item is stored
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        :return: item itself
        """
        pass
