#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################
"""
Base class for Cache interface
"""

import abc


class Cache(metaclass=abc.ABCMeta):
    """
    Base class for Cache classes
    """
    @abc.abstractmethod
    def add(self, item_id, item, task_name=None, flow_name=None):
        """
        Add item to cache

        :param item_id: item id under which item should be referenced
        :param item: item itself
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        """
        pass

    @abc.abstractmethod
    def get(self, item_id, task_name=None, flow_name=None):
        """
        Get item from cache

        :param item_id: item id under which the item is stored
        :param task_name: name of task that result should/shouldn't be cached, unused when caching Celery's AsyncResult
        :param flow_name: name of flow in which task was executed, unused when caching Celery's AsyncResult
        :return: item itself
        """
        pass
