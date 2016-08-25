#!/usr/bin/env python
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


class Task(object):
    def __init__(self, task_name=None, flow_name=None, parent=None, args=None, retried_count=None):
        # TODO: We could introduce a new CeleriacTask and remove this from test/celery since CeleriacTask is
        # not Celery.Task anymore
        # In case of instantiating CeleriacTaskEnvelope or Dispatcher, we are not passing any arguments
        self._task_name = task_name
        self._flow_name = flow_name
        self._parent = parent
        self._args = args
        self._retried_count = retried_count

    @property
    def task_name(self):
        return self._task_name

    @property
    def flow_name(self):
        return self._flow_name

    @property
    def parent(self):
        return self._parent

    @property
    def args(self):
        return self._args

    @property
    def task_id(self):
        return "%s" % id(self)

    @property
    def task_name(self):
        return self._task_name

    def delay(self, task_name, flow_name, parent, args, retried_count):
        # Ensure that CeleriacTaskEnvelope kept parameters consistent
        assert(self._task_name == task_name)
        assert(self._flow_name == flow_name)
        assert(self._parent == parent)
        self._args = args
        self._retried_count = retried_count
        return self

    @staticmethod
    def retry(exc, max_retry):
        # ensure that we are raising with max_retry equal to 0 so we will not get into an infinite loop
        assert(max_retry == 0)
        raise exc

