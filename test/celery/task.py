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


class Task(object):
    def __init__(self, task_name=None, flow_name=None, parent=None, node_args=None, finished=None,
                 retried_count=None):
        # TODO: We could introduce a new SelinonTask and remove this from test/celery since SelinonTask is
        # not Celery.Task anymore
        # In case of instantiating SelinonTaskEnvelope or Dispatcher, we are not passing any arguments
        self._task_name = task_name
        self._flow_name = flow_name
        self._parent = parent
        self._node_args = node_args
        self._retried_count = retried_count
        self._finished = finished
        self._queue = None

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
    def node_args(self):
        return self._node_args

    @property
    def task_id(self):
        return "%s" % id(self)

    @property
    def task_name(self):
        return self._task_name

    @property
    def finished(self):
        return self._finished

    @property
    def queue(self):
        return self._queue

    @queue.setter
    def queue(self, queue):
        self._queue = queue

    def apply_async(self, kwargs, queue):
        # Ensure that SelinonTaskEnvelope kept parameters consistent
        assert self._task_name == kwargs['task_name']
        assert self._flow_name == kwargs['flow_name']
        assert self._parent == kwargs['parent']
        assert self._finished == kwargs['finished']

        self._queue = queue
        self._node_args = kwargs['node_args']
        self._retried_count = kwargs['retried_count']
        return self

    @staticmethod
    def retry(exc, max_retry):
        # ensure that we are raising with max_retry equal to 0 so we will not get into an infinite loop
        assert(max_retry == 0)
        raise exc

