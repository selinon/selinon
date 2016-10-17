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
    def __init__(self):
        # In case of instantiating SelinonTaskEnvelope or Dispatcher, we are not passing any arguments
        self.task_name = None
        self.flow_name = None
        self.parent = None
        self.node_args = None
        self.retried_count = None
        self.queue = None
        self.countdown = None
        self.dispatcher_id = None

    @property
    def task_id(self):
        return id(self)

    def apply_async(self, kwargs, queue, countdown=None):
        from selinon.config import Config

        # Ensure that SelinonTaskEnvelope kept parameters consistent
        self.flow_name = kwargs['flow_name']
        self.node_args = kwargs.get('node_args')
        self.parent = kwargs.get('parent')
        self.dispatcher_id = kwargs.get('dispatcher_id')

        # None if we have flow
        self.task_name = kwargs.get('task_name')
        self.retried_count = kwargs.get('retried_count')
        self.countdown = countdown

        self.queue = queue
        Config.get_task_instance.register_node(self)

        return self

    @staticmethod
    def retry(exc, max_retry):
        # ensure that we are raising with max_retry equal to 0 so we will not get into an infinite loop
        assert max_retry == 0
        raise exc

