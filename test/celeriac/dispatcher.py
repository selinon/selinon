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

from getTaskInstance import GetTaskInstance
from celery import Task


class Dispatcher(Task):
    def __init__(self):
        self._flow_name = None
        self._node_args = None
        self._retry = None
        self._state = None
        self._parent = None

    @property
    def flow_name(self):
        return self._flow_name

    @property
    def node_args(self):
        return self._node_args

    @property
    def state(self):
        return self._state

    @property
    def parent(self):
        return self._parent

    def delay(self, flow_name, node_args=None, retry=None, state=None, parent=None):
        self._flow_name = flow_name
        self._node_args = node_args
        self._retry = retry
        self._state = state
        self._parent = parent
        GetTaskInstance.register_flow(self)
        return self
