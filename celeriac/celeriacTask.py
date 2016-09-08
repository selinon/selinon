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

import abc
from .storagePool import StoragePool


class CeleriacTask(metaclass=abc.ABCMeta):
    def __init__(self, flow_name, task_name, parent, finished):
        self.flow_name = flow_name
        self.task_name = task_name
        self.parent = parent
        self.finished = finished

    @property
    def storage(self):
        return StoragePool.get_storage_by_task_name(self.task_name)

    def parent_task_result(self, parent_name):
        return StoragePool.retrieve(parent_name, self.parent[parent_name])

    def parent_flow_result(self, flow_name, task_name, index):
        # TODO: can be list
        return StoragePool.retrieve(task_name, self.parent[flow_name][task_name][index])

    @abc.abstractmethod
    def run(self, node_args):
        pass

