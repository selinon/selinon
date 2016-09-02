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

from .storagePool import StoragePool


class CeleriacTask(object):
    def __init__(self, flow_name, task_name, parent):
        self.flow_name = flow_name
        self.task_name = task_name
        self.parent = parent

    def parent_task_result(self, parent_name):
        return StoragePool.retrieve(self.flow_name, parent_name, self.parent[parent_name])

    def parent_flow_result(self, flow_name, task_name, index):
        return StoragePool.retrieve(flow_name, task_name, self.parent[flow_name][task_name][index])

    def parent_all_results(self):
        ret = {}

        for task in self.parent.items():
            # task[0] is task_name/flow_name
            # task[1] is id or dict (list in case of flow)
            if isinstance(task[1], dict):
                for task_name, task_id in task[1].items():
                    if task_name not in ret:
                        ret[task_name] = []
                    ret[task_name].append(StoragePool.retrieve(self.flow_name, task_name, task_id))
            else:
                if task[0] not in ret:
                    ret[task[0]] = []
                ret[task[0]].append(StoragePool.retrieve(self.flow_name, task[0], task[1]))

        return ret

    def execute(self, node_args):
        pass

