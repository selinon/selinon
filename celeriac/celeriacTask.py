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

import jsonschema
from .storagePool import StoragePool


class CeleriacTask(object):
    def __init__(self, flow_name, task_name, parent):
        self.flow_name = flow_name
        self.task_name = task_name
        self.parent = parent

    def parent_result(self, parent):
        task_name = parent.keys()
        task_id = parent.values()

        # parent should be {'Task': '<id>'}, but a single entry
        assert(len(task_name) == 1)
        assert(len(task_id) == 1)

        task_name = task_name[0]

        storage_pool = StoragePool(parent)
        return storage_pool.get(self.flow_name, task_name)

    def parent_all_results(self, parent):
        ret = {}

        storage_pool = StoragePool(parent)
        for task_name in parent.keys():
            ret[task_name] = storage_pool.get(self.flow_name, task_name)

        return ret

    def execute(self, args):
        pass

