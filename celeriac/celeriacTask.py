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
import jsonschema
from celery import Task
from .helpers import ABC_from_parent
from .storagePool import StoragePool


class CeleriacTask(ABC_from_parent(Task)):
    """
    A base class for user defined workers
    """
    # Celery configuration
    ignore_result = False
    abstract = True
    acks_late = True
    track_started = True
    name = None

    # Celeriac specific attributes
    # Note: will be overwritten by configuration
    output_schema_path = None
    output_schema = None
    max_retries = None
    time_limit = None

    def __init__(self, *args, **kwargs):
        """
        :param args: args for Celery task
        :param kwargs: kwargs for Celery task
        """
        super(CeleriacTask, self).__init__(*args, **kwargs)
        self.task_name = None
        self.task_id = None
        self.flow_name = None
        self.parent = None

    @staticmethod
    def parent_result(flow_name, parent):
        task_name = parent.keys()
        task_id = parent.values()

        # parent should be {'Task': '<id>'}, but a single entry
        assert(len(task_name) == 1)
        assert(len(task_id) == 1)

        task_name = task_name[0]

        storage_pool = StoragePool(parent)
        return storage_pool.get(flow_name, task_name)

    @staticmethod
    def parent_all_results(flow_name, parent):
        ret = {}

        storage_pool = StoragePool(parent)
        for task_name in parent.keys():
            ret[task_name] = storage_pool.get(flow_name, task_name)

        return ret

    @classmethod
    def validate_result(cls, result):
        if cls.output_schema is None:
            if cls.output_schema_path is None:
                return

            with open(cls.output_schema_path, "r") as f:
                cls.output_schema = f.read()

        jsonschema.validate(result, cls.output_schema)

    def run(self, task_name, flow_name, parent, args):
        # we are passing args as one argument explicitly for now not to have troubles with *args and **kwargs mapping
        # since we depend on previous task and the result can be anything
        result = self.execute(flow_name, task_name, parent, args)
        self.validate_result(result)

        if self.storage:
            StoragePool.set(flow_name=flow_name, task_name=task_name, task_id=self.task_id, result=result)
        elif not self.storage and result is not None:
            # TODO: make warning that the result is discarded
            pass

    @abc.abstractmethod
    def execute(self, flow_name, task_name, parent, args):
        """
        Celeriac task entrypoint
        :param flow_name: a name of a flow that triggered this task
        :param task_name: name of the task
        :param parent: mapping to parent tasks
        :param args: task arguments
        :return: data that should be stored to a storage
        """
        pass

