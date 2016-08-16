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
from .celeriacStorageTask import CeleriacStorageTask
from .helpers import ABC_from_parent
from .storagePool import StoragePool


class CeleriacTask(ABC_from_parent(CeleriacStorageTask)):
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
    get_storage = None

    def __init__(self, *args, **kwargs):
        """
        :param args: args for Celery task
        :param kwargs: kwargs for Celery task
        """
        super(CeleriacStorageTask, self).__init__(*args, **kwargs)
        self.task_name = None
        self.task_id = None
        self.flow_name = None
        self.parent = None
        # TODO: db connections initialization
        self._db_connections = {}

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

