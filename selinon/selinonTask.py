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

import abc
from .storagePool import StoragePool


class SelinonTask(metaclass=abc.ABCMeta):
    """
    Base class for user-defined tasks
    """
    def __init__(self, flow_name, task_name, parent, finished):
        """
        :param flow_name: name of flow under which this tasks runs on
        :param task_name: name of task, note it can be aliased since we can have different task name and class name
        :param parent: direct task's predecessors stated in flow dependency
        :param finished: tasks that were finished in parent subflow in case task is fallback from fallback
        """
        self.flow_name = flow_name
        self.task_name = task_name
        self.parent = parent
        self.finished = finished

    @property
    def storage(self):
        """
        :return: tasks's configured storage as stated in YAML config
        """
        return StoragePool.get_storage_by_task_name(self.task_name)

    def parent_task_result(self, parent_name):
        """
        :param parent_name: name of parent task to retrieve result from
        :return: result of parent task
        """
        return StoragePool.retrieve(parent_name, self.parent[parent_name])

    def finished_task_result(self, finished_name):
        """
        :param finished_name: name of finished node in parent subflow
        :return: result of finished task in parent subflow
        """
        # TODO: we should should distinguish sub-subflows here
        return StoragePool.retrieve(finished_name, self.finished[finished_name])

    def parent_flow_result(self, flow_name, task_name, index):
        """
        Get parent subflow results; note that parent flows can return multiple results from task of same type
        because of loops in flows

        :param flow_name: name of parent flow
        :param task_name: name of task in parent flow
        :param index: index of result if more than one subflow was run
        :return: result of task in parent subflow
        """
        # TODO: we should should distinguish sub-subflows here
        return StoragePool.retrieve(task_name, self.parent[flow_name][task_name][index])

    @abc.abstractmethod
    def run(self, node_args):
        """
        Task's entrypoint - user defined computation

        :param node_args: arguments passed to flow/node
        :return: tasks's result that will be stored in database as configured
        """
        pass

