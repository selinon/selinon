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

import unittest
from getTaskInstance import GetTaskInstance
from queueMock import QueueMock
from strategyMock import strategy_function
from isFlow import IsFlow

from celery.result import AsyncResult
from celeriac.config import Config


class CeleriacTestCase(unittest.TestCase):
    """
    Main class for Celeriac testing
    """
    def setUp(self):
        """
        Clean up all class attributes from previous runs
        """
        AsyncResult.clear()
        GetTaskInstance.clear()
        Config.storage_mapping = {}

    def init(self, edge_table, **kwargs):
        """
        :param edge_table: table dict as defined in YAML file containing edges
        :param kwargs: additional parameters for test configuration
        """
        Config.edge_table = edge_table

        Config.is_flow = IsFlow(kwargs.get('is_flow', list(edge_table.keys())))
        Config.nowait_nodes = kwargs.get('nowait_nodes', dict.fromkeys(edge_table.keys(), []))

        Config.get_task_instance = kwargs.get('get_task_instance', GetTaskInstance())
        Config.failures = kwargs.get('failures', {})
        Config.propagate_finished = kwargs.get('propagate_finished', {})
        Config.propagate_node_args = kwargs.get('propagate_node_args', {})
        Config.propagate_parent = kwargs.get('propagate_parent', {})
        Config.retry_countdown = kwargs.get('retry_countdown', {})
        Config.task_queues = kwargs.get('task_queues', QueueMock())
        Config.dispatcher_queue = kwargs.get('dispatcher_queue', QueueMock())
        Config.strategy_function = kwargs.get('strategy_function', strategy_function)

    @staticmethod
    def cond_true(db, node_args):
        """
        Condition for edge transition

        :return: always true
        """
        return True

    @staticmethod
    def cond_false(db, node_args):
        """
        Condition for edge transition

        :return: always false
        """
        return False

    @property
    def instantiated_tasks(self):
        """
        :return: list of all instantiated tasks
        """
        return Config.get_task_instance.tasks

    @property
    def instantiated_flows(self):
        """
        :return: list of all instantiated flows
        """
        return Config.get_task_instance.flows

    @staticmethod
    def set_finished(node, result=None):
        """
        Mark node (task or flow) as successfully finished

        :param node: node to mark as finished
        :param result: result of node, if None, result is not set
        """
        AsyncResult.set_finished(node.task_id)
        if result is not None:
            AsyncResult.set_result(node.task_id, result)

    @staticmethod
    def set_failed(node, exc=None):
        """
        Mark node (task or flow) as failed

        :param node: node to mark as failed
        :param result: exception (result for Celery) that was raised
        """
        AsyncResult.set_failed(node.task_id)
        AsyncResult.set_result(node.task_id, exc or ValueError("Some unexpected exception in node"))

    def get_task(self, task_name, idx=None):
        """
        Get task by its name

        :param task_name: name of task
        :param idx: index of task in instantiated tasks, if there are more tasks of type, the last is taken
        :return: task
        """
        tasks = Config.get_task_instance.task_by_name(task_name)
        return tasks[idx if idx is not None else -1]

    def get_flow(self, flow_name, idx=None):
        """
        Get flow by its name

        :param flow_name: name of flow
        :param idx: index of flow in instantiated flows, if there are more tasks of type, the last is taken
        :return: flow
        """
        tasks = Config.get_task_instance.flow_by_name(flow_name)
        return tasks[idx if idx is not None else -1]

    @property
    def get_task_instance(self):
        """
        :return: GetTaskInstance in the current context
        """
        return Config.get_task_instance
