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

import unittest

from getTaskInstance import GetTaskInstance
from queueMock import QueueMock
from isFlow import IsFlow
from strategyMock import strategy_function

from celery.result import AsyncResult
from celeriac import SystemState
from celeriac.config import Config


def _cond_true(db, node_args):
    return True


def _conf_false(db, node_args):
    return False


class TestNodeFailures(unittest.TestCase):
    def setUp(self):
        AsyncResult.clear()
        GetTaskInstance.clear()

    @staticmethod
    def init(get_task_instance, is_flow, edge_table, failures, nowait_nodes):
        Config.get_task_instance = get_task_instance
        Config.is_flow = is_flow
        Config.edge_table = edge_table
        Config.failures = failures
        Config.nowait_nodes = nowait_nodes
        Config.propagate_finished = {}
        Config.propagate_node_args = {}
        Config.propagate_parent = {}
        Config.retry_countdown = {}
        Config.task_queues = QueueMock()
        Config.dispatcher_queue = QueueMock()
        Config.strategy_function = strategy_function

    def test_nowait_task(self):
        #
        # flow1:
        #
        #     Task1
        #
        # Note:
        #    Task1 is marked as nowait node
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': _cond_true}],
            'flow2': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = {'flow1': ['Task1']}
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task1', state_dict.get('active_nodes'))
        self.assertEqual(len(state_dict.get('waiting_edges')), 0)

    def test_nowait_flow(self):
        #
        # flow1:
        #
        #     flow2
        #
        # Note:
        #    flow2 is marked as nowait node
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2'], 'condition': _cond_true}],
            'flow2': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = {'flow1': ['flow2']}
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertNotIn('flow2', state_dict.get('active_nodes'))
        self.assertEqual(len(state_dict.get('waiting_edges')), 0)

    def test_nowait_in_flow(self):
        #
        # flow1:
        #
        #         Task1
        #           |
        #       ----------
        #      |          |
        #    Task2      Task3
        #
        # Note:
        #   Task3 finishes before Task2 and Task3 is marked as nowait
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}]
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = {'flow1': ['Task3']}
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task1 has finished
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, None)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertIn('Task3', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertNotIn('Task3', state_dict.get('active_nodes'))
        self.assertIn('Task2', state_dict['active_nodes'][0]['name'])
