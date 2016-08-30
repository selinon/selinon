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
from isFlow import IsFlow
from celery.result import AsyncResult
from celeriac import SystemState
from celeriac.storage import DataStorage
from celeriac.config import Config


def _cond_true(x):
    return True


class TestFlow(unittest.TestCase):

    def setUp(self):
        AsyncResult.clear()
        GetTaskInstance.clear()
        Config.storage_mapping = {}

    @staticmethod
    def init(get_task_instance, is_flow, edge_table, failures, nowait_nodes,
             propagate_finished, propagate_node_args, propagate_parent):
        Config.get_task_instance = get_task_instance
        Config.is_flow = is_flow
        Config.edge_table = edge_table
        Config.failures = failures
        Config.nowait_nodes = nowait_nodes
        Config.propagate_finished = propagate_finished
        Config.propagate_node_args = propagate_node_args
        Config.propagate_parent = propagate_parent
        Config.retry_countdown = {}

    def test_simple_flow(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     flow2
        #       |
        #       |
        #     Task2
        #
        # Note:
        #    Result of Task1 is not propagated to flow2, parent are not propagated as well
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['flow2'], 'condition': _cond_true},
                      {'from': ['flow2'], 'to': ['Task2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}],
            'flow2': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes, {'flow1': False}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has finished
        task1 = get_task_instance.task_by_name('Task1')[0]
        task1_result = [1, 2, 3, 4]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, task1_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].node_args)
        self.assertEqual(get_task_instance.flow_by_name('flow2')[0].flow_name, 'flow2')
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].parent)
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].state)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 1)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].node_args)
        self.assertEqual(get_task_instance.flow_by_name('flow2')[0].flow_name, 'flow2')
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].parent)
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].state)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 1)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # flow2 has finished
        flow2 = get_task_instance.flow_by_name('flow2')[0]
        AsyncResult.set_finished(flow2.task_id)
        AsyncResult.set_result(flow2.task_id, {'TaskSubflow': [1]})

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].node_args)
        self.assertEqual(get_task_instance.flow_by_name('flow2')[0].flow_name, 'flow2')
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].parent)
        self.assertIsNone(get_task_instance.flow_by_name('flow2')[0].state)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertIsNone(flow2.node_args)
        self.assertIsNone(flow2.parent)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 2)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 2)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # Task2 has finished
        task2 = get_task_instance.task_by_name('Task2')[0]
        AsyncResult.set_finished(task2.task_id)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertEqual(task2.node_args, task1_result)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 3)
        self.assertEqual(len(state_dict['active_nodes']), 0)

    def test_propagate_finished(self):
        #
        # flow1:
        #
        #     Task1       Task2
        #       |           |
        #     flow2         |
        #       |           |
        #        -----------
        #             |
        #           TaskX
        #
        # flow2:
        #    Not run explicitly, but result finished_nodes is:
        #         {'flow3': [<flow3-id>], 'Task2': [<task2-id1>], 'Task3': [<task3-id1>, <task3-id2>]}
        # flow3
        #    Not run explicitly, but result finished_nodes is:
        #         {'Task2': [<task2-id2>], 'Task4': [<task4-id1>, <task4-id2>]}
        #
        # Since we are propagating finished, TaskX should get parent:
        #      {'Task2': '<task2-id>',
        #       'flow2': {'Task2': [<task2-id1>, task2-id2>],
        #                 'Task3': [<task3-id1>, <task3-id2>],
        #                 'Task4': [<task4-id1>, <task4-id2>]
        #                }
        #      }:
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['flow2', 'Task2'], 'to': ['TaskX'], 'condition': _cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': _cond_true}],
            'flow2': [],
            'flow3': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes, {'flow1': True}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('flow2', get_task_instance.flows)

        # Task1 and Task2 have finished
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, None)

        task2 = get_task_instance.task_by_name('Task2')[0]
        AsyncResult.set_finished(task2.task_id)
        AsyncResult.set_result(task2.task_id, None)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIn('flow2', get_task_instance.flows)

        flow2 = get_task_instance.flow_by_name('flow2')[0]
        AsyncResult.set_finished(flow2.task_id)

        # Create flow3 manually
        flow3 = get_task_instance('flow3', None, None, None, None)
        AsyncResult.set_finished(flow3.task_id)

        flow2_result = {'flow3': [flow3.task_id], 'Task2': ['<task2-id1>'], 'Task3': ['<task3-id1>', '<task3-id2>']}
        AsyncResult.set_result(flow2.task_id, flow2_result)

        flow3_result = {'Task2': ['<task2-id2>'], 'Task4': ['<task4-id1>', '<task4-id2>']}
        AsyncResult.set_result(flow3.task_id, flow3_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)

        task_x = get_task_instance.task_by_name('TaskX')[0]

        # Convert list of ids to set so we are not dependent on positioning
        self.assertIn('flow2', task_x.parent)
        for key, val in task_x.parent['flow2'].items():
            task_x.parent['flow2'][key] = set(val)

        task_x_parent = {'Task2': task2.task_id,
                         'flow2': {'Task2': {'<task2-id1>', '<task2-id2>'},
                                   'Task3': {'<task3-id1>', '<task3-id2>'},
                                   'Task4': {'<task4-id1>', '<task4-id2>'}}}
        self.assertEqual(task_x.parent, task_x_parent)

    def test_propagate_node_args_true(self):
        #
        # flow1:
        #
        #     flow2
        #
        # Note:
        #    Arguments are propagated to flow2
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2'], 'condition': _cond_true}],
            'flow2': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes, {}, {'flow1': True}, {})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('flow2', get_task_instance.flows)

        flow2 = get_task_instance.flow_by_name('flow2')[0]
        self.assertEqual(flow2.node_args, node_args)

    def test_propagate_node_args_flow_name(self):
        #
        # flow1:
        #
        #     flow2  flow3
        #
        # Note:
        #    Arguments are propagated to flow2 but not to flow3
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2', 'flow3'], 'condition': _cond_true}],
            'flow2': [],
            'flow3': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes, {}, {'flow1': ['flow2']}, {})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('flow3', get_task_instance.flows)

        flow2 = get_task_instance.flow_by_name('flow2')[0]
        self.assertEqual(flow2.node_args, node_args)

        flow3 = get_task_instance.flow_by_name('flow3')[0]
        self.assertIsNone(flow3.node_args)

    def test_propagate_parent_true(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     flow2
        #
        # Note:
        #    Parent are propagated to flow2
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': _cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': _cond_true}],
            'flow2': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes, {'flow1': False}, {}, {'flow1': True})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertNotIn('flow2', get_task_instance.flows)
        self.assertIn('Task1', get_task_instance.tasks)

        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('flow2', get_task_instance.flows)

        flow2 = get_task_instance.flow_by_name('flow2')[0]

        self.assertIsNone(flow2.node_args)
        self.assertIn('Task1', flow2.parent)
        self.assertEqual(flow2.parent['Task1'], task1.task_id)

    def test_propagate_parent_flow_name(self):
        #
        # flow1:
        #         Task1
        #           |
        #        -------
        #       |       |
        #     flow2   flow3
        #
        # Note:
        #    Arguments are propagated to flow2 but not to flow3. Result of Task1 is also propagated to flow2 and flow3
        #    as node_args.
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': _cond_true},
                      {'from': ['Task1'], 'to': ['flow2', 'flow3'], 'condition': _cond_true}],
            'flow2': [],
            'flow3': []
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, None, nowait_nodes, {}, {'flow1': True}, {'flow1': ['flow2']})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('flow2', get_task_instance.flows)
        self.assertNotIn('flow3', get_task_instance.flows)

        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('flow3', get_task_instance.flows)

        flow2 = get_task_instance.flow_by_name('flow2')[0]
        self.assertEqual(flow2.node_args, 0xDEADBEEF)
        self.assertIn('Task1', flow2.parent)
        self.assertEqual(flow2.parent['Task1'], task1.task_id)

        flow3 = get_task_instance.flow_by_name('flow3')[0]
        self.assertEqual(flow3.node_args, 0xDEADBEEF)
        self.assertIsNone(flow3.parent)
