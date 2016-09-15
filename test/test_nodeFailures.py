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

from celeriac import FlowError
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
        Config.task_queues = QueueMock()
        Config.dispatcher_queue = QueueMock()
        Config.strategy_function = strategy_function

    def test_single_failure_flow_fail(self):
        #
        # flow1:
        #
        #     Task1 X
        #       |
        #       |
        #     Task2
        #
        # Note:
        #   Task1 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {}, 'fallback': []}}
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has failed
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        with self.assertRaises(FlowError):
            system_state = SystemState(id(self), 'flow1', state=state_dict,
                                       node_args=system_state.node_args)
            system_state.update()

        self.assertNotIn('Task2', get_task_instance.tasks)

    def test_single_failure_flow_fallback_true(self):
        #
        # flow1:
        #
        #     Task1 X .... True
        #       |
        #       |
        #     Task2
        #
        # Note:
        #   Task1 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {}, 'fallback': True}}

        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has failed
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

    def test_single_failure_flow_fallback_start(self):
        #
        # flow1:
        #
        #     Task1 X .... Task3
        #       |            |
        #       |            |
        #     Task2        Task4
        #
        # Note:
        #   Task1 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true},
                      {'from': ['Task3'], 'to': ['Task4'], 'condition': _cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {}, 'fallback': ['Task3']}}

        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has failed
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        task3 = get_task_instance.task_by_name('Task3')[0]
        self.assertEqual(task3.node_args, node_args)

    def test_single_failure_flow_fallback_start2(self):
        #
        # flow1:
        #
        #     Task1 X .... Task3, Task4
        #       |
        #       |
        #     Task2
        #
        #     Task3      Task4
        #       |          |
        #       |          |
        #     Task5      Task6
        #
        # Note:
        #   Task1 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true},
                      {'from': ['Task3'], 'to': ['Task5'], 'condition': _cond_true},
                      {'from': ['Task4'], 'to': ['Task6'], 'condition': _cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {}, 'fallback': ['Task3', 'Task4']}}

        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has failed
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task3', get_task_instance.tasks)
        self.assertIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        task3 = get_task_instance.task_by_name('Task3')[0]
        task4 = get_task_instance.task_by_name('Task4')[0]

        self.assertEqual(task3.node_args, node_args)
        self.assertEqual(task4.node_args, node_args)

        # Now let's finish Task3 and Task4 to see that they correctly continue
        AsyncResult.set_finished(task3.task_id)
        AsyncResult.set_finished(task4.task_id)

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task3', get_task_instance.tasks)
        self.assertIn('Task4', get_task_instance.tasks)
        self.assertIn('Task5', get_task_instance.tasks)
        self.assertIn('Task6', get_task_instance.tasks)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 3)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertIn(2, state_dict['waiting_edges'])
        self.assertIn(3, state_dict['waiting_edges'])
        self.assertIn('Task3', state_dict['finished_nodes'])
        self.assertIn('Task4', state_dict['finished_nodes'])

        # Finish the flow!
        task5 = get_task_instance.task_by_name('Task5')[0]
        task6 = get_task_instance.task_by_name('Task6')[0]

        AsyncResult.set_finished(task5.task_id)
        AsyncResult.set_finished(task6.task_id)

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        self.assertIsNone(retry)

    def test_single_failure_flow_fallback_to_flow(self):
        #
        # flow1:
        #
        #     Task1 X .... flow2
        #       |
        #       |
        #     Task2
        #
        # Note:
        #   Task1 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': _cond_true}],
            'flow2': [{'from': [], 'to': ['Task3'], 'condition': _cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {}, 'fallback': ['flow2']}}
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertNotIn('flow2', get_task_instance.flows)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has failed
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

    def test_singe_failure_flow_fallback(self):
        #
        # flow1:
        #
        #     flow2 X ... Task2
        #       |
        #       |
        #     Task1
        #
        # Note:
        #   flow2 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['flow2'], 'to': ['Task1'], 'condition': _cond_true},
                      {'from': [], 'to': ['flow2'], 'condition': _cond_true}],
            'flow2': []
        }
        failures = {
            'flow1': {'flow2': {'next:': {}, 'fallback': ['Task2']}}
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task1', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # flow2 has failed
        flow2 = get_task_instance.flow_by_name('flow2')[0]
        AsyncResult.set_failed(flow2.task_id)
        flow_info = {'finished_nodes': {'Task1': ['id_task1']}, 'failed_nodes': {'Task2': ['id_task21']}}
        AsyncResult.set_result(flow2.task_id, FlowError(flow_info))

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

    def test_two_failures_no_fallback(self):
        #
        # flow1:
        #
        #    Task1 X     Task2 X
        #       |           |
        #       |           |
        #    Task3        Task4
        #
        # Note:
        #   Task1 will fail, then Task2 will fail
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task3'], 'condition': _cond_true},
                      {'from': ['Task2'], 'to': ['Task4'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': _cond_true}],
        }
        failures = {
            'flow1': {'Task1': {'next': {'Task2': {'next': {}, 'fallback': []}}, 'fallback': []},
                      'Task2': {'next': {'Task1': {'next': {}, 'fallback': []}}, 'fallback': []}
                     }
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])

        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertIn('Task1', state_dict['failed_nodes'])

        # No change so far
        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertIn('Task1', state_dict['failed_nodes'])

        # Task2 has failed
        task2 = get_task_instance.task_by_name('Task2')[0]
        AsyncResult.set_failed(task2.task_id)
        AsyncResult.set_result(task2.task_id, KeyError("Some exception raised"))

        with self.assertRaises(FlowError):
            system_state = SystemState(id(self), 'flow1', state=state_dict,
                                       node_args=system_state.node_args)
            system_state.update()

    def test_two_failures_fallback_start(self):
        #
        # flow1:
        #
        #    Task1 X     Task2 X
        #       |           |
        #       |           |
        #    Task3        Task4
        #
        # Note:
        #   Task1 will fail, then Task2 will fail; fallback for Task1, Task2 is Task5
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task3'], 'condition': _cond_true},
                      {'from': ['Task2'], 'to': ['Task4'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': _cond_true}],
        }
        failures = {
            'flow1': {'Task1': {'next': {'Task2': {'next': {}, 'fallback': ['Task5']}}, 'fallback': []},
                      'Task2': {'next': {'Task1': {'next': {}, 'fallback': ['Task5']}}, 'fallback': []}
                      }
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])

        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_failed(task1.task_id)
        AsyncResult.set_result(task1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertIn('Task1', state_dict['failed_nodes'])

        # Task2 has failed
        task2 = get_task_instance.task_by_name('Task2')[0]
        AsyncResult.set_failed(task2.task_id)
        AsyncResult.set_result(task2.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertNotIn('Task1', state_dict['failed_nodes'])
        self.assertNotIn('Task2', state_dict['failed_nodes'])

    def test_single_failure_finish_wait(self):
        #
        # flow1:
        #
        #    Task1       Task2 X
        #       |           |
        #       |           |
        #    Task3        Task4
        #
        # No fallback defined for Task2
        #
        # Note:
        #   Task2 will fail, no fallback defined, Dispatcher should wait for Task1 to finish and
        #   raise FlowError exception
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task3'], 'condition': _cond_true},
                      {'from': ['Task2'], 'to': ['Task4'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': _cond_true}],
        }
        failures = {
            'flow1': {'Task1': {'next': {'Task2': {'next': {}, 'fallback': []}}, 'fallback': []},
                      'Task2': {'next': {'Task1': {'next': {}, 'fallback': []}}, 'fallback': []}
                      }
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])

        # Task2 has failed
        task2 = get_task_instance.task_by_name('Task2')[0]
        AsyncResult.set_failed(task2.task_id)
        AsyncResult.set_result(task2.task_id, ValueError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertIn('Task2', state_dict['failed_nodes'])

        # No change so far, still wait
        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', get_task_instance.tasks)
        self.assertIn('Task2', get_task_instance.tasks)
        self.assertNotIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)
        self.assertNotIn('Task5', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertIn('Task2', state_dict['failed_nodes'])

        # Task1 has finished successfully
        task1 = get_task_instance.task_by_name('Task1')[0]
        AsyncResult.set_finished(task1.task_id)
        AsyncResult.set_result(task1.task_id, 0)

        # Wait for Task3
        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIn('Task3', get_task_instance.tasks)
        self.assertNotIn('Task4', get_task_instance.tasks)

        # Task3 has finished successfully
        task3 = get_task_instance.task_by_name('Task3')[0]
        AsyncResult.set_finished(task3.task_id)
        AsyncResult.set_result(task3.task_id, 0)

        with self.assertRaises(FlowError):
            system_state = SystemState(id(self), 'flow1', state=state_dict,
                                       node_args=system_state.node_args)
            system_state.update()

    def test_propagate_finished_in_failure(self):
        #
        # flow1:
        #
        #    flow2 X      flow4 X    Task1_f1    Task2_f1 X   flow5
        #       |           |           |         |             |
        #        ................................................
        #                             |
        #                           TaskX
        #
        # flow2:
        #
        #     Task1_f2    Task2_f2 X   Task3_f2   flow3
        #
        # flow3:
        #
        #     Task1_f3    Task2_f3 X   Task3_f3 X
        #
        # flow4:
        #
        #     Task1_f4    Task2_f4 X   Task3_f4
        #
        # flow5:
        #     Task1_f5    Task2_f5 X
        #
        # Note:
        #  All flows fail, we inspect finished propagation in flow1 in case of failure. Tasks marked with X will fail.
        #
        get_task_instance = GetTaskInstance()
        edge_table = {
            'flow1': [{'from': ['flow2', 'flow5', 'flow4', 'Task1_f1', 'Task2_f1'], 'to': ['TaskX'], 'condition': _cond_true},
                      {'from': [], 'to': ['Task1_f1', 'Task2_f1', 'flow2', 'flow4', 'flow5'], 'condition': _cond_true}],
            'flow2': [{'from': [], 'to': ['Task1_f2', 'Task2_f2', 'Task3_f2', 'flow3'], 'condition': _cond_true}],
            'flow3': [{'from': [], 'to': ['Task1_f3', 'Task2_f3', 'Task3_f3'], 'condition': _cond_true}],
            'flow4': [{'from': [], 'to': ['Task1_f4', 'Task2_f4', 'Task3_f4'], 'condition': _cond_true}],
            'flow5': [{'from': [], 'to': ['Task1_f5', 'Task2_f5'], 'condition': _cond_true}]
        }
        is_flow = IsFlow(edge_table.keys())
        nowait_nodes = dict.fromkeys(edge_table.keys(), [])
        failures = {
            'flow1': {
                'Task2_f1': {
                    'next': {
                        'flow2': {
                            'next': {
                                'flow4': {
                                    'next': {},
                                    'fallback': ['TaskX']
                                },
                                'fallback': []
                            },
                            'fallback': []
                        },
                        'fallback': []
                    },
                    'fallback': []
                },
                'fallback': []
            },
            'fallback': []
        }
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes, {'flow1': True}, {}, {})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertIn('flow4', get_task_instance.flows)
        self.assertIn('flow5', get_task_instance.flows)
        self.assertIn('Task1_f1', get_task_instance.tasks)
        self.assertIn('Task2_f1', get_task_instance.tasks)
        self.assertNotIn('flow3', get_task_instance.flows)

        # Task1_f1 have finished
        task1_f1 = get_task_instance.task_by_name('Task1_f1')[0]
        AsyncResult.set_finished(task1_f1.task_id)
        AsyncResult.set_result(task1_f1.task_id, 0)

        # Task2_f1 has failed
        task2_f1 = get_task_instance.task_by_name('Task2_f1')[0]
        AsyncResult.set_failed(task2_f1.task_id)
        AsyncResult.set_result(task2_f1.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertNotIn('flow3', get_task_instance.flows)

        # flow2

        system_state = SystemState(id(self), 'flow2')
        retry = system_state.update()
        state_dict_2 = system_state.to_dict()
        flow2 = get_task_instance.flow_by_name('flow2')[0]

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1_f2', get_task_instance.tasks)
        self.assertIn('Task2_f2', get_task_instance.tasks)
        self.assertIn('Task3_f2', get_task_instance.tasks)
        self.assertIn('flow3', get_task_instance.flows)

        # Task1_f2, Task3_f2 have finished
        task1_f2 = get_task_instance.task_by_name('Task1_f2')[0]
        AsyncResult.set_finished(task1_f2.task_id)
        AsyncResult.set_result(task1_f2.task_id, 1)

        task3_f2 = get_task_instance.task_by_name('Task3_f2')[0]
        AsyncResult.set_finished(task3_f2.task_id)
        AsyncResult.set_result(task3_f2.task_id, 2)

        # Task2_f2 has failed
        task2_f2 = get_task_instance.task_by_name('Task2_f2')[0]
        AsyncResult.set_failed(task2_f2.task_id)
        AsyncResult.set_result(task2_f2.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow2', state=state_dict_2, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict_2 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)

        # flow 3

        system_state = SystemState(id(self), 'flow3')
        retry = system_state.update()
        state_dict_3 = system_state.to_dict()
        flow3 = get_task_instance.flow_by_name('flow3')[0]

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1_f3', get_task_instance.tasks)
        self.assertIn('Task2_f3', get_task_instance.tasks)
        self.assertIn('Task3_f3', get_task_instance.tasks)

        # Task1_f3 has finished
        task1_f3 = get_task_instance.task_by_name('Task1_f3')[0]
        AsyncResult.set_finished(task1_f3.task_id)
        AsyncResult.set_result(task1_f3.task_id, 3)

        # Task2_f3, Task2_f3 have failed
        task2_f3 = get_task_instance.task_by_name('Task2_f3')[0]
        AsyncResult.set_failed(task2_f3.task_id)
        AsyncResult.set_result(task2_f3.task_id, KeyError("Some exception raised"))

        task3_f3 = get_task_instance.task_by_name('Task3_f3')[0]
        AsyncResult.set_failed(task3_f3.task_id)
        AsyncResult.set_result(task3_f3.task_id, KeyError("Some exception raised"))

        try:
            system_state = SystemState(id(self), 'flow3', state=state_dict_3, node_args=system_state.node_args)
            system_state.update()
        except FlowError as exc:
            AsyncResult.set_failed(flow3.task_id)
            AsyncResult.set_result(flow3.task_id, exc)
        else:
            self.assertFalse("Expected FlowError not raised")

        # flow4

        system_state = SystemState(id(self), 'flow4')
        retry = system_state.update()
        state_dict_4 = system_state.to_dict()
        flow4 = get_task_instance.flow_by_name('flow4')[0]

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1_f4', get_task_instance.tasks)
        self.assertIn('Task2_f4', get_task_instance.tasks)
        self.assertIn('Task3_f4', get_task_instance.tasks)

        # Task1_f2, Task3_f2 have finished
        task1_f4 = get_task_instance.task_by_name('Task1_f4')[0]
        AsyncResult.set_finished(task1_f4.task_id)
        AsyncResult.set_result(task1_f4.task_id, 1)

        task3_f4 = get_task_instance.task_by_name('Task3_f4')[0]
        AsyncResult.set_finished(task3_f4.task_id)
        AsyncResult.set_result(task3_f4.task_id, 4)

        # Task2_f4 has failed
        task2_f4 = get_task_instance.task_by_name('Task2_f4')[0]
        AsyncResult.set_failed(task2_f4.task_id)
        AsyncResult.set_result(task2_f4.task_id, KeyError("Some exception raised"))

        try:
            system_state = SystemState(id(self), 'flow4', state=state_dict_4, node_args=system_state.node_args)
            system_state.update()
        except FlowError as exc:
            AsyncResult.set_failed(flow4.task_id)
            AsyncResult.set_result(flow4.task_id, exc)
        else:
            self.assertFalse("Expected FlowError not raised")

        # flow5

        system_state = SystemState(id(self), 'flow5')
        retry = system_state.update()
        state_dict_5 = system_state.to_dict()
        flow5 = get_task_instance.flow_by_name('flow5')[0]

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1_f5', get_task_instance.tasks)
        self.assertIn('Task2_f5', get_task_instance.tasks)

        # Task1_f5, Task2_f5 have finished
        task1_f5 = get_task_instance.task_by_name('Task1_f5')[0]
        AsyncResult.set_finished(task1_f5.task_id)
        AsyncResult.set_result(task1_f5.task_id, 1)
        task2_f5 = get_task_instance.task_by_name('Task2_f5')[0]
        AsyncResult.set_finished(task2_f5.task_id)
        AsyncResult.set_result(task2_f5.task_id, 2)

        system_state = SystemState(id(self), 'flow5', state=state_dict_5, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNone(retry)
        AsyncResult.set_finished(flow5.task_id)
        state_info = {
            'finished_nodes': system_state.to_dict()['finished_nodes'],
            'failed_nodes': system_state.to_dict()['failed_nodes']
        }
        AsyncResult.set_result(flow5.task_id, state_info)

        # Back to flow2, all child finished

        try:
            system_state = SystemState(id(self), 'flow2', state=state_dict_2, node_args=system_state.node_args)
            system_state.update()
        except FlowError as exc:
            AsyncResult.set_failed(flow2.task_id)
            AsyncResult.set_result(flow2.task_id, exc)
        else:
            self.assertFalse("Expected FlowError not raised")

        # finally inspect flow1 result and finished that is propagated at fallback to TaskX

        self.assertNotIn('TaskX', get_task_instance.tasks)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        system_state.update()

        self.assertIn('TaskX', get_task_instance.tasks)

        expected_finished = {
            'flow2': {
                'Task1_f2': [task1_f2.task_id],
                'Task3_f2': [task3_f2.task_id],
                'flow3': {
                    'Task1_f3': [task1_f3.task_id]
                }
            },
            'flow4': {
                'Task1_f4': [task1_f4.task_id],
                'Task3_f4': [task3_f4.task_id]
            },
        }

        expected_parent = {
            'Task2_f1': task2_f1.task_id,
            'flow2': {
                'Task2_f2': [task2_f2.task_id],
                'flow3': {
                    'Task2_f3': [task2_f3.task_id],
                    'Task3_f3': [task3_f3.task_id]
                }
            },
            'flow4': {
                'Task2_f4': [task2_f4.task_id]
            }
        }

        task_x = get_task_instance.task_by_name('TaskX')[0]

        self.assertEqual(expected_parent, task_x.parent)
        self.assertEqual(expected_finished, task_x.finished)
