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

from celeriac import FlowError
from getTaskInstance import GetTaskInstance
from isFlow import IsFlow

from celery.result import AsyncResult
from celeriac import SystemState


def _cond_true(x):
    return True


def _conf_false(x):
    return False


class TestNodeFailures(unittest.TestCase):
    def setUp(self):
        AsyncResult.clear()
        GetTaskInstance.clear()

    @staticmethod
    def init(get_task_instance, is_flow, edge_table, failures, nowait_nodes):
        SystemState.get_task_instance = get_task_instance
        SystemState.is_flow = is_flow
        SystemState.edge_table = edge_table
        SystemState.failures = failures
        SystemState.nowait_nodes = nowait_nodes

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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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
        self.assertEqual(task3.args, node_args)

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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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

        self.assertEqual(task3.args, node_args)
        self.assertEqual(task4.args, node_args)

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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', get_task_instance.flows)
        self.assertNotIn('Task1', get_task_instance.tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # flow2 has failed
        flow2 = get_task_instance.flow_by_name('flow2')[0]
        AsyncResult.set_failed(flow2.task_id)
        AsyncResult.set_result(flow2.task_id, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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
        self.init(get_task_instance, is_flow, edge_table, failures, nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertEqual(retry, SystemState._start_retry)
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

        with self.assertRaises(FlowError):
            system_state = SystemState(id(self), 'flow1', state=state_dict,
                                       node_args=system_state.node_args)
            system_state.update()
