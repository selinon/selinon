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

from selinonTestCase import SelinonTestCase
from selinon import SystemState


class TestFlow(SelinonTestCase):
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
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': ['flow2'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}],
            'flow2': []
        }
        queues = {
            'Task1': 'mytask1queue',
            'Task2': 'mytask2queue',
        }
        dispatcher_queue = 'mydispatcher'
        self.init(edge_table, task_queues=queues, dispatcher_queue=dispatcher_queue)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('flow2', self.instantiated_flows)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # check task's queue configuration
        task1 = self.get_task('Task1')
        self.assertEqual(task1.queue, queues.get(task1.task_name))

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('flow2', self.instantiated_flows)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has finished
        task1_result = [1, 2, 3, 4]
        self.set_finished(task1, task1_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        flow2 = self.get_flow('flow2')

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(flow2.node_args)
        self.assertEqual(flow2.flow_name, 'flow2')
        self.assertIsNone(flow2.parent)
        self.assertIsNone(flow2.state)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertNotIn('Task2', self.instantiated_tasks)
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
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(flow2.node_args)
        self.assertEqual(flow2.flow_name, 'flow2')
        self.assertIsNone(flow2.parent)
        self.assertIsNone(flow2.state)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 1)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # check flow queue propagation
        flow2 = self.get_flow('flow2')
        self.assertEqual(flow2.queue, dispatcher_queue)

        # flow2 has finished
        self.set_finished(flow2, {'TaskSubflow': [1]})

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        flow2 = self.get_flow('flow2')

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(flow2.node_args)
        self.assertEqual(flow2.flow_name, 'flow2')
        self.assertIsNone(flow2.parent)
        self.assertIsNone(flow2.state)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task2', self.instantiated_tasks)
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
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 2)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertEqual(task2.node_args, task1_result)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task2', self.instantiated_tasks)
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
        # We are propagating finished, so we should inspect parent
        #
        edge_table = {
            'flow1': [{'from': ['flow2', 'Task2'], 'to': ['TaskX'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true}],
            'flow2': [],
            'flow3': []
        }
        self.init(edge_table, propagate_finished={'flow1': True})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertNotIn('flow2', self.instantiated_flows)

        # Task1 and Task2 have finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, None)

        task2 = self.get_task('Task2')
        self.set_finished(task2, None)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIn('flow2', self.instantiated_flows)

        # Create flow3 manually
        flow3 = self.get_task_instance('flow3', None, None, None, None, None)

        flow2 = self.get_flow('flow2')
        self.set_finished(flow2)
        flow2_result = {'finished_nodes': {'flow3': [flow3.task_id],
                                           'Task2': ['<task2-id1>'],
                                           'Task3': ['<task3-id1>', '<task3-id2>']},
                        'failed_nodes': {}
                        }
        self.set_finished(flow2, flow2_result)

        flow3_result = {'finished_nodes':
                            {'Task2': ['<task2-id2>'], 'Task4': ['<task4-id1>', '<task4-id2>']},
                        'failed_nodes': {}
                        }
        self.set_finished(flow3, flow3_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)

        task_x = self.get_task('TaskX')

        self.assertIn('flow2', task_x.parent)

        task_x_parent = {'Task2': task2.task_id,
                         'flow2': {'Task2': ['<task2-id1>'],
                                   'Task3': ['<task3-id1>', '<task3-id2>'],
                                   'flow3': {'Task2': ['<task2-id2>'],
                                             'Task4': ['<task4-id1>', '<task4-id2>'],
                                             }
                                   }
                         }
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
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2'], 'condition': self.cond_true}],
            'flow2': []
        }
        self.init(edge_table, propagate_node_args={'flow1': True})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('flow2', self.instantiated_flows)

        flow2 = self.get_flow('flow2')
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
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2', 'flow3'], 'condition': self.cond_true}],
            'flow2': [],
            'flow3': []
        }
        self.init(edge_table, propagate_node_args={'flow1': ['flow2']})

        node_args = {'foo': 'bar'}
        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, node_args)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('flow3', self.instantiated_flows)

        flow2 = self.get_flow('flow2')
        self.assertEqual(flow2.node_args, node_args)

        flow3 = self.get_flow('flow3')
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
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true}],
            'flow2': []
        }
        self.init(edge_table, propagate_parent={'flow1': True})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertNotIn('flow2', self.instantiated_flows)
        self.assertIn('Task1', self.instantiated_tasks)

        task1 = self.get_task('Task1')
        self.set_finished(task1, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('flow2', self.instantiated_flows)

        flow2 = self.get_flow('flow2')

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
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2', 'flow3'], 'condition': self.cond_true}],
            'flow2': [],
            'flow3': []
        }
        self.init(edge_table, propagate_parent={'flow1': ['flow2']}, propagate_node_args={'flow1': True})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('flow2', self.instantiated_flows)
        self.assertNotIn('flow3', self.instantiated_flows)

        task1 = self.get_task('Task1')
        self.set_finished(task1, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('flow3', self.instantiated_flows)

        flow2 = self.get_flow('flow2')
        self.assertEqual(flow2.node_args, 0xDEADBEEF)
        self.assertIn('Task1', flow2.parent)
        self.assertEqual(flow2.parent['Task1'], task1.task_id)

        flow3 = self.get_flow('flow3')
        self.assertEqual(flow3.node_args, 0xDEADBEEF)
        self.assertIsNone(flow3.parent)
