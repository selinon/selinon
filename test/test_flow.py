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

import time
import datetime
from selinonTestCase import SelinonTestCase
from selinon import SystemState, Dispatcher


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
        flow2_queue = 'flow2_queue'
        dispatcher_queues = {
            'flow2': flow2_queue,
            'flow1': 'flow1_queue'
        }
        self.init(edge_table, task_queues=queues, dispatcher_queues=dispatcher_queues)

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
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(flow2.node_args)
        self.assertEqual(flow2.flow_name, 'flow2')
        self.assertIsNone(flow2.parent)
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
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(flow2.node_args)
        self.assertEqual(flow2.flow_name, 'flow2')
        self.assertIsNone(flow2.parent)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 1)
        self.assertEqual(len(state_dict['active_nodes']), 1)

        # check flow queue propagation
        flow2 = self.get_flow('flow2')
        self.assertEqual(flow2.queue, flow2_queue)

        # flow2 has finished
        self.set_finished(flow2, {'TaskSubflow': [1]})

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        flow2 = self.get_flow('flow2')

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(flow2.node_args)
        self.assertEqual(flow2.flow_name, 'flow2')
        self.assertIsNone(flow2.parent)
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
        self.assertIsNone(system_state.node_args)
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
        self.assertIsNone(system_state.node_args)
        self.assertIsNone(task2.node_args)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertEqual(state_dict['waiting_edges'][0], 0)
        self.assertEqual(len(state_dict['finished_nodes']), 3)
        self.assertEqual(len(state_dict['active_nodes']), 0)

    def test_throttle(self):
        #
        # flow1:
        #           |
        #         flow2
        #
        # Note:
        #    flow2 should be throttled by 2s in next flow1 run
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2'], 'condition': self.cond_true}],
            'flow2': []
        }
        self.init(edge_table, throttle_flows={'flow2': datetime.timedelta(seconds=2)})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertIn('flow2', self.instantiated_flows)
        self.assertIsNone(self.get_flow('flow2').countdown)

        # Let's sleep to ensure we get less then 2s delay
        time.sleep(0.01)

        # run flow for the second time, we should be postponed by ~2s
        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()

        self.assertIsNotNone(retry)
        self.assertIsNotNone(self.get_flow('flow2').countdown)
        self.assertLess(self.get_flow('flow2').countdown, 2)


