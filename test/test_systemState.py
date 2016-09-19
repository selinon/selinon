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


class TestSystemState(SelinonTestCase):
    def test_simple1(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     Task2
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Run without change at first
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(system_state.node_args)
        self.assertIsNotNone(retry)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, 1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(len(state_dict['active_nodes']), 1)
        self.assertEqual(len(state_dict['finished_nodes']), 1)
        self.assertEqual(len(state_dict['waiting_edges']), 1)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task2', state_dict['finished_nodes'].keys())
        # the result of Task1 should be propagated to Task2
        self.assertEqual(system_state.node_args, 1)

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2, 1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        # All tasks done
        self.assertIsNone(retry)
        self.assertEqual(len(state_dict['active_nodes']), 0)
        self.assertEqual(len(state_dict['finished_nodes']), 2)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())

    def test_2_to_1(self):
        #
        # flow1:
        #
        #    Task1      Task2
        #      |          |
        #       ----------
        #           |
        #         Task3
        #
        edge_table = {
                      'flow1': [{'from': ['Task1', 'Task2'], 'to': ['Task3'], 'condition': self.cond_true},
                                {'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Run without change at first
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task2 has finished
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task1')
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task1', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task3', state_dict['finished_nodes'].keys())
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task1 has finished
        self.set_finished(task1, "some result of task1 that shouldn't be propagated")

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task3')
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task3', state_dict['finished_nodes'].keys())
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # No change so far
        task3 = self.get_task('Task3')

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task3')
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task3', state_dict['finished_nodes'].keys())
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(state_dict['waiting_edges'][0], 0)

        # Task 3 has finished
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task3', state_dict['finished_nodes'].keys())

    def test_2_to_1_separate(self):
        #
        # flow1:
        #
        #    Task1   Task2
        #      \      /
        #       \    /
        #        \  /
        #        Task3
        #
        # Note:
        #   Each time only one task instance finishes
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])

        # Run without change at first
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])

        # Task1 has finished
        self.set_finished(task1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 2)
        self.assertIn(state_dict['active_nodes'][0]['name'], ['Task2', 'Task3'])
        self.assertIn(state_dict['active_nodes'][1]['name'], ['Task2', 'Task3'])
        self.assertIn('Task1', state_dict['finished_nodes'].keys())

        # No change so far
        task3_0 = self.get_task('Task3')

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 2)
        self.assertIn(state_dict['active_nodes'][0]['name'], ['Task2', 'Task3'])
        self.assertIn(state_dict['active_nodes'][1]['name'], ['Task2', 'Task3'])
        self.assertIn('Task1', state_dict['finished_nodes'].keys())

        # Task2 has finished
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 2)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task3')
        self.assertEqual(state_dict['active_nodes'][1]['name'], 'Task3')
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())

        # Again, no change
        task3_1 = self.get_task('Task3')

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 2)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task3')
        self.assertEqual(state_dict['active_nodes'][1]['name'], 'Task3')
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())

        # Now the second instance of Task3 has finished
        self.set_finished(task3_1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task3')
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task3', state_dict['finished_nodes'].keys())

        # The last task - the instance of Task3 has just finished
        self.set_finished(task3_0)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task3']), 2)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())

    def test_2_to_1_separate_2(self):
        #
        # flow1:
        #
        #    Task1   Task2
        #      \      /
        #       \    /
        #        \  /
        #        Task3
        #
        # Note:
        #   Task1 and Task2 finish at the same time
        #   Both instances of Task3 finish at the same time
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])

        # Task1, Task2 has finished
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')
        self.set_finished(task1)
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 2)
        self.assertEqual(state_dict['active_nodes'][0]['name'], 'Task3')
        self.assertEqual(state_dict['active_nodes'][1]['name'], 'Task3')
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())

        # Now both Task3 instances finished
        task3_0 = self.get_task('Task3', 0)
        task3_1 = self.get_task('Task3', 1)
        self.set_finished(task3_0)
        self.set_finished(task3_1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task3']), 2)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())

    def test_3_to_1(self):
        #
        # flow1:
        #
        #    Task1    Task2   Task3
        #      |        |       |
        #       ----------------
        #               |
        #             Task4
        #
        edge_table = {
            'flow1': [{'from': ['Task1', 'Task2', 'Task3'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2', 'Task3'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])

        # Task1 has finished
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')
        task3 = self.get_task('Task3')
        self.set_finished(task1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 2)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task4', self.instantiated_tasks)

        # Task2 and Task3 have finished
        self.set_finished(task2)
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task3', state_dict['finished_nodes'].keys())
        self.assertIn('Task4', self.instantiated_tasks)

        # Wait for Task4
        task4 = self.get_task('Task4')

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('active_nodes')), 1)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task3', state_dict['finished_nodes'].keys())
        self.assertIn('Task4', self.instantiated_tasks)

        # Finish the flow
        self.set_finished(task4)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)
        self.assertEqual(len(state_dict.get('finished_nodes')), 4)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task3', state_dict['finished_nodes'].keys())
        self.assertIn('Task4', state_dict['finished_nodes'].keys())

    def test_3_to_1_fail(self):
        #
        # flow1:
        #
        #    Task1    Task2   Task3
        #      |        |       |
        #       ----------------
        #               |
        #               X
        #               |
        #             Task4
        #
        # Note:
        #   The condition will fail
        #
        edge_table = {
            'flow1': [{'from': ['Task1', 'Task2', 'Task3'], 'to': ['Task4'], 'condition': self.cond_false},
                      {'from': [], 'to': ['Task1', 'Task2', 'Task3'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])

        # Task1, Task2 and Task3 have finished
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')
        task3 = self.get_task('Task3')
        self.set_finished(task1)
        self.set_finished(task2)
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertIn('Task1', state_dict['finished_nodes'].keys())
        self.assertIn('Task2', state_dict['finished_nodes'].keys())
        self.assertIn('Task3', state_dict['finished_nodes'].keys())
        self.assertNotIn('Task4', state_dict['finished_nodes'].keys())

    def test_1_to_2_separate(self):
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
        #   Task3 finishes before Task2
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change first
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task3 has finished
        task3 = self.get_task('Task3')
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change so far
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task2 has finished
        task2= self.get_task('Task2')
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)

    def test_1_to_2(self):
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
        #   Task2 and Task3 finish at the same time
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task2, Task3 have finished
        task2 = self.get_task('Task2')
        task3 = self.get_task('Task3')
        self.set_finished(task2)
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertEqual(system_state.node_args, 0xDEADBEEF)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)

    def test_1_to_3_separate(self):
        #
        # flow1:
        #
        #             Task1
        #               |
        #       ----------------
        #      |        |       |
        #    Task2    Task3   Task4
        #
        # Note:
        #   Child tasks do not finish at the same time
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3', 'Task4'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task1 has finished
        task1_result = {'foo': ['bar']}
        task1 = self.get_task('Task1')
        self.set_finished(task1, task1_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task3 has finished
        task3 = self.get_task('Task3')
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task4 has finished
        task4 = self.get_task('Task4')
        self.set_finished(task4)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 4)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)

    def test_1_to_3(self):
        #
        # flow1:
        #
        #             Task1
        #               |
        #       ----------------
        #      |        |       |
        #    Task2    Task3   Task4
        #
        # Note:
        #   Child tasks finish at the same time
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3', 'Task4'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task1 has finished
        task1_result = {'foo': ['bar']}
        task1 = self.get_task('Task1')
        self.set_finished(task1, task1_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # Task2 has finished
        task2 = self.get_task('Task2')
        task3 = self.get_task('Task3')
        task4 = self.get_task('Task4')
        self.set_finished(task2)
        self.set_finished(task3)
        self.set_finished(task4)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 4)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)

    def test_3_to_3(self):
        #
        # flow1:
        #
        #    Task1    Task2   Task3
        #      |        |       |
        #       ----------------
        #               |
        #               |
        #       ----------------
        #      |        |       |
        #    Task4    Task5   Task6
        #
        # Note:
        #   Task1 and Task2 finish at the same time, Task3 afterwards. Task4, Task5 and Task6 finish at the same time
        #
        edge_table = {
            'flow1': [{'from': ['Task1', 'Task2', 'Task3'], 'to': ['Task4', 'Task5', 'Task6'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2', 'Task3'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertNotIn('Task4', self.instantiated_tasks)
        self.assertNotIn('Task5', self.instantiated_tasks)
        self.assertNotIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertNotIn('Task4', self.instantiated_tasks)
        self.assertNotIn('Task5', self.instantiated_tasks)
        self.assertNotIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # Task1, Task2 have finished
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')
        self.set_finished(task1)
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertNotIn('Task4', self.instantiated_tasks)
        self.assertNotIn('Task5', self.instantiated_tasks)
        self.assertNotIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertNotIn('Task4', self.instantiated_tasks)
        self.assertNotIn('Task5', self.instantiated_tasks)
        self.assertNotIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task3 has finished
        task3 = self.get_task('Task3')
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertIn('Task5', self.instantiated_tasks)
        self.assertIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # Task4, Task5, Task6 have finished
        task4 = self.get_task('Task4')
        task5 = self.get_task('Task5')
        task6 = self.get_task('Task6')
        self.set_finished(task4)
        self.set_finished(task5)
        self.set_finished(task6)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertIn('Task5', self.instantiated_tasks)
        self.assertIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 6)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)

    def test_3_to_3_2(self):
        #
        # flow1:
        #
        #    Task1    Task2   Task3
        #      |        |       |
        #       ----------------
        #               |
        #               |
        #       ----------------
        #      |        |       |
        #    Task4    Task5   Task6
        #
        # Note:
        #   Task1, Task2 and Task3 finish at the same time; Task4, Task5 finish at the same time, then Task6
        #
        edge_table = {
            'flow1': [{'from': ['Task1', 'Task2', 'Task3'], 'to': ['Task4', 'Task5', 'Task6'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2', 'Task3'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertNotIn('Task4', self.instantiated_tasks)
        self.assertNotIn('Task5', self.instantiated_tasks)
        self.assertNotIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # Task1, Task2, Task3 have finished
        task1 = self.get_task('Task1')
        task2 = self.get_task('Task2')
        task3 = self.get_task('Task3')
        self.set_finished(task1)
        self.set_finished(task2)
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertIn('Task5', self.instantiated_tasks)
        self.assertIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 3)

        # Task4 has finished
        task4 = self.get_task('Task4')
        self.set_finished(task4)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertIn('Task5', self.instantiated_tasks)
        self.assertIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 4)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task5, Task6 have finished
        task5 = self.get_task('Task5')
        task6 = self.get_task('Task6')
        self.set_finished(task5)
        self.set_finished(task6)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertIn('Task4', self.instantiated_tasks)
        self.assertIn('Task5', self.instantiated_tasks)
        self.assertIn('Task6', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 6)
        self.assertEqual(len(state_dict.get('active_nodes')), 0)

    def test_1_to_2_recursive(self):
        #
        # flow1:
        #
        #         Task1  <------
        #           |           |
        #       ----------      |
        #      |          |     |
        #    Task2      Task3 --
        #
        # Note:
        #   Task2 finishes before Task3 in the first iteration
        #   Task3 finishes before Task2 in the second iteration
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3'], 'condition': self.cond_true},
                      {'from': ['Task3'], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change so far
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertNotIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        task1 = self.get_task('Task1')
        self.set_finished(task1)
        # There was no Result set for Task1
        with self.assertRaises(KeyError):
            system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
            system_state.update()

        # Task1 has finished
        task1 = self.get_task('Task1')
        task1_result = "some task result"
        self.set_finished(task1, task1_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task3 has finished
        task3 = self.get_task('Task3')
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # The next iteration
        # No change first
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # The second instance of Task1 has finished
        task1_1 = self.get_task('Task1')
        self.set_finished(task1_1)
        # We do not set task result, since it should be propagated from the first run of Task1

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        # There should be a second instance of Task2 and Task3 spawned as well
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task1']), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task1']), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task3 has finished
        task3_1 = self.get_task('Task3')
        self.set_finished(task3_1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 3)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task1']), 2)
        self.assertEqual(len(state_dict['finished_nodes']['Task2']), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 3)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task1']), 2)
        self.assertEqual(len(state_dict['finished_nodes']['Task2']), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task2 has finished
        task2_1 = self.get_task('Task2')
        self.set_finished(task2_1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 3)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task1']), 2)
        self.assertEqual(len(state_dict['finished_nodes']['Task2']), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Once more with no change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertEqual(system_state.node_args, task1_result)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task1')), 3)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task1']), 2)
        self.assertEqual(len(state_dict['finished_nodes']['Task2']), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

    def test_2_to_1_recursive(self):
        #
        # flow1:
        #
        #    Task1      Task2 <-
        #      |          |     |
        #       ----------      |
        #           |           |
        #         Task3   ------
        #
        # Note:
        #   Task1 finishes before Task2 in the first iteration
        #   Task2 finishes before Task1 in the second iteration
        #
        #   Also note that Task3 is run:
        #      With Task1_1 from the first iteration and Task2 from the first iteration
        #      With Task1_1 from the first iteration and Task2 from the second iteration
        #
        edge_table = {
            'flow1': [{'from': ['Task1', 'Task2'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task3'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1', 'Task2'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 0)
        self.assertEqual(len(state_dict.get('active_nodes')), 2)

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertNotIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 1)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change so far
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 1)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Task3 has finished
        task3 = self.get_task('Task3')
        self.set_finished(task3)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        # Newly spawned Task2 has finished, Task3 should be spawned
        task2_1 = self.get_task('Task2')
        self.set_finished(task2_1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        system_state.update()
        state_dict = system_state.to_dict()

        self.assertIsNotNone(retry)
        self.assertIsNone(system_state.node_args)
        self.assertIn('Task1', self.instantiated_tasks)
        self.assertIn('Task2', self.instantiated_tasks)
        self.assertIn('Task3', self.instantiated_tasks)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task2')), 2)
        self.assertEqual(len(self.get_task_instance.task_by_name('Task3')), 2)
        self.assertEqual(len(state_dict.get('waiting_edges')), 2)
        self.assertIn(0, state_dict['waiting_edges'])
        self.assertIn(1, state_dict['waiting_edges'])
        self.assertEqual(len(state_dict.get('finished_nodes')), 3)
        self.assertEqual(len(state_dict['finished_nodes']['Task2']), 2)
        self.assertEqual(len(state_dict.get('active_nodes')), 1)

        task3_1 = self.get_task('Task3')

        # Check parent tasks of Task3 from the first iteration and Task3_1 from the second iteration
        self.assertEqual(task3_1.parent['Task1'], task3.parent['Task1'])
        self.assertEqual(task3_1.parent['Task1'], task1.task_id)

        self.assertNotEqual(task3_1.parent['Task2'], task3.parent['Task2'])
        self.assertEqual(task3.parent['Task2'], task2.task_id)
        self.assertEqual(task3_1.parent['Task2'], task2_1.task_id)
