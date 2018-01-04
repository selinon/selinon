#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

from selinon_test_case import SelinonTestCase
from selinon import SystemState, Dispatcher, Config


class TestPropagate(SelinonTestCase):
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

        assert retry is not None
        assert system_state.node_args == node_args
        assert 'flow2' in self.instantiated_flows

        flow2 = self.get_flow('flow2')
        assert flow2.node_args == node_args

    def test_propagate_node_args_flow(self):
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

        assert retry is not None
        assert system_state.node_args == node_args
        assert 'flow2' in self.instantiated_flows
        assert 'flow3' in self.instantiated_flows

        flow2 = self.get_flow('flow2')
        assert flow2.node_args == node_args

        flow3 = self.get_flow('flow3')
        assert flow3.node_args is None

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

        assert retry is not None
        assert system_state.node_args is None
        assert 'flow2' not in self.instantiated_flows
        assert 'Task1' in self.instantiated_tasks

        task1 = self.get_task('Task1')
        self.set_finished(task1, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        assert retry is not None
        assert system_state.node_args is None
        assert 'flow2' in self.instantiated_flows

        flow2 = self.get_flow('flow2')

        assert flow2.node_args is None
        assert 'Task1' in flow2.parent
        assert flow2.parent['Task1'] == task1.task_id

    def test_propagate_parent_flow(self):
        #
        # flow1:
        #         Task1
        #           |
        #        -------
        #       |       |
        #     flow2   flow3
        #
        # Note:
        #    Arguments are propagated to flow2 but not to flow3. Result of Task1 is not propagated to flow2 nor flow3.
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['flow2', 'flow3'], 'condition': self.cond_true}],
            'flow2': [],
            'flow3': []
        }
        self.init(edge_table, propagate_parent={'flow1': ['flow2']}, propagate_node_args={'flow1': ['flow2']})

        args = 123
        system_state = SystemState(id(self), 'flow1', args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' not in self.instantiated_flows
        assert 'flow3' not in self.instantiated_flows

        task1 = self.get_task('Task1')
        self.set_finished(task1, 0xDEADBEEF)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        assert retry is not None
        assert 'flow2' in self.instantiated_flows
        assert 'flow3' in self.instantiated_flows

        flow2 = self.get_flow('flow2')
        assert flow2.node_args == args
        assert 'Task1' in flow2.parent
        assert flow2.parent['Task1'] == task1.task_id

        flow3 = self.get_flow('flow3')
        assert flow3.node_args is None
        assert flow3.parent is None

    def test_propagate_parent(self):
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
        #    Run explicitly, but result finished_nodes is:
        #         {'flow3': [<flow3-id>], 'Task2': [<task2-id1>], 'Task3': [<task3-id1>, <task3-id2>]}
        # flow3
        #    Run explicitly, but result finished_nodes is:
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
        self.init(edge_table, propagate_parent={'flow1': True}, propagate_finished={'flow1': True})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' in self.instantiated_tasks
        assert 'flow2' not in self.instantiated_flows

        # Task1 and Task2 have finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, None)

        task2 = self.get_task('Task2')
        self.set_finished(task2, None)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        assert retry is not None
        assert 'flow2' in self.instantiated_flows

        # Create flow3 manually
        flow3 = Dispatcher().apply_async(kwargs={'flow_name': 'flow3'}, queue=Config.dispatcher_queues['flow3'])
        self.get_task_instance.register_node(flow3)

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

        assert retry is not None

        task_x = self.get_task('TaskX')

        task_x_parent = {'Task2': task2.task_id,
                         'flow2': {'Task2': ['<task2-id1>'],
                                   'Task3': ['<task3-id1>', '<task3-id2>'],
                                   'flow3': {'Task2': ['<task2-id2>'],
                                             'Task4': ['<task4-id1>', '<task4-id2>'],
                                             }
                                   }
                         }
        assert task_x.parent == task_x_parent

    def test_propagate_parent_2(self):
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
        # flow3:
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
        # Make sure propagate_finished is set to False as they are disjoint with propagate_compound_finished;
        # this is checked in selinon
        self.init(edge_table, propagate_parent=dict.fromkeys(edge_table.keys(), True),
                  propagate_finished={'flow1': False},
                  propagate_compound_finished={'flow1': True})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' in self.instantiated_tasks
        assert 'flow2' not in self.instantiated_flows

        # Task1 and Task2 have finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, None)

        task2 = self.get_task('Task2')
        self.set_finished(task2, None)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict_1 = system_state.to_dict()

        assert retry is not None
        assert 'flow2' in self.instantiated_flows

        # Create flow3 manually
        flow3 = Dispatcher().apply_async(kwargs={'flow_name': 'flow3'}, queue=Config.dispatcher_queues['flow3'])
        self.get_task_instance.register_node(flow3)

        flow2 = self.get_flow('flow2')
        self.set_finished(flow2)
        flow2_result = {'finished_nodes': {'flow3': [flow3.task_id],
                                           'Task2': ['<task2-id1>'],
                                           'Task3': ['<task3-id1>', '<task3-id2>']},
                        'failed_nodes': {}
                        }
        self.set_finished(flow2, flow2_result)

        flow3_result = {'finished_nodes': {'Task2': ['<task2-id2>'], 'Task4': ['<task4-id1>', '<task4-id2>']},
                        'failed_nodes': {}
                        }
        self.set_finished(flow3, flow3_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict_1, node_args=system_state.node_args)
        retry = system_state.update()

        assert retry is not None

        task_x = self.get_task('TaskX')

        task_x_parent = {'Task2': task2.task_id,
                         'flow2': {'Task2': ['<task2-id1>', '<task2-id2>'],
                                   'Task3': ['<task3-id1>', '<task3-id2>'],
                                   'Task4': ['<task4-id1>', '<task4-id2>']
                                   }
                         }

        # we have to check this as a set due to dict randomization
        assert 'Task2' in task_x.parent
        assert task_x.parent['Task2'] == task_x_parent['Task2']

        assert 'flow2' in task_x.parent
        assert 'Task2' in task_x.parent['flow2']
        assert 'Task3' in task_x.parent['flow2']
        assert 'Task4' in task_x.parent['flow2']

        assert set(task_x.parent['flow2']['Task2']) == set(task_x_parent['flow2']['Task2'])
        assert set(task_x.parent['flow2']['Task3']) == set(task_x_parent['flow2']['Task3'])
        assert set(task_x.parent['flow2']['Task4']) == set(task_x_parent['flow2']['Task4'])

    def test_propagate_compound_mixed(self):
        #
        # flow1:
        #
        #     flow2       flow3
        #       |           |
        #        -----------
        #             |
        #           TaskX
        #
        # flow2:
        #    Not run explicitly, but result finished_nodes is:
        #         {'Task2': [<task2-id21>],
        #          'Task3': [<task3-id21>, <task3-id22>]}
        #          'flow4': {'Task2': [<task2-id41>]}
        # flow3:
        #    Not run explicitly, but result finished_nodes is:
        #         {'Task2': [<task2-id32>],
        #          'Task3': [<task3-id31>, <task3-id32>],
        #          'flow4': {'Task2': [<task2-id42>]}
        #
        # We are propagating finished, so we should inspect parent
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2', 'flow3'], 'condition': self.cond_true},
                      {'from': ['flow2', 'flow3'], 'to': ['TaskX'], 'condition': self.cond_true}],
            'flow2': [],
            'flow3': [],
            'flow4': []
        }
        # Make sure propagate_finished is negated propagate_compound_finished
        # this is checked in selinon
        self.init(edge_table,
                  propagate_finished={'flow1': ['flow2']},
                  propagate_compound_finished={'flow1': ['flow3']})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert 'flow2' in self.instantiated_flows

        # Create flow4 manually, we will reuse it, but we pretend that there are 2 instances - one run in flow2
        # another one in flow3
        flow4 = Dispatcher().apply_async(kwargs={'flow_name': 'flow4'}, queue=Config.dispatcher_queues['flow4'])
        self.get_task_instance.register_node(flow4)
        self.set_finished(flow4, {'finished_nodes': {'Task2': ['<task2-id41']}, 'failed_nodes': {}})

        # Create results of flow2 and flow3 manually but they are instantiated by dispatcher
        flow2_result = {'finished_nodes': {'Task2': ['<task2-id21>'],
                                           'Task3': ['<task3-id21>', '<task3-id22>'],
                                           'flow4': [flow4.task_id]},
                        'failed_nodes': {}
                        }
        flow2 = self.get_flow('flow2')
        self.set_finished(flow2, flow2_result)

        flow3_result = {'finished_nodes': {'Task2': ['<task2-id32>'],
                                           'Task3': ['<task3-id31>', '<task3-id32>'],
                                           'flow4': [flow4.task_id]},
                        'failed_nodes': {}
                        }
        flow3 = self.get_flow('flow3')
        self.set_finished(flow3, flow3_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        assert retry is not None
        assert 'TaskX' in self.instantiated_tasks

        task_x_parent = {'flow2': {'Task2': ['<task2-id21>'],
                                   'Task3': ['<task3-id21>', '<task3-id22>'],
                                   'flow4': {'Task2': ['<task2-id41']}},
                         # flow4 is hidden in flow3 as it is compound, see 'Task2'
                         'flow3': {'Task2': ['<task2-id41', '<task2-id32>'],
                                   'Task3': ['<task3-id31>', '<task3-id32>']}}
        task_x = self.get_task('TaskX')

        # we have to check this as a set due to dict randomization
        assert 'flow2' in task_x.parent
        assert 'flow3' in task_x.parent

        assert 'Task2' in task_x.parent['flow2']
        assert 'Task3' in task_x.parent['flow2']
        assert set(task_x.parent['flow2']['Task2']) == set(task_x_parent['flow2']['Task2'])
        assert set(task_x.parent['flow2']['Task3']) == set(task_x_parent['flow2']['Task3'])

        assert 'Task2' in task_x.parent['flow3']
        assert 'Task3' in task_x.parent['flow3']
        assert set(task_x.parent['flow3']['Task2']) == set(task_x_parent['flow3']['Task2'])
        assert set(task_x.parent['flow3']['Task3']) == set(task_x_parent['flow3']['Task3'])

        assert 'flow4' in task_x.parent['flow2']
        assert set(task_x.parent['flow2']['flow4']) == set(task_x_parent['flow2']['flow4'])


