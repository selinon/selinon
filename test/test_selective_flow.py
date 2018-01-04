#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

from selinon import SystemState
from selinon.selective import compute_selective_run
from selinon_test_case import SelinonTestCase


class TestSelectiveFlow(SelinonTestCase):
    def get_initial_system_state(self, flow_name, node_args, task_names, follow_subflows=False, run_subsequent=False):
        """ Get SystemState instance for the given selective flow

        :param flow_name: name of the flow to get SystemState instance for
        :param node_args: arguments that are passed to the flow
        :param task_names: a list of tasks that should be selectively run
        :param follow_subflows: if True, subflows will be inspected for task_names to be run
        :param run_subsequent: run all nodes that are subsequent nodes of the tasks_names nodes
        """
        selective = compute_selective_run(flow_name, task_names, follow_subflows, run_subsequent)
        return SystemState(id(self), 'flow1', node_args, selective=selective)

    def test_selective_flow_first_task(self):
        #
        # flow1:
        #
        #     Task1    Task2    Task3
        #                |        |
        #              Task4    Task5
        #
        # Note: Only Task2 should be run
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1', 'Task2', 'Task3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task3'], 'to': ['Task5'], 'condition': self.cond_true}],
        }
        node_args = {'foo': 'bar'}
        self.init(edge_table)

        system_state = self.get_initial_system_state('flow1', node_args, ['Task2'],
                                                     follow_subflows=False, run_subsequent=False)

        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task2'} == set(self.instantiated_tasks)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task2'} == set(self.instantiated_tasks)

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2, None)

        assert task2.node_args == node_args

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()

        assert retry is None

    def test_selective_flow_one_task(self):
        #
        # flow1:
        #
        #         Task1
        #           |  \
        #           |   \
        #         Task2  Task4
        #           |
        #           |
        #         Task3
        #
        # Note: Only Task2 and Task1 should be run
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task3'], 'condition': self.cond_true}],
        }
        node_args = {'foo': 'bar'}
        self.init(edge_table)

        system_state = self.get_initial_system_state('flow1', node_args, ['Task2'],
                                                     follow_subflows=False, run_subsequent=False)

        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1'} == set(self.instantiated_tasks)

        self.set_finished(self.get_task('Task1'), None)
        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2'} == set(self.instantiated_tasks)

        self.set_finished(self.get_task('Task2'), None)
        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        assert retry is None

    def test_selective_flow_two_tasks(self):
        #
        # flow1:
        #
        #         Task1        Task2        Task3        Task4     Task5
        #           \            /             \         /           |
        #            \          /               \       /          Task10
        #             ----------                 -------
        #                  |                     /    \
        #                Task6              Task7      Task8
        #                  |                  |
        #                  |                  |
        #                Task11             Task9
        #
        # Note: Only Task6 and Task7 should be run
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1', 'Task2', 'Task3', 'Task4', 'Task5'], 'condition': self.cond_true},
                      {'from': ['Task1', 'Task2'], 'to': ['Task6'], 'condition': self.cond_true},
                      {'from': ['Task3', 'Task4'], 'to': ['Task7', 'Task8'], 'condition': self.cond_true},
                      {'from': ['Task7'], 'to': ['Task9'], 'condition': self.cond_true},
                      {'from': ['Task5'], 'to': ['Task10'], 'condition': self.cond_true}],
        }
        self.init(edge_table)

        system_state = self.get_initial_system_state('flow1', None, ['Task6', 'Task7'],
                                                     follow_subflows=False, run_subsequent=False)

        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3', 'Task4'} == set(self.instantiated_tasks)

        # Task1 has finished
        self.set_finished(self.get_task('Task1'), None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3', 'Task4'} == set(self.instantiated_tasks)

        # Task2 has finished
        self.set_finished(self.get_task('Task2'), None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3', 'Task4', 'Task6'} == set(self.instantiated_tasks)

        # Task3 has finished
        self.set_finished(self.get_task('Task3'), None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3', 'Task4', 'Task6'} == set(self.instantiated_tasks)

        # Task4 has finished
        self.set_finished(self.get_task('Task4'), None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3', 'Task4', 'Task6', 'Task7'} == set(self.instantiated_tasks)

        # Task7 has finished
        self.set_finished(self.get_task('Task7'), None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3', 'Task4', 'Task6', 'Task7'} == set(self.instantiated_tasks)

        # Task6 has finished
        task6 = self.get_task('Task6')
        self.set_finished(task6, None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        assert retry is None

    def test_selective_flow_subflow(self):
        #
        # flow1:
        #
        #     flow2    Task1
        #        |       |
        #        |      Task2
        #        |       |  \
        #         -------    Task3
        #            |
        #            |
        #          Task4
        #            |
        #            |
        #          Task5
        #
        # flow2:
        #
        #     TaskF2
        #       |
        #       |
        #     Task4
        #       |  \
        #       |   \
        #     flow3  flow4
        #
        # flow3:
        #
        #    Task4
        #      |
        #      |
        #    TaskF3
        #
        # flow4:
        #
        #    TaskF3
        #
        # Note: Only Task4 should be run, flow4 is not executed
        #
        def simulate_flow2(flow2, f2_already_instantiated_tasks):
            f2_system_state = flow2.get_initial_system_state()

            f2_retry = f2_system_state.update()
            f2_state_dict = f2_system_state.to_dict()
            f2_selective = f2_system_state.selective

            assert f2_retry is not None
            assert f2_already_instantiated_tasks | {'TaskF2'} == set(self.instantiated_tasks)
            self.set_finished(self.get_task('TaskF2'))

            f2_system_state = SystemState(id(self), 'flow2', state=f2_state_dict,
                                          node_args=f2_system_state.node_args, selective=f2_selective)
            f2_retry = f2_system_state.update()
            f2_state_dict = f2_system_state.to_dict()
            f2_selective = f2_system_state.selective

            assert f2_retry is not None
            assert f2_already_instantiated_tasks | {'TaskF2', 'Task4'} == set(self.instantiated_tasks)
            assert 'flow3' not in self.instantiated_flows

            self.set_finished(self.get_task('Task4'))
            f2_system_state = SystemState(id(self), 'flow2', state=f2_state_dict,
                                          node_args=f2_system_state.node_args, selective=f2_selective)
            f2_retry = f2_system_state.update()
            f2_state_dict = f2_system_state.to_dict()
            f2_selective = f2_system_state.selective

            assert f2_retry is not None
            assert 'flow3' in self.instantiated_flows
            simulate_flow3(self.get_flow('flow3'), set(self.instantiated_tasks))

            f2_system_state = SystemState(id(self), 'flow2', state=f2_state_dict,
                                          node_args=f2_system_state.node_args, selective=f2_selective)
            f2_retry = f2_system_state.update()

            assert f2_retry is None
            self.set_finished(flow2, None)

            # let's clean after self so we inspect purely flow1 in callee
            self.remove_all_tasks_by_name('TaskF2')
            self.remove_all_tasks_by_name('Task4')
            self.remove_all_flows_by_name('flow2')

        def simulate_flow3(flow3, f3_already_instantiated_tasks):
            f3_system_state = flow3.get_initial_system_state()

            f3_retry = f3_system_state.update()
            f3_state_dict = f3_system_state.to_dict()
            f3_selective = f3_system_state.selective

            assert f3_retry is not None
            assert f3_already_instantiated_tasks | {'Task4'} == set(self.instantiated_tasks)
            self.set_finished(self.get_task('Task4'))

            f3_system_state = SystemState(id(self), 'flow3', state=f3_state_dict,
                                          node_args=f3_system_state.node_args, selective=f3_selective)
            f3_retry = f3_system_state.update()
            assert f3_retry is None

            self.set_finished(flow3, None)
            self.remove_all_flows_by_name('flow3')

        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2', 'Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['flow2', 'Task2'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['Task5'], 'condition': self.cond_true}],
            'flow2': [{'from': [], 'to': ['TaskF2'], 'condition': self.cond_true},
                      {'from': ['TaskF2'], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['flow4', 'flow3'], 'condition': self.cond_true}],
            'flow3': [{'from': [], 'to': ['Task4'], 'condition': self.cond_true},
                      {'from': ['Task4'], 'to': ['TaskF3'], 'condition': self.cond_true}],
            'flow4': [{'from': [], 'to': ['TaskF3']}]
        }
        self.init(edge_table)

        system_state = self.get_initial_system_state('flow1', None, ['Task4'],
                                                     follow_subflows=True, run_subsequent=False)

        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1'} == set(self.instantiated_tasks)
        assert {'flow2'} == set(self.instantiated_flows)

        simulate_flow2(self.get_flow('flow2'), set(self.instantiated_tasks))
        self.set_finished(self.get_task('Task1'), None)
        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2'} == set(self.instantiated_tasks)

        self.set_finished(self.get_task('Task2'), None)
        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task4'} == set(self.instantiated_tasks)

        self.set_finished(self.get_task('Task4'), None)

        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        assert retry is None
        assert 'flow4' not in self.instantiated_flows

    def test_selective_flow_sybsequent_cyclic(self):
        #
        #      Task1  <-
        #       /  |    |
        #      /   |    |
        # Task3  Task2 --
        #
        # This is a very special case, where we don't want to keep state in dispatcher, so all tasks will be run
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': ['Task3'], 'condition': self.cond_true},
                      {'from': ['Task2'], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = self.get_initial_system_state('flow1', None, ['Task2'],
                                                     follow_subflows=False, run_subsequent=True)

        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1'} == set(self.instantiated_tasks)

        self.set_finished(self.get_task('Task1'), None)
        system_state = SystemState(id(self), 'flow1',
                                   state=state_dict, node_args=system_state.node_args, selective=selective)
        retry = system_state.update()
        state_dict = system_state.to_dict()
        selective = system_state.selective

        assert retry is not None
        assert {'Task1', 'Task2', 'Task3'} == set(self.instantiated_tasks)
