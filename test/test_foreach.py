#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

from selinonTestCase import SelinonTestCase

from selinon import SystemState


# Let's make it constant, this shouldn't affect tests at all
_FOREACH_COUNT = 20


class TestForeach(SelinonTestCase):
    def test_foreach_start(self):
        #
        # flow1:
        #
        #       |      |             |
        #     Task1  Task1   ...   Task1
        #
        # Note:
        #   There will be spawned _FOREACH_COUNT Task2
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true,
                       'foreach': lambda x, y: range(_FOREACH_COUNT), 'foreach_propagate_result': False}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert len(self.get_all_tasks('Task1')) == _FOREACH_COUNT
        tasks_state_dict = [node for node in state_dict['active_nodes'] if node['name'] == 'Task1']
        assert len(tasks_state_dict) == _FOREACH_COUNT

    def test_foreach_basic(self):
        #
        # flow1:
        #
        #             Task1
        #               |
        #               |
        #       ---------------------
        #       |      |             |
        #       |      |             |
        #     Task2  Task2   ...   Task2
        #
        # Note:
        #   There will be spawned _FOREACH_COUNT Task2
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true,
                       'foreach': lambda x, y: range(_FOREACH_COUNT), 'foreach_propagate_result': False},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' not in self.instantiated_tasks

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, "some result")

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' in self.instantiated_tasks

        assert len(self.get_all_tasks('Task2')) == _FOREACH_COUNT
        tasks_state_dict = [node for node in state_dict['active_nodes'] if node['name'] == 'Task2']
        assert len(tasks_state_dict) == _FOREACH_COUNT

    def test_foreach_propagate_result(self):
        #
        # flow1:
        #
        #             Task1
        #               |
        #               |
        #       ---------------------
        #       |      |             |
        #       |      |             |
        #     flow2  flow2   ...   flow2
        #
        # Note:
        #   There will be spawned _FOREACH_COUNT flow2, arguments are passed from foreach function
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true,
                       'foreach': lambda x, y: range(_FOREACH_COUNT), 'foreach_propagate_result': True},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}],
            'flow2': []
        }
        self.init(edge_table)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' not in self.instantiated_tasks

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, "some result")

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' in self.instantiated_flows

        tasks_state_dict = [node for node in state_dict['active_nodes'] if node['name'] == 'flow2']
        assert len(tasks_state_dict) == _FOREACH_COUNT

        # Inspect node_args as we set propagate_result for foreach
        all_flow_args = [flow.node_args for flow in self.get_all_flows('flow2')]
        assert all_flow_args == list(range(_FOREACH_COUNT))

