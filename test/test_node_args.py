#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

from selinon import SystemState
from selinon import DataStorage
from selinon_test_case import SelinonTestCase


class _MyStorage(DataStorage):
    result = None

    def __init__(self):
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def is_connected(self):
        # return True so we can test retrieve()
        return True

    def store(self, node_args, flow_name, task_name, task_id, result):
        pass

    def retrieve(self, flow_name, task_name, task_id):
        return self.result


class TestNodeArgs(SelinonTestCase):
    def test_task2task(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     Task2
        #
        # Note:
        #    Result of Task1 is propagated to Task2 as node_args
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table,
                  node_args_from_first={'flow1': True},
                  task2storage_mapping={'Task1': 'Storage1'},
                  storage_mapping={'Storage1': _MyStorage()})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        # Run without change at first
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert system_state.node_args is None
        assert retry is not None
        assert 'Task1' in self.instantiated_tasks

        # Task1 has finished
        task1 = self.get_task('Task1')
        task1_result = "propagated result of Task1"
        self.set_finished(task1, task1_result)
        _MyStorage.result = task1_result

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        system_state.to_dict()

        task2 = self.get_task('Task2')
        assert 'Task2' in self.instantiated_tasks
        assert task2.node_args == task1_result
        assert retry is not None

    def test_task2flow(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     flow2
        #
        # Note:
        #    Result of Task1 is not propagated to flow2 as even node_args_from_first is set - but propagate_node_args
        #    is not set
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}],
            'flow2': []
        }
        self.init(edge_table,
                  node_args_from_first={'flow1': True},
                  task2storage_mapping={'Task1': 'Storage1'},
                  storage_mapping={'Storage1': _MyStorage()})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks

        # Run without change at first
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert system_state.node_args is None
        assert retry is not None
        assert 'Task1' in self.instantiated_tasks

        # Task1 has finished
        task1 = self.get_task('Task1')
        task1_result = "propagated result of Task1"
        self.set_finished(task1, task1_result)
        _MyStorage.result = task1_result

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        flow2 = self.get_flow('flow2')
        assert 'flow2' in self.instantiated_flows
        assert flow2.node_args is None
        assert retry is not None

    def test_task2flow_propagate(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #       |
        #     flow2
        #
        # Note:
        #    Result of Task1 is not propagated to flow2 as even node_args_from_first is set - but propagate_node_args
        #    is not set
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['flow2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}],
            'flow2': []
        }
        self.init(edge_table,
                  node_args_from_first={'flow1': True},
                  propagate_node_args={'flow1': True},
                  task2storage_mapping={'Task1': 'Storage1'},
                  storage_mapping={'Storage1': _MyStorage()})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks

        # Task1 has finished
        task1 = self.get_task('Task1')
        task1_result = "propagated result of Task1"
        self.set_finished(task1, task1_result)
        _MyStorage.result = task1_result

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        flow2 = self.get_flow('flow2')
        assert 'flow2' in self.instantiated_flows
        assert flow2.node_args == task1_result
        assert retry is not None

    def test_task2tasks(self):
        #
        # flow1:
        #
        #     Task1
        #       |
        #    --------
        #   |        |
        # Task2    Task3
        #
        # Note:
        #    Result of Task1 is propagated to flow1 as node_args
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table,
                  node_args_from_first={'flow1': True},
                  task2storage_mapping={'Task1': 'Storage1'},
                  storage_mapping={'Storage1': _MyStorage()})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks

        # Task1 has finished
        task1 = self.get_task('Task1')
        task1_result = "propagated result of Task1"
        self.set_finished(task1, task1_result)
        _MyStorage.result = task1_result

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        assert retry is not None
        assert 'Task2' in self.instantiated_tasks
        assert 'Task3' in self.instantiated_tasks

        task2 = self.get_task('Task2')
        task3 = self.get_task('Task3')

        assert task2.node_args == task1_result
        assert task3.node_args == task1_result

    def test_recurse(self):
        #
        # flow1:
        #
        #     Task1 <----
        #       |       |
        #       |       |
        #       ---------
        #
        # Note:
        #    Result of Task1 is propagated to flow1 as node_args
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table,
                  node_args_from_first={'flow1': True},
                  task2storage_mapping={'Task1': 'Storage1'},
                  storage_mapping={'Storage1': _MyStorage()})

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks

        # Task1 has finished
        task1_1 = self.get_task('Task1')
        task1_result = "propagated result of Task1"
        self.set_finished(task1_1, task1_result)
        _MyStorage.result = task1_result

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()

        assert 'Task1' in self.instantiated_tasks
        task1_2 = self.get_task('Task1')
        assert task1_2.node_args == task1_result
        assert retry is not None
