#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from selinonTestCase import SelinonTestCase

from selinon.errors import FlowError
from selinon import SystemState


class TestNodeFailuresCond(SelinonTestCase):
    def test_single_failure_cond_false(self):
        #
        # flow1:
        #
        #     Task1 X .. Task3
        #       |
        #       |
        #     Task2
        #
        # Note:
        #   Task1 will fail, Task2 shouldn't be run
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {},
                                'fallback': [['Task2']],
                                'conditions': [self.cond_false],
                                'condition_strs': ['false']}}
        }
        self.init(edge_table, failures=failures)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0

        # Task1 has failed
        task1 = self.get_task('Task1')
        self.set_failed(task1, KeyError("Some exception raised"))

        with pytest.raises(FlowError):
            system_state = SystemState(id(self), 'flow1', state=state_dict,
                                       node_args=system_state.node_args)
            system_state.update()

        assert 'Task2' not in self.instantiated_tasks
        assert 'Task3' not in self.instantiated_tasks

    def test_single_failure_cond_true(self):
        #
        # flow1:
        #
        #     Task1 X
        #       |
        #       |
        #     Task2
        #
        # Note:
        #   Task1 will fail, Task2 should be run
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {},
                                'fallback': [['Task3']],
                                'conditions': [self.cond_true],
                                'condition_strs': ['true']}}
        }
        self.init(edge_table, failures=failures)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0

        # Task1 has failed
        task1 = self.get_task('Task1')
        self.set_failed(task1, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()

        assert 'Task2' not in self.instantiated_tasks
        assert 'Task3' in self.instantiated_tasks
        assert retry is not None

    def test_multi_failures(self):
        #
        # flow1:
        #
        #     Task1 X
        #       |
        #       |
        #     Task2
        #
        # Note:
        #   Task1 will fail, Task2 should be run
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        failures = {
            'flow1': {'Task1': {'next:': {},
                                'fallback': [['Task3'], ['Task4', 'Task5'], ['Task6', 'Task7']],
                                'conditions': [self.cond_true, self.cond_false, self.cond_true],
                                'condition_strs': ['true', 'false', 'true']}}
        }
        self.init(edge_table, failures=failures)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0

        # Task1 has failed
        task1 = self.get_task('Task1')
        self.set_failed(task1, KeyError("Some exception raised"))

        system_state = SystemState(id(self), 'flow1', state=state_dict,
                                   node_args=system_state.node_args)
        retry = system_state.update()

        assert 'Task2' not in self.instantiated_tasks
        assert 'Task3' in self.instantiated_tasks
        assert 'Task4' not in self.instantiated_tasks
        assert 'Task5' not in self.instantiated_tasks
        assert 'Task6' in self.instantiated_tasks
        assert 'Task7' in self.instantiated_tasks
        assert retry is not None

