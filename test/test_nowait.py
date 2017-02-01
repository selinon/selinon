#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

from selinonTestCase import SelinonTestCase

from selinon import SystemState


class TestNodeFailures(SelinonTestCase):
    def test_nowait_task(self):
        #
        # flow1:
        #
        #     Task1
        #
        # Note:
        #    Task1 is marked as nowait node
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}],
            'flow2': []
        }
        nowait_nodes = {'flow1': ['Task1']}
        self.init(edge_table, nowait_nodes=nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task1' not in state_dict.get('active_nodes')
        assert len(state_dict.get('waiting_edges')) == 0

    def test_nowait_flow(self):
        #
        # flow1:
        #
        #     flow2
        #
        # Note:
        #    flow2 is marked as nowait node
        #
        edge_table = {
            'flow1': [{'from': [], 'to': ['flow2'], 'condition': self.cond_true}],
            'flow2': []
        }
        nowait_nodes = {'flow1': ['flow2']}
        self.init(edge_table, nowait_nodes=nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is None
        assert system_state.node_args is None
        assert 'flow2' in self.instantiated_flows
        assert 'flow2' not in state_dict.get('active_nodes')
        assert len(state_dict.get('waiting_edges')) == 0

    def test_nowait_in_flow(self):
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
        #   Task3 finishes before Task2 and Task3 is marked as nowait
        #
        edge_table = {
            'flow1': [{'from': ['Task1'], 'to': ['Task2', 'Task3'], 'condition': self.cond_true},
                      {'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        nowait_nodes = {'flow1': ['Task3']}
        self.init(edge_table, nowait_nodes=nowait_nodes)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' not in self.instantiated_tasks
        assert 'Task3' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert 0 in state_dict['waiting_edges']
        assert len(state_dict.get('finished_nodes')) == 0
        assert len(state_dict.get('active_nodes')) == 1

        # Task1 has finished
        task1 = self.get_task('Task1')
        self.set_finished(task1, 1)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert 'Task1' in self.instantiated_tasks
        assert 'Task2' in self.instantiated_tasks
        assert 'Task3' in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert 0 in state_dict['waiting_edges']
        assert len(state_dict.get('finished_nodes')) == 1
        assert len(state_dict.get('active_nodes')) == 1
        assert 'Task3' not in state_dict.get('active_nodes')
        assert 'Task2' in state_dict['active_nodes'][0]['name']
