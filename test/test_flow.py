#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2018  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import time
import datetime
from selinon_test_case import SelinonTestCase
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
        flow2_queue = 'flow2_queue'
        dispatcher_queues = {
            'flow2': flow2_queue,
            'flow1': 'flow1_queue'
        }
        self.init(edge_table, task_queues=queues, dispatcher_queues=dispatcher_queues)

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' not in self.instantiated_flows
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0

        # check task's queue configuration
        task1 = self.get_task('Task1')
        assert task1.queue == queues.get(task1.task_name)

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' not in self.instantiated_flows
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0

        # Task1 has finished
        task1_result = [1, 2, 3, 4]
        self.set_finished(task1, task1_result)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        flow2 = self.get_flow('flow2')

        assert retry is not None
        assert system_state.node_args is None
        assert 'flow2' in self.instantiated_flows
        assert flow2.node_args is None
        assert flow2.flow_name == 'flow2'
        assert flow2.parent is None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' in self.instantiated_flows
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0
        assert len(state_dict['finished_nodes']) == 1
        assert len(state_dict['active_nodes']) == 1

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'flow2' in self.instantiated_flows
        assert flow2.node_args is None
        assert flow2.flow_name == 'flow2'
        assert flow2.parent is None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' in self.instantiated_flows
        assert 'Task2' not in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 1
        assert state_dict['waiting_edges'][0] == 0
        assert len(state_dict['finished_nodes']) == 1
        assert len(state_dict['active_nodes']) == 1

        # check flow queue propagation
        flow2 = self.get_flow('flow2')
        assert flow2.queue == flow2_queue

        # flow2 has finished
        self.set_finished(flow2, {'TaskSubflow': [1]})

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        flow2 = self.get_flow('flow2')

        assert retry is not None
        assert system_state.node_args is None
        assert 'flow2' in self.instantiated_flows
        assert flow2.node_args is None
        assert flow2.flow_name == 'flow2'
        assert flow2.parent is None
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' in self.instantiated_flows
        assert 'Task2' in self.instantiated_tasks
        assert flow2.node_args is None
        assert flow2.parent is None
        assert len(state_dict.get('waiting_edges')) == 2
        assert state_dict['waiting_edges'][0] == 0
        assert len(state_dict['finished_nodes']) == 2
        assert len(state_dict['active_nodes']) == 1

        # No change
        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None
        assert system_state.node_args is None
        assert 'flow2' in self.instantiated_flows
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' in self.instantiated_flows
        assert 'Task2' in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 2
        assert state_dict['waiting_edges'][0] == 0
        assert len(state_dict['finished_nodes']) == 2
        assert len(state_dict['active_nodes']) == 1

        # Task2 has finished
        task2 = self.get_task('Task2')
        self.set_finished(task2)

        system_state = SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is None
        assert system_state.node_args is None
        assert task2.node_args is None
        assert 'flow2' in self.instantiated_flows
        assert 'Task1' in self.instantiated_tasks
        assert 'flow2' in self.instantiated_flows
        assert 'Task2' in self.instantiated_tasks
        assert len(state_dict.get('waiting_edges')) == 2
        assert state_dict['waiting_edges'][0] == 0
        assert len(state_dict['finished_nodes']) == 3
        assert len(state_dict['active_nodes']) == 0

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

        assert retry is not None
        assert 'flow2' in self.instantiated_flows
        assert self.get_flow('flow2').countdown is None

        # Let's sleep to ensure we get less then 2s delay
        time.sleep(0.01)

        # run flow for the second time, we should be postponed by ~2s
        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()

        assert retry is not None
        assert self.get_flow('flow2').countdown is not None
        assert self.get_flow('flow2').countdown < 2


