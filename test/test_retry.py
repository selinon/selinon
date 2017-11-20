#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
import flexmock
from selinonTestCase import SelinonTestCase
from selinon import SystemState
from selinon.errors import DispatcherRetry
from selinon import DataStorage
from celery.result import AsyncResult


class TestFlowRetry(SelinonTestCase):
    class MyStorage(DataStorage):
        def __init__(self):
            pass

        def connect(self):
            # shouldn't be called
            raise NotImplementedError()

        def disconnect(self):
            # called on destruction
            pass

        def is_connected(self):
            return True

        def store(self, node_args, flow_name, task_name, task_id, result):
            raise ValueError("Some exception raised")

        def retrieve(self, flow_name, task_name, task_id):
            raise IndexError("Another exception raised on retrieval")

    def test_retrieve_issue(self):
        #
        # flow1:
        #
        #     Task1
        #
        # Checks that if there is an issue with retrieving task result from task storage in condition, dispatcher
        # retries flow
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true},
                      {'from': ['Task1'], 'to': 'Task2', 'condition': lambda db, _: db.get('Task1')}]
        }
        self.init(
            edge_table,
            storage_mapping={'Storage1': self.MyStorage()},
            task2storage_mapping={'Task1': 'Storage1'}
        )

        system_state = SystemState(id(self), 'flow1')
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None

        task1 = self.get_task('Task1')
        self.set_finished(task1, "some result")

        with pytest.raises(DispatcherRetry) as exc_info:
            SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args).update()

        assert exc_info.value.keep_state is True
        assert exc_info.value.adjust_retry_count is False

    def test_result_backend_issue(self):
        #
        # flow1:
        #
        #     Task1
        #
        # Checks that if there is an issue with result backend, dispatcher retries as there is no info on how
        # to continue
        edge_table = {
            'flow1': [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(
            edge_table,
            storage_mapping={'Storage1': self.MyStorage()},
            task2storage_mapping={'Task1': 'Storage1'}
        )
        node_args = {'Foo': 'bar'}

        system_state = SystemState(id(self), 'flow1', node_args=node_args)
        retry = system_state.update()
        state_dict = system_state.to_dict()

        assert retry is not None

        task1 = self.get_task('Task1')
        self.set_finished(task1, "some result")

        flexmock(AsyncResult).\
            should_receive('successful').\
            and_raise(ValueError, "Some error raised due to result backed issues")

        with pytest.raises(DispatcherRetry) as exc_info:
            SystemState(id(self), 'flow1', state=state_dict, node_args=system_state.node_args).update()

        assert exc_info.value.keep_state is True
        assert exc_info.value.adjust_retry_count is False

