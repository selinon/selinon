#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import json
import pytest
from flexmock import flexmock
from selinonTestCase import SelinonTestCase
from selinon import Dispatcher
from selinon.systemState import SystemState
from selinon import FlowError
from requestMock import RequestMock


class TestDispatcher(SelinonTestCase):
    def test_run(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        state_dict = {'failed_nodes': {}, 'finished_nodes': {'Task1': ['<task-id>']}}

        flexmock(SystemState).should_receive('update').and_return(None)
        flexmock(SystemState).should_receive('to_dict').and_return(state_dict)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()

        assert dispatcher.run(flow_name) == state_dict

    def test_flow_error(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        state_dict = {'failed_nodes': {'Task1': ['<task1-id>']}, 'finished_nodes': {}}

        flexmock(SystemState).should_receive('update').and_raise(FlowError(json.dumps(state_dict)))
        flexmock(SystemState).should_receive('to_dict').and_return(state_dict)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').and_return(FlowError)

        # We should improve this by actually checking exception value
        with pytest.raises(FlowError):
            dispatcher.run(flow_name)

    def test_dispacher_error(self):
        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)

        exc = KeyError("some exception in dispatcher")
        flexmock(SystemState).should_receive('update').and_raise(exc)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').and_return(exc)

        # We should improve this by actually checking exception value
        with pytest.raises(KeyError):
            dispatcher.run(flow_name)

    def test_retry(self):
        def my_retry(max_retries, exc):
            assert max_retries == 0
            assert exc == raised_exc
            raise RuntimeError()  # we re-raise as stated in Celery doc

        flow_name = 'flow1'
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table)
        state_dict = {'finished_nodes': {'Task1': ['<task1-id>']}, 'failed_nodes': {}}

        raised_exc = FlowError(json.dumps(state_dict))
        flexmock(SystemState).should_receive('update').and_raise(raised_exc)  # we will retry
        flexmock(SystemState).should_receive('to_dict').and_return(state_dict)

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').replace_with(my_retry)

        with pytest.raises(RuntimeError):
            dispatcher.run(flow_name)

    def test_selinon_retry(self):
        def my_retry(kwargs, max_retries, countdown, queue):
            assert kwargs.get('flow_name') == flow_name
            assert kwargs.get('node_args') == node_args
            assert 'parent' in kwargs
            assert kwargs.get('retried_count') == 1
            assert max_retries == 1
            assert countdown == 5
            assert queue == 'queue_flow1'
            raise RuntimeError()  # we re-raise as stated in Celery doc

        flow_name = 'flow1'
        node_args = {'foo': 'bar'}
        edge_table = {
            flow_name: [{'from': [], 'to': ['Task1'], 'condition': self.cond_true}]
        }
        self.init(edge_table, retry_countdown={'flow1': 5}, max_retry={'flow1': 1})
        state_dict = {'finished_nodes': {'Task1': ['<task1-id>']}, 'failed_nodes': {}}

        flexmock(SystemState).should_receive('update').and_raise(FlowError(json.dumps(state_dict)))

        dispatcher = Dispatcher()
        dispatcher.request = RequestMock()
        flexmock(dispatcher).should_receive('retry').replace_with(my_retry)

        with pytest.raises(RuntimeError):
            dispatcher.run(flow_name, node_args)
