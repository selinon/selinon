#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ######################################################################
# Copyright (C) 2016-2017  Fridolin Pokorny, fridolin.pokorny@gmail.com
# This file is part of Selinon project.
# ######################################################################

import pytest
from flexmock import flexmock
from selinonTestCase import SelinonTestCase

from selinon.trace import Trace
from selinon import run_flow
from selinon.dispatcher import Dispatcher
from selinonlib import Retry
from selinonlib import UnknownFlowError


class TestOthers(SelinonTestCase):
    def test_trace_naming(self):
        for i, event_string in enumerate(Trace._event_strings):
            assert getattr(Trace, event_string) == i

    def test_run_flow(self):
        self.init(edge_table={'flow1': []}, dispatcher_queues={'flow1': 'flow1_queue'})

        flexmock(Dispatcher)
        Dispatcher.should_receive('apply_async').and_return("<dispatcher_id>")

        assert run_flow('flow1', node_args={'foo': 'bar'}) == "<dispatcher_id>"

    def test_run_unknown_flow_error(self):
        # We run uninitialized Config
        self.init(edge_table={}, dispatcher_queues=None)

        with pytest.raises(UnknownFlowError):
            run_flow('some_flow', node_args={'foo': 'bar'})

    def test_retry(self):
        countdown = 10
        assert Retry(countdown=countdown).countdown == countdown

