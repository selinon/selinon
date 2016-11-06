#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ####################################################################
# Copyright (C) 2016  Fridolin Pokorny, fpokorny@redhat.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# ####################################################################

import pytest
from flexmock import flexmock
from selinonTestCase import SelinonTestCase

from selinon.trace import Trace
from selinon import run_flow
from selinon.dispatcher import Dispatcher


class TestOthers(SelinonTestCase):
    def test_trace_naming(self):
        for i, event_string in enumerate(Trace._event_strings):
            assert getattr(Trace, event_string) == i

    def test_run_flow(self):
        self.init(edge_table={'flow1': []}, dispatcher_queues={'flow1': 'flow1_queue'})

        flexmock(Dispatcher)
        Dispatcher.should_receive('apply_async').and_return("<dispatcher_id>")

        assert run_flow('flow1', node_args={'foo': 'bar'}) == "<dispatcher_id>"

    def test_run_flow_error(self):
        # We run uninitialized Config
        self.init(edge_table={}, dispatcher_queues=None)

        with pytest.raises(KeyError):
            run_flow('some_flow', node_args={'foo': 'bar'})

